use std::collections::hash_map::Entry;
use crate::relayer::tpu::Tpu;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use log::{error, info, warn};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use jwt::{AlgorithmType, PKeyWithDigest};
use openssl::pkey::{Private, Public};
use prost_types::Timestamp;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, error::TrySendError, Sender as TokioSender};
use crate::protos::{
    convert::packet_to_proto_packet,
    packet::PacketBatch as ProtoPacketBatch,
    relayer::SubscribePacketsResponse
};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use crate::helper::shutdown_signal;
use crate::protos::auth::auth_service_server::AuthServiceServer;
use crate::protos::relayer::relayer_server::{Relayer, RelayerServer};
use crate::protos::relayer::{subscribe_packets_response, GetTpuConfigsRequest, GetTpuConfigsResponse, SubscribePacketsRequest};
use crate::protos::shared::{Header, Heartbeat, Socket};
use crate::relayer::auth::interceptor::AuthInterceptor;
use crate::relayer::auth::service::AuthServiceImpl;
use crate::relayer::auth::ValidatorAutherImpl;

type PacketSubscriptions = Arc<RwLock<HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>>>;

#[derive(Clone)]
pub struct Forwarder {
    sender: crossbeam_channel::Sender<BankingPacketBatch>
}

impl Forwarder {
    pub fn new(
        public_ip: &IpAddr,
        tpu_quic_port: u16,
        tpu_fwd_quic_port: u16,
        grpc_bind_ip: IpAddr,
        relayer_bind_port: u16,
        signing_key: PKeyWithDigest<Private>,
        verifying_key: Arc<PKeyWithDigest<Public>>,
        rt: &Handle,
        exit: &Arc<AtomicBool>,
    ) -> (Self, JoinHandle<()>, std::thread::JoinHandle<()>) {
        let (delay_packet_sender, delay_packet_receiver) = crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);
        let forwarder_service = ForwarderService::new(public_ip, tpu_quic_port, tpu_fwd_quic_port);

        let auth_svc = AuthServiceImpl::new(
            ValidatorAutherImpl::default(),
            &rt,
            signing_key,
            verifying_key.clone(),
            &exit,
        );

        let exit_clone = exit.clone();
        let forwarder_service_clone = forwarder_service.clone();
        let event_task = std::thread::Builder::new()
            .name("forwarder-event-loop".to_string())
            .spawn(move || {
                if let Err(err) = forwarder_service_clone.run_event_loop(
                    delay_packet_receiver,
                    &exit_clone,
                ) && !exit_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    error!("Forwarder thread exited with result {err}");
                    exit_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            })
            .expect("spawn forwarder-event-loop thread");

        let exit = exit.clone();
        let server_addr = SocketAddr::new(grpc_bind_ip.clone(), relayer_bind_port);

        info!("Starting Relayer at: {:?}", server_addr);
        let join = rt.spawn(async move {
            Server::builder()
                .add_service(RelayerServer::with_interceptor(
                    forwarder_service,
                    AuthInterceptor::new(verifying_key.clone(), AlgorithmType::Rs256),
                ))
                .add_service(AuthServiceServer::new(auth_svc))
                .serve_with_shutdown(server_addr, shutdown_signal(exit))
                .await
                .expect("Serve relayer");
        });

        let forwarder = Forwarder {
            sender: delay_packet_sender
        };

        (forwarder, join, event_task)
    }

    pub fn sender(&self) -> crossbeam_channel::Sender<BankingPacketBatch> {
        self.sender.clone()
    }
}

#[derive(Clone)]
pub struct ForwarderService {
    public_ip: IpAddr,
    tpu_quic_port: u16,
    tpu_fwd_quic_port: u16,
    packet_subscriptions: PacketSubscriptions,
}

impl ForwarderService {
    pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 50_000;
    pub const VALIDATOR_PACKET_BATCH_SIZE: usize = 64;

    pub fn new(
        public_ip: &IpAddr,
        tpu_quic_port: u16,
        tpu_fwd_quic_port: u16,
    ) -> Self {
        ForwarderService {
            public_ip: public_ip.clone(),
            tpu_quic_port,
            tpu_fwd_quic_port,
            packet_subscriptions: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    fn add_subscription(
        &self,
        identity: Pubkey,
        sender: TokioSender<Result<SubscribePacketsResponse, Status>>,
    ) -> Result<(), String> {
        let mut l_subscriptions = self.packet_subscriptions.write().unwrap();
        let len = l_subscriptions.len();

        match l_subscriptions.entry(identity) {
            Entry::Vacant(entry) => {
                if len >= 1 {
                    return Err(format!(
                        "Too many subscriptions, max is 1, currently connected: {:?}",
                        l_subscriptions.keys().map(|k| k.to_string()).collect::<Vec<String>>()
                    ));
                }

                entry.insert(sender);
            }
            Entry::Occupied(mut entry) => {
                error!("Already connected, dropping old connection: {identity:?}");
                entry.insert(sender);
            }
        }

        Ok(())
    }

    fn run_event_loop(
        &self,
        delay_packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
        exit: &Arc<AtomicBool>,
    ) -> Result<(), String>  {
        let heartbeat_tick = crossbeam_channel::tick(Duration::from_millis(100));
        let drop_log_tick = crossbeam_channel::tick(Duration::from_secs(5));
        let mut heartbeat_count = 0;
        let mut dropped_sends: u64 = 0;

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(delay_packet_receiver) -> maybe_packet_batches => {
                    let (failed_forwards, dropped) = self.forward_packets(maybe_packet_batches)?;
                    dropped_sends += dropped;
                    self.drop_connections(failed_forwards);
                }
                recv(heartbeat_tick) -> _ => {
                    let (failed_forwards, dropped) = self.handle_heartbeat(&mut heartbeat_count)?;
                    dropped_sends += dropped;
                    self.drop_connections(failed_forwards);
                }
                recv(drop_log_tick) -> _ => {
                    if dropped_sends > 0 {
                        warn!("Dropped {} sends to subscribers (channel full) in the last 5s", dropped_sends);
                        dropped_sends = 0;
                    }
                }
            }
        }

        Ok(())
    }

    fn forward_packets(
        &self,
        maybe_packet_batches: Result<BankingPacketBatch, crossbeam_channel::RecvError>,
    ) -> Result<(Vec<Pubkey>, u64), String> {
        let packet_batches = maybe_packet_batches.map_err(|err| err.to_string())?;

        let mut proto_packet_batches: Vec<ProtoPacketBatch> = Vec::new();
        let mut current = Vec::with_capacity(Self::VALIDATOR_PACKET_BATCH_SIZE);
        for proto_packet in packet_batches.iter().flat_map(|batch| {
            batch
                .iter()
                .filter(|p| !p.meta().discard())
                .filter_map(packet_to_proto_packet)
        }) {
            current.push(proto_packet);
            if current.len() == Self::VALIDATOR_PACKET_BATCH_SIZE {
                proto_packet_batches.push(ProtoPacketBatch {
                    packets: std::mem::replace(
                        &mut current,
                        Vec::with_capacity(Self::VALIDATOR_PACKET_BATCH_SIZE),
                    ),
                });
            }
        }
        if !current.is_empty() {
            proto_packet_batches.push(ProtoPacketBatch { packets: current });
        }

        let subscribers: Vec<(Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>)> = {
            let l_subscriptions = self.packet_subscriptions.read().unwrap();
            l_subscriptions.iter().map(|(k, v)| (*k, v.clone())).collect()
        };

        let mut failed_forwards = Vec::new();
        let mut dropped = 0u64;
        if subscribers.is_empty() {
            return Ok((failed_forwards, dropped));
        }

        let ts = Some(Timestamp::from(SystemTime::now()));
        let last = subscribers.len() - 1;
        for batch in proto_packet_batches {
            if batch.packets.is_empty() {
                continue;
            }

            for (pubkey, sender) in &subscribers[..last] {
                let response = SubscribePacketsResponse {
                    header: Some(Header { ts: ts.clone() }),
                    msg: Some(subscribe_packets_response::Msg::Batch(batch.clone())),
                };
                Self::try_forward(sender, pubkey, response, &mut failed_forwards, &mut dropped);
            }
            let (pubkey, sender) = &subscribers[last];
            let response = SubscribePacketsResponse {
                header: Some(Header { ts: ts.clone() }),
                msg: Some(subscribe_packets_response::Msg::Batch(batch)),
            };
            Self::try_forward(sender, pubkey, response, &mut failed_forwards, &mut dropped);
        }

        Ok((failed_forwards, dropped))
    }

    fn try_forward(
        sender: &TokioSender<Result<SubscribePacketsResponse, Status>>,
        pubkey: &Pubkey,
        response: SubscribePacketsResponse,
        failed_forwards: &mut Vec<Pubkey>,
        dropped: &mut u64,
    ) {
        match sender.try_send(Ok(response)) {
            Ok(_) => {}
            Err(TrySendError::Full(_)) => {
                *dropped += 1;
            }
            Err(TrySendError::Closed(_)) => {
                failed_forwards.push(*pubkey);
            }
        }
    }

    fn drop_connections(
        &self,
        failed_forwards: Vec<Pubkey>,
    ) {
        let mut l_subscriptions = self.packet_subscriptions.write().unwrap();
        for disconnected in failed_forwards {
            if let Some(sender) = l_subscriptions.remove(&disconnected) {
                drop(sender);
            }
        }
    }

    fn handle_heartbeat(
        &self,
        heartbeat_count: &mut u64,
    ) -> Result<(Vec<Pubkey>, u64), String> {
        let mut failed_pubkey_updates = Vec::new();
        let mut dropped = 0u64;

        let l_subscriptions = self.packet_subscriptions.read().unwrap();
        for (pubkey, sender) in l_subscriptions.iter() {
            match sender.try_send(Ok(SubscribePacketsResponse {
                header: None,
                msg: Some(subscribe_packets_response::Msg::Heartbeat(Heartbeat {
                    count: *heartbeat_count,
                })),
            })) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => failed_pubkey_updates.push(*pubkey),
                Err(TrySendError::Full(_)) => dropped += 1,
            }
        }

        *heartbeat_count += 1;
        Ok((failed_pubkey_updates, dropped))
    }
}

#[tonic::async_trait]
impl Relayer for ForwarderService {
    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: self.public_ip.to_string(),
                port: (self.tpu_quic_port - 6) as i64,
            }),
            tpu_forward: Some(Socket {
                ip: self.public_ip.to_string(),
                port: (self.tpu_fwd_quic_port - 6) as i64,
            }),
        }))
    }

    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or_else(|| Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(Self::SUBSCRIBER_QUEUE_CAPACITY);
        self.add_subscription(pubkey.clone(), sender)
            .map_err(|err| Status::internal(err))?;

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
