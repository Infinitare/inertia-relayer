use crate::relayer::staked_nodes_updater_service::StakedNodesUpdaterService;
use crate::rpc::Rpc;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use solana_core::banking_trace::BankingTracer;
use solana_core::sigverify::TransactionSigVerifier;
use solana_core::sigverify_stage::SigVerifyStage;
use solana_net_utils::sockets::{bind_to_with_config, SocketConfiguration};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_streamer::nonblocking::swqos::SwQosConfig;
use solana_streamer::quic::{spawn_stake_wighted_qos_server, QuicStreamerConfig};
use solana_streamer::streamer::StakedNodes;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use log::info;
use tokio_util::sync::CancellationToken;

pub struct Tpu {
    receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
}

impl Tpu {
    pub const TPU_QUEUE_CAPACITY: usize = 100_000;
    pub const MAX_QUIC_CONNECTIONS_PER_UNSTAKED_PEER: usize = 16;
    pub const MAX_QUIC_CONNECTIONS_PER_STAKED_PEER: usize = 64;
    pub const MAX_CONNECTIONS_PER_IPADDR_PER_MIN: u64 = 64;
    pub const QUIC_SOCKET_RECV_BUFFER_SIZE: usize = 64 * 1024 * 1024;
    pub const MAX_STREAMS_PER_MS: u64 = 4_000;

    pub fn new(
        keypair: &Arc<Keypair>,
        rpc: &Rpc,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        tpu_quic_port: u16,
        tpu_quic_fwd_port: u16,
        exit: &Arc<AtomicBool>,
    ) -> (Self, std::thread::JoinHandle<()>) {
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
        let quic_threads = NonZeroUsize::new((cores / 8).max(2)).expect("quic_threads is non-zero");
        let sigverify_threads = cores.saturating_sub(2 * quic_threads.get()).max(2);

        let (tpu_packet_sender, tpu_packet_receiver) = crossbeam_channel::bounded(Self::TPU_QUEUE_CAPACITY);

        let socket_config = SocketConfiguration::default()
            .recv_buffer_size(Self::QUIC_SOCKET_RECV_BUFFER_SIZE);
        let tpu_quic_socket = bind_to_with_config(
            IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
            tpu_quic_port,
            socket_config,
        ).unwrap();

        let tpu_quic_fwd_socket = bind_to_with_config(
            IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
            tpu_quic_fwd_port,
            socket_config,
        ).unwrap();

        let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
        let snus_task = StakedNodesUpdaterService::new(
            rpc,
            staked_nodes.clone(),
            staked_nodes_overrides,
            exit
        );

        let cancel = CancellationToken::new();
        std::thread::Builder::new()
            .name("tpu-cancel-watcher".to_string())
            .spawn({
                let cancel = cancel.clone();
                let exit = exit.clone();
                move || {
                    while !exit.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(500));
                    }
                    cancel.cancel();
                }
            })
            .expect("spawn tpu-cancel-watcher thread");

        info!("Starting TPU at: {} / {}", tpu_quic_port, tpu_quic_fwd_port);

        let quic_task = spawn_stake_wighted_qos_server(
            "quic_streamer_tpu",
            "quic_streamer_tpu",
            vec![tpu_quic_socket],
            keypair,
            tpu_packet_sender.clone(),
            staked_nodes.clone(),
            QuicStreamerConfig {
                max_connections_per_ipaddr_per_min: Self::MAX_CONNECTIONS_PER_IPADDR_PER_MIN,
                num_threads: quic_threads,
                ..Default::default()
            },
            SwQosConfig {
                max_streams_per_ms: Self::MAX_STREAMS_PER_MS,
                max_connections_per_staked_peer: Self::MAX_QUIC_CONNECTIONS_PER_STAKED_PEER,
                max_connections_per_unstaked_peer: Self::MAX_QUIC_CONNECTIONS_PER_UNSTAKED_PEER,
                ..SwQosConfig::default()
            },
            cancel.clone(),
        ).unwrap().thread;

        let quic_fwd_task = spawn_stake_wighted_qos_server(
            "quic_streamer_tpu_fwd",
            "quic_streamer_tpu_fwd",
            vec![tpu_quic_fwd_socket],
            keypair,
            tpu_packet_sender.clone(),
            staked_nodes.clone(),
            QuicStreamerConfig {
                max_connections_per_ipaddr_per_min: Self::MAX_CONNECTIONS_PER_IPADDR_PER_MIN,
                num_threads: quic_threads,
                ..Default::default()
            },
            SwQosConfig {
                max_streams_per_ms: Self::MAX_STREAMS_PER_MS,
                max_connections_per_staked_peer: Self::MAX_QUIC_CONNECTIONS_PER_STAKED_PEER,
                max_connections_per_unstaked_peer: Self::MAX_QUIC_CONNECTIONS_PER_UNSTAKED_PEER,
                ..SwQosConfig::default()
            },
            cancel,
        ).unwrap().thread;

        let (banking_packet_sender, banking_packet_receiver) =
            BankingTracer::new_disabled().create_channel_non_vote();

        let sigverify_threadpool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(sigverify_threads)
                .thread_name(|i| format!("solSigVerTpu{i:02}"))
                .build()
                .expect("New rayon threadpool"),
        );

        let sigverify_stage = SigVerifyStage::new(
            tpu_packet_receiver,
            TransactionSigVerifier::new(
                sigverify_threadpool,
                banking_packet_sender,
                None
            ),
            "tpu-verifier",
            "tpu-verifier",
        );

        let task = std::thread::Builder::new()
            .name("tpu".to_string())
            .spawn(move || {
                snus_task.join().expect("Staked nodes updater service thread join");
                quic_task.join().expect("Quic streamer tpu thread join");
                quic_fwd_task.join().expect("Quic streamer tpu fwd thread join");
                sigverify_stage.join().expect("Sigverify stage thread join");
            })
            .expect("Spawn tpu thread");

        let tpu = Self {
            receiver: banking_packet_receiver,
        };

        (tpu, task)
    }

    pub fn receiver(&self) -> crossbeam_channel::Receiver<BankingPacketBatch> {
        self.receiver.clone()
    }
}
