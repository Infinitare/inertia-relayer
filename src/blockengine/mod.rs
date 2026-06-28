mod service;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use log::info;
use tokio::runtime::Handle;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use crate::blockengine::service::BlockengineService;
use crate::helper::shutdown_signal;
use crate::protos::auth::auth_service_server::AuthServiceServer;
use crate::protos::block_engine::{SubscribeBundlesResponse, SubscribePacketsResponse};
use crate::protos::block_engine::block_engine_validator_server::BlockEngineValidatorServer;

pub struct Blockengine {
    packet_from_proxy: broadcast::Sender<SubscribePacketsResponse>,
    packet_from_blockengine: broadcast::Sender<SubscribePacketsResponse>,
    bundle_from_proxy: broadcast::Sender<SubscribeBundlesResponse>,
    bundle_from_blockengine: broadcast::Sender<SubscribeBundlesResponse>,
}

impl Blockengine {
    pub const BLOCKENGINE_CHANNEL_LIMIT: usize = 50_000;

    pub fn new(
        block_engine_url: String,
        relayer_bind_ip: IpAddr,
        blockengine_bind_port: u16,
        rt: &Handle,
        exit: &Arc<AtomicBool>,
    ) -> (Blockengine, JoinHandle<()>) {
        let (bundle_from_proxy, _) = broadcast::channel::<SubscribeBundlesResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);
        let (packet_from_blockengine, _) = broadcast::channel::<SubscribePacketsResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);
        let (packet_from_proxy, _) = broadcast::channel::<SubscribePacketsResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);
        let (bundle_from_blockengine, _) = broadcast::channel::<SubscribeBundlesResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);

        let local_blockengine_url = format!("http://{}:{}", relayer_bind_ip, blockengine_bind_port);
        let blockengine_service = BlockengineService::new(
            block_engine_url,
            local_blockengine_url,
            packet_from_proxy.clone(),
            packet_from_blockengine.clone(),
            bundle_from_proxy.clone(),
            bundle_from_blockengine.clone(),
            exit,
        );

        let exit = exit.clone();
        let server_addr = SocketAddr::new(relayer_bind_ip, blockengine_bind_port);

        info!("Starting blockengine at: {:?}", server_addr);
        let join = rt.spawn(async move {
            Server::builder()
                .add_service(BlockEngineValidatorServer::new(blockengine_service.clone()))
                .add_service(AuthServiceServer::new(blockengine_service))
                .serve_with_shutdown(server_addr, shutdown_signal(exit.clone()))
                .await
                .expect("Serve Blockengine");
        });

        let blockengine = Blockengine {
            packet_from_proxy,
            packet_from_blockengine,
            bundle_from_proxy,
            bundle_from_blockengine,
        };

        (blockengine, join)
    }

    pub fn packet_from_blockengine(&self) -> broadcast::Sender<SubscribePacketsResponse> {
        self.packet_from_blockengine.clone()
    }

    pub fn packet_from_proxy(&self) -> broadcast::Sender<SubscribePacketsResponse> {
        self.packet_from_proxy.clone()
    }

    pub fn bundle_from_blockengine(&self) -> broadcast::Sender<SubscribeBundlesResponse> {
        self.bundle_from_blockengine.clone()
    }

    pub fn bundle_from_proxy(&self) -> broadcast::Sender<SubscribeBundlesResponse> {
        self.bundle_from_proxy.clone()
    }
}
