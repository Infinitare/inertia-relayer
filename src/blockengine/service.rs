use crate::protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use crate::protos::block_engine::block_engine_validator_server::BlockEngineValidator;
use crate::protos::block_engine::{BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, BlockEngineEndpoint, GetBlockEngineEndpointRequest, GetBlockEngineEndpointResponse, SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse};
use std::collections::HashMap;
use std::future::Future;
use std::net::{SocketAddr};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use log::{info, warn};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::codegen::BoxStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status, Streaming};
use crate::protos::auth::auth_service_client::AuthServiceClient;
use crate::protos::auth::auth_service_server::AuthService;
use crate::protos::auth::{GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse};

#[derive(Clone)]
pub struct BlockengineService {
    exit: Arc<AtomicBool>,
    blockengine_url: String,
    local_blockengine_url: String,
    client_pool: Arc<RwLock<HashMap<SocketAddr, BlockEngineValidatorClient<Channel>>>>,
    auth_client_pool: Arc<RwLock<HashMap<SocketAddr, AuthServiceClient<Channel>>>>,

    packet_from_proxy: broadcast::Sender<SubscribePacketsResponse>,
    packet_from_blockengine: broadcast::Sender<SubscribePacketsResponse>,
    bundle_from_proxy: broadcast::Sender<SubscribeBundlesResponse>,
    bundle_from_blockengine: broadcast::Sender<SubscribeBundlesResponse>,
}

impl BlockengineService {
    pub fn new(
        blockengine_url: String,
        local_blockengine_url: String,
        packet_from_proxy: broadcast::Sender<SubscribePacketsResponse>,
        packet_from_blockengine: broadcast::Sender<SubscribePacketsResponse>,
        bundle_from_proxy: broadcast::Sender<SubscribeBundlesResponse>,
        bundle_from_blockengine: broadcast::Sender<SubscribeBundlesResponse>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        BlockengineService {
            exit: exit.clone(),
            blockengine_url,
            local_blockengine_url,
            client_pool: Arc::new(RwLock::new(HashMap::new())),
            auth_client_pool: Arc::new(RwLock::new(HashMap::new())),
            packet_from_proxy,
            packet_from_blockengine,
            bundle_from_proxy,
            bundle_from_blockengine,
        }
    }

    async fn get_client(
        &self,
    ) -> Result<Channel, Status> {
        let channel = Endpoint::from_shared(self.blockengine_url.clone())
            .map_err(|e| Status::internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;

        Ok(channel)
    }

    async fn get_pooled_client<C, F>(
        &self,
        peer: Option<SocketAddr>,
        pool: &RwLock<HashMap<SocketAddr, C>>,
        make: F,
        pool_name: &str,
    ) -> Result<C, Status>
    where
        C: Clone,
        F: FnOnce(Channel) -> C,
    {
        info!("1.1");
        if let Some(addr) = peer {
            info!("1.2");
            let pool = pool.read().unwrap();
            info!("1.3");
            if let Some(existing) = pool.get(&addr) {
                info!("1.4");
                return Ok(existing.clone());
            }
        }

        info!("1.5");
        let channel = self.get_client().await?;
        info!("1.6");
        let client = make(channel);

        info!("1.7");
        if let Some(addr) = peer {
            info!("Adding client to {pool_name} pool: {addr}");

            info!("1.8");
            pool.write().unwrap().insert(addr, client.clone());
        }

        info!("1.9");
        Ok(client)
    }

    async fn get_block_engine_client(
        &self,
        peer: Option<SocketAddr>,
    ) -> Result<BlockEngineValidatorClient<Channel>, Status> {
        self.get_pooled_client(peer, &self.client_pool, |c| BlockEngineValidatorClient::new(c), "block engine")
            .await
    }

    async fn get_auth_client(
        &self,
        peer: Option<SocketAddr>,
    ) -> Result<AuthServiceClient<Channel>, Status> {
        self.get_pooled_client(peer, &self.auth_client_pool, |c| AuthServiceClient::new(c), "auth")
            .await
    }

    async fn pump_upstream<T>(
        mut upstream: Streaming<T>,
        blockengine: broadcast::Sender<T>,
        token: CancellationToken,
    ) where
        T: Clone + Send + 'static,
    {
        let cancelled = token.cancelled_owned();
        tokio::pin!(cancelled);

        loop {
            tokio::select! {
                biased;
                _ = &mut cancelled => break,
                message = upstream.message() => match message {
                    Ok(Some(item)) => { let _ = blockengine.send(item); }
                    Ok(None) => break,
                    Err(status) => {
                        warn!("upstream block engine stream error: {status:?}");
                        break;
                    }
                }
            }
        }
    }

    fn serve_proxy_stream<T>(
        &self,
        from_proxy: broadcast::Receiver<T>,
        blockengine_tx: broadcast::Sender<T>,
        upstream: Option<Streaming<T>>,
    ) -> Response<BoxStream<T>>
    where
        T: Clone + Send + 'static,
    {
        let token = CancellationToken::new();

        if let Some(upstream) = upstream {
            tokio::spawn(Self::pump_upstream(upstream, blockengine_tx, token.clone()));
        }

        let exit = self.exit.clone();
        let bridge_token = token.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = crate::helper::wait_for_exit(&exit) => bridge_token.cancel(),
                _ = bridge_token.cancelled() => {}
            }
        });

        Response::new(Self::proxy_stream(from_proxy, token))
    }

    fn proxy_stream<T>(
        from_proxy: broadcast::Receiver<T>,
        token: CancellationToken,
    ) -> BoxStream<T>
    where
        T: Clone + Send + 'static,
    {
        let proxy = BroadcastStream::new(from_proxy)
            .filter_map(|r| r.ok().map(Result::<_, Status>::Ok));

        Box::pin(ExitAwareStream {
            inner: Box::pin(proxy),
            cancelled: Box::pin(token.clone().cancelled_owned()),
            _guard: token.drop_guard(),
        })
    }
}

struct ExitAwareStream<T> {
    inner: BoxStream<T>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    _guard: DropGuard,
}

impl<T> Stream for ExitAwareStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        this.inner.as_mut().poll_next(cx)
    }
}

#[tonic::async_trait]
impl BlockEngineValidator for BlockengineService {
    type SubscribePacketsStream = BoxStream<SubscribePacketsResponse>;

    async fn subscribe_packets(
        &self,
        req: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let peer = req.remote_addr();
        info!("Received subscribe_packets request from: {peer:?}");

        let from_proxy = self.packet_from_proxy.subscribe();

        let mut upstream = self.get_block_engine_client(peer).await?;
        let up_resp = match upstream.subscribe_packets(req).await {
            Ok(resp) => Some(resp.into_inner()),
            Err(e) => {
                if e.code() != Code::PermissionDenied {
                    return Err(e);
                }

                warn!("Validator is blocked from Block Engine packets, serving proxy only: {peer:?}");
                None
            }
        };

        Ok(self.serve_proxy_stream(
            from_proxy,
            self.packet_from_blockengine.clone(),
            up_resp,
        ))
    }

    type SubscribeBundlesStream = BoxStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        req: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        let peer = req.remote_addr();
        info!("Received subscribe_bundles request from: {peer:?}");

        let from_proxy = self.bundle_from_proxy.subscribe();

        let mut upstream = self.get_block_engine_client(peer).await?;
        let up_resp = match upstream.subscribe_bundles(req).await {
            Ok(resp) => Some(resp.into_inner()),
            Err(e) => {
                if e.code() != Code::PermissionDenied {
                    return Err(e);
                }

                warn!("Validator is blocked from Block Engine bundles, serving proxy only: {peer:?}");
                None
            }
        };

        Ok(self.serve_proxy_stream(
            from_proxy,
            self.bundle_from_blockengine.clone(),
            up_resp,
        ))
    }

    async fn get_block_builder_fee_info(
        &self,
        req: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        let peer = req.remote_addr();
        info!("Received get_block_builder_fee_info request from: {:?}", peer);

        let res = {
            let mut upstream = self.get_block_engine_client(peer).await?;
            upstream.get_block_builder_fee_info(req).await
        };

        info!("get_block_builder_fee_info response: {:?}", res);
        res
    }

    async fn get_block_engine_endpoints(
        &self,
        req: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        let peer = req.remote_addr();
        info!("Received get_block_engine_endpoints request from: {:?}", peer);
        let res = {
            info!("1");
            let mut upstream = self.get_block_engine_client(peer).await?;
            info!("2");

            let jito_res = upstream.get_block_engine_endpoints(req).await?.into_inner();
            info!("3");
            let res = GetBlockEngineEndpointResponse {
                global_endpoint: jito_res.global_endpoint.map(|global| BlockEngineEndpoint {
                    block_engine_url: self.local_blockengine_url.clone(),
                    shredstream_receiver_address: global.shredstream_receiver_address,
                }),
                regioned_endpoints: jito_res.regioned_endpoints.iter().map(|endpoint| {
                    BlockEngineEndpoint {
                        block_engine_url: self.local_blockengine_url.clone(),
                        shredstream_receiver_address: endpoint.shredstream_receiver_address.clone(),
                    }
                }).collect(),
            };

            Ok(Response::new(res))
        };

        info!("get_block_engine_endpoints response: {:?}", res);
        res
    }
}

#[tonic::async_trait]
impl AuthService for BlockengineService {
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        let peer = req.remote_addr();
        info!("Received generate_auth_challenge request from: {:?}", peer);
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.generate_auth_challenge(req).await
    }

    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let peer = req.remote_addr();
        info!("Received generate_auth_tokens request from: {:?}", peer);
        let peer = req.remote_addr();
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.generate_auth_tokens(req).await
    }

    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        let peer = req.remote_addr();
        info!("Received refresh_access_token request from: {:?}", peer);
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.refresh_access_token(req).await
    }
}
