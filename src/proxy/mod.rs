mod mirror;

use crate::blockengine::Blockengine;
use crate::protos::block_engine::{SubscribeBundlesResponse, SubscribePacketsResponse};
use crate::proxy::mirror::Mirror;
use crate::proxy::mirror::parse_cert_pin;
use crate::relayer::Relayer;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use log::warn;
use std::collections::{HashMap, VecDeque};
use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct FilterSet {
    map: Mutex<HashMap<[u8; 64], Instant>>,
    len: AtomicUsize,
}

impl FilterSet {
    const TTL: Duration = Duration::from_secs(2);

    pub fn insert(&self, signature: [u8; 64]) {
        let mut map = self.map.lock().unwrap();
        map.insert(signature, Instant::now());
        self.len.store(map.len(), Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool {
        self.len.load(Ordering::Relaxed) == 0
    }

    fn locked(&self) -> MutexGuard<'_, HashMap<[u8; 64], Instant>> {
        self.map.lock().unwrap()
    }

    fn sweep(&self) {
        if self.is_empty() {
            return;
        }
        let mut map = self.map.lock().unwrap();
        map.retain(|_, at| at.elapsed() < Self::TTL);
        self.len.store(map.len(), Ordering::Relaxed);
    }
}

fn first_signature(data: &[u8]) -> Option<[u8; 64]> {
    data.get(1..65)?.try_into().ok()
}

pub struct Proxy;

impl Proxy {
    const IDLE_TICK: Duration = Duration::from_millis(100);
    pub const SOURCE_RELAYER: u8 = 0;
    pub const SOURCE_BLOCKENGINE: u8 = 1;
    pub const PACKET_DELAY: Duration = Duration::from_millis(100);

    pub fn new(
        relayer: Relayer,
        blockengine: Blockengine,
        proxy_bind_ip: IpAddr,
        proxy_bind_port: u16,
        inertia_server: SocketAddr,
        inertia_cert_sha256: String,
        rt: &Handle,
        exit: &Arc<AtomicBool>,
    ) -> (JoinHandle<()>, std::thread::JoinHandle<()>) {
        let cert_pin = parse_cert_pin(&inertia_cert_sha256).expect("Invalid inertia certificate fingerprint (--inertia-cert-sha256)");
        let tpu_receiver = relayer.tpu_receiver();
        let forwarder_sender = relayer.forwarder_sender();
        let be_packet_in = blockengine.packet_from_blockengine().subscribe();
        let be_packet_out = blockengine.packet_from_proxy();
        let be_bundle_in = blockengine.bundle_from_blockengine().subscribe();
        let be_bundle_mirror_in = blockengine.bundle_from_blockengine().subscribe();
        let bundle_out = blockengine.bundle_from_proxy();

        let proxy_addr = SocketAddr::new(proxy_bind_ip, proxy_bind_port);
        let shutdown = shutdown_watch(exit.clone(), rt);
        let filter_set = Arc::new(FilterSet::default());

        let mirror = Mirror::new(
            proxy_addr,
            inertia_server,
            cert_pin,
            filter_set.clone(),
            shutdown.clone(),
        );

        let relayer_delay_task = spawn_relayer_delay(
            tpu_receiver,
            forwarder_sender,
            mirror.clone(),
            filter_set.clone(),
            Self::PACKET_DELAY,
            exit.clone(),
        );

        let packets_mirror = mirror.clone();
        let packets_filter = filter_set.clone();
        let packets_join = rt.spawn(delay_forward(
            be_packet_in,
            be_packet_out,
            Self::PACKET_DELAY,
            shutdown.clone(),
            "block-engine-packet",
            move |resp| mirror_be_packets(&packets_mirror, resp),
            move |resp: SubscribePacketsResponse| filter_be_packets(&packets_filter, resp),
        ));

        let bundles_filter = filter_set.clone();
        let bundles_join = rt.spawn(delay_forward(
            be_bundle_in,
            bundle_out.clone(),
            Self::PACKET_DELAY,
            shutdown.clone(),
            "block-engine-bundle",
            |_| {},
            move |resp: SubscribeBundlesResponse| filter_be_bundles(&bundles_filter, resp),
        ));

        let mirror_join = rt.spawn(async move {
            mirror.run(bundle_out, be_bundle_mirror_in).await;
        });

        let join = rt.spawn(async move {
            mirror_join.await.expect("Mirror packets");
            packets_join.await.expect("Blockengine packets");
            bundles_join.await.expect("Blockengine bundles");
        });

        (join, relayer_delay_task)
    }
}

fn spawn_relayer_delay(
    rx: Receiver<BankingPacketBatch>,
    tx: Sender<BankingPacketBatch>,
    mirror: Mirror,
    filter_set: Arc<FilterSet>,
    delay: Duration,
    exit: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("proxy-relayer-delay".to_string())
        .spawn(move || {
            let mut queue: VecDeque<(Instant, BankingPacketBatch)> = VecDeque::new();

            while !exit.load(Ordering::Relaxed) {
                let timeout = match queue.front() {
                    Some((release_at, _)) => release_at.saturating_duration_since(Instant::now()),
                    None => Proxy::IDLE_TICK,
                };

                match rx.recv_timeout(timeout) {
                    Ok(batch) => {
                        mirror_banking_batch(&mirror, &batch);
                        queue.push_back((Instant::now() + delay, batch));
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                filter_set.sweep();

                let now = Instant::now();
                while queue.front().is_some_and(|(release_at, _)| *release_at <= now) {
                    let (_, batch) = queue.pop_front().unwrap();
                    if tx.send(discard_filtered(&filter_set, batch)).is_err() {
                        return;
                    }
                }
            }
        })
        .expect("spawn proxy-relayer-delay thread")
}

fn discard_filtered(filter_set: &FilterSet, mut batch: BankingPacketBatch) -> BankingPacketBatch {
    if filter_set.is_empty() {
        return batch;
    }

    let blocked = filter_set.locked();
    for packet_batch in Arc::make_mut(&mut batch).iter_mut() {
        for mut packet in packet_batch.iter_mut() {
            if packet.meta().discard() {
                continue;
            }
            if packet.data(..).and_then(first_signature).is_some_and(|sig| blocked.contains_key(&sig)) {
                packet.meta_mut().set_discard(true);
            }
        }
    }

    batch
}

fn filter_be_packets(filter_set: &FilterSet, mut resp: SubscribePacketsResponse) -> SubscribePacketsResponse {
    if filter_set.is_empty() {
        return resp;
    }

    if let Some(batch) = resp.batch.as_mut() {
        let blocked = filter_set.locked();
        batch.packets.retain(|p| first_signature(&p.data).is_none_or(|sig| !blocked.contains_key(&sig)));
    }

    resp
}

fn filter_be_bundles(filter_set: &FilterSet, mut resp: SubscribeBundlesResponse) -> SubscribeBundlesResponse {
    if filter_set.is_empty() {
        return resp;
    }

    let blocked = filter_set.locked();
    resp.bundles.retain(|bu| {
        let hit = bu.bundle.as_ref().is_some_and(|b| {
            b.packets.iter().any(|p| first_signature(&p.data).is_some_and(|sig| blocked.contains_key(&sig)))
        });
        !hit
    });

    resp
}

fn mirror_banking_batch(mirror: &Mirror, batch: &BankingPacketBatch) {
    for packet_batch in batch.iter() {
        for packet in packet_batch.iter() {
            if packet.meta().discard() {
                continue;
            }
            if let Some(data) = packet.data(..) {
                mirror.send(Proxy::SOURCE_RELAYER, data);
            }
        }
    }
}

fn shutdown_watch(exit: Arc<AtomicBool>, rt: &Handle) -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    rt.spawn(async move {
        crate::helper::wait_for_exit(&exit).await;
        let _ = tx.send(true);
    });
    rx
}

async fn delay_forward<T, F, G>(
    mut rx: broadcast::Receiver<T>,
    tx: broadcast::Sender<T>,
    delay: Duration,
    mut shutdown: watch::Receiver<bool>,
    label: &'static str,
    on_receive: F,
    on_release: G,
) where
    T: Clone + Send + 'static,
    F: Fn(&T) + Send + 'static,
    G: Fn(T) -> T + Send + 'static,
{
    let mut queue: VecDeque<(tokio::time::Instant, T)> = VecDeque::new();

    loop {
        let release_at = queue.front().map(|(release_at, _)| *release_at);
        let release_due = async {
            match release_at {
                Some(release_at) => tokio::time::sleep_until(release_at).await,
                None => pending().await,
            }
        };

        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = release_due => {
                let now = tokio::time::Instant::now();
                while queue.front().is_some_and(|(release_at, _)| *release_at <= now) {
                    let (_, resp) = queue.pop_front().unwrap();
                    let _ = tx.send(on_release(resp));
                }
            }
            received = rx.recv() => match received {
                Ok(resp) => {
                    on_receive(&resp);
                    queue.push_back((tokio::time::Instant::now() + delay, resp));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Proxy: {label} receiver lagged, dropped {n}");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}

fn mirror_be_packets(mirror: &Mirror, resp: &SubscribePacketsResponse) {
    if let Some(batch) = &resp.batch {
        for packet in &batch.packets {
            mirror.send(Proxy::SOURCE_BLOCKENGINE, &packet.data);
        }
    }
}