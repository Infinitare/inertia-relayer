use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{info, warn};
use prost::Message;
use prost::bytes::{BufMut, BytesMut};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, Endpoint, RecvStream, SendDatagramError};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::sync::{broadcast, watch};

use crate::protos::block_engine::SubscribeBundlesResponse;
use crate::proxy::{FilterSet, Proxy};
use crate::relayer::forwarder::ConnectedValidator;

#[derive(Clone)]
pub struct Mirror {
    shutdown: watch::Receiver<bool>,
    proxy_server: SocketAddr,
    inertia_server: SocketAddr,
    cert_pin: [u8; 32],
    conn: Arc<RwLock<Option<Connection>>>,
    filter_set: Arc<FilterSet>,
    validator: ConnectedValidator,
    stats: Arc<MirrorStats>,
}

#[derive(Default)]
struct MirrorStats {
    tpu_packets_sent: AtomicU64,
    be_packets_sent: AtomicU64,
    control_sent: AtomicU64,
    bundles_sent: AtomicU64,
    bundles_received: AtomicU64,
    filters_received: AtomicU64,
    dropped_oversized: AtomicU64,
    dropped_other: AtomicU64,
}

thread_local! {
    static SEND_BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(64 * 1024));
}

impl Mirror {
    pub const KEEP_ALIVE: Duration = Duration::from_secs(5);
    pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
    pub const BACKOFF_INITIAL: Duration = Duration::from_millis(250);
    pub const BACKOFF_MAX: Duration = Duration::from_secs(5);
    pub const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    pub const MAX_BUNDLE_FRAME: usize = 4 * 1024 * 1024;
    pub const DATAGRAM_SEND_BUFFER: usize = 4 * 1024 * 1024;

    pub const ALPN: &[u8] = b"inertia-mirror";
    pub const SERVER_NAME: &'static str = "inertia-relayer";

    pub const FILTER_TAG: u8 = 0;
    pub const CONTROL_INTERVAL: Duration = Duration::from_secs(1);

    pub fn new(
        proxy_server: SocketAddr,
        inertia_server: SocketAddr,
        cert_pin: [u8; 32],
        filter_set: Arc<FilterSet>,
        validator: ConnectedValidator,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Mirror {
            proxy_server,
            inertia_server,
            cert_pin,
            shutdown,
            conn: Arc::new(RwLock::new(None)),
            filter_set,
            validator,
            stats: Arc::new(MirrorStats::default()),
        }
    }

    pub fn send(&self, source: u8, data: &[u8]) {
        let conn = match self.conn.read().unwrap().as_ref() {
            Some(conn) => conn.clone(),
            None => return,
        };

        let frame = SEND_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.reserve(1 + 8 + data.len());
            buf.put_u8(source);
            buf.put_u64_le(now_nanos());
            buf.put_slice(data);
            buf.split().freeze()
        });
        match conn.send_datagram(frame) {
            Ok(()) => {
                let sent = match source {
                    Proxy::SOURCE_RELAYER => &self.stats.tpu_packets_sent,
                    Proxy::SOURCE_CONTROL => &self.stats.control_sent,
                    _ => &self.stats.be_packets_sent,
                };
                sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(SendDatagramError::TooLarge) => {
                self.stats.dropped_oversized.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.dropped_other.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn set(&self, conn: Option<Connection>) {
        *self.conn.write().unwrap() = conn;
    }

    pub async fn run(
        &self,
        bundle_out: broadcast::Sender<SubscribeBundlesResponse>,
        mut bundle_mirror_in: broadcast::Receiver<SubscribeBundlesResponse>,
    ) {
        let endpoint = match self.client_endpoint() {
            Ok(endpoint) => endpoint,
            Err(e) => {
                warn!("Mirror: failed to build QUIC endpoint: {e}");
                return;
            }
        };

        let reporter = tokio::spawn(report_stats(self.stats.clone(), self.shutdown.clone()));

        let mut backoff = Self::BACKOFF_INITIAL;
        while !*self.shutdown.borrow() {
            match self.connect(&endpoint).await {
                Ok(conn) => {
                    match conn.max_datagram_size() {
                        Some(size) => info!(
                            "Mirror: connected to {} (max datagram {size} bytes)",
                            self.inertia_server
                        ),
                        None => warn!(
                            "Mirror: connected to {} but peer rejects datagrams; packet mirroring disabled",
                            self.inertia_server
                        ),
                    }
                    backoff = Self::BACKOFF_INITIAL;

                    self.set(Some(conn.clone()));
                    self.serve_connection(&conn, &bundle_out, &mut bundle_mirror_in).await;

                    self.set(None);
                    warn!("Mirror: connection to {} closed", self.inertia_server);
                }
                Err(e) => warn!("Mirror: connect to {} failed: {e}", self.inertia_server),
            }

            if *self.shutdown.borrow() {
                break;
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Self::BACKOFF_MAX);
        }

        reporter.abort();
        endpoint.close(0u32.into(), b"shutdown");
    }

    async fn connect(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Connection, String> {
        endpoint
            .connect(self.inertia_server, Self::SERVER_NAME)
            .map_err(|e| e.to_string())?
            .await
            .map_err(|e| e.to_string())
    }

    async fn serve_connection(
        &self,
        conn: &Connection,
        out: &broadcast::Sender<SubscribeBundlesResponse>,
        bundle_mirror_in: &mut broadcast::Receiver<SubscribeBundlesResponse>,
    ) {
        tokio::select! {
            _ = self.read_bundles(conn, out) => {}
            _ = self.read_filters(conn) => {}
            _ = self.mirror_bundles_out(conn, bundle_mirror_in) => {}
            _ = self.mirror_control() => {}
        }
    }

    async fn mirror_control(&self) {
        let mut tick = tokio::time::interval(Self::CONTROL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut last = None;
        loop {
            tokio::select! {
                biased;
                _ = self.wait_for_shutdown() => return,
                _ = tick.tick() => {
                    let Some(identity) = self.validator.get() else { continue };
                    if last != Some(identity) {
                        info!("Mirror: reporting connected validator {identity}");
                        last = Some(identity);
                    }

                    self.send(Proxy::SOURCE_CONTROL, identity.as_ref());
                }
            }
        }
    }

    async fn read_filters(&self, conn: &Connection) {
        loop {
            tokio::select! {
                biased;
                _ = self.wait_for_shutdown() => return,
                datagram = conn.read_datagram() => match datagram {
                    Ok(bytes) => self.ingest_filter(&bytes),
                    Err(e) => {
                        warn!("Mirror: filter datagram stream ended: {e}");
                        return;
                    }
                }
            }
        }
    }

    fn ingest_filter(&self, bytes: &[u8]) {
        let Some((&tag, signature)) = bytes.split_first() else { return };
        if tag != Self::FILTER_TAG {
            return;
        }
        
        let Ok(signature) = <[u8; 64]>::try_from(signature) else { return };
        self.filter_set.insert(signature);
        self.stats.filters_received.fetch_add(1, Ordering::Relaxed);
    }

    async fn mirror_bundles_out(
        &self,
        conn: &Connection,
        rx: &mut broadcast::Receiver<SubscribeBundlesResponse>,
    ) {
        let mut send = match conn.open_uni().await {
            Ok(send) => send,
            Err(e) => {
                warn!("Mirror: failed to open bundle mirror stream: {e}");
                return;
            }
        };

        loop {
            tokio::select! {
                biased;
                _ = self.wait_for_shutdown() => {
                    let _ = send.finish();
                    return;
                }
                received = rx.recv() => match received {
                    Ok(resp) => {
                        let mut buf = Vec::new();
                        if let Err(e) = resp.encode(&mut buf) {
                            warn!("Mirror: failed to encode bundle frame: {e}");
                            continue;
                        }
                        if send.write_all(&(buf.len() as u32).to_le_bytes()).await.is_err()
                            || send.write_all(&buf).await.is_err()
                        {
                            return;
                        }
                        self.stats
                            .bundles_sent
                            .fetch_add(resp.bundles.len() as u64, Ordering::Relaxed);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Mirror: bundle mirror receiver lagged, dropped {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let _ = send.finish();
                        return;
                    }
                }
            }
        }
    }

    async fn read_bundles(
        &self,
        conn: &Connection,
        out: &broadcast::Sender<SubscribeBundlesResponse>,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = self.wait_for_shutdown() => return,
                accepted = conn.accept_uni() => match accepted {
                    Ok(recv) => {
                        tokio::spawn(read_bundle_stream(
                            recv,
                            out.clone(),
                            self.stats.clone(),
                            self.shutdown.clone(),
                        ));
                    }
                    Err(e) => {
                        warn!("Mirror: bundle stream ended: {e}");
                        return;
                    }
                }
            }
        }
    }

    async fn wait_for_shutdown(&self) {
        let mut shutdown = self.shutdown.clone();
        loop {
            if *shutdown.borrow() {
                return;
            }
            if shutdown.changed().await.is_err() {
                return;
            }
        }
    }

    fn client_endpoint(&self) -> Result<Endpoint, String> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let verifier = Arc::new(PinnedServerCertVerifier {
            provider: provider.clone(),
            pinned_sha256: self.cert_pin,
        });

        let mut crypto = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![Self::ALPN.to_vec()];


        let quic_crypto = QuicClientConfig::try_from(crypto).map_err(|e| e.to_string())?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));

        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(Self::KEEP_ALIVE));
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Self::IDLE_TIMEOUT).map_err(|e| e.to_string())?,
        ));
        transport.datagram_send_buffer_size(Self::DATAGRAM_SEND_BUFFER);
        client_config.transport_config(Arc::new(transport));

        let mut endpoint = Endpoint::client(self.proxy_server).map_err(|e| e.to_string())?;
        endpoint.set_default_client_config(client_config);
        Ok(endpoint)
    }
}

async fn report_stats(stats: Arc<MirrorStats>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Mirror::STATS_REPORT_INTERVAL);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tick.tick() => {
                let secs = Mirror::STATS_REPORT_INTERVAL.as_secs();
                let tpu_packets = stats.tpu_packets_sent.swap(0, Ordering::Relaxed);
                let be_packets = stats.be_packets_sent.swap(0, Ordering::Relaxed);
                let control_sent = stats.control_sent.swap(0, Ordering::Relaxed);
                let bundles_sent = stats.bundles_sent.swap(0, Ordering::Relaxed);
                let bundles_received = stats.bundles_received.swap(0, Ordering::Relaxed);
                let filters_received = stats.filters_received.swap(0, Ordering::Relaxed);
                if tpu_packets > 0 || be_packets > 0 || bundles_sent > 0 || bundles_received > 0 || filters_received > 0 {
                    info!(
                        "Mirror: last {secs}s: sent {tpu_packets} tpu packets, {be_packets} block engine packets, {bundles_sent} bundles, {control_sent} control; received {bundles_received} bundles, {filters_received} filters"
                    );
                }
                let oversized = stats.dropped_oversized.swap(0, Ordering::Relaxed);
                let other = stats.dropped_other.swap(0, Ordering::Relaxed);
                if oversized > 0 || other > 0 {
                    warn!(
                        "Mirror: dropped datagrams in last {secs}s: {oversized} oversized (too large), {other} other"
                    );
                }
            }
        }
    }
}

async fn read_bundle_stream(
    mut recv: RecvStream,
    out: broadcast::Sender<SubscribeBundlesResponse>,
    stats: Arc<MirrorStats>,
    shutdown: watch::Receiver<bool>,
) {
    let mut len_buf = [0u8; 4];
    while !*shutdown.borrow() {
        if recv.read_exact(&mut len_buf).await.is_err() {
            return;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > Mirror::MAX_BUNDLE_FRAME {
            warn!("Mirror: oversized bundle frame ({len} bytes), closing stream");
            return;
        }

        let mut buf = vec![0u8; len];
        if recv.read_exact(&mut buf).await.is_err() {
            return;
        }
        match SubscribeBundlesResponse::decode(buf.as_slice()) {
            Ok(resp) => {
                stats
                    .bundles_received
                    .fetch_add(resp.bundles.len() as u64, Ordering::Relaxed);
                let _ = out.send(resp);
            }
            Err(e) => warn!("Mirror: failed to decode bundle frame: {e}"),
        }
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

pub fn parse_cert_pin(s: &str) -> Result<[u8; 32], String> {
    let cleaned: Vec<u8> = s
        .bytes()
        .filter(|b| !b.is_ascii_whitespace() && *b != b':')
        .collect();
    if cleaned.len() != 64 {
        return Err(format!(
            "expected 64 hex characters (32-byte SHA-256), got {}",
            cleaned.len()
        ));
    }

    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_val(cleaned[i * 2])?;
        let lo = hex_val(cleaned[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex character: {:?}", c as char)),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[derive(Debug)]
struct PinnedServerCertVerifier {
    provider: Arc<CryptoProvider>,
    pinned_sha256: [u8; 32],
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let presented = openssl::sha::sha256(end_entity.as_ref());
        if openssl::memcmp::eq(&presented, &self.pinned_sha256) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "server certificate fingerprint mismatch: expected {}, got {}",
                hex_encode(&self.pinned_sha256),
                hex_encode(&presented),
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}