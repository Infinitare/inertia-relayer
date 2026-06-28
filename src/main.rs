mod helper;
mod rpc;
mod relayer;
mod protos;
mod blockengine;
mod proxy;

use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use agave_validator::admin_rpc_service::StakedNodesOverrides;
use clap::Parser;
use env_logger::Env;
use log::{info, LevelFilter};
use simplelog::{Config, WriteLogger};
use solana_sdk::signature::{read_keypair_file, Signer};
use tokio::runtime::Builder;
use crate::blockengine::Blockengine;
use crate::helper::graceful_panic;
use crate::proxy::Proxy;
use crate::relayer::Relayer;
use crate::rpc::Rpc;

#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    /// Path to keypair file used to authenticate on Solana
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// The private key used to sign tokens to authenticate the Relayer
    #[arg(long, env)]
    signing_key_pem_path: PathBuf,

    /// The public key used to verify tokens to authenticate the Relayer
    #[arg(long, env)]
    verifying_key_pem_path: PathBuf,

    /// Port for TPU QUIC packets
    #[arg(long, env, default_value_t = 11_228)]
    tpu_quic_port: u16,

    /// Port for TPU QUIC forward packets
    #[arg(long, env, default_value_t = 11_229)]
    tpu_quic_fwd_port: u16,

    /// Bind IP address for Relayer server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    relayer_bind_ip: IpAddr,

    /// Bind port for Relayer server
    #[arg(long, env, default_value_t = 11_225)]
    relayer_bind_port: u16,

    /// Bind IP address for Blockengine server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    blockengine_bind_ip: IpAddr,

    /// Bind port for Blockengine server
    #[arg(long, env, default_value_t = 11_226)]
    blockengine_bind_port: u16,

    /// Jito Blockengine the Relayer will connect to please choose the one closest to your server
    #[arg(long, env)]
    blockengine_url: String,

    /// Bind IP address for Proxy server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    proxy_bind_ip: IpAddr,

    /// Bind port for Blockengine server
    #[arg(long, env, default_value_t = 11_227)]
    proxy_bind_port: u16,

    /// Inertia server the Relayer will connect to for data
    #[arg(long, env)]
    inertia_server: SocketAddr,

    /// Inertia server certificate SHA256 fingerprint the Relayer will use to verify the server's identity
    #[arg(long, env)]
    inertia_cert_sha256: String,

    /// RPC server the Relayer will connect to for data
    #[arg(
        long,
        env,
        default_value = "http://127.0.0.1:8899"
    )]
    rpc_server: String,

    /// WebSocket server the Relayer will connect to for data
    #[arg(
        long,
        env,
        default_value = "ws://127.0.0.1:8900"
    )]
    websocket_server: String,

    /// Path to staked nodes overrides file
    #[arg(long, env)]
    staked_nodes_overrides: Option<PathBuf>,

    /// Public IP address of the validator - if not provided, it will be determined automatically
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Path to log file
    #[arg(long, env, default_value = "/etc/inertia-relayer/relayer.log")]
    log_path: PathBuf,
}

fn main() {
    let args: Args = Args::parse();

    if cfg!(debug_assertions) {
        env_logger::Builder::from_env(Env::new().default_filter_or("debug"))
            .format_timestamp_millis()
            .init();
    } else {
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&args.log_path)
            .unwrap();

        if let Err(e) = WriteLogger::init(
            LevelFilter::Info,
            Config::default(),
            log_file,
        ) {
            panic!("Failed to initialize logger: {}", e);
        }
    }

    info!("Starting inertia-relayer");
    let keypair = Arc::new(read_keypair_file(args.keypair_path).expect("Keypair file does not exist"));
    info!("Using Keypair: {}", keypair.pubkey());

    let staked_nodes_overrides = match args.staked_nodes_overrides {
        None => StakedNodesOverrides::default(),
        Some(p) => {
            let file = fs::File::open(&p).expect(&format!(
                "Failed to open staked nodes overrides file: {:?}",
                &p
            ));
            serde_yaml::from_reader(file).expect(&format!(
                "Failed to read staked nodes overrides file: {:?}",
                &p,
            ))
        }
    };

    let public_ip = if args.public_ip.is_some() {
        args.public_ip.unwrap()
    } else {
        let entrypoint = solana_net_utils::parse_host_port("entrypoint.mainnet-beta.solana.com:8001")
            .expect("parse entrypoint");

        info!(
            "Contacting {} to determine the relayer's public IP address",
            entrypoint
        );

        let bind_address = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        solana_net_utils::get_public_ip_addr_with_binding(&entrypoint, bind_address).expect("Could not get public IP address")
    };

    let exit = graceful_panic(None);
    let rt = Builder::new_multi_thread()
        .enable_all()
        .disable_lifo_slot()
        .build()
        .unwrap();

    let (rpc, rpc_task) = Rpc::new(
        args.rpc_server,
        args.websocket_server,
        &exit
    );

    let (relayer, relayer_join, relayer_task) = Relayer::new(
        &keypair,
        &rpc,
        public_ip,
        staked_nodes_overrides.staked_map_id,
        args.tpu_quic_port,
        args.tpu_quic_fwd_port,
        args.relayer_bind_ip,
        args.relayer_bind_port,
        args.signing_key_pem_path,
        args.verifying_key_pem_path,
        rt.handle(),
        &exit,
    );

    let (blockengine, blockengine_join) = Blockengine::new(
        args.blockengine_url,
        args.blockengine_bind_ip,
        args.blockengine_bind_port,
        rt.handle(),
        &exit,
    );

    let (proxy_join, proxy_delay_task) = Proxy::new(
        relayer,
        blockengine,
        args.proxy_bind_ip,
        args.proxy_bind_port,
        args.inertia_server,
        args.inertia_cert_sha256,
        rt.handle(),
        &exit,
    );

    rt.block_on(async move {
        relayer_join.await.expect("Relayer grpc server");
        blockengine_join.await.expect("Blockengine server");
        proxy_join.await.expect("Proxy tasks");
    });

    rpc_task.join().expect("Rpc worker thread");
    relayer_task.join().expect("Relayer worker thread");
    proxy_delay_task.join().expect("Proxy delay thread");

    info!("Exiting inertia-relayer");
}
