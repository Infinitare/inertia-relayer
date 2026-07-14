mod tpu;
mod staked_nodes_updater_service;
mod forwarder;
mod auth;

use crate::relayer::forwarder::Forwarder;
use crate::relayer::tpu::Tpu;
use crate::rpc::Rpc;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use jwt::PKeyWithDigest;
use log::info;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::collections::HashMap;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub struct Relayer {
    tpu: Tpu,
    forwarder: Forwarder,
}

impl Relayer {
    pub fn new(
        keypair: &Arc<Keypair>,
        rpc: &Rpc,
        public_ip: IpAddr,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        tpu_quic_port: u16,
        tpu_quic_fwd_port: u16,
        grpc_bind_ip: IpAddr,
        relayer_bind_port: u16,
        signing_key_pem_path: PathBuf,
        verifying_key_pem_path: PathBuf,
        rt: &Handle,
        exit: &Arc<AtomicBool>,
    ) -> (Self, JoinHandle<()>, std::thread::JoinHandle<()>) {
        let (private_key, public_key) =
            load_or_generate_keys(&signing_key_pem_path, &verifying_key_pem_path);

        let signing_key = PKeyWithDigest {
            digest: MessageDigest::sha256(),
            key: PKey::private_key_from_pem(&private_key).unwrap(),
        };

        let verifying_key = Arc::new(PKeyWithDigest {
            digest: MessageDigest::sha256(),
            key: PKey::public_key_from_pem(&public_key).unwrap(),
        });

        let (tpu, tpu_task) = Tpu::new(
            keypair,
            rpc,
            staked_nodes_overrides,
            tpu_quic_port,
            tpu_quic_fwd_port,
            exit
        );

        let (forwarder, grpc_join, event_task) = Forwarder::new(
            &public_ip,
            tpu_quic_port,
            tpu_quic_fwd_port,
            grpc_bind_ip,
            relayer_bind_port,
            signing_key,
            verifying_key,
            rt,
            exit
        );

        let relayer = Relayer {
            tpu,
            forwarder,
        };

        let task = std::thread::Builder::new()
            .name("relayer".to_string())
            .spawn(move || {
                tpu_task.join().expect("Tpu task join thread");
                event_task.join().expect("Event task join thread");
            })
            .expect("Spawn tpu thread");

        (relayer, grpc_join, task)
    }

    pub fn tpu_receiver(&self) -> crossbeam_channel::Receiver<BankingPacketBatch> {
        self.tpu.receiver()
    }

    pub fn forwarder_sender(&self) -> crossbeam_channel::Sender<BankingPacketBatch> {
        self.forwarder.sender()
    }
}

fn load_or_generate_keys(
    signing_key_pem_path: &Path,
    verifying_key_pem_path: &Path,
) -> (Vec<u8>, Vec<u8>) {
    if !signing_key_pem_path.exists() {
        ensure_parent_dir(signing_key_pem_path);
        let rsa = Rsa::generate(2048).expect("Failed to generate RSA signing key");
        let pem = rsa
            .private_key_to_pem()
            .expect("Failed to serialize signing key to PEM");
        fs::write(signing_key_pem_path, &pem).unwrap_or_else(|e| {
            panic!("Failed to write signing key {signing_key_pem_path:?}: {e}")
        });
        restrict_permissions(signing_key_pem_path);
        info!("Generated new JWT signing key at {signing_key_pem_path:?}");
    }

    let private_key = fs::read(signing_key_pem_path)
        .unwrap_or_else(|_| panic!("Failed to read signing key file: {signing_key_pem_path:?}"));

    if !verifying_key_pem_path.exists() {
        ensure_parent_dir(verifying_key_pem_path);
        let pkey = PKey::private_key_from_pem(&private_key)
            .expect("Failed to parse signing key when deriving public key");
        let pem = pkey
            .public_key_to_pem()
            .expect("Failed to serialize verifying key to PEM");
        fs::write(verifying_key_pem_path, &pem).unwrap_or_else(|e| {
            panic!("Failed to write verifying key {verifying_key_pem_path:?}: {e}")
        });
        info!("Generated new JWT verifying key at {verifying_key_pem_path:?}");
    }

    let public_key = fs::read(verifying_key_pem_path)
        .unwrap_or_else(|_| panic!("Failed to read verifying key file: {verifying_key_pem_path:?}"));

    (private_key, public_key)
}

fn ensure_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("Failed to create key directory {parent:?}: {e}"));
        }
    }
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|e| panic!("Failed to set permissions on {path:?}: {e}"));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) {}
