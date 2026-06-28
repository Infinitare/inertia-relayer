mod tpu;
mod staked_nodes_updater_service;
mod forwarder;
mod auth;

use crate::relayer::forwarder::Forwarder;
use crate::relayer::tpu::Tpu;
use crate::rpc::Rpc;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use jwt::PKeyWithDigest;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::collections::HashMap;
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
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
        let private_key = fs::read(&signing_key_pem_path).unwrap_or_else(|_| {
            panic!(
                "Failed to read signing key file: {:?}",
                &signing_key_pem_path
            )
        });

        let public_key = fs::read(&verifying_key_pem_path).unwrap_or_else(|_| {
            panic!(
                "Failed to read signing key file: {:?}",
                &verifying_key_pem_path
            )
        });

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
