use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::{sleep, Builder};
use std::time::Duration;
use crossbeam_channel::RecvTimeoutError;
use log::{error, info};
use solana_pubsub_client::pubsub_client::PubsubClient;
use solana_rpc_client::api::config::CommitmentConfig;
use solana_rpc_client::rpc_client::RpcClient;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Rpc {
    client: Arc<RpcClient>,
    slot_sender: broadcast::Sender<u64>,
}

impl Rpc {
    pub fn new(
        rpc_server: String,
        websocket_server: String,
        exit: &Arc<AtomicBool>,
    ) -> (Self, std::thread::JoinHandle<()>) {
        let (slot_sender, _) = broadcast::channel(128);
        let commitment_config = CommitmentConfig::processed();
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_server,
            commitment_config,
        ));

        let rpc = Self {
            client: rpc_client,
            slot_sender,
        };

        let task = rpc.start_slot_sender(websocket_server, exit);
        (rpc, task)
    }

    fn start_slot_sender(
        &self,
        websocket_server: String,
        exit: &Arc<AtomicBool>
    ) -> std::thread::JoinHandle<()> {
        let exit = exit.clone();
        let self_clone = self.clone();
        Builder::new().name("slot-subscribe".to_string()).spawn(move || {
            while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                match PubsubClient::slot_subscribe(&websocket_server) {
                    Ok((_, receiver)) => {
                        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                            match receiver.recv_timeout(Duration::from_millis(1000)) {
                                Ok(slot) => {
                                    _ = self_clone.slot_sender.send(slot.slot);
                                }
                                Err(RecvTimeoutError::Timeout) => {
                                    error!("Timeout waiting for slot subscription");
                                    break;
                                }
                                Err(RecvTimeoutError::Disconnected) => {
                                    info!("Slot subscribe disconnected. url");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to subscribe to {} error: {}", websocket_server, e);
                    }
                }

                if !exit.load(std::sync::atomic::Ordering::Relaxed) {
                    sleep(Duration::from_secs(1));
                }
            }
        }).expect("spawn slot-subscribe thread")
    }

    pub fn rpc_client(&self) -> &Arc<RpcClient> {
        &self.client
    }
}
