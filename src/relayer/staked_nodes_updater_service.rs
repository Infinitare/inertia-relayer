use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::thread::{sleep, Builder};
use std::time::{Duration, Instant};
use log::warn;
use solana_rpc_client::api::client_error;
use solana_sdk::pubkey::Pubkey;
use solana_streamer::streamer::StakedNodes;
use crate::rpc::Rpc;

pub struct StakedNodesUpdaterService;

impl StakedNodesUpdaterService {
    pub const PK_TO_STAKE_REFRESH_DURATION: Duration = Duration::from_secs(5);
    
    pub fn new(
        rpc: &Rpc,
        shared_staked_nodes: Arc<RwLock<StakedNodes>>,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        exit: &Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let rpc = rpc.clone();
        let exit = exit.clone();
        Builder::new()
            .name("staked-nodes-updater".to_string())
            .spawn(move || {
                let mut last_stakes = Instant::now();
                while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                    if last_stakes.elapsed().as_secs() >= 60 {
                        let mut stake_map = Arc::new(HashMap::new());
                        match Self::try_refresh_pk_to_stake(
                            &mut last_stakes,
                            &mut stake_map,
                            &rpc,
                        ) {
                            Ok(true) => {
                                let shared = StakedNodes::new(stake_map, staked_nodes_overrides.clone());
                                *shared_staked_nodes.write().unwrap() = shared;
                            }
                            Ok(false) => {}
                            Err(err) => {
                                warn!("Failed to refresh pk to stake map! Error: {:?}", err);
                            }
                        }
                    }
                    sleep(Duration::from_secs(1));
                }
            })
            .expect("spawn staked-nodes-updater thread")
    }

    fn try_refresh_pk_to_stake(
        last_stakes: &mut Instant,
        pubkey_stake_map: &mut Arc<HashMap<Pubkey, u64>>,
        rpc_load_balancer: &Rpc,
    ) -> client_error::Result<bool> {
        if last_stakes.elapsed() > Self::PK_TO_STAKE_REFRESH_DURATION {
            let client = rpc_load_balancer.rpc_client();
            let vote_accounts = client.get_vote_accounts()?;

            *pubkey_stake_map = Arc::new(
                vote_accounts
                    .current
                    .iter()
                    .chain(vote_accounts.delinquent.iter())
                    .filter_map(|vote_account| {
                        Some((
                            Pubkey::from_str(&vote_account.node_pubkey).ok()?,
                            vote_account.activated_stake,
                        ))
                    })
                    .collect(),
            );

            *last_stakes = Instant::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
