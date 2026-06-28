use solana_sdk::pubkey::Pubkey;
use crate::relayer::auth::service::ValidatorAuther;

pub mod service;
pub mod challenges;
pub mod interceptor;

#[derive(Default, Clone)]
pub struct ValidatorAutherImpl {}

impl ValidatorAuther for ValidatorAutherImpl {
    fn is_authorized(&self, _: &Pubkey) -> bool {
        true
    }
}