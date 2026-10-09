use crate::{
    engine::now_duration,
    keystore::{Election, Keystore, Transaction},
    shard_state::ShardStateStuff,
};

use anyhow::Result;
use ever_block::{KeyId, ValidatorSet};
use std::{collections::HashSet, sync::Arc};

const ADNL_KEY_COUNT: usize = 2;
const MAX_CHAIN_VIEW_AGE_SEC: u32 = 600;

#[derive(Debug, thiserror::Error)]
pub enum ElectionKeysError {
    #[error("invalid election id 0")]
    InvalidElectionId,

    #[error("election {0} is over: the current validator set ends after it")]
    ElectionFinished(u32),

    #[error("no masterchain state from the last {} sec", MAX_CHAIN_VIEW_AGE_SEC)]
    NotSynced,

    #[error("both validator ADNL keys are used by the current and the next sets")]
    AdnlKeysBusy,

    #[error(transparent)]
    Keystore(#[from] anyhow::Error),
}

/// What the current (p34) and next (p36) validator sets of a masterchain state use.
pub struct ChainView {
    now: u32,
    current_since: u32,
    current_until: u32,
    keys: HashSet<Arc<KeyId>>,
    adnl: HashSet<Arc<KeyId>>,
}

impl ChainView {
    pub fn new(now: u32, current: &ValidatorSet, next: &ValidatorSet) -> Self {
        let members = || current.list().iter().chain(next.list());

        Self {
            now,
            current_since: current.utime_since(),
            current_until: current.utime_until(),
            keys: members()
                .map(|descr| descr.public_key.pub_key().id().clone())
                .collect(),
            adnl: members().map(|descr| descr.adnl_addr()).collect(),
        }
    }

    pub fn from_mc_state(state: &ShardStateStuff) -> Result<Self> {
        let config = state.config_params()?;

        Ok(Self::new(
            state.state()?.gen_time(),
            &config.validator_set()?,
            &config.next_validator_set()?,
        ))
    }
}

///
/// Keys of election `election_id` - created on first use, returned as they are afterwards.
///
/// Creating keys needs a recent `chain` and an election not before the end of the current set, as
/// the elector opens no other; without a chain, only a keystore with no elections gets keys
/// (zerostate validators). Keys of finished elections are dropped in the same transaction.
///
pub fn get_or_create(
    keystore: &Keystore,
    election_id: u32,
    chain: Option<&ChainView>,
) -> Result<Election, ElectionKeysError> {
    if election_id == 0 {
        return Err(ElectionKeysError::InvalidElectionId);
    }

    let now = now_duration().as_secs() as u32;
    if chain.is_some_and(|chain| now > chain.now.saturating_add(MAX_CHAIN_VIEW_AGE_SEC)) {
        return Err(ElectionKeysError::NotSynced);
    }

    keystore.update(|transaction| get_or_add(transaction, election_id, chain))
}

fn get_or_add(
    transaction: &mut Transaction,
    election_id: u32,
    chain: Option<&ChainView>,
) -> Result<Election, ElectionKeysError> {
    if let Some(election) = transaction.snapshot().election(election_id) {
        return Ok(election.clone());
    }

    match chain {
        Some(chain) if election_id < chain.current_until => {
            return Err(ElectionKeysError::ElectionFinished(election_id));
        }
        None if !transaction.snapshot().elections.is_empty() => {
            return Err(ElectionKeysError::NotSynced);
        }
        _ => {}
    }

    let adnl = free_adnl_key(transaction, chain)?;
    if let Some(chain) = chain {
        drop_finished(transaction, chain);
    }

    Ok(transaction.add_election(election_id, &adnl))
}

fn free_adnl_key(
    transaction: &mut Transaction,
    chain: Option<&ChainView>,
) -> Result<Arc<KeyId>, ElectionKeysError> {
    let mut adnl: Vec<Arc<KeyId>> = transaction
        .snapshot()
        .validator_adnl_keys
        .iter()
        .map(|key| key.id().clone())
        .collect();

    while adnl.len() < ADNL_KEY_COUNT {
        adnl.push(transaction.add_validator_adnl_key());
    }

    adnl.into_iter()
        .find(|id| chain.is_none_or(|chain| !chain.adnl.contains(id)))
        .ok_or(ElectionKeysError::AdnlKeysBusy)
}

fn drop_finished(transaction: &mut Transaction, chain: &ChainView) {
    for election in &transaction.snapshot().elections {
        if election.election_id < chain.current_since && !chain.keys.contains(election.key.id()) {
            transaction.remove_election(election.election_id);
        }
    }
}

#[cfg(test)]
#[path = "tests/test_election_keys.rs"]
mod tests;
