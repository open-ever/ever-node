use super::{
    keys::{encode_id, generate_key},
    snapshot::Snapshot,
    types::{Election, KeystoreFile, StoredElection},
};

use ever_block::KeyId;
use std::sync::Arc;

pub struct Transaction<'a> {
    pub(super) snapshot: &'a Snapshot,
    pub(super) file: KeystoreFile,
}

impl<'a> Transaction<'a> {
    pub fn snapshot(&self) -> &'a Snapshot {
        self.snapshot
    }

    pub fn add_validator_adnl_key(&mut self) -> Arc<KeyId> {
        let (stored, key) = generate_key();
        self.file.validator_adnl.push(stored);

        key.id().clone()
    }

    pub fn add_election(&mut self, election_id: u32, adnl: &Arc<KeyId>) -> Election {
        let (stored, key) = generate_key();

        self.file.elections.push(StoredElection {
            election_id,
            key: stored,
            adnl: encode_id(adnl),
        });

        Election {
            election_id,
            key,
            adnl: adnl.clone(),
        }
    }

    pub fn remove_election(&mut self, election_id: u32) {
        self.file
            .elections
            .retain(|stored| stored.election_id != election_id);
    }
}
