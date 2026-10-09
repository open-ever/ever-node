use ever_block::{KeyId, KeyOption};
use std::{fmt, sync::Arc};

pub const KEYSTORE_FILE_NAME: &str = "keystore.json";
pub(super) const KEYSTORE_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeystoreFile {
    /// Version of the file format
    pub(super) version: u32,

    /// The node's DHT key
    pub(super) dht: StoredKey,

    /// The node's key in public overlays
    pub(super) public_overlay: StoredKey,

    /// The control server key
    pub(super) control_server: StoredKey,

    /// The lite server key
    pub(super) lite_server: StoredKey,

    /// The node's validator ADNL keys
    pub(super) validator_adnl: Vec<StoredKey>,

    /// Elections, sorted by election id
    pub(super) elections: Vec<StoredElection>,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredElection {
    /// Election id, the start of the validation round, unixtime
    pub(super) election_id: u32,

    /// Validator signing key
    pub(super) key: StoredKey,

    /// ADNL address, the id of one of the node's validator ADNL keys, base64
    pub(super) adnl: String,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredKey {
    /// Ed25519 private key, base64
    pub(super) private_key: String,
}

/// Keys of an election the node takes part in.
#[derive(Clone)]
pub struct Election {
    /// Election id, the start of the validation round, unixtime
    pub election_id: u32,

    /// Validator signing key
    pub key: Arc<dyn KeyOption>,

    /// Validator ADNL address
    pub adnl: Arc<KeyId>,
}

impl fmt::Debug for Election {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Election")
            .field("election_id", &self.election_id)
            .field("key", self.key.id())
            .field("adnl", &self.adnl)
            .finish()
    }
}
