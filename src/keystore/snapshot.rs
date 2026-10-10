use super::{
    keys::{encode_id, encode_public_key, private_key},
    types::{Election, KeystoreFile, StoredElection, StoredKey, KEYSTORE_VERSION},
};

use anyhow::{bail, format_err, Result};
use ever_block::{base64_decode, KeyId, KeyOption};
use std::sync::Arc;

pub struct Snapshot {
    pub(super) file: KeystoreFile,
    pub dht_key: Arc<dyn KeyOption>,
    pub public_overlay_key: Arc<dyn KeyOption>,
    pub control_server_key: Arc<dyn KeyOption>,
    pub lite_server_key: Arc<dyn KeyOption>,
    pub validator_adnl_keys: Vec<Arc<dyn KeyOption>>,
    pub elections: Vec<Election>,
}

impl Snapshot {
    pub(super) fn build(mut file: KeystoreFile) -> Result<Self> {
        if file.version != KEYSTORE_VERSION {
            bail!("version {} is not supported", file.version);
        }

        let dht_key = Self::load_key(&mut file.dht, "dht")?;
        let public_overlay_key = Self::load_key(&mut file.public_overlay, "public_overlay")?;
        let control_server_key = Self::load_key(&mut file.control_server, "control_server")?;
        let lite_server_key = Self::load_key(&mut file.lite_server, "lite_server")?;

        let mut validator_adnl_keys = Vec::new();

        for (n, stored) in file.validator_adnl.iter_mut().enumerate() {
            validator_adnl_keys.push(Self::load_key(stored, &format!("validator_adnl[{n}]"))?);
        }

        file.elections.sort_by_key(|stored| stored.election_id);
        let mut elections: Vec<Election> = Vec::new();

        for stored in &mut file.elections {
            let id = stored.election_id;

            if elections.iter().any(|other| other.election_id == id) {
                bail!("election_id {id} is stored more than once");
            }

            elections.push(Self::load_election(stored, &validator_adnl_keys)?);
        }

        Ok(Self {
            file,
            dht_key,
            public_overlay_key,
            control_server_key,
            lite_server_key,
            validator_adnl_keys,
            elections,
        })
    }

    pub fn election(&self, election_id: u32) -> Option<&Election> {
        self.elections
            .iter()
            .find(|election| election.election_id == election_id)
    }

    pub fn election_by_key(&self, key: &KeyId) -> Option<&Election> {
        self.elections
            .iter()
            .find(|election| election.key.id().as_ref() == key)
    }

    /// Decodes the private key and rewrites the public key from it
    fn load_key(stored: &mut StoredKey, field: &str) -> Result<Arc<dyn KeyOption>> {
        let data = base64_decode(&stored.private_key)
            .map_err(|e| format_err!("{field} private key: invalid base64: {e}"))?;

        let secret: &[u8; 32] = data
            .as_slice()
            .try_into()
            .map_err(|_| format_err!("{field} private key: {} bytes instead of 32", data.len()))?;

        let key = private_key(secret);
        stored.public_key = encode_public_key(&key);

        Ok(key)
    }

    fn load_election(
        stored: &mut StoredElection,
        adnl_keys: &[Arc<dyn KeyOption>],
    ) -> Result<Election> {
        let id = stored.election_id;
        let adnl_id = &stored.adnl;

        let key = Self::load_key(&mut stored.key, &format!("election {id}"))?;

        let Some(adnl_key) = adnl_keys.iter().find(|k| encode_id(k.id()) == *adnl_id) else {
            bail!("election {id} adnl {adnl_id} matches no validator_adnl key");
        };

        Ok(Election {
            election_id: id,
            key,
            adnl: adnl_key.id().clone(),
        })
    }
}
