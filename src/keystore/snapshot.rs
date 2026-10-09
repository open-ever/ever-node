use super::{
    keys::{decode_key, encode_id},
    types::{Election, KeystoreFile, StoredElection, KEYSTORE_VERSION},
};

use ever_block::{KeyId, KeyOption};
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
    pub(super) fn build(mut file: KeystoreFile) -> Result<Self, String> {
        if file.version != KEYSTORE_VERSION {
            return Err(format!("version {} is not supported", file.version));
        }

        let dht_key = decode_key(&file.dht).map_err(|e| format!("DHT key: {e}"))?;
        let public_overlay_key =
            decode_key(&file.public_overlay).map_err(|e| format!("public overlay key: {e}"))?;
        let control_server_key =
            decode_key(&file.control_server).map_err(|e| format!("control server key: {e}"))?;
        let lite_server_key =
            decode_key(&file.lite_server).map_err(|e| format!("lite server key: {e}"))?;

        let mut validator_adnl_keys = Vec::new();

        for (n, stored) in file.validator_adnl.iter().enumerate() {
            let key = decode_key(stored).map_err(|e| format!("validator ADNL key #{n}: {e}"))?;
            validator_adnl_keys.push(key);
        }

        file.elections.sort_by_key(|stored| stored.election_id);
        let mut elections: Vec<Election> = Vec::new();

        for stored in &file.elections {
            let id = stored.election_id;
            if elections.iter().any(|other| other.election_id == id) {
                return Err(format!("election {id} is stored more than once"));
            }

            elections.push(decode_election(stored, &validator_adnl_keys)?);
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
}

fn decode_election(
    stored: &StoredElection,
    adnl_keys: &[Arc<dyn KeyOption>],
) -> Result<Election, String> {
    let id = stored.election_id;
    let adnl_id = &stored.adnl;

    let key = decode_key(&stored.key).map_err(|e| format!("signing key of election {id}: {e}"))?;

    let Some(adnl_key) = adnl_keys.iter().find(|k| encode_id(k.id()) == *adnl_id) else {
        return Err(format!("ADNL key {adnl_id} of election {id} isn't valid"));
    };

    Ok(Election {
        election_id: id,
        key,
        adnl: adnl_key.id().clone(),
    })
}
