use super::{
    keys::generate_key,
    snapshot::Snapshot,
    transaction::Transaction,
    types::{KeystoreFile, KEYSTORE_VERSION},
};
use crate::utils::atomic_write::write_file_atomic;

use anyhow::{bail, format_err, Result};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Keystore {
    path: PathBuf,
    snapshot: RwLock<Arc<Snapshot>>,
}

impl Keystore {
    pub fn open_or_create(path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let path = path.into();

        let file = match read_file(&path)? {
            Some(file) => file,
            None => create_file(&path)?,
        };

        let snapshot = Snapshot::build(file)
            .map_err(|reason| format_err!("keystore {}: {reason}", path.display()))?;

        Ok(Arc::new(Self {
            path,
            snapshot: RwLock::new(Arc::new(snapshot)),
        }))
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.read().clone()
    }

    pub fn update<T, E: From<anyhow::Error>>(
        &self,
        change: impl FnOnce(&mut Transaction) -> Result<T, E>,
    ) -> Result<T, E> {
        let current = self.snapshot.upgradable_read();

        let mut transaction = Transaction {
            snapshot: &current,
            file: current.file.clone(),
        };

        let value = change(&mut transaction)?;
        if transaction.file == current.file {
            return Ok(value);
        }

        let next = Snapshot::build(transaction.file)
            .map_err(|reason| format_err!("keystore {}: {reason}", self.path.display()))?;

        write_file(&self.path, &next.file)?;
        log_changes(&current, &next);
        *RwLockUpgradableReadGuard::upgrade(current) = Arc::new(next);

        Ok(value)
    }
}

fn read_file(path: &Path) -> Result<Option<KeystoreFile>> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => bail!("keystore {}: cannot read: {e}", path.display()),
    };

    serde_json::from_slice(&data)
        .map(Some)
        .map_err(|e| format_err!("keystore {}: invalid JSON: {e}", path.display()))
}

fn create_file(path: &Path) -> Result<KeystoreFile> {
    let file = KeystoreFile {
        version: KEYSTORE_VERSION,
        dht: generate_key().0,
        public_overlay: generate_key().0,
        control_server: generate_key().0,
        lite_server: generate_key().0,
        validator_adnl: Vec::new(),
        elections: Vec::new(),
    };

    write_file(path, &file)?;
    log::info!("Keystore: created {}", path.display());

    Ok(file)
}

fn write_file(path: &Path, file: &KeystoreFile) -> Result<()> {
    let mut data = serde_json::to_vec_pretty(file)?;
    data.push(b'\n');

    write_file_atomic(path, &data)
        .map_err(|e| format_err!("keystore {}: cannot write: {e}", path.display()))
}

fn log_changes(before: &Snapshot, after: &Snapshot) {
    let old_count = before.validator_adnl_keys.len();
    for key in after.validator_adnl_keys.iter().skip(old_count) {
        log::info!("Keystore: added validator ADNL key {}", key.id());
    }

    for election in &after.elections {
        if before.election(election.election_id).is_none() {
            log::info!(
                "Keystore: added election {}: signing key {}, ADNL address {}",
                election.election_id,
                election.key.id(),
                election.adnl
            );
        }
    }

    for election in &before.elections {
        if after.election(election.election_id).is_none() {
            log::info!("Keystore: removed election {}", election.election_id);
        }
    }
}
