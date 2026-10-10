use super::*;
use crate::utils::atomic_write::tmp_path;
use anyhow::{bail, Result};
use ever_block::{base64_encode, Ed25519KeyOption, KeyId};
use std::{fs, path::PathBuf, sync::Arc};

fn test_path(name: &str) -> PathBuf {
    let dir = PathBuf::from(format!(
        "./target/keystore_tests/{name}_{}",
        std::process::id()
    ));

    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir.join(KEYSTORE_FILE_NAME)
}

fn open(name: &str) -> (Arc<Keystore>, PathBuf) {
    let path = test_path(name);
    (Keystore::open_or_create(&path).unwrap(), path)
}

/// Ids of the DHT, public overlay, control server and lite server keys.
fn node_key_ids(keystore: &Keystore) -> Vec<Arc<KeyId>> {
    let snapshot = keystore.snapshot();
    [
        &snapshot.dht_key,
        &snapshot.public_overlay_key,
        &snapshot.control_server_key,
        &snapshot.lite_server_key,
    ]
    .iter()
    .map(|key| key.id().clone())
    .collect()
}

/// Commits a change that can't fail by itself.
fn commit<T>(keystore: &Keystore, change: impl FnOnce(&mut Transaction) -> T) -> Result<T> {
    keystore.update(|transaction| Ok(change(transaction)))
}

fn add_election(keystore: &Keystore, election_id: u32) -> Election {
    commit(keystore, |transaction| {
        let adnl = transaction.add_validator_adnl_key();
        transaction.add_election(election_id, &adnl)
    })
    .unwrap()
}

#[test]
fn test_round_trip() {
    let (keystore, path) = open("round_trip");
    let election = add_election(&keystore, 1000);
    let node_keys = node_key_ids(&keystore);

    drop(keystore);

    let keystore = Keystore::open_or_create(&path).unwrap();
    assert_eq!(node_key_ids(&keystore), node_keys);

    let snapshot = keystore.snapshot();
    let loaded = snapshot.election(1000).unwrap();

    assert_eq!(loaded.key.id(), election.key.id());
    assert_eq!(loaded.adnl, election.adnl);
}

#[test]
fn test_failed_update_changes_nothing() {
    let (keystore, path) = open("failed_update");
    add_election(&keystore, 1000);
    let file = fs::read(&path).unwrap();
    let snapshot = keystore.snapshot();

    // A transaction that fails after a change
    keystore
        .update(|transaction| -> Result<()> {
            transaction.remove_election(1000);
            bail!("changed our mind")
        })
        .unwrap_err();

    // A change that would leave a keystore that doesn't load: an ADNL key it doesn't have
    let foreign = Ed25519KeyOption::generate().unwrap().id().clone();
    commit(&keystore, |transaction| {
        transaction.add_election(2000, &foreign)
    })
    .unwrap_err();

    // A file that can't be written: a directory is where the temporary file goes
    fs::create_dir(tmp_path(&path)).unwrap();
    commit(&keystore, |transaction| {
        transaction.add_validator_adnl_key()
    })
    .unwrap_err();

    assert_eq!(fs::read(&path).unwrap(), file);
    assert!(Arc::ptr_eq(&keystore.snapshot(), &snapshot));
}

#[test]
fn test_public_keys_are_rewritten() {
    let (keystore, path) = open("public_keys");
    add_election(&keystore, 1000);
    drop(keystore);

    // A missing public key, a broken one and one left from a replaced private key
    let mut json: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    json["dht"].as_object_mut().unwrap().remove("public_key");
    json["lite_server"]["public_key"] = "broken".into();
    json["control_server"]["private_key"] = base64_encode(rand::random::<[u8; 32]>()).into();
    fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();

    let keystore = Keystore::open_or_create(&path).unwrap();
    let snapshot = keystore.snapshot();
    let json: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

    let stored_keys = [
        (&json["dht"], &snapshot.dht_key),
        (&json["public_overlay"], &snapshot.public_overlay_key),
        (&json["control_server"], &snapshot.control_server_key),
        (&json["lite_server"], &snapshot.lite_server_key),
        (&json["validator_adnl"][0], &snapshot.validator_adnl_keys[0]),
        (&json["elections"][0]["key"], &snapshot.elections[0].key),
    ];

    for (stored, key) in stored_keys {
        assert_eq!(stored["public_key"], base64_encode(key.pub_key().unwrap()));
    }

    drop(keystore);
    fs::create_dir(tmp_path(&path)).unwrap();
    Keystore::open_or_create(&path).unwrap();
}

#[test]
fn test_corrupted_files_are_rejected() {
    let (_, path) = open("corrupted_source");
    let valid = fs::read(&path).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&valid).unwrap();

    let edited = |edit: fn(&mut serde_json::Value)| {
        let mut json = json.clone();
        edit(&mut json);
        serde_json::to_vec(&json).unwrap()
    };

    let cases = [
        ("truncated", valid[..valid.len() / 2].to_vec()),
        ("version", edited(|json| json["version"] = 2.into())),
        ("field", edited(|json| json["extra"] = true.into())),
    ];

    for (name, data) in cases {
        let path = test_path(name);
        fs::write(&path, &data).unwrap();

        assert!(Keystore::open_or_create(&path).is_err(), "{name}");
        assert_eq!(fs::read(&path).unwrap(), data, "{name} is left as it is");
    }
}

#[test]
fn test_secrets_are_not_printed() {
    let (keystore, _) = open("secrets");
    let election = add_election(&keystore, 1000);

    assert_eq!(
        format!("{election:?}"),
        format!(
            "Election {{ election_id: 1000, key: {:?}, adnl: {:?} }}",
            election.key.id(),
            election.adnl
        )
    );
}
