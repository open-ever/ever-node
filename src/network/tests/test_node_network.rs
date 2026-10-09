use super::*;
#[cfg(feature = "telemetry")]
use crate::collator_test_bundle::create_engine_telemetry;
use crate::{
    collator_test_bundle::create_engine_allocated, test_helper::get_config,
    validator::election_keys,
};
use ever_block::Ed25519KeyOption;

fn fresh_keystore(name: &str) -> Arc<Keystore> {
    let path = format!(
        "./target/test_node_network_{}_{}.json",
        name,
        std::process::id()
    );
    std::fs::remove_file(&path).ok();
    Keystore::open_or_create(path).unwrap()
}

async fn start_network(ip: &str, keystore: Arc<Keystore>) -> Arc<NodeNetwork> {
    crate::test_helper::prepare_global_config();
    let config = get_config(ip, Some("./target")).await.unwrap();
    NodeNetwork::new(
        config,
        keystore,
        tokio_util::sync::CancellationToken::new(),
        #[cfg(feature = "telemetry")]
        create_engine_telemetry(),
        create_engine_allocated(),
    )
    .await
    .unwrap()
}

fn member(key: &Arc<dyn KeyOption>, adnl_id: &Arc<KeyId>) -> CatchainNode {
    CatchainNode {
        adnl_id: adnl_id.clone(),
        public_key: Ed25519KeyOption::from_public_key(key.pub_key().unwrap().try_into().unwrap()),
    }
}

fn foreign() -> CatchainNode {
    let key = Ed25519KeyOption::generate().unwrap();
    member(&key, key.id())
}

fn is_loaded(network: &NodeNetwork, key: &Arc<dyn KeyOption>) -> bool {
    network.network_context.adnl.key_by_id(key.id()).is_ok()
}

/// The ADNL key the network uses for a validator list it has set up.
fn list_adnl(network: &NodeNetwork, id: &UInt256) -> Arc<KeyId> {
    network
        .validator_context
        .sets_contexts
        .get(id)
        .unwrap()
        .val()
        .validator_adnl_key
        .id()
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_validator_list() {
    let keystore = fresh_keystore("lists");
    let election = election_keys::get_or_create(&keystore, 1000, None).unwrap();
    let network = start_network("127.0.0.1:4197", keystore.clone()).await;

    // Our member with the ADNL address of its election
    let id = UInt256::from([1; 32]);
    let list = vec![foreign(), member(&election.key, &election.adnl)];
    let local = network
        .set_validator_list(id.clone(), &list)
        .await
        .unwrap()
        .expect("our list");

    assert_eq!(local.id(), election.key.id());
    assert_eq!(list_adnl(&network, &id), election.adnl);

    // Our key with an ADNL address the node does not have: the list is not ours
    let list = vec![foreign(), member(&election.key, &KeyId::from_data([5; 32]))];

    assert!(network
        .set_validator_list(UInt256::from([2; 32]), &list)
        .await
        .unwrap()
        .is_none());

    // A zerostate list without ADNL addresses uses the signing key as the ADNL key
    let id = UInt256::from([3; 32]);
    let list = vec![foreign(), member(&election.key, election.key.id())];

    assert!(network
        .set_validator_list(id.clone(), &list)
        .await
        .unwrap()
        .is_some());

    assert_eq!(&list_adnl(&network, &id), election.key.id());
    assert!(is_loaded(&network, &election.key));

    network.cancellation_token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_adnl_keys_are_loaded() {
    let keystore = fresh_keystore("load");
    let network = start_network("127.0.0.1:4199", keystore.clone()).await;

    // The DHT and overlay keys come from the keystore
    let snapshot = keystore.snapshot();

    assert_eq!(
        &network.get_key_id_by_tag(NodeNetwork::TAG_DHT_KEY).unwrap(),
        snapshot.dht_key.id()
    );
    assert_eq!(
        &network
            .get_key_id_by_tag(NodeNetwork::TAG_OVERLAY_KEY)
            .unwrap(),
        snapshot.public_overlay_key.id()
    );

    // The first election creates the node's ADNL keys
    election_keys::get_or_create(&keystore, 1000, None).unwrap();
    let adnl = keystore.snapshot().validator_adnl_keys.clone();

    assert_eq!(adnl.len(), 2);
    assert!(adnl.iter().all(|key| !is_loaded(&network, key)));

    network.load_validator_adnl_keys().unwrap();
    network.load_validator_adnl_keys().unwrap();

    assert!(adnl.iter().all(|key| is_loaded(&network, key)));

    network.cancellation_token.cancel();
}
