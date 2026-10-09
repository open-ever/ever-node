use super::*;
use ever_block::{Ed25519KeyOption, KeyOption, SigPubKey, UInt256, ValidatorDescr};
use std::{fs, path::PathBuf};

/// A fresh keystore per test.
fn open(name: &str) -> (Arc<Keystore>, PathBuf) {
    let dir = PathBuf::from(format!(
        "./target/election_keys_tests/{}_{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let path = dir.join(crate::keystore::KEYSTORE_FILE_NAME);
    (Keystore::open_or_create(&path).unwrap(), path)
}

fn now() -> u32 {
    now_duration().as_secs() as u32
}

const ROUND: u32 = 10_000;

/// A recent chain view: the current set runs for a round from `since`, and the sets use `members`,
/// given as (signing key, ADNL address).
fn chain(since: u32, members: &[(&Arc<dyn KeyOption>, &Arc<KeyId>)]) -> ChainView {
    ChainView {
        now: now(),
        current_since: since,
        current_until: since + ROUND,
        keys: members.iter().map(|(key, _)| key.id().clone()).collect(),
        adnl: members.iter().map(|(_, adnl)| (*adnl).clone()).collect(),
    }
}

fn election_ids(keystore: &Keystore) -> Vec<u32> {
    keystore
        .snapshot()
        .elections
        .iter()
        .map(|election| election.election_id)
        .collect()
}

#[test]
fn test_repeated_election_is_unchanged() {
    let (keystore, path) = open("repeated");
    let election = get_or_create(&keystore, 1000, None).unwrap();
    let before = fs::read(&path).unwrap();

    // Whatever the chain shows, the keys of an existing election are returned as they are
    let since = now() - 10_000;
    let again = get_or_create(&keystore, 1000, Some(&chain(since, &[]))).unwrap();
    assert_eq!(again.key.id(), election.key.id());
    assert_eq!(again.adnl, election.adnl);

    let again = get_or_create(&keystore, 1000, None).unwrap();
    assert_eq!(again.key.id(), election.key.id());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn test_adnl_keys_alternate() {
    let (keystore, _) = open("alternate");
    let since = now() - 10_000;
    let next = since + ROUND;
    let first = get_or_create(&keystore, next, Some(&chain(since, &[]))).unwrap();

    // The current set uses the first ADNL key, so the next election gets the other one...
    let view = chain(since, &[(&first.key, &first.adnl)]);
    let second = get_or_create(&keystore, next + 1000, Some(&view)).unwrap();
    assert_ne!(second.adnl, first.adnl);

    // ...and back
    let view = chain(since, &[(&second.key, &second.adnl)]);
    let third = get_or_create(&keystore, next + 2000, Some(&view)).unwrap();
    assert_eq!(third.adnl, first.adnl);

    // With both keys in the current and next sets there is no free one
    let busy = chain(
        since,
        &[(&first.key, &first.adnl), (&second.key, &second.adnl)],
    );
    assert!(matches!(
        get_or_create(&keystore, next + 3000, Some(&busy)),
        Err(ElectionKeysError::AdnlKeysBusy)
    ));
}

#[test]
fn test_refusals() {
    let (keystore, _) = open("refusals");
    assert!(matches!(
        get_or_create(&keystore, 0, None),
        Err(ElectionKeysError::InvalidElectionId)
    ));

    // Without any state keys are made only for zerostate validators: the keystore has no elections
    get_or_create(&keystore, 1000, None).unwrap();
    assert!(matches!(
        get_or_create(&keystore, 2000, None),
        Err(ElectionKeysError::NotSynced)
    ));

    // A state from before a downtime does not show which keys the chain uses now
    let since = now() - 10_000;
    let mut stale = chain(since, &[]);
    stale.now -= MAX_CHAIN_VIEW_AGE_SEC + 60;
    assert!(matches!(
        get_or_create(&keystore, since + ROUND, Some(&stale)),
        Err(ElectionKeysError::NotSynced)
    ));

    // The elector opens elections only for rounds after the current validator set
    let view = chain(since, &[]);
    assert!(matches!(
        get_or_create(&keystore, since + ROUND - 1, Some(&view)),
        Err(ElectionKeysError::ElectionFinished(_))
    ));
    get_or_create(&keystore, since + ROUND, Some(&view)).unwrap();
}

#[test]
fn test_elected_keys_are_kept() {
    let (keystore, _) = open("elected");
    let since = now() - 3 * ROUND;
    let old = get_or_create(&keystore, 1000, None).unwrap();

    // The validator set of election 1000 still runs (its start was postponed)...
    let view = chain(since, &[(&old.key, &old.adnl)]);
    get_or_create(&keystore, since + ROUND, Some(&view)).unwrap();
    assert_eq!(election_ids(&keystore), vec![1000, since + ROUND]);

    // ...and its keys go once no set uses them
    get_or_create(&keystore, since + 2 * ROUND, Some(&chain(since, &[]))).unwrap();
    assert_eq!(
        election_ids(&keystore),
        vec![since + ROUND, since + 2 * ROUND]
    );
}

#[test]
fn test_concurrent_bids_for_one_election() {
    let (keystore, _) = open("one_election");
    let since = now() - 10_000;
    let view = chain(since, &[]);

    // A manager retrying a bid gets the keys of the first one
    let elections: Vec<Election> = std::thread::scope(|scope| {
        let bids: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| get_or_create(&keystore, since + ROUND, Some(&view)).unwrap()))
            .collect();
        bids.into_iter().map(|bid| bid.join().unwrap()).collect()
    });

    for election in &elections {
        assert_eq!(election.key.id(), elections[0].key.id());
    }
}

#[test]
fn test_chain_view_from_validator_sets() {
    let (a, b) = (
        Ed25519KeyOption::generate().unwrap(),
        Ed25519KeyOption::generate().unwrap(),
    );
    let descr = |key: &Arc<dyn KeyOption>, adnl: u8| {
        let key = SigPubKey::from_bytes(key.pub_key().unwrap()).unwrap();
        ValidatorDescr::with_params(key, 1, Some(UInt256::from([adnl; 32])), None)
    };

    let current = ValidatorSet::new(5000, 6000, 1, vec![descr(&a, 1)]).unwrap();
    let next = ValidatorSet::new(6000, 7000, 1, vec![descr(&b, 2)]).unwrap();
    let view = ChainView::new(5500, &current, &next);

    assert_eq!((view.current_since, view.current_until), (5000, 6000));
    assert_eq!(view.keys, HashSet::from([a.id().clone(), b.id().clone()]));
    assert_eq!(
        view.adnl,
        HashSet::from([KeyId::from_data([1; 32]), KeyId::from_data([2; 32])])
    );
}
