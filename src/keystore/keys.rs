use super::types::StoredKey;

use ever_block::{base64_encode, Ed25519KeyOption, KeyId, KeyOption};
use std::sync::Arc;

pub(super) fn generate_key() -> (StoredKey, Arc<dyn KeyOption>) {
    let secret = rand::random::<[u8; 32]>();
    let key = private_key(&secret);

    let stored = StoredKey {
        private_key: base64_encode(secret),
        public_key: encode_public_key(&key),
    };

    (stored, key)
}

pub(super) fn encode_id(id: &KeyId) -> String {
    base64_encode(id.data())
}

pub(super) fn private_key(secret: &[u8; 32]) -> Arc<dyn KeyOption> {
    Ed25519KeyOption::from_private_key(secret).expect("any 32 bytes are an Ed25519 private key")
}

pub(super) fn encode_public_key(key: &Arc<dyn KeyOption>) -> String {
    base64_encode(key.pub_key().expect("Ed25519 keys have a public key"))
}
