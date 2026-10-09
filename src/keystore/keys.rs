use super::types::StoredKey;

use ever_block::{base64_decode, base64_encode, Ed25519KeyOption, KeyId, KeyOption};
use std::sync::Arc;

pub(super) fn generate_key() -> (StoredKey, Arc<dyn KeyOption>) {
    let secret = rand::random::<[u8; 32]>();
    let stored = StoredKey {
        private_key: base64_encode(secret),
    };

    (stored, private_key(&secret))
}

pub(super) fn decode_key(stored: &StoredKey) -> Result<Arc<dyn KeyOption>, String> {
    let data = base64_decode(&stored.private_key).map_err(|e| format!("invalid base64: {e}"))?;
    let secret: &[u8; 32] = data
        .as_slice()
        .try_into()
        .map_err(|_| format!("{} bytes instead of 32", data.len()))?;

    Ok(private_key(secret))
}

pub(super) fn encode_id(id: &KeyId) -> String {
    base64_encode(id.data())
}

fn private_key(secret: &[u8; 32]) -> Arc<dyn KeyOption> {
    Ed25519KeyOption::from_private_key(secret).expect("any 32 bytes are an Ed25519 private key")
}
