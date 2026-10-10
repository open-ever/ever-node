//! The node's private keys, kept in `keystore.json` next to `node.config.json`: the DHT, public
//! overlay, control server and lite server keys, created with the file, and the validator keys.
//! Every key is stored with its public key for operators to read. The node never uses the stored
//! public keys and rewrites them from the private keys on every load.
//!
//! Readers work on an immutable `Snapshot`. Changes go through [`Keystore::update`], which
//! publishes a new snapshot only after the file has been committed to disk.

mod keys;
mod snapshot;
mod store;
mod transaction;
mod types;

pub use store::Keystore;
pub use transaction::Transaction;
pub use types::{Election, KEYSTORE_FILE_NAME};

#[cfg(test)]
#[path = "../tests/test_keystore.rs"]
mod tests;
