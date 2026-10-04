use crate::{db_impl_base, traits::Serializable};
use ever_block::{AccountIdPrefixFull, BlockIdExt, Result, ShardIdent, MAX_SPLIT_DEPTH};

// #[cfg(test)]
// #[path = "tests/test_lt_index_db.rs"]
// mod tests;

// Applied blocks indexed by (workchain, shard, start_lt)
db_impl_base!(LtIndexDb, Vec<u8>);

impl LtIndexDb {

    const SHARD_KEY_LEN: usize = 12;

    pub fn put_block(&self, id: &BlockIdExt, start_lt: u64) -> Result<()> {
        self.put(&Self::key(id.shard(), start_lt), &id.to_vec()?)
    }

    pub fn delete_block(&self, id: &BlockIdExt, start_lt: u64) -> Result<()> {
        self.delete(&Self::key(id.shard(), start_lt))
    }

    /// Returns the block of the account's shard chain with the greatest start_lt <= `lt`
    pub fn find_block(&self, account: &AccountIdPrefixFull, lt: u64) -> Result<Option<BlockIdExt>> {
        // The shard containing the account changes with splits and merges, so check every depth
        let mut found: Option<(u64, BlockIdExt)> = None;

        for len in 0..=MAX_SPLIT_DEPTH {
            let shard = ShardIdent::with_prefix_len(len, account.workchain_id, account.prefix)?;
            if let Some((start_lt, id)) = self.find_shard_block(&shard, lt)? {
                if found.as_ref().is_none_or(|(found_lt, _)| start_lt > *found_lt) {
                    found = Some((start_lt, id));
                }
            }
        }

        Ok(found.map(|(_, id)| id))
    }

    /// Deletes the oldest entries of every shard while `is_unneeded` returns true for them,
    /// returns the number of deleted entries
    pub fn gc(&self, mut is_unneeded: impl FnMut(&BlockIdExt) -> Result<bool>) -> Result<usize> {
        let mut deleted = 0;
        let mut from = Vec::new();

        while let Some((key, value)) = self.find_first_ge(&from)? {
            if is_unneeded(&BlockIdExt::from_slice(&value)?)? {
                self.delete_raw(&key)?;
                deleted += 1;
                from = key.into_vec();
            } else {
                match Self::next_shard_key(&key) {
                    Some(next) => from = next,
                    None => break
                }
            }
        }

        Ok(deleted)
    }

    // The smallest key of the next shard
    fn next_shard_key(key: &[u8]) -> Option<Vec<u8>> {
        let mut next = key[..Self::SHARD_KEY_LEN].to_vec();

        for byte in next.iter_mut().rev() {
            let (value, overflow) = byte.overflowing_add(1);
            *byte = value;
            if !overflow {
                return Some(next)
            }
        }

        None
    }

    fn find_shard_block(&self, shard: &ShardIdent, lt: u64) -> Result<Option<(u64, BlockIdExt)>> {
        let key = Self::key(shard, lt);

        let Some((found, value)) = self.find_last_le(&key)? else {
            return Ok(None)
        };

        if found.len() != key.len() || found[..Self::SHARD_KEY_LEN] != key[..Self::SHARD_KEY_LEN] {
            return Ok(None)
        }

        let start_lt = u64::from_be_bytes(found[Self::SHARD_KEY_LEN..].try_into()?);

        Ok(Some((start_lt, BlockIdExt::from_slice(&value)?)))
    }

    fn key(shard: &ShardIdent, lt: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::SHARD_KEY_LEN + 8);

        key.extend_from_slice(&shard.workchain_id().to_be_bytes());
        key.extend_from_slice(&shard.shard_prefix_with_tag().to_be_bytes());
        key.extend_from_slice(&lt.to_be_bytes());
        key
    }
}
