use ever_block::{
    error, fail, AccountIdPrefixFull, Block, BlockIdExt, BocWriter, Cell, Deserializable,
    HashmapAugType, MerkleProof, Result, Serializable, ShardStateUnsplit, UInt256, UsageTree
};

/// Proves the state root hash through the block header
pub fn state_root_proof(block_root: &Cell, state_root_hash: &UInt256) -> Result<Cell> {
    let usage_tree = UsageTree::with_root(block_root.clone());
    let block = Block::construct_from_cell(usage_tree.root_cell())?;

    block.read_info()?.read_prev_ref()?;

    if block.read_state_update()?.new_hash != *state_root_hash {
        fail!("state hash mismatch with the block {:x}", block_root.repr_hash())
    }

    make_proof(block_root, usage_tree)
}

/// Proves the top shard block which contains the account in a masterchain state
pub fn shard_info_proof(
    mc_state_root: &Cell,
    account: &AccountIdPrefixFull
) -> Result<(Cell, Option<BlockIdExt>)> {
    let usage_tree = UsageTree::with_root(mc_state_root.clone());
    let state = ShardStateUnsplit::construct_from_cell(usage_tree.root_cell())?;

    let shard = state
        .read_custom()?
        .ok_or_else(|| error!("masterchain state has no extra data"))?
        .shards()
        .find_shard_by_prefix(account)?;

    Ok((make_proof(mc_state_root, usage_tree)?, shard.map(|shard| shard.block_id)))
}

/// Proves the account (or its absence) in a shard state
pub fn account_proof(state_root: &Cell, account: &UInt256) -> Result<Cell> {
    let usage_tree = UsageTree::with_root(state_root.clone());
    let state = ShardStateUnsplit::construct_from_cell(usage_tree.root_cell())?;

    state.read_accounts()?.account(&account.clone().into())?;

    make_proof(state_root, usage_tree)
}

/// Proves the transaction (or its absence) in a block,
/// returns the proof and the transaction
pub fn transaction_proof(
    block_root: &Cell,
    account: &UInt256,
    lt: u64
) -> Result<(Cell, Option<Cell>)> {
    let usage_tree = UsageTree::with_root(block_root.clone());
    let block = Block::construct_from_cell(usage_tree.root_cell())?;
    let transaction = find_transaction(&block, account, lt)?;

    Ok((make_proof(block_root, usage_tree)?, transaction))
}

pub fn find_transaction(block: &Block, account: &UInt256, lt: u64) -> Result<Option<Cell>> {
    match block.read_extra()?.read_account_blocks()?.get(account)? {
        Some(account_block) => account_block.transactions().get_as_cell(&lt),
        None => Ok(None)
    }
}

pub fn make_proof(root: &Cell, usage_tree: UsageTree) -> Result<Cell> {
    MerkleProof::create_by_usage_tree(root, usage_tree)?.serialize()
}

pub fn write_boc(roots: Vec<Cell>) -> Result<Vec<u8>> {
    let mut data = Vec::new();

    if !roots.is_empty() {
        BocWriter::with_roots(roots)?.write(&mut data)?;
    }

    Ok(data)
}
