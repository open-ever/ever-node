use super::proof::{make_proof, write_boc};
use crate::{shard_state::ShardStateStuff, validating_utils::supported_version};

use ever_block::{
    read_single_root_boc, Account, Cell, Deserializable, ExceptionCode, HashmapType, Result,
    Serializable, SliceData, UsageTree
};

use ever_vm::{
    error::{tvm_exception_code, tvm_exception_or_custom_code},
    executor::{gas::gas_state::Gas, Engine},
    stack::{integer::IntegerData, savelist::SaveList, Stack, StackItem},
    SmartContractInfo
};

pub const MODE_STATE_PROOF: i32 = 2;
pub const MODE_RESULT: i32 = 4;
pub const MODE_INIT_C7: i32 = 8;
pub const MODE_FULL_C7: i32 = 32;

const GAS_LIMIT: i64 = 300_000;
const EXIT_CODE_NO_ACCOUNT: i32 = -0x100;

#[derive(Default)]
pub struct MethodResult {
    pub exit_code: i32,
    pub state_proof: Vec<u8>,
    pub init_c7: Vec<u8>,
    pub result: Vec<u8>,
}

/// Runs a get-method the same way TON liteserver does (see LiteQuery::finish_runSmcMethod)
pub fn run_get_method(
    account_root: Option<Cell>,
    stack: Stack,
    mode: i32,
    shard_state: &ShardStateStuff,
    mc_state: &ShardStateStuff,
) -> Result<MethodResult> {
    let Some(account_root) = account_root else {
        return Ok(MethodResult { exit_code: EXIT_CODE_NO_ACCOUNT, ..Default::default() })
    };
    let usage_tree = UsageTree::with_root(account_root.clone());
    let account = Account::construct_from_cell(usage_tree.root_cell())?;
    // Code is present only in active accounts
    let (Some(code), Some(address), Some(balance)) =
        (account.get_code(), account.get_addr(), account.balance())
    else {
        let state_proof = state_proof(mode, &account_root, usage_tree)?;
        return Ok(MethodResult { exit_code: EXIT_CODE_NO_ACCOUNT, state_proof, ..Default::default() })
    };

    let config = mc_state.config_params()?;
    let state = shard_state.state()?;

    let smc_info = SmartContractInfo {
        capabilities: config.capabilities(),
        myself: SliceData::load_builder(address.write_to_new_cell()?)?,
        unix_time: state.gen_time(),
        block_lt: state.gen_lt(),
        trans_lt: state.gen_lt(),
        seq_no: state.seq_no(),
        rand_seed: IntegerData::from_unsigned_bytes_be(rand::random::<[u8; 32]>()),
        balance: balance.clone(),
        config_params: config.config_params.data().cloned(),
        mycode: code.clone(),
        init_code_hash: account.init_code_hash().cloned().unwrap_or_default(),
        ..Default::default()
    };

    let init_c7 = if mode & MODE_INIT_C7 != 0 {
        let mut c7_info = smc_info.clone();
        if mode & MODE_FULL_C7 == 0 {
            c7_info.config_params = None;
        }

        write_boc(vec![serialize_item(&c7_info.into_temp_data_item())?])?
    } else {
        Vec::new()
    };

    let mut ctrls = SaveList::new();
    ctrls.put(4, &mut StackItem::Cell(account.get_data().unwrap_or_default()))?;
    ctrls.put(7, &mut smc_info.into_temp_data_item())?;

    let gas_price = config.gas_prices(address.is_masterchain())?.get_real_gas_price() as i64;
    let libraries = vec![
        account.libraries().inner(),
        mc_state.state()?.libraries().clone().inner()
    ];

    let mut vm = Engine::with_capabilities(config.capabilities()).setup_with_libraries(
        SliceData::load_cell(code)?,
        Some(ctrls),
        Some(stack),
        Some(Gas::new(GAS_LIMIT, 0, GAS_LIMIT, gas_price)),
        libraries
    );

    vm.set_block_version(supported_version());

    #[cfg(feature = "signature_with_id")]
    vm.set_signature_id(mc_state.state()?.global_id());

    let exit_code = match vm.execute() {
        Ok(exit_code) => exit_code,
        Err(err) => match tvm_exception_code(&err) {
            Some(ExceptionCode::OutOfGas) => !(ExceptionCode::OutOfGas as i32),
            _ => tvm_exception_or_custom_code(&err)
        }
    };

    // The result is serialized before the proof to get the cells it refers to into the proof
    let result = serialize_item(&StackItem::tuple(vm.withdraw_stack().storage))?;
    let result = if mode & MODE_RESULT != 0 { write_boc(vec![result])? } else { Vec::new() };
    let state_proof = state_proof(mode, &account_root, usage_tree)?;

    Ok(MethodResult { exit_code, state_proof, init_c7, result })
}

/// Stack is passed as a tuple in ever_vm format (StackItem::serialize_old)
pub fn deserialize_stack(data: &[u8]) -> Result<Stack> {
    let mut slice = SliceData::load_cell(read_single_root_boc(data)?)?;
    let (item, _) = StackItem::deserialize_old(&mut slice)?;

    Ok(Stack::with_storage(item.as_tuple()?.to_vec()))
}

pub fn serialize_item(item: &StackItem) -> Result<Cell> {
    item.serialize_old()?.0.into_cell()
}

fn state_proof(mode: i32, account_root: &Cell, usage_tree: UsageTree) -> Result<Vec<u8>> {
    if mode & MODE_STATE_PROOF != 0 {
        write_boc(vec![make_proof(account_root, usage_tree)?])
    } else {
        Ok(Vec::new())
    }
}
