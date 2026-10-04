use super::{proof, run_method};
use crate::{
    block::BlockStuff, engine_traits::EngineOperations, shard_states_keeper::PinnedShardStateGuard,
};

use ever_block::{
    error, fail, read_single_root_boc, AccountIdPrefixFull, BlockIdExt, Cell, Deserializable,
    Result, Transaction, UInt256, MASTERCHAIN_ID,
};

use adnl::common::{AdnlPeers, QueryResult, Subscriber};
use ever_vm::stack::{Stack, StackItem};
use std::{sync::Arc, time::Duration};
use storage::block_handle_db::BlockHandle;

use ton_api::{
    deserialize_boxed,
    ton::{
        lite_server::{
            accountid::AccountId as LiteAccountId, accountstate::AccountState,
            error::Error as LiteServerError, runmethodresult::RunMethodResult,
            sendmsgstatus::SendMsgStatus, transactioninfo::TransactionInfo,
            transactionlist::TransactionList, AccountState as AccountStateBoxed,
            RunMethodResult as RunMethodResultBoxed, SendMsgStatus as SendMsgStatusBoxed,
            TransactionInfo as TransactionInfoBoxed, TransactionList as TransactionListBoxed,
        },
        rpc::lite_server::{
            GetAccountState, GetOneTransaction, GetTransactions, Query, RunSmcMethod, SendMessage,
        },
        TLObject,
    },
    IntoBoxed,
};

const QUERY_TIMEOUT: Duration = Duration::from_millis(4500);
const MAX_TRANSACTION_COUNT: u32 = 16;
const MAX_PARAMS_SIZE: usize = 65536;
const MODE_PROOFS: i32 = 1;
const MODE_LIB_EXTRAS: i32 = 16;
const SUPPORTED_MODES: i32 = 0x3f;

const ERROR_GENERIC: i32 = -400;
const ERROR_TIMEOUT: i32 = -503;
const ERROR_PROTOVIOLATION: i32 = 621;
const ERROR_NOT_READY: i32 = 651;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct LiteError {
    code: i32,
    message: String,
}

fn lite_error(code: i32, message: impl ToString) -> ever_block::Error {
    LiteError {
        code,
        message: message.to_string(),
    }
    .into()
}

struct AccountData {
    base_id: BlockIdExt,
    shard_id: BlockIdExt,
    shard_proof: Vec<u8>,
    proof: Vec<u8>,
    account: Option<Cell>,

    // State of `shard_id`, absent if there is no shard for the account
    state: Option<PinnedShardStateGuard>,

    // State of `base_id` if it is a masterchain block
    mc_state: Option<PinnedShardStateGuard>,
}

// Passes the query to the method of its type
macro_rules! dispatch {
    ($self:ident, $query:ident,) => {{
        drop($query);
        Err(lite_error(ERROR_PROTOVIOLATION, "unknown query"))
    }};
    ($self:ident, $query:ident, $type:ty => $method:ident, $($rest:tt)*) => {
        match $query.downcast::<$type>() {
            Ok(query) => Ok(TLObject::new($self.$method(query).await?)),
            Err(query) => dispatch!($self, query, $($rest)*),
        }
    };
}

pub(super) struct QueryHandler {
    engine: Arc<dyn EngineOperations>,
}

impl QueryHandler {
    pub fn new(engine: Arc<dyn EngineOperations>) -> Self {
        Self { engine }
    }

    /// Returns the answer to the query from liteServer.query or liteServer.error
    async fn process(&self, data: &[u8]) -> TLObject {
        let result = tokio::time::timeout(QUERY_TIMEOUT, self.process_query(data))
            .await
            .unwrap_or_else(|_| Err(lite_error(ERROR_TIMEOUT, "timeout")));

        result.unwrap_or_else(|err| {
            log::debug!("Lite server query failed: {}", err);

            let code = err
                .downcast_ref::<LiteError>()
                .map_or(ERROR_GENERIC, |err| err.code);

            let error = LiteServerError {
                code,
                message: err.to_string(),
            };

            TLObject::new(error.into_boxed())
        })
    }

    async fn process_query(&self, data: &[u8]) -> Result<TLObject> {
        if !self.engine.check_sync().await.unwrap_or(false) {
            return Err(lite_error(ERROR_NOT_READY, "node not synced"));
        }

        let query = deserialize_boxed(data).map_err(|err| lite_error(ERROR_PROTOVIOLATION, err))?;

        log::trace!("Lite server query: {:?}", query);

        dispatch!(self, query,
            SendMessage => send_message,
            GetAccountState => get_account_state,
            RunSmcMethod => run_smc_method,
            GetOneTransaction => get_one_transaction,
            GetTransactions => get_transactions,
        )
    }

    async fn send_message(&self, query: SendMessage) -> Result<SendMsgStatusBoxed> {
        let id = read_single_root_boc(&query.body)?.repr_hash();

        self.engine
            .redirect_external_message(&query.body, id)
            .await?;

        let result = SendMsgStatus { status: 1 };

        Ok(result.into_boxed())
    }

    async fn get_account_state(&self, query: GetAccountState) -> Result<AccountStateBoxed> {
        let data = self.load_account(query.id, &query.account).await?;

        let result = AccountState {
            id: data.base_id,
            shardblk: data.shard_id,
            shard_proof: data.shard_proof,
            proof: data.proof,
            state: proof::write_boc(data.account.into_iter().collect())?,
        };

        Ok(result.into_boxed())
    }

    async fn run_smc_method(&self, query: RunSmcMethod) -> Result<RunMethodResultBoxed> {
        if query.params.len() >= MAX_PARAMS_SIZE {
            fail!("more than 64k parameter bytes passed")
        }

        if query.mode & !SUPPORTED_MODES != 0 {
            fail!("unsupported mode in runSmcMethod")
        }

        let mut stack = if query.params.is_empty() {
            Stack::new()
        } else {
            run_method::deserialize_stack(&query.params)
                .map_err(|err| error!("cannot deserialize parameter list: {}", err))?
        };

        stack.push(StackItem::int(query.method_id));

        let data = self.load_account(query.id, &query.account).await?;
        let state = data
            .state
            .ok_or_else(|| error!("cannot find shard for account {:x}", query.account.id))?;

        let mc_state = match data.mc_state {
            Some(mc_state) => mc_state,
            None => self.load_master_state(&state).await?,
        };

        let (account, mode) = (data.account, query.mode);

        let output = tokio::task::spawn_blocking(move || {
            run_method::run_get_method(account, stack, mode, state.state(), mc_state.state())
        })
        .await??;

        let field = |bit: i32, value: Vec<u8>| (mode & bit != 0).then_some(value);

        let result = RunMethodResult {
            mode,
            id: data.base_id,
            shardblk: data.shard_id,
            shard_proof: field(MODE_PROOFS, data.shard_proof),
            proof: field(MODE_PROOFS, data.proof),
            state_proof: field(run_method::MODE_STATE_PROOF, output.state_proof),
            init_c7: field(run_method::MODE_INIT_C7, output.init_c7),
            lib_extras: field(MODE_LIB_EXTRAS, Vec::new()),
            exit_code: output.exit_code,
            result: field(run_method::MODE_RESULT, output.result),
        };

        Ok(result.into_boxed())
    }

    async fn get_one_transaction(&self, query: GetOneTransaction) -> Result<TransactionInfoBoxed> {
        let prefix = account_prefix(&query.account);

        if !query.id.shard().contains_full_prefix(&prefix) {
            fail!("requested account id is not contained in the shard of the specified block")
        }

        let block = self.load_block(&query.id).await?;
        let (proof, transaction) =
            proof::transaction_proof(block.root_cell(), &query.account.id, query.lt as u64)?;

        let result = TransactionInfo {
            id: query.id,
            proof: proof::write_boc(vec![proof])?,
            transaction: proof::write_boc(transaction.into_iter().collect())?,
        };

        Ok(result.into_boxed())
    }

    async fn get_transactions(&self, query: GetTransactions) -> Result<TransactionListBoxed> {
        let count = (query.count as u32).min(MAX_TRANSACTION_COUNT) as usize;
        let prefix = account_prefix(&query.account);
        let account = query.account.id;

        let (mut lt, mut hash) = (query.lt as u64, query.hash);

        let mut ids = Vec::new();
        let mut transactions = Vec::new();
        let mut block: Option<BlockStuff> = None;

        while transactions.len() < count && lt != 0 {
            // `exact` means the block was looked up by the transaction lt
            let (current, exact) = match block.take() {
                Some(block) => (block, false),
                None => match self.load_block_by_lt(&prefix, lt).await {
                    Ok(block) => (block, true),
                    Err(err) if transactions.is_empty() => return Err(err),
                    Err(_) => break,
                },
            };

            let Some(cell) = proof::find_transaction(current.block()?, &account, lt)? else {
                if !exact {
                    continue;
                }

                if transactions.is_empty() {
                    fail!("cannot locate transaction in block with specified logical time")
                }

                break;
            };

            if cell.repr_hash() != hash {
                fail!("transaction hash mismatch")
            }

            let transaction = Transaction::construct_from_cell(cell.clone())?;

            if transaction.prev_trans_lt() >= lt {
                fail!("previous transaction time is not less than the current one")
            }

            lt = transaction.prev_trans_lt();
            hash = transaction.prev_trans_hash().clone();

            ids.push(current.id().clone());
            transactions.push(cell);
            block = Some(current);
        }

        let result = TransactionList {
            ids,
            transactions: proof::write_boc(transactions)?,
        };

        Ok(result.into_boxed())
    }

    async fn load_account(&self, id: BlockIdExt, account: &LiteAccountId) -> Result<AccountData> {
        let workchain = account.workchain;
        let prefix = account_prefix(account);

        if !id.shard().is_masterchain() && id.shard().workchain_id() != workchain {
            fail!("reference block for a getAccountState() must belong to the masterchain")
        }

        if id.shard().workchain_id() == workchain && !id.shard().contains_full_prefix(&prefix) {
            fail!("requested account id is not contained in the shard of the reference block")
        }

        if !id.shard().is_masterchain() {
            let state = self.load_state(&id).await?;
            let (proof, account) = self.prove_account(&id, &state, &account.id).await?;

            let account_data = AccountData {
                base_id: id.clone(),
                shard_id: id,
                shard_proof: Vec::new(),
                proof,
                account,
                state: Some(state),
                mc_state: None,
            };

            return Ok(account_data);
        }

        let base_id = if id.seq_no() == u32::MAX {
            self.engine
                .load_last_applied_mc_block_id()?
                .ok_or_else(|| error!("no last applied masterchain block"))?
                .as_ref()
                .clone()
        } else {
            id
        };

        let mc_state = self.load_state(&base_id).await?;

        if workchain == MASTERCHAIN_ID {
            let (proof, account) = self.prove_account(&base_id, &mc_state, &account.id).await?;

            let account_data = AccountData {
                base_id: base_id.clone(),
                shard_id: base_id,
                shard_proof: Vec::new(),
                proof,
                account,
                state: Some(mc_state.clone()),
                mc_state: Some(mc_state),
            };

            return Ok(account_data);
        }

        let mc_block = self.load_block(&base_id).await?;
        let mc_state_root = mc_state.state().root_cell();
        let st_proof = proof::state_root_proof(mc_block.root_cell(), &mc_state_root.repr_hash())?;

        let (shard_info_proof, shard_id) = proof::shard_info_proof(mc_state_root, &prefix)?;
        let shard_proof = proof::write_boc(vec![st_proof, shard_info_proof])?;

        let Some(shard_id) = shard_id else {
            let account_data = AccountData {
                base_id,
                shard_id: BlockIdExt::default(),
                shard_proof,
                proof: Vec::new(),
                account: None,
                state: None,
                mc_state: Some(mc_state),
            };

            return Ok(account_data);
        };

        let state = self.load_state(&shard_id).await?;
        let (proof, account) = self.prove_account(&shard_id, &state, &account.id).await?;

        let account_data = AccountData {
            base_id,
            shard_id,
            shard_proof,
            proof,
            account,
            state: Some(state),
            mc_state: Some(mc_state),
        };

        Ok(account_data)
    }

    async fn prove_account(
        &self,
        id: &BlockIdExt,
        state: &PinnedShardStateGuard,
        account: &UInt256,
    ) -> Result<(Vec<u8>, Option<Cell>)> {
        let block = self.load_block(id).await?;
        let state_root = state.state().root_cell();
        let block_proof = proof::state_root_proof(block.root_cell(), &state_root.repr_hash())?;
        let account_proof = proof::account_proof(state_root, account)?;

        let account = state
            .state()
            .shard_account(&account.clone().into())?
            .map(|account| account.account_cell());

        Ok((proof::write_boc(vec![block_proof, account_proof])?, account))
    }

    async fn load_master_state(
        &self,
        state: &PinnedShardStateGuard,
    ) -> Result<PinnedShardStateGuard> {
        let master = state
            .state()
            .state()?
            .master_ref()
            .ok_or_else(|| error!("masterchain ref block is not available"))?;

        self.load_state(&BlockIdExt::from_ext_blk(master.master.clone()))
            .await
    }

    async fn load_block_by_lt(&self, account: &AccountIdPrefixFull, lt: u64) -> Result<BlockStuff> {
        let id = self
            .engine
            .find_block_by_lt(account, lt)?
            .ok_or_else(|| error!("cannot find block with transaction lt {}", lt))?;

        self.load_block(&id).await
    }

    async fn load_block(&self, id: &BlockIdExt) -> Result<BlockStuff> {
        let handle = self.applied_handle(id)?;
        self.engine.load_block(&handle).await
    }

    async fn load_state(&self, id: &BlockIdExt) -> Result<PinnedShardStateGuard> {
        self.applied_handle(id)?;
        self.engine.load_and_pin_state(id).await
    }

    fn applied_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        match self.engine.load_block_handle(id)? {
            Some(handle) if handle.id() == id && handle.is_applied() => Ok(handle),
            _ => fail!("block {} is not applied", id),
        }
    }
}

#[async_trait::async_trait]
impl Subscriber for QueryHandler {
    async fn try_consume_query(&self, object: TLObject, _peers: &AdnlPeers) -> Result<QueryResult> {
        match object.downcast::<Query>() {
            Ok(query) => QueryResult::consume_boxed(
                self.process(&query.data).await,
                #[cfg(feature = "telemetry")]
                None,
            ),

            Err(object) => Ok(QueryResult::Rejected(object)),
        }
    }
}

fn account_prefix(account: &LiteAccountId) -> AccountIdPrefixFull {
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&account.id.as_slice()[..8]);

    AccountIdPrefixFull {
        workchain_id: account.workchain,
        prefix: u64::from_be_bytes(prefix),
    }
}
