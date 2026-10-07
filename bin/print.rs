/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use clap::Parser;
use ever_block::{
    error, read_boc, Account, Block, BlockIdExt, ConfigParams, Deserializable, HashmapAugType, McShardRecord, Result, ShardStateUnsplit
};
use ever_block_json::{debug_account, debug_block, debug_block_full, debug_state, debug_state_full};
use ever_node::{
    collator_test_bundle::create_engine_allocated, 
    internal_db::{InternalDb, InternalDbConfig, LAST_APPLIED_MC_BLOCK}
};
#[cfg(feature = "telemetry")]
use ever_node::collator_test_bundle::create_engine_telemetry;

fn print_block(block: &Block, brief: bool) -> Result<()> {
    if brief {
        println!("{}", debug_block(block.clone())?);
    } else {
        println!("{}", debug_block_full(block)?);
    }
    Ok(())
}

fn print_state(state: &ShardStateUnsplit, brief: bool) -> Result<()> {
    if brief {
        println!("{}", debug_state(state.clone())?);
    } else {
        println!("{}", debug_state_full(state.clone())?);
    }
    Ok(())
}

async fn print_db_block(db: &InternalDb, block_id: BlockIdExt, brief: bool) -> Result<()> {
    println!("loading block: {}", block_id);
    let handle = db.load_block_handle(&block_id)?.ok_or_else(
        || error!("Cannot load block {}", block_id)
    )?;
    let block = db.load_block_data(&handle).await?;
    print_block(block.block()?, brief)
}

async fn print_db_state(db: &InternalDb, block_id: BlockIdExt, brief: bool) -> Result<()> {
    println!("loading state: {}", block_id);
    let state = db.load_shard_state_dynamic(&block_id)?;
    print_state(state.state()?, brief)
}

async fn print_shards(db: &InternalDb, block_id: BlockIdExt) -> Result<()> {
    println!("loading state: {}", block_id);
    let state = db.load_shard_state_dynamic(&block_id)?;
    if let Ok(shards) = state.shards() {
        shards.iterate_shards(|shard, descr| {
            let descr = McShardRecord::from_shard_descr(shard, descr);
            println!("before_merge: {} {}", descr.descr.before_merge, descr.block_id());
            Ok(true)
        })?;
    }
    Ok(())
}

// full BlockIdExt or masterchain seq_no
fn get_block_id(db: &InternalDb, id: &str) -> Result<BlockIdExt> {
    if let Ok(id) = id.parse() {
        Ok(id)
    } else {
        let mc_seqno = id.parse()?;
        let handle = db.find_mc_block_by_seq_no(mc_seqno)?;
        Ok(handle.id().clone())
    }
}

#[derive(clap::Parser)]
#[command(version)]
struct Cli {
    /// path to DB
    #[arg(short, long, default_value = "node_db")]
    path: String,

    /// print block
    #[arg(short, long)]
    block: Option<String>,

    /// print state
    #[arg(short, long)]
    state: Option<String>,

    /// shard ids from master with seqno
    #[arg(short = 'r', long)]
    shards: Option<String>,

    /// print all accounts from all shards of workchains and masterchain
    /// for last applied state
    #[arg(long = "accounts")]
    last_accounts: bool,

    /// print containtment of bag of cells
    #[arg(short = 'c', long)]
    boc: Option<String>,

    /// print brief info
    /// (block without messages and transactions, state without accounts)
    #[arg(short = 'i', long)]
    brief: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    let brief = args.brief;
    if let Some(path) = args.boc {
        let bytes = std::fs::read(path)?;
        let res = read_boc(&bytes)?;
        println!("{:?}", res.header);
        if res.roots.len() > 1 {
            for root in res.roots {
                println!("{0:#.1$}", root, res.header.cells_count);
            }
        } else if let Ok(block) = Block::construct_from_cell(res.roots[0].clone()) {
            print_block(&block, brief)?;
        } else if let Ok(state) = ShardStateUnsplit::construct_from_cell(res.roots[0].clone()) {
            print_state(&state, brief)?;
        } else if let Ok(account) = Account::construct_from_cell(res.roots[0].clone()) {
            if let Some(data) = account.data().and_then(|data| data.reference(0).ok()) {
                let config_params = ConfigParams::with_root(data);
                let mut json = Default::default();
                let mode = ever_block_json::SerializationMode::Debug;
                if ever_block_json::serialize_config(&mut json, &config_params, mode).is_ok() {
                    println!("config params: {}", serde_json::to_string_pretty(&json)?);
                }
            }
            println!("{}", debug_account(account)?);
        }
    } else {
        let db_config = InternalDbConfig { 
            db_directory: args.path, 
            ..Default::default()
        };
        let db = InternalDb::with_update(
            db_config,
            false,
            false,
            false,
            &|| Ok(()),
            None,
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated(),
        ).await?;

        if let Some(block_id) = args.block {
            let block_id = get_block_id(&db, &block_id)?;
            print_db_block(&db, block_id, brief).await?;
        }

        if let Some(block_id) = args.state {
            let block_id = get_block_id(&db, &block_id)?;
            print_db_state(&db, block_id, brief).await?;
        }

        if let Some(block_id) = args.shards {
            let block_id = get_block_id(&db, &block_id)?;
            print_shards(&db, block_id).await?;
        }

        if args.last_accounts {
            let last_mc_id = db
                .load_full_node_state(LAST_APPLIED_MC_BLOCK)?
                .ok_or_else(|| error!("no info about last applied mc block"))?;
            println!("{{\"accounts\":[");
            let mut first = true; 
            let last_mc_state = db.load_shard_state_dynamic(&last_mc_id)?;
            let mut top_blocks = last_mc_state.top_blocks_all()?;
            top_blocks.push((*last_mc_id).clone());
            for block_id in &top_blocks {
                let state = db.load_shard_state_dynamic(block_id)?;
                state.state()?.read_accounts()?.iterate_objects(|shard_account| {
                    let account = shard_account.read_account()?;
                    let addr = account.get_addr().unwrap();
                    let balance = account.balance().unwrap();
                    let mut acc = serde_json::json!({
                        "id": addr.to_string(),
                        "last_paid": account.storage_info().unwrap().last_paid(),
                        "last_trans_lt": account.last_tr_time().unwrap_or_default(),
                        "balance": balance.grams.as_u128(),
                    });
                    if !balance.other.is_empty() {
                        let mut other = serde_json::Map::new();
                        balance.other.iterate_with_keys(|k: u32, v| {
                            other.insert(k.to_string(), v.value().to_string().into());
                            Ok(true)
                        })?;
                        if let Some(map) = acc.as_object_mut() {
                            map.insert("balance_other".to_string(), other.into());
                        }
                    };
                    if !first {
                        println!(",");
                    } else {
                        first = false;
                    }
                    print!("{:#}", acc);
                    Ok(true)
                })?;
            }
            println!("]}}");
        }
    }
    Ok(())
}
