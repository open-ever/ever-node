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

mod block;
mod block_proof;
mod boot;
mod collator_test_bundle;
mod config;
mod engine;
mod engine_traits;
mod engine_operations;
mod error;
#[cfg(feature = "external_db")]
mod external_db;
mod full_node;
mod internal_db;
mod lite_server;
mod macros;
mod network;
mod rng;
mod shard_state;
mod sync;
mod types;
mod validating_utils;
mod validator;
mod shard_states_keeper;
mod mesh_queues_keeper;

mod ext_messages;

mod shard_blocks;

use crate::{
    config::TonNodeConfig, engine::{Engine, Stopper, EngineFlags},
    internal_db::restore::set_graceful_termination
};
#[cfg(feature = "external_db")]
use crate::engine_traits::ExternalDb;

use clap::Parser;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::os::raw::c_void;
#[cfg(feature = "trace_alloc")]
use std::{
    alloc::{GlobalAlloc, System, Layout}, sync::atomic::{AtomicBool, AtomicU64, Ordering}, 
    thread, time::Duration
};
#[cfg(feature = "trace_alloc_detail")]
use std::{
    fs::File, io::Write, mem::{self, MaybeUninit}, sync::atomic::{AtomicIsize, AtomicUsize}
};
use ever_block::Result;

#[cfg(test)]
#[path = "tests/test_helper.rs"]
pub mod test_helper;

#[cfg(target_os = "linux")]
#[link(name = "tcmalloc_minimal", kind = "dylib")]
extern "C" {
    pub fn tc_memalign(alignment: usize, size: usize) -> *mut c_void;
    pub fn tc_free(ptr: *mut c_void);
}

#[cfg(target_os = "linux")]
fn check_tcmalloc() {
    unsafe {
        let ptr = tc_memalign(10, 10);
        tc_free(ptr);
    }
}

#[cfg(feature = "trace_alloc")]
struct TracingAllocator {
    count: AtomicU64,
    allocated: AtomicU64,
    overhead: AtomicU64
}

#[cfg(feature = "trace_alloc_detail")]
struct AllocDetail {
    start: AtomicUsize,
    size: AtomicIsize,
}

#[cfg(feature = "trace_alloc_detail")]
const SIZE_TRACEBUF: usize = 20000000;

#[cfg(feature = "trace_alloc_detail")]
lazy_static::lazy_static!{
    static ref TRACEBUF: [AllocDetail; SIZE_TRACEBUF] = {
        let mut data: [MaybeUninit<AllocDetail>; SIZE_TRACEBUF] = unsafe {
            MaybeUninit::uninit().assume_init()
        };
        for elem in &mut data[..] {
            elem.write(
                AllocDetail {
                    start: AtomicUsize::new(0),
                    size: AtomicIsize::new(0)
                }
            );
        }
        unsafe { mem::transmute::<_, [AllocDetail; SIZE_TRACEBUF]>(data) }
    };
    static ref TRACEBUF_HEAD: AtomicUsize = AtomicUsize::new(0);
    static ref TRACEBUF_TAIL: AtomicUsize = AtomicUsize::new(0);
}

#[cfg(feature = "trace_alloc")]
thread_local!(
    static NOCALC: AtomicBool = AtomicBool::new(false)
);

#[cfg(feature = "trace_alloc")]
unsafe impl GlobalAlloc for TracingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        self.check_alloc(ret, layout.size());
        ret
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc_zeroed(layout);
        self.check_alloc(ret, layout.size());
        ret
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.check_dealloc(ptr, layout.size());
        let ret = System.realloc(ptr, layout, new_size);
        self.check_alloc(ret, new_size);
        ret
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.check_dealloc(ptr, layout.size());
        System.dealloc(ptr, layout);
    }
}

#[cfg(feature = "trace_alloc")]
impl TracingAllocator {

    fn check_alloc(&self, _ptr: *mut u8, size: usize) {
        if NOCALC.with(
            |f| f.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        ) {
            self.allocated.fetch_add(size as u64, Ordering::Relaxed);
            #[cfg(feature = "trace_alloc_detail")]
            Self::post_trace(_ptr as usize, size as isize);
            self.count.fetch_add(1, Ordering::Relaxed);
            NOCALC.with(|f| f.store(false, Ordering::Relaxed));
        } else {
            self.overhead.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    fn check_dealloc(&self, _ptr: *mut u8, size: usize) {
        if NOCALC.with(
            |f| f.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        ) {
            self.allocated.fetch_sub(size as u64, Ordering::Relaxed);
            #[cfg(feature = "trace_alloc_detail")]
            Self::post_trace(_ptr as usize, -(size as isize));
            self.count.fetch_sub(1, Ordering::Relaxed);
            NOCALC.with(|f| f.store(false, Ordering::Relaxed));
        } else {
            self.overhead.fetch_sub(size as u64, Ordering::Relaxed);
        }
    }

    #[cfg(feature = "trace_alloc_detail")]
    fn post_trace(start: usize, size: isize) {
        loop {
            let this = TRACEBUF_HEAD.load(Ordering::Relaxed);
            let next = if this == SIZE_TRACEBUF - 1 {
                0
            } else {
                this + 1
            };
            if next == TRACEBUF_TAIL.load(Ordering::Acquire) {
                thread::yield_now();
                continue
            }
            if TRACEBUF_HEAD.compare_exchange(
                this, next, Ordering::Relaxed, Ordering::Relaxed
            ).is_err() {
                thread::yield_now();
                continue;
            }
            if start == 0 {
                panic!("ZEROADDR_WRITE")
            }
            TRACEBUF[this].start.store(start, Ordering::Relaxed);
            TRACEBUF[this].size.store(size, Ordering::Release);
            break
        }
    }

}

#[cfg(feature = "trace_alloc")]
#[global_allocator]
static GLOBAL: TracingAllocator = TracingAllocator { 
    count: AtomicU64::new(0),
    allocated: AtomicU64::new(0),
    overhead: AtomicU64::new(0)
};

fn init_logger_from_file(path: &std::path::Path) -> Result<()> {
    let config = log4rs::config::load_config_file(path, Default::default())?;

    let has_node_appenders = config.loggers().iter().any(
        |logger| logger.name() == env!("CARGO_CRATE_NAME") && !logger.appenders().is_empty()
    );

    if config.root().appenders().is_empty() && !has_node_appenders {
        ever_block::fail!("no usable appenders for node messages (see log4rs errors above)")
    }

    drop(config); // Close the files opened while validating
    log4rs::init_file(path, Default::default())
}

fn init_logger<T: AsRef<std::path::Path>>(log_config_path: Option<T>) {
    if let Some(path) = log_config_path {
        let path = path.as_ref();
        match init_logger_from_file(path) {
            Ok(()) => return,
            Err(err) => eprintln!(
                "Can't init log from {}: {:#}. Falling back to stdout logging at INFO level",
                path.display(), err
            )
        }
    }

    let level = log::LevelFilter::Info;
    let stdout = log4rs::append::console::ConsoleAppender::builder()
        .target(log4rs::append::console::Target::Stdout)
        .build();

    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .filter(Box::new(log4rs::filter::threshold::ThresholdFilter::new(level)))
                .build("stdout", Box::new(stdout)),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Info),
        )
        .unwrap();

    let result = log4rs::init_config(config);
    if let Err(e) = result {
        eprintln!("Error init log: {}", e);
    }
}

const PANIC_LOG_THREAD: &str = "panic-log";

fn set_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        let thread = std::thread::current();

        if thread.name() == Some(PANIC_LOG_THREAD) {
            return
        }

        let thread_name = thread.name().unwrap_or("<unnamed>");
        let backtrace = std::backtrace::Backtrace::force_capture();
        let msg = format!("thread '{}' {}\n{}", thread_name, info, backtrace);

        let (sender, receiver) = std::sync::mpsc::channel();

        let spawned = std::thread::Builder::new()
            .name(PANIC_LOG_THREAD.to_string())
            .spawn(move || {
                log::error!(target: "panic", "{}", msg);
                sender.send(()).ok();
            });

        if spawned.is_ok() {
            receiver.recv_timeout(std::time::Duration::from_secs(1)).ok();
        }
    }));
}

#[cfg(feature = "external_db")]
fn start_external_db(config: &TonNodeConfig) -> Result<Vec<Arc<dyn ExternalDb>>> {
    let control_id = config.control_server()?.map(|config| *config.server_id());
    Ok(vec!(
        external_db::create_external_db(
            config.external_db_config().ok_or_else(
                || ever_block::error!("Can't load external database config!")
            )?,
            config.front_workchain_ids(),
            control_id,
        )?
    ))
}

async fn start_engine(
    config: TonNodeConfig, 
    zerostate_path: Option<&str>, 
    validator_runtime: tokio::runtime::Handle, 
    flags: EngineFlags,
    stopper: Arc<Stopper>
) -> Result<(Arc<Engine>, tokio::task::JoinHandle<()>)> {
    #[cfg(feature = "external_db")]
    let external_db = start_external_db(&config)?;

    crate::engine::run(
        config, 
        zerostate_path, 
        #[cfg(feature = "external_db")]
        external_db, 
        validator_runtime,
        flags,
        stopper,
    ).await
}

const CONFIG_NAME: &str = "config.json";
const DEFAULT_CONFIG_NAME: &str = "default_config.json";

fn cli_long_version() -> String {
    let package = env!("CARGO_PKG_VERSION");
    let block = validating_utils::supported_version();
    let commit = option_env!("BUILD_GIT_COMMIT").unwrap_or("not set");

    format!("{} (block: v{}, commit: {})", package, block, commit)
}

#[derive(clap::Parser)]
#[command(version, long_version = cli_long_version())]
struct Cli {
    #[arg(short = 'c', long, default_value = "./")]
    config: String,

    /// Directory with zerostate files (<file_hash>.boc) to load at boot
    #[arg(long)]
    zero_state: Option<String>,

    /// Use console key in json format
    #[arg(long)]
    console_key: Option<String>,

    /// Disable key blocks sync on boot, zero state init block will be used instead
    #[arg(long)]
    initial_sync_disabled: bool,

    /// Disable downloading starting block for sync, proof will be used instead
    #[arg(long)]
    starting_block_disabled: bool,

    /// Start check and restore db process forcedly with refilling cells database
    #[arg(long)]
    force_check_db: bool,

    /// Finish the process after config file processing (reading or generating)
    #[arg(long, verbatim_doc_comment)]
    process_conf_and_exit: bool,
}

fn main() {
    #[cfg(target_os = "linux")]
    check_tcmalloc();

    let cli = Cli::parse();

    let flags = EngineFlags {
        initial_sync_disabled: cli.initial_sync_disabled,
        starting_block_disabled: cli.starting_block_disabled,
        force_check_db: cli.force_check_db,
    };

    let zerostate_path = cli.zero_state;

    let config = match TonNodeConfig::from_file(
        &cli.config,
        CONFIG_NAME,
        None,
        DEFAULT_CONFIG_NAME,
        cli.console_key
    ) {
        Err(e) => {
            eprintln!("Can't load config: {:?}", e);
            std::process::exit(1);
        },
        Ok(c) => c
    };

    if cli.process_conf_and_exit {
        println!("Finish node because of --process-conf-and-exit flag is set");
        return;
    }

    init_logger(config.log_config_path());
    set_panic_hook();

    log::info!(target: "boot", "Starting ever-node {}", cli_long_version());

    #[cfg(feature = "statsd")]
    engine::init_statsd_exporter();

    #[cfg(feature = "prometheus")]
    engine::init_prometheus_exporter();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create Engine tokio runtime");

    let validator_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create Validator tokio runtime");

    #[cfg(feature = "trace_alloc_detail")]
    thread::spawn(
        || {
            let mut file = File::create("trace.bin").unwrap();
            loop {
                let this = TRACEBUF_TAIL.load(Ordering::Relaxed);
                let next = if this == SIZE_TRACEBUF - 1 {
                    0
                } else {
                    this + 1
                };
                if this == TRACEBUF_HEAD.load(Ordering::Relaxed) {
                    thread::yield_now();
                    continue
                }
                let size = TRACEBUF[this].size.load(Ordering::Acquire);
                if size == 0 {
                    thread::yield_now();
                    continue
                }
                let start = TRACEBUF[this].start.load(Ordering::Relaxed);
                if start == 0 {
                    panic!("ZEROADDR_READ")
                }
                file.write_all(&start.to_le_bytes()).ok();
                file.write_all(&size.to_le_bytes()).ok();
                TRACEBUF[this].size.store(0, Ordering::Release);
                TRACEBUF_TAIL.store(next, Ordering::Release);
            }
        }
    );

    #[cfg(feature = "trace_alloc")]
    thread::spawn(
        || {
            loop {
                thread::sleep(Duration::from_millis(30000));
                let count = GLOBAL.count.load(Ordering::Relaxed);
                let allocated = GLOBAL.allocated.load(Ordering::Relaxed);
                let overhead = GLOBAL.overhead.load(Ordering::Relaxed);
                log::info!(
                    "Allocated {} + {} = {} bytes, {} objects", 
                    allocated, overhead, allocated + overhead, count
                ); 
            }
        }
    );

    let stopper = Arc::new(Stopper::new());
    let stopper_ctrl_c = stopper.clone();

    ctrlc::set_handler(move || {
        log::warn!(target: "boot", "Got termination signal, starting node's safe stopping...");
        stopper_ctrl_c.set_stop();
    }).expect("Error setting termination signals handler");

    let validator_rt_handle = validator_runtime.handle().clone();
    let db_dir = config.internal_db_path().to_string();

    let failed = runtime.block_on(async move {
        match start_engine(
            config,
            zerostate_path.as_deref(),
            validator_rt_handle,
            flags,
            stopper.clone(),
        ).await {
            Err(e) => {
                if stopper.check_stop() {
                    log::warn!(target: "boot", "Node stopped ({})", e);
                    set_graceful_termination(&db_dir);
                    false
                } else {
                    log::error!(target: "boot", "Can't start node's Engine: {:?}", e);
                    true
                }
            }
            Ok((engine, join_handle)) => {
                join_handle.await.ok();

                log::warn!(target: "boot", "Still safe stopping node...");
                engine.wait_stop().await;
                log::warn!(target: "boot", "Node stopped");
                set_graceful_termination(&db_dir);
                false
            }
        }
    });

    if failed {
        std::process::exit(1);
    }
}
