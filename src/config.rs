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

use crate::{keystore::KEYSTORE_FILE_NAME, utils::atomic_write::write_file_atomic};
use adnl::{common::Timeouts, node::AdnlNodeConfig, server::AdnlServerConfig};
use storage::shardstate_db_async::CellsDbConfig;
use std::{
    collections::{HashMap, HashSet}, convert::TryInto, fs::{File, read_dir}, fmt::{Display, Formatter},
    io::BufReader, net::{Ipv4Addr, SocketAddr}, path::{Path, PathBuf}, sync::Arc, time::Duration
};

use ton_api::{
    IntoBoxed, 
    ton::{
        adnl::{address::address::Udp, addresslist::AddressList as AdnlAddressList}, 
        dht::node::Node as DhtNodeConfig, pub_::publickey::Ed25519
    }
};

use ever_block::{BlockIdExt, ShardIdent};

use ever_block::{
    error, fail, base64_decode, base64_encode, Ed25519KeyOption, KeyOption, KeyOptionJson, Result,
    UInt256
};

#[cfg(feature="external_db")]
use ever_block::{BASE_WORKCHAIN_ID, MASTERCHAIN_ID};

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct CellsGcConfig {
    pub gc_interval_sec: u32,
    pub cells_lifetime_sec: u64,
}

impl Default for CellsGcConfig {
    fn default() -> Self {
        CellsGcConfig {
            gc_interval_sec: 900,
            cells_lifetime_sec: 1800,
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct CollatorConfig {
    pub cutoff_timeout_ms: u32,
    pub stop_timeout_ms: u32,
    pub clean_timeout_percentage_points: u32,
    pub optimistic_clean_percentage_points: u32,
    pub max_secondary_clean_timeout_percentage_points: u32,
    pub max_collate_threads: u32,
    pub retry_if_empty: bool,
    pub finalize_empty_after_ms: u32,
    pub empty_collation_sleep_ms: u32,
    pub external_messages_timeout_percentage_points: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_messages_maximum_queue_length: Option<u32>, // None - unlimited
}
impl Default for CollatorConfig {
    fn default() -> Self {
        Self {
            cutoff_timeout_ms: 1000,
            stop_timeout_ms: 1500,
            clean_timeout_percentage_points: 150, // 0.150 = 15% = 150ms
            optimistic_clean_percentage_points: 1000, // 1.000 = 100% = 150ms
            max_secondary_clean_timeout_percentage_points: 350, // 0.350 = 35% = 350ms
            max_collate_threads: 10,
            retry_if_empty: false,
            finalize_empty_after_ms: 800,
            empty_collation_sleep_ms: 100,
            external_messages_timeout_percentage_points: 100, // 0.1 = 10% = 100ms
            external_messages_maximum_queue_length: Some(25600),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Copy)]
#[derive(Default)]
pub enum ShardStatesCacheMode {
    Off, // States saved sinchronously and not cached.
    #[default]
    Moderate, // States saved asiynchronously.
}
impl ShardStatesCacheMode {
    pub fn _is_enabled(&self) -> bool {
        matches!(self, ShardStatesCacheMode::Moderate)
    }
    pub fn is_disabled(&self) -> bool {
        matches!(self, ShardStatesCacheMode::Off)
    }
}

#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct NodeConfig {
    log_config_name: Option<String>,
    global_config_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mesh_global_configs_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workchain: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    boot_from_zerostate: Option<bool>,
    internal_db_path: Option<String>,
    validation_countdown_mode: Option<String>,
    unsafe_catchain_patches_path: Option<String>,
    adnl_node: Option<AdnlNodeSettings>,
    #[serde(skip_serializing_if = "NodeExtensions::is_default")]
    #[serde(default)]
    extensions: NodeExtensions,
    control_server: Option<AdnlServerSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_server: Option<AdnlServerSettings>,
    kafka_consumer_config: Option<KafkaConsumerConfig>,
    external_db_config: Option<ExternalDbConfig>,
    default_rldp_roundtrip_ms: Option<u32>,
    #[serde(default)]
    test_bundles_config: CollatorTestBundlesGeneralConfig,
    #[serde(default)]
    connectivity_check_config: ConnectivityCheckBroadcastConfig,
    gc: Option<GC>,
    #[serde(skip)]
    configs_dir: String,
    #[serde(skip)]
    port: Option<u16>,
    #[serde(skip)]
    file_name: String,
    remp: Option<RempConfig>,
    #[serde(default)]
    restore_db: bool,
    #[serde(default)]
    cells_db_config: CellsDbConfig,
    #[serde(default)]
    collator_config: CollatorConfig,
    #[serde(default)]
    skip_saving_persistent_states: bool,
    #[serde(default)]
    states_cache_mode: ShardStatesCacheMode,
    #[serde(default)]
    sync_by_archives: bool,
    #[serde(default)]
    smft_disabled: bool,
    #[serde(default)]
    smft_max_mc_delivery_timeout_ms: Option<u32>,
}

/// The `adnl_node` section, the node's keys are in the keystore
#[derive(Default, serde::Deserialize, serde::Serialize)]
struct AdnlNodeSettings {
    ip_address: String,
    recv_pipeline_pool: Option<u8>,
    recv_priority_pool: Option<u8>,
    #[cfg(feature = "telemetry")]
    telemetry_peer_packets: Option<bool>,
    throughput: Option<u32>,
    #[cfg(feature = "telemetry")]
    timeout_check_packet_processing_mcs: Option<u64>,
    timeout_expire_queued_packet_sec: Option<u32>,
}

/// The `control_server` and `lite_server` sections, the server keys are in the keystore
#[derive(serde::Deserialize, serde::Serialize)]
struct AdnlServerSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "AdnlServerSettings::default_max_packet_size")]
    max_packet_size: usize,
    address: SocketAddr,
    clients: AdnlServerClients,
    timeouts: Option<Timeouts>,
}

/// Any client or the public keys of allowed clients
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum AdnlServerClients {
    Any,
    List(Vec<KeyOptionJson>),
}

impl AdnlServerSettings {
    fn default_max_packet_size() -> usize {
        16 * 1024 * 1024 // 16 MB
    }

    fn adnl_config(&self, key: Arc<dyn KeyOption>) -> Result<AdnlServerConfig> {
        let mut config = AdnlServerConfig::new(self.address, key)
            .with_timeouts(self.timeouts.clone().unwrap_or_default())
            .with_max_packet_size(Some(self.max_packet_size));

        if let AdnlServerClients::List(list) = &self.clients {
            let clients = list.iter()
                .map(Ed25519KeyOption::from_public_key_json)
                .collect::<Result<Vec<_>>>()?;

            config = config.with_clients(&clients)?;
        }

        Ok(config)
    }
}

pub struct NodeGlobalConfig(NodeGlobalConfigJson);

#[derive(Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct NodeExtensions {
    pub disable_broadcast_retransmit: bool,
    pub disable_compression: bool,
    pub broadcast_hops: Option<u8>
}


impl NodeExtensions {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub group_id: String,
    pub brokers: String,
    pub topic: String,
    pub session_timeout_ms: u32,
    pub run_attempt_timeout_ms: u32
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Clone)]
pub struct GC {
    enable_for_archives: bool,
    archives_life_time_hours: Option<u32>, // Hours
    enable_for_shard_state_persistent: bool,
    #[serde(default)]
    cells_gc_config: CellsGcConfig,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct TopicMask {
    pub mask: String,
    pub name: String,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct KafkaProducerConfig {
    pub enabled: bool,
    pub brokers: String,
    pub message_timeout_ms: u32,
    pub topic: Option<String>,
    pub sharded_topics: Option<Vec<TopicMask>>,
    #[serde(default)]
    pub sharding_depth: u32,
    pub attempt_timeout_ms: u32,
    pub message_max_size: usize,
    pub big_messages_storage: Option<String>,
    pub big_message_max_size: Option<usize>,
    pub external_message_ref_address_prefix: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct ExternalDbConfig {
    pub block_producer: KafkaProducerConfig,
    pub raw_block_producer: KafkaProducerConfig,
    pub message_producer: KafkaProducerConfig,
    pub transaction_producer: KafkaProducerConfig,
    pub account_producer: KafkaProducerConfig,
    pub block_proof_producer: KafkaProducerConfig,
    pub raw_block_proof_producer: KafkaProducerConfig,
    pub chain_range_producer: KafkaProducerConfig,
    pub remp_statuses_producer: KafkaProducerConfig,
    pub shard_hashes_producer: KafkaProducerConfig,
    pub bad_blocks_storage: String,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct RempConfig {
    client_enabled: Option<bool>,
    remp_client_pool: Option<u8>,
    service_enabled: Option<bool>,
    message_queue_max_len: Option<usize>,
    max_incoming_broadcast_delay_millis: Option<u32>,
}

impl RempConfig {
    #[cfg(test)]
    pub fn create_empty() -> Self {
        Self {
            client_enabled: None,
            remp_client_pool: None,
            service_enabled: None,
            message_queue_max_len: None,
            max_incoming_broadcast_delay_millis: None,
        }
    }

    pub fn is_client_enabled(&self) -> bool {
        self.client_enabled.unwrap_or(true)
    }

    pub fn is_service_enabled(&self) -> bool {
        self.service_enabled.unwrap_or(true)
    }

    pub fn get_message_queue_max_len(&self) -> Option<usize> {
        self.message_queue_max_len
    }

    pub fn get_max_incoming_broadcast_delay_millis(&self) -> u32 { self.max_incoming_broadcast_delay_millis.unwrap_or(1000) }
/*
    pub fn get_catchain_options(&self) -> Option<catchain::Options> {
        if self.is_service_enabled() {
            let opts = catchain::Options {
                idle_timeout: std::time::Duration::from_secs(5),
                max_deps: 2,
                ..Default::default()
            };
            Some(opts)
        } else {
            None
        }
    }
*/
    pub fn remp_client_pool(&self) -> Option<u8> {
        self.remp_client_pool
    }

}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct CollatorTestBundlesConfig {
    build_for_unknown_errors: bool,
    known_errors: Vec<String>,
    build_for_errors: bool,
    errors: Vec<String>,
    path: String,
}

impl CollatorTestBundlesConfig {

    pub fn is_enable(&self) -> bool {
        self.build_for_unknown_errors ||
            (self.build_for_errors && !self.errors.is_empty())
    }

    pub fn need_to_build_for(&self, error: &str) -> bool {
        self.build_for_unknown_errors &&
            self.known_errors.iter().all(|e| !error.contains(e))
        || self.build_for_errors && 
            self.errors.iter().any(|e| error.contains(e))
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct ConnectivityCheckBroadcastConfig {
    pub enabled: bool,
    pub long_len: usize,
    pub short_period_ms: u64,
    pub long_mult: u8,
}

impl Default for ConnectivityCheckBroadcastConfig {
    fn default() -> Self {
        ConnectivityCheckBroadcastConfig {
            enabled: true,
            long_len: 2 * 1024,
            short_period_ms: 1000,
            long_mult: 5,
        }
    }
}

impl ConnectivityCheckBroadcastConfig {
    pub const LONG_BCAST_MIN_LEN: usize = 769;

    pub fn check(&self) -> Result<()> {
        if self.long_len < Self::LONG_BCAST_MIN_LEN {
            fail!("long_len should be >= {}", Self::LONG_BCAST_MIN_LEN);
        }
        if self.short_period_ms == 0 {
            fail!("short_period_ms can't have zero value");
        }
        if self.short_period_ms > 1_000_000 {
            fail!("short_period_ms should be <= 1_000_000");
        }
        if self.short_period_ms < 100 {
            fail!("short_period_ms should be >= 100");
        }
        if self.long_mult == 0 {
            fail!("long_mult can't have zero value");
        }
        Ok(())
    }
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct CollatorTestBundlesGeneralConfig {
    pub collator: CollatorTestBundlesConfig,
    pub validator: CollatorTestBundlesConfig,
}

impl NodeConfig {
    pub const DEFAULT_DB_ROOT: &'static str = "node_db";    
    pub const DEFAULT_ADNL_ADDRESS: &str = "0.0.0.0:30100";
    pub const DEFAULT_CONTROL_SERVER_PORT: u16 = 4001;
    pub const DEFAULT_LITE_SERVER_PORT: u16 = 4002;
    pub const DEFAULT_LOG_CONFIG_NAME: &str = "log_cfg.yml";
    pub const DEFAULT_GLOBAL_CONFIG_NAME: &str = "ever-global.config.json";

    #[cfg(feature="external_db")]
    pub fn front_workchain_ids(&self) -> Vec<i32> {
        match self.workchain {
            None | Some(0) | Some(-1) => vec![MASTERCHAIN_ID, BASE_WORKCHAIN_ID],
            Some(workchain_id) => vec![workchain_id]
        }
    }

    pub fn workchain(&self) -> Option<i32> {
        self.workchain
    }
    pub fn boot_from_zerostate(&self) -> bool {
        self.boot_from_zerostate.unwrap_or(false)
    }

    pub fn is_smft_disabled(&self) -> bool {
        self.smft_disabled
    }

    pub fn smft_max_mc_delivery_timeout(&self) -> Option<std::time::Duration> {
        self.smft_max_mc_delivery_timeout_ms
            .map(|timeout_ms| std::time::Duration::from_millis(timeout_ms as u64))
    }

    pub fn from_file(
        configs_dir: &str,
        json_file_name: &str,
        ip_address: Option<&str>,
        client_console_key: Option<String>,
        control_server_key: Option<&Arc<dyn KeyOption>>
    ) -> Result<Self> {
        let config_file_path = NodeConfig::build_path(configs_dir, json_file_name);

        let (mut config_json, console_client_key) = match File::open(&config_file_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let config: NodeConfig = serde_json::from_reader(reader)?;

                if client_console_key.is_some() {
                    println!("Can't add the console key: delete {} before", json_file_name);
                }
                (config, None)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let (config, console_client_key) =
                    NodeConfig::with_defaults(ip_address, client_console_key)?;

                let data = serde_json::to_string_pretty(&config)?;
                write_file_atomic(&config_file_path, data.as_bytes())?;

                (config, console_client_key)
            }
            Err(err) => fail!("Can't open {}: {}", config_file_path.display(), err)
        };

        config_json.connectivity_check_config.check()?;

        config_json.configs_dir = configs_dir.to_string();
        config_json.file_name = json_file_name.to_string();

        if let Some(server_key) = control_server_key {
            config_json.create_console_config(server_key, console_client_key)?;
        }
        Ok(config_json)
    }

    pub fn adnl_node(&self, keys: Vec<(Arc<dyn KeyOption>, usize)>) -> Result<AdnlNodeConfig> {
        let settings = self.adnl_node.as_ref().ok_or_else(|| error!("ADNL node is not configured!"))?;

        let mut ret = AdnlNodeConfig::from_ip_address_and_keys(&settings.ip_address, keys)?;
        ret.set_recv_worker_pools(settings.recv_pipeline_pool, settings.recv_priority_pool)?;
        ret.set_throughput(settings.throughput);
        ret.set_timeout_expire_queued_packet_sec(settings.timeout_expire_queued_packet_sec);
        #[cfg(feature = "telemetry")] {
            ret.set_telemetry_peer_packets(settings.telemetry_peer_packets);
            ret.set_timeout_check_packet_processing_mcs(settings.timeout_check_packet_processing_mcs);
        }

        if let Some(port) = self.port {
            ret.set_port(port)
        }
        Ok(ret)
    }

    pub fn control_server(&self, key: Arc<dyn KeyOption>) -> Result<Option<AdnlServerConfig>> {
        let settings = self.control_server.as_ref().filter(|settings| settings.enabled);
        settings.map(|settings| settings.adnl_config(key)).transpose()
    }

    pub fn lite_server(&self, key: Arc<dyn KeyOption>) -> Result<Option<AdnlServerConfig>> {
        let settings = self.lite_server.as_ref().filter(|settings| settings.enabled);
        settings.map(|settings| settings.adnl_config(key)).transpose()
    }

    fn create_console_config(
        &self,
        server_key: &Arc<dyn KeyOption>,
        client_key: Option<KeyOptionJson>
    ) -> Result<()> {
        let Some(control_server) = self.control_server.as_ref().filter(|settings| settings.enabled) else {
            return Ok(())
        };
        let path = self.build_config_path("console.config.json");
        if path.exists() {
            return Ok(())
        }

        let config = serde_json::json!({
            "config": {
                "server_address": control_server.address,
                "server_key": {
                    "type_id": Ed25519KeyOption::KEY_TYPE,
                    "pub_key": base64_encode(server_key.pub_key()?)
                },
                "client_key": client_key
            }
        });
        std::fs::write(&path, serde_json::to_string_pretty(&config)?)
            .map_err(|err| error!("Can`t create console.config.json: {}", err))?;

        Ok(())
    }

    pub fn log_config_path(&self) -> Option<PathBuf> {
        if let Some(log_config_name) = &self.log_config_name {
            return Some(self.build_config_path(log_config_name))
        }
        None
    }

    pub fn unsafe_catchain_patches_files(&self) -> Vec<String> {
        let mut result = Vec::new();
        if let Some(catchain_patches) = &self.unsafe_catchain_patches_path {
            let log_path = self.build_config_path(catchain_patches);
            if let Ok(dir) = std::fs::read_dir(log_path) {
                for filename in dir.into_iter().flatten() {
                    if let Some(path_str) = filename.path().to_str() {
                        if path_str.ends_with(".json") {
                            result.push(path_str.to_string());
                        }
                    }
                }
            }
        }
        result
    }

    pub fn validation_countdown_mode(&self) -> Option<String> {
        self.validation_countdown_mode.clone()
    }

    pub fn gc_archives_life_time_hours(&self) -> Option<u32> {
        if let Some(gc) = &self.gc {
            if gc.enable_for_archives {
                return gc.archives_life_time_hours.or(Some(0));
            }
        }
        None
    }

    #[cfg(feature = "external_db")]
    pub fn kafka_consumer_config(&self) -> Option<KafkaConsumerConfig> {
        self.kafka_consumer_config.clone()
    }

    pub fn internal_db_path(&self) -> &str {
        self.internal_db_path.as_deref().unwrap_or(Self::DEFAULT_DB_ROOT)
    }

    pub fn cells_gc_config(&self) -> CellsGcConfig {
        match &self.gc {
            Some(conf) => conf.cells_gc_config.clone(),
            None => CellsGcConfig::default(),
        }
    }

    pub fn enable_shard_state_persistent_gc(&self) -> bool {
        self.gc.as_ref().map(|c| c.enable_for_shard_state_persistent).unwrap_or(false)
    }
    
    #[cfg(test)]
    pub fn set_internal_db_path(&mut self, path: String) {
        self.internal_db_path.replace(path);
    }

    #[cfg(test)]
    pub fn set_global_config_name(&mut self, name: &str) {
        self.global_config_name.replace(name.to_string());
    }
  
    pub fn default_rldp_roundtrip(&self) -> Option<u32> {
        self.default_rldp_roundtrip_ms
    }

    #[cfg(feature = "external_db")]
    pub fn external_db_config(&self) -> Option<ExternalDbConfig> {
        self.external_db_config.clone()
    }

    pub fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
        &self.test_bundles_config
    }
    pub fn connectivity_check_config(&self) -> &ConnectivityCheckBroadcastConfig {
        &self.connectivity_check_config
    }
    pub fn extensions(&self) -> &NodeExtensions {
        &self.extensions
    }
    pub fn remp_config(&self) -> RempConfig {
        match &self.remp {
            Some(x) => x.clone(),
            None => RempConfig::default()
        }
    }
    pub fn restore_db(&self) -> bool {
        self.restore_db
    }
    pub fn skip_saving_persistent_states(&self) -> bool {
        self.skip_saving_persistent_states
    }
    pub fn states_cache_mode(&self) -> ShardStatesCacheMode {
        self.states_cache_mode
    }
    pub fn sync_by_archives(&self) -> bool {
        self.sync_by_archives
    }
    pub fn cells_db_config(&self) -> &CellsDbConfig {
        &self.cells_db_config
    }

    #[cfg(test)]
    pub fn set_port(&mut self, port: u16) {
        self.port.replace(port);
    }

    pub fn collator_config(&self) -> &CollatorConfig {
        &self.collator_config
    }
 
    pub fn load_global_config(&self) -> Result<NodeGlobalConfig> {
        let name = self.global_config_name.as_ref().ok_or_else(
            || error!("global_config_name is not set in {}", self.file_name)
        )?;

        let global_config_path = self.build_config_path(name);

        NodeGlobalConfig::from_json_file(global_config_path)
    }

    pub fn mesh_global_configs_dir(&self) -> String {
        self.mesh_global_configs_dir.clone().unwrap_or(".".to_string())
    }

    pub fn load_global_config_of_network(
        global_configs_dir: &str,
        network_id: i32,
        zerostate: &BlockIdExt,
    ) -> Result<NodeGlobalConfig> {
        for entry in read_dir(global_configs_dir)?.flatten() {
            if entry.file_type()?.is_file() &&
                entry.file_name().to_str().map(|n| n.ends_with(".json")).unwrap_or(false)
            {
                if let Ok(config) = NodeGlobalConfig::from_json_file(entry.path()) {
                    if let Ok(id) = config.0.zero_state() {
                        if id == *zerostate {
                            return Ok(config);
                        }
                    }
                }
            }
        }
        fail!(
            "Global config file for network {} with zerostate {} is not found in {}!",
            network_id, zerostate, global_configs_dir,
        );
    }

    fn with_defaults(
        ip_address: Option<&str>,
        console_key: Option<String>
    ) -> Result<(Self, Option<KeyOptionJson>)> {
        let (clients, console_client_key) = match console_key {
            Some(console_key) => (vec![serde_json::from_str(&console_key)?], None),
            None => {
                let (private_key, key) = Ed25519KeyOption::generate_with_json()?;
                let public_key = serde_json::json!({
                    "type_id": Ed25519KeyOption::KEY_TYPE,
                    "pub_key": base64_encode(key.pub_key()?)
                });
                (vec![serde_json::from_value(public_key)?], Some(private_key))
            }
        };

        let config = NodeConfig {
            log_config_name: Some(Self::DEFAULT_LOG_CONFIG_NAME.to_string()),
            global_config_name: Some(Self::DEFAULT_GLOBAL_CONFIG_NAME.to_string()),
            adnl_node: Some(AdnlNodeSettings {
                ip_address: ip_address.unwrap_or(Self::DEFAULT_ADNL_ADDRESS).to_string(),
                ..Default::default()
            }),
            control_server: Some(AdnlServerSettings {
                enabled: true,
                max_packet_size: AdnlServerSettings::default_max_packet_size(),
                address: SocketAddr::from((Ipv4Addr::LOCALHOST, Self::DEFAULT_CONTROL_SERVER_PORT)),
                clients: AdnlServerClients::List(clients),
                timeouts: None
            }),
            lite_server: Some(AdnlServerSettings {
                enabled: false,
                max_packet_size: AdnlServerSettings::default_max_packet_size(),
                address: SocketAddr::from((Ipv4Addr::UNSPECIFIED, Self::DEFAULT_LITE_SERVER_PORT)),
                clients: AdnlServerClients::Any,
                timeouts: None
            }),
            gc: Some(GC {
                enable_for_archives: true,
                archives_life_time_hours: None,
                enable_for_shard_state_persistent: true,
                cells_gc_config: CellsGcConfig::default()
            }),
            cells_db_config: CellsDbConfig {
                cache_cells_counters: true,
                cache_size_bytes: 4 * 1024 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        };
        Ok((config, console_client_key))
    }

    pub fn build_config_path(&self, file_name: &str) -> PathBuf {
        Self::build_path(&self.configs_dir, file_name)
    }

    fn build_path(directory_name: &str, file_name: &str) -> PathBuf {
        let path = Path::new(directory_name);
        path.join(file_name)
    }

    pub fn keystore_path(configs_dir: &str) -> PathBuf {
        Self::build_path(configs_dir, KEYSTORE_FILE_NAME)
    }

    fn save_to_file(&self, file_name: &str) -> Result<()> {
        let config_file_path = self.build_config_path(file_name);
        let data = serde_json::to_string_pretty(&self)?;
        write_file_atomic(&config_file_path, data.as_bytes())?;

        Ok(())
    }

}

/// Keeps the part of the node configuration that can change at runtime and saves it to the
/// configuration file.
pub struct NodeConfigHandler {
    config: parking_lot::Mutex<NodeConfig>,
}

impl NodeConfigHandler {
    pub fn new(config: NodeConfig) -> Arc<Self> {
        Arc::new(Self { config: parking_lot::Mutex::new(config) })
    }

    pub fn store_states_gc_interval(&self, interval: u32) -> Result<()> {
        let mut config = self.config.lock();

        if let Some(gc) = &mut config.gc {
            gc.cells_gc_config.gc_interval_sec = interval;
        } else {
            config.gc = Some(GC {
                cells_gc_config: CellsGcConfig {
                    gc_interval_sec: interval,
                    ..Default::default()
                },
                ..Default::default()
            });
        }

        let file_name = config.file_name.clone();

        config.save_to_file(&file_name)
    }
}

impl NodeGlobalConfig {
    /// Constructor from json file
    pub fn from_json_file(json_file: impl AsRef<Path>) -> Result<Self> {
        let ton_node_global_cfg_json = NodeGlobalConfigJson::from_json_file(json_file)?;
        Ok(NodeGlobalConfig(ton_node_global_cfg_json))
    }

    pub fn zero_state(&self) -> Result<BlockIdExt> {
        self.0.zero_state()
    }

    pub fn init_block(&self) -> Result<Option<BlockIdExt>> {
        self.0.init_block()
    }

    pub fn hardforks(&self) -> Result<Vec<BlockIdExt>> {
        self.0.hardforks()
    }

    pub fn dht_nodes(&self) -> Result<Vec<DhtNodeConfig>> {
        self.0.get_dht_nodes_configs()
    }

// Unused
//    pub fn dht_param_a(&self) -> Result<i32> {
//        self.0.dht.a.ok_or_else(|| error!("Dht param a is not set!"))
//    }

// Unused
//    pub fn dht_param_k(&self) -> Result<i32> {
//        self.0.dht.k.ok_or_else(|| error!("Dht param k is not set!"))
//    }

}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct NodeGlobalConfigJson {
    #[serde(alias = "@type")]
    type_node : String,
    dht : DhtGlobalConfig,
    validator : Validator,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtGlobalConfig {
    #[serde(alias = "@type")]
    type_dht : Option<String>,
    k : Option<i32>,
    a : Option<i32>,
    static_nodes : DhtNodes,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtNodes {
    #[serde(alias = "@type")]
    type_dht : Option<String>,
    nodes : Vec<DhtNode>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtNode {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    id : IdDhtNode,
    addr_list : AddressList,
    version : Option <i32>,
    signature : Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct IdDhtNode {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    key : Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct AddressList {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    addrs : Vec<Address>,
    version : Option<i32>,
    reinit_date : Option<i32>,
    priority : Option<i32>,
    expire_at : Option<i32>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct Address {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    ip : Option<i64>,
    port : Option<u16>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct Validator {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    zero_state : ConfigBlockId,
    init_block : Option<ConfigBlockId>,
    hardforks : Vec<ConfigBlockId>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ConfigBlockId {
    workchain : Option<i32>,
    shard : Option<i64>,
    seqno : Option<i32>,
    root_hash : Option<String>,
    file_hash : Option<String>,
}

pub const PUB_ED25519 : &str = "pub.ed25519";

impl IdDhtNode {

    pub fn convert_key(&self) -> Result<Arc<dyn KeyOption>> {
        let type_id = self.type_node.as_ref().ok_or_else(|| error!("Type_node is not set!"))?;
       
        if !type_id.eq(PUB_ED25519) {
            fail!("unknown type_node!")
        };

        let key = if let Some(key) = &self.key {
            base64_decode(key)?
        } else {
            fail!("No public key!");
        };

        let pub_key = key[..32].try_into()?;
        Ok(Ed25519KeyOption::from_public_key(pub_key))
    }
}

impl NodeGlobalConfigJson {
    
    /// Constructs new configuration from JSON data
    pub fn from_json_file(json_file: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(json_file.as_ref())
            .map_err(|err| error!("cannot open file {:?} : {}", json_file.as_ref(), err))?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }
/*
    pub fn from_json(json: &str) -> Result<Self> {
        let json_config: NodeGlobalConfigJson = serde_json::from_str(json)?;
        Ok(json_config)
    }
*/

    pub fn get_dht_nodes_configs(&self) -> Result<Vec<DhtNodeConfig>> {
        let mut result = Vec::new();
        for dht_node in self.dht.static_nodes.nodes.iter() {
            let key = dht_node.id.convert_key()?;
            let mut addrs = Vec::new();
            for addr in dht_node.addr_list.addrs.iter() {
                let ip = if let Some(ip) = addr.ip {
                    ip
                } else {
                    continue;
                };
                let port = if let Some(port) = addr.port {
                    port
                } else {
                    continue
                };
                let addr = Udp {
                    ip: ip as i32,
                    port: port as i32
                }.into_boxed();
                addrs.push(addr);
            }
            let version = if let Some(version) = dht_node.addr_list.version {
                version
            } else {
                continue
            };
            let reinit_date = if let Some(reinit_date) = dht_node.addr_list.reinit_date {
                reinit_date
            } else {
                continue
            };
            let priority = if let Some(priority) = dht_node.addr_list.priority {
                priority
            } else {
                continue
            };
            let expire_at = if let Some(expire_at) = dht_node.addr_list.expire_at {
                expire_at
            } else {
                continue
            };           
            let addr_list = AdnlAddressList {
                addrs,
                version,
                reinit_date,
                priority,
                expire_at
            }; 
            let version = if let Some(version) = dht_node.version {
                version
            } else {
                continue
            };
            let signature = if let Some(signature) = &dht_node.signature {
                signature
            } else {
                continue
            };
            let node = DhtNodeConfig {
                id: Ed25519 {
                    key: UInt256::with_array(key
                        .pub_key()?
                        .try_into()?
                    )
                }.into_boxed(),
                addr_list,
                version,
                signature: base64_decode(signature)?
            };
            result.push(node)//convert_to_dht_node_cfg()?);
        }
        Ok(result)
    }

    fn parse_block_id(&self, block_id: &ConfigBlockId) -> Result<BlockIdExt> {
        let workchain_id = block_id
            .workchain
            .ok_or_else(|| error!("Unknown workchain id (of zero_state)!"))?;

        let seqno = block_id
            .seqno
            .ok_or_else(|| error!("Unknown workchain seqno (of zero_state)!"))?;

        let shard = block_id
            .shard
            .ok_or_else(|| error!("Unknown workchain shard (of zero_state)!"))?;

        let root_hash = block_id
            .root_hash
            .as_ref()
            .ok_or_else(|| error!("Unknown workchain root_hash (of zero_state)!"))?
            .parse()?;

        let file_hash = block_id
            .file_hash
            .as_ref()
            .ok_or_else(|| error!("Unknown workchain file_hash (of zero_state)!"))?
            .parse()?;

        Ok(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(workchain_id, shard as u64)?,
            seq_no: seqno as u32,
            root_hash,
            file_hash,
        })
    }

    pub fn zero_state(&self) -> Result<BlockIdExt> {
        self.parse_block_id(&self.validator.zero_state)
            .map_err(|err| error!("zero state parse error: {}", err))
    }

    pub fn init_block(&self) -> Result<Option<BlockIdExt>> {
        match self.validator.init_block {
            Some(ref init_block) => {
                match self.parse_block_id(init_block) {
                    Ok(block_id) => Ok(Some(block_id)),
                    Err(err) => fail!("init block parse error: {}", err)
                }
            }
            None => Ok(None)
        }
    }

    fn hardforks(&self) -> Result<Vec<BlockIdExt>> {
        log::info!("hardforks count {}", self.validator.hardforks.len());
        self.validator
            .hardforks
            .iter()
            .try_fold(Vec::new(), |mut vec, block_id| {
                match self.parse_block_id(block_id) {
                    Ok(block_id) => {
                        vec.push(block_id);
                        Ok(vec)
                    }
                    Err(err) => fail!("hardforks parse error: {}", err),
                }
            })
    }
}

pub struct ValidatorManagerConfig {
    pub update_interval: Duration,
    pub unsafe_resync_catchains: HashSet<u32>,
    /// Maps catchain_seqno to block_seqno and unsafe rotation id
    pub unsafe_catchain_rotates: HashMap<u32, (u32, u32)>,
    pub no_countdown_for_zerostate: bool,
    pub smft_disabled: bool,
    pub smft_max_mc_delivery_timeout: Option<std::time::Duration>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct UnsafeCatchainRotation {
    catchain_seqno: u32,
    block_seqno: u32,
    unsafe_rotation_id: u32
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ValidatorManagerConfigImpl {
    unsafe_resync_catchains: Vec<u32>,
    unsafe_catchain_rotates: Vec<UnsafeCatchainRotation>
}

impl Display for ValidatorManagerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "validation countdown mode: {}; update interval: {} ms; resync: [{}]; rotates: [{}]",
            if self.no_countdown_for_zerostate { "except-zerostate" } else { "always" },
            self.update_interval.as_millis(),
            self.unsafe_resync_catchains.iter().map(|n| format!("{} ", n)).collect::<String>(),
            self.unsafe_catchain_rotates.iter().map(
                |(cc, (blk, uid))| format!("({},{})=>{} ",cc,blk,uid)
            ).collect::<String>()
        )
    }
}

impl ValidatorManagerConfig {
    pub fn read_configs(config_files: Vec<String>, validation_countdown_mode: Option<String>, smft_disabled: bool, smft_max_mc_delivery_timeout: Option<std::time::Duration>) -> ValidatorManagerConfig {
        log::debug!(target: "validator", "Reading validator manager config files: {}",
            config_files.iter().map(|x| format!("{}; ",x)).collect::<String>());

        let mut validator_config = ValidatorManagerConfig::default();
        match validation_countdown_mode {
            Some(x) if x == "always" => validator_config.no_countdown_for_zerostate = false,
            Some(x) if x == "except-zerostate" => validator_config.no_countdown_for_zerostate = true,
            Some(x) => log::error!(
                "Incorrect option: validation_countdown_mode must be either 'always' or 'except-zerostate', '{}' found",
                x
            ),
            None => ()
        }

        validator_config.smft_disabled = smft_disabled;
        validator_config.smft_max_mc_delivery_timeout = smft_max_mc_delivery_timeout;

        'iterate_configs: for one_config in config_files.into_iter() {
            if let Ok(config_file) = std::fs::File::open(one_config.clone()) {
                let reader = std::io::BufReader::new(config_file);
                let config: ValidatorManagerConfigImpl = match serde_json::from_reader(reader) {
                    Err(e) => {
                        log::warn!("Not ValidatorManagerConfig, but expected to be: {}, error: {}",
                            one_config, e
                        );
                        continue 'iterate_configs
                    },
                    Ok(cfg) => cfg
                };

                for resync in config.unsafe_resync_catchains.into_iter() {
                    validator_config.unsafe_resync_catchains.insert(resync);
                }

                for rotate in config.unsafe_catchain_rotates.into_iter() {
                    validator_config.unsafe_catchain_rotates.insert(
                        rotate.catchain_seqno,
                        (rotate.block_seqno, rotate.unsafe_rotation_id)
                    );
                }
            }
        }

        log::info!(target: "validator", "Validator manager config has been read: {}", validator_config);

        validator_config
    }

    pub fn check_unsafe_catchain_rotation(&self, block_seqno_opt: Option<u32>, catchain_seqno: u32) -> Option<u32> {
        if let Some(blk) = block_seqno_opt {
            match self.unsafe_catchain_rotates.get(&catchain_seqno) {
                Some((required_block_seqno, rotation_id)) if *required_block_seqno <= blk => Some(*rotation_id),
                _ => None
            }
        }
        else {
            None
        }
    }
}

impl Default for ValidatorManagerConfig {
    fn default() -> Self {
        ValidatorManagerConfig {
            update_interval: Duration::from_secs(3),
            unsafe_resync_catchains: HashSet::new(),
            unsafe_catchain_rotates: HashMap::new(),
            no_countdown_for_zerostate: false,
            smft_disabled: false,
            smft_max_mc_delivery_timeout: None,
        }
    }
}
