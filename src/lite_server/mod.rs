mod handler;
mod proof;
mod run_method;

// #[cfg(test)]
// #[path = "../tests/test_lite_server.rs"]
// mod tests;

use crate::engine_traits::EngineOperations;
use handler::QueryHandler;

use adnl::server::{AdnlServer, AdnlServerConfig, AdnlServerConfigJson};
use ever_block::{base64_encode, Result};
use std::sync::Arc;

/// Same format as the control server config plus `enabled` and `max_packet_size`
#[derive(serde::Deserialize, serde::Serialize)]
pub struct LiteServerConfigJson {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "LiteServerConfigJson::default_max_packet_size")]
    pub max_packet_size: usize,

    #[serde(flatten)]
    pub server: AdnlServerConfigJson,
}

impl LiteServerConfigJson {
    fn default_max_packet_size() -> usize {
        16 * 1024 * 1024 // 16 MB
    }

    pub fn adnl_config(&self) -> Result<AdnlServerConfig> {
        let config = AdnlServerConfig::from_json_config(&self.server)?
            .with_max_packet_size(Some(self.max_packet_size));

        Ok(config)
    }
}

pub struct LiteServer {
    adnl: AdnlServer,
}

impl LiteServer {
    pub async fn start(
        config: AdnlServerConfig,
        engine: Arc<dyn EngineOperations>,
    ) -> Result<Self> {
        let id = base64_encode(config.server_id());
        log::info!("Starting lite server with id {}", id);

        let subc = Arc::new(QueryHandler::new(engine));
        let adnl = AdnlServer::listen(config, vec![subc]).await?;

        Ok(Self { adnl })
    }

    pub async fn shutdown(self) {
        self.adnl.shutdown().await
    }
}
