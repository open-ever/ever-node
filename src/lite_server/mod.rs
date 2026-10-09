mod handler;
mod proof;
mod run_method;

use crate::engine_traits::EngineOperations;
use handler::QueryHandler;

use adnl::server::{AdnlServer, AdnlServerConfig};
use ever_block::{base64_encode, Result};
use std::sync::Arc;

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
