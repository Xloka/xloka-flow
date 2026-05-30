//! Shared configuration, types, and application state for xloka-flow.

pub mod cdc;
pub mod error;
pub mod store;
pub mod api;

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::error::FlowResult;

// ─── Config ──────────────────────────────────────────────────────────────────

/// Top-level configuration loaded from `config.toml` / env vars.
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    /// MySQL connection URL.  Example: `mysql://user:pass@host:3306/db`
    #[serde(default = "default_mysql_url")]
    pub mysql_url: String,

    /// Path to the DuckDB database file.
    #[serde(default = "default_db_path")]
    pub db_path: String,

    /// Path to persist the binlog checkpoint.
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,

    /// HTTP listen address for the Axum API.
    #[serde(default = "default_listen")]
    pub listen: String,

    /// Number of CDC row-ops to buffer before forcing a DuckDB flush.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum ms to wait before flushing a partial batch.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

fn default_mysql_url() -> String {
    "mysql://root:password@127.0.0.1:3306/mydb".into()
}
fn default_db_path() -> String {
    "xloka-flow.db".into()
}
fn default_checkpoint_path() -> String {
    "checkpoint.json".into()
}
fn default_listen() -> String {
    "0.0.0.0:3000".into()
}
fn default_batch_size() -> usize {
    1_000
}
fn default_flush_interval_ms() -> u64 {
    1_000
}

impl Settings {
    /// Load from `config.toml` (optional) and environment variables.
    pub fn load() -> FlowResult<Self> {
        dotenvy::dotenv().ok();
        let cfg = config::Config::builder()
            .add_source(
                config::File::with_name("config")
                    .format(config::FileFormat::Toml)
                    .required(false),
            )
            .add_source(config::Environment::with_prefix("XLOKA").separator("__"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}

// ─── Binlog Checkpoint ───────────────────────────────────────────────────────

/// Persisted binlog position so we can resume after restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Binlog filename (e.g. `mysql-bin.000001`).
    pub filename: Option<String>,
    /// Byte position inside the binlog file.
    pub position: Option<u64>,
    /// GTID set string (alternative to file/position).
    pub gtid_set: Option<String>,
}

impl Checkpoint {
    pub fn load(path: &str) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &str) -> FlowResult<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

// ─── Shared AppState ─────────────────────────────────────────────────────────

/// State shared between the Axum router handlers.
#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub db_path: String,
}

impl AppState {
    pub fn new(settings: Settings) -> Self {
        let db_path = settings.db_path.clone();
        AppState {
            settings: Arc::new(settings),
            db_path,
        }
    }
}
