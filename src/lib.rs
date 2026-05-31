//! Shared configuration, types, and application state for xloka-flow.

pub mod accounts;
pub mod cdc;
pub mod error;
pub mod store;
pub mod api;

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::error::FlowResult;
use crate::accounts::AccountManager;

// ─── Config ──────────────────────────────────────────────────────────────────

/// Top-level configuration loaded from `config.toml` / env vars.
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default = "default_listen")]
    pub listen: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    pub filename: Option<String>,
    pub position: Option<u64>,
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

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub account_manager: AccountManager,
}

impl AppState {
    pub fn new(settings: Settings, manager: AccountManager) -> Self {
        AppState {
            settings: Arc::new(settings),
            account_manager: manager,
        }
    }
}
