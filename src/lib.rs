//! `xloka-flow` — High-Performance MySQL CDC to DuckDB Analytical Engine & Real-Time API Server.
//!
//! Replicates transactional MySQL row mutations into local DuckDB storage with primary-key
//! batch coalescing, atomic GTID checkpointing, live schema evolution, declarative query endpoints,
//! and multiplexed WebSocket streaming push notifications.
//!
//! # Architecture & Concurrency Model
//!
//! - **Single-Writer Lock Invariant:** Only `store::StoreWriter` holds the read-write `duckdb::Connection`.
//!   All API REST and WebSocket (`ws`) reader queries use isolated read-only handles (`Connection::open_with_flags`).
//! - **Non-Blocking Ingestion Invariant:** Commit notifications use non-blocking `tokio::sync::broadcast`
//!   channels. Slow WebSocket subscribers never stall ingestion.
//! - **Blocking Thread Pool Offloading:** DuckDB queries inside Axum or WebSocket tasks execute
//!   inside `tokio::task::spawn_blocking` or `tokio::task::JoinSet` so the Tokio async reactor is never starved.
//!
//! # Example: Embedded Store Ingestion & Concurrent Read Query
//!
//! ```rust
//! use xloka_flow::store::writer::StoreWriter;
//! use xloka_flow::cdc::types::{CdcEvent, ColumnDef, DuckDbType, RowOp, TableSchema, Value};
//! use xloka_flow::api::metrics::FlowMetrics;
//! use xloka_flow::Settings;
//! use std::sync::Arc;
//! use tokio::sync::broadcast;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let conn = duckdb::Connection::open_in_memory()?;
//!     let (commit_tx, _) = broadcast::channel(16);
//!     let metrics = Arc::new(FlowMetrics::new());
//!     let settings = Arc::new(Settings::default());
//!     
//!     let writer = StoreWriter::new(settings, conn, commit_tx, metrics)?;
//!     
//!     let schema = TableSchema {
//!         table_name: "items".into(),
//!         columns: vec![
//!             ColumnDef {
//!                 name: "id".into(),
//!                 duck_type: DuckDbType::BigInt,
//!                 nullable: false,
//!                 is_primary_key: true,
//!             },
//!             ColumnDef {
//!                 name: "price".into(),
//!                 duck_type: DuckDbType::Double,
//!                 nullable: false,
//!                 is_primary_key: false,
//!             },
//!         ],
//!     };
//!     
//!     let (tx, rx) = tokio::sync::mpsc::channel(10);
//!     tx.send(CdcEvent::Schema(schema)).await?;
//!     tx.send(CdcEvent::Row(RowOp::Insert {
//!         table: "items".into(),
//!         pk_indices: vec![0],
//!         row: vec![Value::Integer(100), Value::Float(19.99)],
//!         event_ts_ms: 1000,
//!         checkpoint: None,
//!     })).await?;
//!     drop(tx);
//!     
//!     writer.run(rx).await?;
//!     Ok(())
//! }
//! ```

// DuckDB's bundled Windows build links against the Restart Manager API.
// The `#[link]` attribute propagates this native library requirement to every
// binary in this package (the `cargo:rustc-link-lib` directive in `build.rs`
// only covers the library target itself).
#[cfg(target_os = "windows")]
#[link(name = "rstrtmgr")]
extern "C" {}

pub mod accounts;
pub mod api;
pub mod cdc;
pub mod error;
pub mod store;

/// Re-export of the `duckdb` crate so that auxiliary binaries in this package
/// can use it without duplicating the Windows `#[link]` requirement.
pub use duckdb;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::accounts::AccountManager;
use crate::api::metrics::FlowMetrics;
use crate::api::queries::QueryManifest;
use crate::error::FlowResult;
use crate::store::writer::CommitNotification;
// ─── Settings ─────────────────────────────────────────────────────────────────

/// Top-level configuration loaded from CLI flags, environment variables, or config files.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Settings {
    #[serde(default = "default_listen")]
    pub listen: String,

    #[serde(default = "default_db_path")]
    pub db_path: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,

    #[serde(default = "default_max_ws_connections")]
    pub max_ws_connections: usize,

    pub admin_token: Option<String>,
    pub allowed_domains: Option<String>,
    pub query_manifest_path: Option<String>,

    /// Optional API key for the read-only data plane (`/api/v1/query/*`, `/ws`).
    /// When unset the data plane is open (single-tenant / trusted-network setups).
    pub data_api_key: Option<String>,

    /// Path of the account registry file. Relative paths are resolved against
    /// the current working directory. In a container this should point into a
    /// persistent volume, otherwise the accounts are lost on restart.
    pub accounts_file: Option<String>,

    /// Maximum number of rows a single manifest statement may materialize.
    /// Guards the daemon against a query without a `LIMIT`.
    #[serde(default = "default_max_query_rows")]
    pub max_query_rows: usize,

    /// Reload `accounts.json` every N seconds (`0` = disabled).
    #[serde(default)]
    pub accounts_reload_secs: u64,

    /// Treat `accounts.json` as read-only desired state and reject API mutations with `409 Conflict`.
    #[serde(default)]
    pub accounts_read_only: bool,

    /// Per-account DuckDB memory limit (e.g. `"1GB"`, `"2GB"`).
    #[serde(default = "default_account_memory_limit")]
    pub account_memory_limit: String,

    /// DuckDB worker threads per account.
    #[serde(default = "default_duckdb_threads")]
    pub duckdb_threads: usize,

    /// Hard cap on concurrently replicated accounts.
    #[serde(default = "default_max_accounts")]
    pub max_accounts: usize,

    /// Delete `<id>.db`, WAL, and checkpoint when an account is removed during reconciliation.
    #[serde(default)]
    pub accounts_prune_data: bool,

    /// Optional directory containing per-account query manifests (`<dir>/<account_id>.toml`).
    #[serde(default)]
    pub query_manifest_dir: Option<String>,
}

fn default_listen() -> String {
    "0.0.0.0:3000".into()
}
fn default_db_path() -> String {
    "xloka-flow.db".into()
}
fn default_batch_size() -> usize {
    10_000
}
fn default_flush_interval_ms() -> u64 {
    50
}
fn default_max_ws_connections() -> usize {
    1_000
}
fn default_max_query_rows() -> usize {
    100_000
}
fn default_account_memory_limit() -> String {
    "1GB".into()
}
fn default_duckdb_threads() -> usize {
    2
}
fn default_max_accounts() -> usize {
    10
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            db_path: default_db_path(),
            batch_size: default_batch_size(),
            flush_interval_ms: default_flush_interval_ms(),
            max_ws_connections: default_max_ws_connections(),
            admin_token: None,
            allowed_domains: None,
            query_manifest_path: None,
            data_api_key: None,
            accounts_file: None,
            max_query_rows: default_max_query_rows(),
            accounts_reload_secs: 0,
            accounts_read_only: false,
            account_memory_limit: default_account_memory_limit(),
            duckdb_threads: default_duckdb_threads(),
            max_accounts: default_max_accounts(),
            accounts_prune_data: false,
            query_manifest_dir: None,
        }
    }
}

impl Settings {
    /// Load settings from `.env` and `config.toml` or environment variables
    /// prefixed with `XLOKA__`.
    ///
    /// The configuration file is looked up relative to the current working
    /// directory. Use [`Settings::load_from`] to point at an explicit path.
    pub fn load() -> FlowResult<Self> {
        Self::load_from(None)
    }

    /// Load settings, optionally from an explicit configuration file path.
    ///
    /// Precedence (lowest to highest): `Settings::default` -> `config.toml` -> `XLOKA__*` env vars.
    pub fn load_from(config_path: Option<&std::path::Path>) -> FlowResult<Self> {
        dotenvy::dotenv().ok();
        let builder = config::Config::builder();
        let builder = match config_path {
            Some(path) => builder.add_source(
                config::File::from(path)
                    .format(config::FileFormat::Toml)
                    .required(false),
            ),
            None => builder.add_source(
                config::File::with_name("config")
                    .format(config::FileFormat::Toml)
                    .required(false),
            ),
        };
        let builder = builder.add_source(config::Environment::with_prefix("XLOKA").separator("__"));
        let cfg = builder.build()?;
        Ok(cfg.try_deserialize()?)
    }
}

// ─── Legacy JSON Checkpoint Support ───────────────────────────────────────────

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

/// Application state shared across Axum REST endpoints and WebSocket handlers.
#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub metrics: Arc<FlowMetrics>,
    pub query_manifest: Arc<QueryManifest>,
    pub commit_tx: broadcast::Sender<CommitNotification>,
    pub account_manager: AccountManager,
}

impl AppState {
    /// Create a new `AppState` instance.
    pub fn new(
        settings: Settings,
        metrics: Arc<FlowMetrics>,
        query_manifest: Arc<QueryManifest>,
        commit_tx: broadcast::Sender<CommitNotification>,
        account_manager: AccountManager,
    ) -> Self {
        AppState {
            settings: Arc::new(settings),
            metrics,
            query_manifest,
            commit_tx,
            account_manager,
        }
    }

    /// Resolve a named query for an optional target account, checking the per-account
    /// manifest first and falling back to the global manifest.
    pub fn resolve_query(
        &self,
        account_id: Option<&str>,
        query_name: &str,
    ) -> Option<crate::api::queries::QueryDef> {
        if let Some(id) = account_id {
            if let Some(def) = self.account_manager.get_account_query(id, query_name) {
                return Some(def);
            }
        }
        self.query_manifest.queries.get(query_name).cloned()
    }

    /// Return all named queries available to an optional target account (global queries
    /// overlaid with any per-account manifest queries).
    pub fn resolve_all_queries(
        &self,
        account_id: Option<&str>,
    ) -> std::collections::HashMap<String, crate::api::queries::QueryDef> {
        let mut merged = self.query_manifest.queries.clone();
        if let Some(id) = account_id {
            if let Some(acct_manifest) = self.account_manager.get_account_manifest(id) {
                for (k, v) in &acct_manifest.queries {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        merged
    }
}
