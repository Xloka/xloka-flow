//! Live MySQL CDC via `mysql_async` binlog replication.
//!
//! Requires the MySQL server to have:
//!   `binlog_format  = ROW`
//!   `binlog_row_image = FULL`
//! And the replication user needs REPLICATION SLAVE + REPLICATION CLIENT grants.
//!
//! NOTE: Full row-level binlog parsing (INSERT/UPDATE/DELETE) is currently
//! stubbed.  The infrastructure (connection, checkpoint, schema events) is in
//! place.  Row parsing will be implemented once the `mysql_async` binlog API
//! surface is confirmed against the exact installed version.  For now use the
//! `--mock` flag for end-to-end pipeline testing.

use async_trait::async_trait;
use mysql_async::prelude::*;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::cdc::{CdcSource, RowOp};
use crate::error::{FlowError, FlowResult};
use crate::Checkpoint;

// ─── Config ───────────────────────────────────────────────────────────────────

pub struct LiveCdcConfig {
    /// MySQL connection URL.  E.g. `mysql://user:pass@host:3306/db`
    pub mysql_url: String,
    /// Path where the binlog checkpoint is persisted.
    pub checkpoint_path: String,
    /// If non-empty, only replicate rows from these databases.
    pub filter_databases: Vec<String>,
    /// If non-empty, only replicate rows from these tables.
    pub filter_tables: Vec<String>,
    /// Unique replication server-id (must not clash with MySQL server's own id).
    pub server_id: u32,
}

pub struct LiveCdc {
    config: LiveCdcConfig,
}

impl LiveCdc {
    pub fn new(config: LiveCdcConfig) -> Self {
        LiveCdc { config }
    }
}

// ─── CdcSource impl ──────────────────────────────────────────────────────────

#[async_trait]
impl CdcSource for LiveCdc {
    async fn run(self, _tx: mpsc::Sender<RowOp>) -> FlowResult<()> {
        info!(
            "LiveCdc: connecting to MySQL at {}",
            mask_password(&self.config.mysql_url)
        );

        // ── 1. Verify connectivity and binlog settings ────────────────────────
        let pool = mysql_async::Pool::new(self.config.mysql_url.as_str());
        let mut conn = pool.get_conn().await?;

        // Check binlog_format = ROW
        let binlog_format: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'binlog_format'")
            .await?;

        match binlog_format {
            Some((_, fmt)) if fmt.eq_ignore_ascii_case("ROW") => {
                info!("LiveCdc: binlog_format=ROW ✓");
            }
            Some((_, fmt)) => {
                warn!(
                    "LiveCdc: binlog_format={fmt} — must be ROW. \
                     Set `binlog_format = ROW` in my.cnf and restart MySQL."
                );
            }
            None => {
                warn!(
                    "LiveCdc: binary logging does not appear to be enabled. \
                     Set `log_bin = ON` in my.cnf."
                );
            }
        }

        // Check binlog_row_image = FULL
        let row_image: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'binlog_row_image'")
            .await?;

        if let Some((_, img)) = row_image {
            if !img.eq_ignore_ascii_case("FULL") {
                warn!(
                    "LiveCdc: binlog_row_image={img} — FULL is required for \
                     complete before/after row data."
                );
            } else {
                info!("LiveCdc: binlog_row_image=FULL ✓");
            }
        }

        // Load and log current binlog position
        let master_status: Option<mysql_async::Row> = conn
            .query_first("SHOW MASTER STATUS")
            .await?;

        if let Some(row) = master_status {
            let file: Option<String> = row.get(0);
            let position: Option<u64> = row.get(1);
            info!(
                "LiveCdc: current MySQL binlog position — file={:?} pos={:?}",
                file, position
            );
        }

        // Load checkpoint
        let checkpoint = Checkpoint::load(&self.config.checkpoint_path);
        info!("LiveCdc: resuming from checkpoint: {:?}", checkpoint);

        pool.disconnect().await?;

        // ── 2. Full binlog streaming is not yet implemented ───────────────────
        //
        // To implement: use `mysql_async::BinlogStream` (if available at this
        // API version) or a compatible replication stream crate.
        //
        // The pipeline infrastructure is complete:
        //   • CheckPoint save/load ✓
        //   • Channel to StoreWriter ✓
        //   • RowOp / TableSchema types ✓
        //
        // Use `xloka-flow sync --mock` for full end-to-end testing.
        Err(FlowError::other(
            "Live CDC binlog streaming is not fully implemented yet. \
             Please use `xloka-flow sync --mock` (or `run-all --mock`) for \
             simulated end-to-end testing.",
        ))
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Replace the password in a MySQL URL with `***` for safe logging.
fn mask_password(url: &str) -> String {
    // mysql://user:PASSWORD@host:port/db  →  mysql://user:***@host:port/db
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            // Ensure we're not masking the scheme colon
            if url[..colon_pos].contains("//") {
                let mut masked = url.to_string();
                masked.replace_range(colon_pos + 1..at_pos, "***");
                return masked;
            }
        }
    }
    url.to_string()
}
