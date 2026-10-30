//! DuckDB single-writer thread owning the read-write connection.
//!
//! Applies batched primary-key deduplicated mutations via native DuckDB `Appender`,
//! manages atomic binlog checkpoints in DuckDB transactions, and broadcasts commit
//! notifications to WebSocket streaming clients.
//!
//! # Column binding is by NAME, never by position
//!
//! MySQL and DuckDB disagree about where a newly added column physically lives:
//! `ALTER TABLE t ADD COLUMN c FIRST` inserts the column in the middle in MySQL but
//! always appends it at the end in DuckDB. Binlog row images follow MySQL's physical
//! order, so a positional append silently writes every value into the wrong column.
//!
//! To stay correct the writer keeps two views of every table:
//!
//! * `source` — the authoritative **MySQL** schema (physical column order, primary
//!   keys). Row vectors arriving from the CDC source follow this order.
//! * `local_order` — the physical column order of the **DuckDB** table.
//!
//! Before appending, every row is projected from the source order into the local
//! order by column name. Column order differences therefore cannot corrupt data.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use duckdb::types::ToSql;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::api::metrics::FlowMetrics;
use crate::cdc::checkpoint::BinlogCheckpoint;
use crate::cdc::schema_evolver::{apply_ddl_to_source_schema, diff_schema, translate_alter};
use crate::cdc::types::{CdcEvent, DdlOp, TableSchema, Value};
use crate::store::coalescer::{BatchCoalescer, CoalescedRowOp};
use crate::Settings;

// ─── Broadcast Notification Types ─────────────────────────────────────────────

/// Individual row delta emitted in a commit notification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDelta {
    pub table: String,
    pub op: String, // "insert" | "update" | "delete"
    pub pk: Vec<serde_json::Value>,
    pub row: Option<serde_json::Value>,
}

/// Notification broadcast over `tokio::sync::broadcast` when a transaction commits.
#[derive(Debug, Clone, Serialize)]
pub struct CommitNotification {
    pub batch_id: u64,
    pub committed_at_ms: u64,
    pub replication_lag_ms: u64,
    /// Account/tenant that owns the data, `None` for the default (single-tenant) store.
    pub account_id: Option<String>,
    /// DuckDB file the batch was written to — subscribers use it to select data.
    pub db_path: String,
    pub affected_tables: Arc<HashSet<String>>,
    pub row_counts: Arc<HashMap<String, usize>>,
    /// Capped at max_delta_broadcast_rows (default 500) per batch to bound memory
    pub deltas: Arc<Vec<TableDelta>>,
}

// ─── Table Layout ─────────────────────────────────────────────────────────────

/// Everything the writer knows about one replicated table.
#[derive(Debug, Clone)]
struct TableLayout {
    /// Authoritative MySQL-side schema: physical column order + primary keys.
    /// Row vectors from the CDC source follow this order.
    source: TableSchema,
    /// Physical column order of the local DuckDB table.
    local_order: Vec<String>,
}

impl TableLayout {
    /// Project a source-ordered row into the local DuckDB column order.
    fn project(&self, row: &[Value]) -> Vec<Value> {
        self.local_order
            .iter()
            .map(|name| {
                self.source
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .and_then(|idx| row.get(idx))
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect()
    }
}

// ─── StoreWriter ──────────────────────────────────────────────────────────────

/// High-performance DuckDB writer owning the exclusive write handle.
pub struct StoreWriter {
    pub settings: Arc<Settings>,
    conn: Connection,
    coalescer: BatchCoalescer,
    commit_tx: broadcast::Sender<CommitNotification>,
    metrics: Arc<FlowMetrics>,
    layouts: HashMap<String, TableLayout>,
    batch_counter: u64,
    max_delta_broadcast_rows: usize,
    /// Account/tenant identifier reported in commit notifications.
    pub account_id: Option<String>,
    /// Optional per-account telemetry collector.
    pub telemetry: Option<Arc<crate::accounts::AccountTelemetry>>,
}

impl StoreWriter {
    /// Create and initialize a new `StoreWriter`.
    pub fn new(
        settings: Arc<Settings>,
        conn: Connection,
        commit_tx: broadcast::Sender<CommitNotification>,
        metrics: Arc<FlowMetrics>,
    ) -> anyhow::Result<Self> {
        let _ = crate::api::queries::register_db_connection(Path::new(&settings.db_path), &conn);
        let mut writer = Self {
            settings,
            conn,
            coalescer: BatchCoalescer::new(),
            commit_tx,
            metrics,
            layouts: HashMap::new(),
            batch_counter: 0,
            max_delta_broadcast_rows: 500,
            account_id: None,
            telemetry: None,
        };
        writer.init_database()?;
        Ok(writer)
    }

    /// Tag subsequent commit notifications with an account/tenant identifier.
    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Attach a per-account telemetry collector.
    pub fn with_telemetry(mut self, telemetry: Arc<crate::accounts::AccountTelemetry>) -> Self {
        self.telemetry = Some(telemetry);
        self
    }
}

impl Drop for StoreWriter {
    fn drop(&mut self) {
        crate::api::queries::unregister_db_connection(Path::new(&self.settings.db_path));
    }
}

impl StoreWriter {
    /// Read the persisted checkpoint from the DuckDB database for crash recovery.
    pub fn load_checkpoint(
        db_path: &Path,
        source_id: &str,
    ) -> anyhow::Result<Option<BinlogCheckpoint>> {
        if !db_path.exists() {
            return Ok(None);
        }

        // Use the shared registry: try_clone the writer's connection when it already
        // holds the file read-write in-process. Opening with ReadOnly flags directly
        // would fail with a DuckDB configuration-conflict error in that case.
        let conn = crate::api::queries::open_read_only(db_path)?;

        // Ensure table exists check
        let table_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = '_xloka_checkpoint'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(false);

        if !table_exists {
            return Ok(None);
        }

        let mut stmt = conn.prepare(
            "SELECT source_id, binlog_file, binlog_pos, gtid_set, event_ts_ms \
             FROM _xloka_checkpoint WHERE source_id = ?",
        )?;

        let mut rows = stmt.query([source_id])?;
        if let Some(row) = rows.next()? {
            let src: String = row.get(0)?;
            let file: String = row.get(1)?;
            let pos: u64 = row.get(2)?;
            let gtid: Option<String> = row.get(3)?;
            let ts: u64 = row.get(4)?;
            Ok(Some(BinlogCheckpoint::new(src, file, pos, gtid, ts)))
        } else {
            Ok(None)
        }
    }

    /// Initialize the internal metadata tables.
    fn init_database(&mut self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _xloka_checkpoint (\n\
                 source_id VARCHAR PRIMARY KEY,\n\
                 binlog_file VARCHAR NOT NULL,\n\
                 binlog_pos UBIGINT NOT NULL,\n\
                 gtid_set VARCHAR,\n\
                 event_ts_ms UBIGINT NOT NULL,\n\
                 updated_at TIMESTAMP DEFAULT current_timestamp\n\
             );",
        )?;
        Ok(())
    }

    /// Read the physical column order of a local DuckDB table.
    fn local_column_order(&self, table: &str) -> anyhow::Result<Vec<String>> {
        // PRAGMA takes a string literal — escape embedded quotes.
        let escaped = table.replace('\'', "''");
        let pragma_sql = format!("PRAGMA table_info('{}');", escaped);
        let mut stmt = self.conn.prepare(&pragma_sql)?;
        let mut rows = stmt.query([])?;

        let mut order = Vec::new();
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            order.push(name);
        }
        Ok(order)
    }

    /// Install (or refresh) the layout of a replicated table.
    fn install_layout(&mut self, source: TableSchema) -> anyhow::Result<()> {
        self.ensure_foreign_key_indexes(&source);
        let local_order = self.local_column_order(&source.table_name)?;
        let local_order = if local_order.is_empty() {
            source.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            local_order
        };
        self.layouts.insert(
            source.table_name.clone(),
            TableLayout {
                source,
                local_order,
            },
        );
        if let Some(ref tel) = self.telemetry {
            tel.set_tables_count(self.layouts.len());
        }
        Ok(())
    }

    /// Add any columns of `schema` that are missing from the local table.
    fn add_missing_columns(&self, schema: &TableSchema) -> anyhow::Result<()> {
        let local = self.local_column_order(&schema.table_name)?;
        if local.is_empty() {
            return Ok(());
        }
        for col in &schema.columns {
            if !local.iter().any(|l| l == &col.name) {
                self.conn.execute_batch(&format!(
                    "ALTER TABLE \"{}\" ADD COLUMN IF NOT EXISTS \"{}\" {}",
                    schema.table_name,
                    col.name,
                    col.duck_type.to_ddl()
                ))?;
            }
        }
        Ok(())
    }

    /// Auto-index foreign key columns (`*_id`) for fast analytical JOINs.
    fn ensure_foreign_key_indexes(&self, schema: &TableSchema) {
        let table = &schema.table_name;
        for col in &schema.columns {
            if !col.is_primary_key && col.name.ends_with("_id") {
                let idx_name = format!("idx_{}_{}", table, col.name);
                let idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS \"{}\" ON \"{}\" (\"{}\")",
                    idx_name, table, col.name
                );
                if let Err(e) = self.conn.execute_batch(&idx_sql) {
                    warn!("Failed to create index {}: {}", idx_name, e);
                } else {
                    debug!("Ensured foreign-key index {}", idx_name);
                }
            }
        }
    }

    /// Run the ingestion writer loop.
    pub async fn run(mut self, mut rx: mpsc::Receiver<CdcEvent>) -> anyhow::Result<()> {
        let flush_interval = std::time::Duration::from_millis(self.settings.flush_interval_ms);
        let mut interval_timer = tokio::time::interval(flush_interval);
        const MAX_CONSECUTIVE_FLUSH_FAILURES: u32 = 5;
        let mut consecutive_flush_failures: u32 = 0;

        info!(
            "StoreWriter running: max_batch_size={}, flush_interval_ms={}",
            self.settings.batch_size, self.settings.flush_interval_ms
        );

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    if !self.coalescer.is_empty() {
                        if let Err(e) = self.flush_batch() {
                            error!("Scheduled flush error: {e}");
                            consecutive_flush_failures += 1;
                            if consecutive_flush_failures >= MAX_CONSECUTIVE_FLUSH_FAILURES {
                                error!(
                                    "StoreWriter halting after {consecutive_flush_failures} consecutive \
                                     flush failures; pending data is retained and will replay from the \
                                     last persisted checkpoint on restart."
                                );
                                self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                return Err(e);
                            }
                        } else {
                            consecutive_flush_failures = 0;
                        }
                    }
                }
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            self.metrics.cdc_events_received_total.fetch_add(1, Ordering::Relaxed);
                            match event {
                                CdcEvent::Row(row_op) => {
                                    self.coalescer.push(row_op);
                                    if self.coalescer.len() >= self.settings.batch_size {
                                        if let Err(e) = self.flush_batch() {
                                            error!("Batch flush error: {e}");
                                            consecutive_flush_failures += 1;
                                            if consecutive_flush_failures >= MAX_CONSECUTIVE_FLUSH_FAILURES {
                                                error!(
                                                    "StoreWriter halting after {consecutive_flush_failures} \
                                                     consecutive flush failures; pending data is retained and \
                                                     will replay from the last persisted checkpoint on restart."
                                                );
                                                self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                                return Err(e);
                                            }
                                        } else {
                                            consecutive_flush_failures = 0;
                                        }
                                    }
                                }
                                CdcEvent::Ddl { table, raw_sql, op, new_schema, checkpoint, .. } => {
                                    // Flush pending rows under the previous schema. If this
                                    // fails we must NOT apply the DDL: the pending rows have
                                    // the old shape and would be written against the new
                                    // table. Fail closed instead.
                                    if !self.coalescer.is_empty() {
                                        if let Err(e) = self.flush_batch() {
                                            error!("Pre-DDL flush failed: {e}. Aborting DDL application.");
                                            self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                            return Err(e);
                                        }
                                        consecutive_flush_failures = 0;
                                    }

                                    info!("Applying intercepted DDL for table '{}': {}", table, raw_sql);
                                    if let Err(e) = self.apply_ddl(&table, &raw_sql, &op, new_schema.as_ref()) {
                                        error!("DDL application failed for table '{table}': {e}");
                                        // Fail closed: the checkpoint is NOT advanced, so the
                                        // DDL is replayed on the next start instead of being
                                        // silently skipped.
                                        self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                        return Err(e);
                                    }

                                    // Persist the checkpoint only after the DDL was applied
                                    // successfully.
                                    if let Some(cp) = checkpoint {
                                        if let Err(e) = self.persist_checkpoint(&cp) {
                                            error!("Failed to persist checkpoint after DDL: {e}");
                                            self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                            return Err(e);
                                        }
                                    }
                                }
                                CdcEvent::Schema(schema) => {
                                    if !self.coalescer.is_empty() {
                                        if let Err(e) = self.flush_batch() {
                                            error!("Pre-schema flush error: {e}");
                                            self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                            return Err(e);
                                        }
                                        consecutive_flush_failures = 0;
                                    }
                                    let ddl = schema.create_ddl();
                                    if let Err(e) = self.conn.execute_batch(&ddl) {
                                        warn!("Schema create error for {}: {e}", schema.table_name);
                                    }
                                    // The local table may predate the schema (e.g. the
                                    // process restarted after a schema change). Add any
                                    // missing columns so the Appender can bind by name.
                                    if let Err(e) = self.add_missing_columns(&schema) {
                                        warn!("Could not reconcile local table {}: {e}", schema.table_name);
                                    }
                                    if let Err(e) = self.install_layout(schema) {
                                        warn!("Failed to cache layout for table: {e}");
                                    }
                                }
                                CdcEvent::BulkLoadCsv { table, csv_path, checkpoint } => {
                                    if !self.coalescer.is_empty() {
                                        if let Err(e) = self.flush_batch() {
                                            error!("Pre-bulk-load flush error: {e}");
                                            self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                            return Err(e);
                                        }
                                        consecutive_flush_failures = 0;
                                    }
                                    info!("StoreWriter: Bulk loading '{}' from {}", table, csv_path);
                                    let escaped_path = csv_path.replace('\'', "''");
                                    let sql = format!(
                                        "COPY \"{}\" FROM '{}' (FORMAT CSV, HEADER TRUE, NULLSTR '\\N')",
                                        table, escaped_path
                                    );
                                    if let Err(e) = self.conn.execute_batch(&sql) {
                                        error!("Bulk load error for {}: {e}", table);
                                    } else {
                                        info!("StoreWriter: Bulk load for '{}' complete.", table);
                                    }
                                    if let Err(e) = std::fs::remove_file(&csv_path) {
                                        warn!("Failed to delete temp CSV file {}: {}", csv_path, e);
                                    }
                                    if let Some(cp) = checkpoint {
                                        if let Err(e) = self.persist_checkpoint(&cp) {
                                            error!("Failed to persist checkpoint after bulk load: {e}");
                                            self.metrics.writer_halted.store(1, Ordering::Relaxed);
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            info!("CDC event channel closed — draining final batch.");
                            if !self.coalescer.is_empty() {
                                if let Err(e) = self.flush_batch() {
                                    error!("Final flush error: {e}");
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        info!("StoreWriter terminated cleanly.");
        Ok(())
    }

    /// Apply an intercepted DDL statement to the local DuckDB store.
    ///
    /// The DDL and the binlog checkpoint are applied in a single DuckDB transaction:
    /// either both land or neither does.
    fn apply_ddl(
        &mut self,
        table: &str,
        raw_sql: &str,
        op: &DdlOp,
        new_schema: Option<&TableSchema>,
    ) -> anyhow::Result<()> {
        let current_source = self.layouts.get(table).map(|l| l.source.clone());

        let statements: Vec<String> = match op {
            DdlOp::Drop => vec![format!("DROP TABLE IF EXISTS \"{}\"", table)],
            DdlOp::Truncate => vec![format!("DELETE FROM \"{}\"", table)],
            DdlOp::Rename { from, to } => {
                vec![format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", from, to)]
            }
            DdlOp::Create | DdlOp::Alter => {
                let mut translated_ok = false;
                let mut stmts = match translate_alter(raw_sql, current_source.as_ref(), new_schema)
                {
                    Ok(ops) => {
                        translated_ok = true;
                        ops.into_iter().map(|o| o.sql).collect::<Vec<_>>()
                    }
                    Err(e) => {
                        warn!("DDL translation failed ({e}); falling back to schema diff.");
                        Vec::new()
                    }
                };

                // The authoritative MySQL schema is the ground truth. If the textual
                // translation produced nothing (or failed), reconcile the local table
                // against the target schema by column name.
                if let Some(target) = new_schema {
                    if stmts.is_empty() {
                        if let Some(cur) = current_source.as_ref() {
                            stmts = diff_schema(cur, target)
                                .map_err(|e| anyhow::anyhow!("schema diff failed: {e}"))?
                                .into_iter()
                                .map(|o| o.sql)
                                .collect();
                        }
                    } else {
                        // Make sure every column of the target schema exists locally.
                        let local = self.local_column_order(table).unwrap_or_default();
                        let missing: Vec<&crate::cdc::types::ColumnDef> = target
                            .columns
                            .iter()
                            .filter(|c| !local.iter().any(|l| l == &c.name))
                            .collect();
                        for col in missing {
                            stmts.push(format!(
                                "ALTER TABLE \"{}\" ADD COLUMN IF NOT EXISTS \"{}\" {}",
                                table,
                                col.name,
                                col.duck_type.to_ddl()
                            ));
                        }
                    }
                }

                // When `new_schema` is provided (or `translate_alter` succeeded for a known
                // table and produced no statements because the altered column was excluded by
                // projection), an empty statement list is a legitimate no-op on the projected
                // DuckDB table.
                let is_noop_for_projection =
                    translated_ok && current_source.is_some() && new_schema.is_some();
                if stmts.is_empty() && !is_noop_for_projection {
                    return Err(anyhow::anyhow!(
                        "unable to translate DDL for table '{table}': {raw_sql}"
                    ));
                }
                stmts
            }
        };

        // Execute the DDL atomically.
        let tx = self.conn.transaction()?;
        for sql in &statements {
            debug!("Executing DDL: {sql}");
            tx.execute_batch(sql)
                .map_err(|e| anyhow::anyhow!("failed to execute DDL '{sql}': {e}"))?;
        }
        tx.commit()?;

        // Refresh the cached layout so subsequent rows bind to the new shape.
        self.refresh_layout(table, op, raw_sql, new_schema)?;
        Ok(())
    }

    /// Update the cached table layout after a DDL was applied.
    fn refresh_layout(
        &mut self,
        table: &str,
        op: &DdlOp,
        raw_sql: &str,
        new_schema: Option<&TableSchema>,
    ) -> anyhow::Result<()> {
        match op {
            DdlOp::Drop => {
                self.layouts.remove(table);
            }
            DdlOp::Rename { from, to } => {
                if let Some(mut layout) = self.layouts.remove(from) {
                    layout.source.table_name = to.clone();
                    self.layouts.insert(to.clone(), layout);
                }
            }
            DdlOp::Create | DdlOp::Alter => {
                let mut source = match new_schema {
                    // The authoritative schema observed in MySQL wins.
                    Some(s) => s.clone(),
                    None => match self.layouts.get(table) {
                        Some(l) => {
                            let mut s = l.source.clone();
                            apply_ddl_to_source_schema(raw_sql, &mut s)?;
                            s
                        }
                        None => return Ok(()),
                    },
                };
                source.table_name = table.to_string();
                self.install_layout(source)?;
            }
            DdlOp::Truncate => {
                // The shape of the table is unchanged.
            }
        }
        Ok(())
    }

    /// Persist a binlog checkpoint in its own transaction.
    fn persist_checkpoint(&mut self, cp: &BinlogCheckpoint) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO _xloka_checkpoint \
             (source_id, binlog_file, binlog_pos, gtid_set, event_ts_ms, updated_at) \
             VALUES (?, ?, ?, ?, ?, current_timestamp);",
            duckdb::params![
                cp.source_id,
                cp.binlog_file,
                cp.binlog_pos,
                cp.gtid_set,
                cp.event_ts_ms
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Flush the accumulated coalesced batch to DuckDB using native transactions and Appender.
    ///
    /// The coalescer is only drained AFTER a successful commit: if the transaction
    /// fails and rolls back, every pending operation is still in the coalescer and
    /// will be retried on the next flush, so no batch is ever silently lost.
    pub fn flush_batch(&mut self) -> anyhow::Result<()> {
        if self.coalescer.is_empty() {
            return Ok(());
        }

        let t0 = Instant::now();
        self.batch_counter += 1;
        let batch_id = self.batch_counter;

        let latest_checkpoint = self.coalescer.latest_checkpoint().cloned();
        let latest_ts = self.coalescer.latest_event_ts_ms();

        let mut affected_tables = HashSet::new();
        let mut row_counts = HashMap::new();
        let mut broadcast_deltas = Vec::new();
        let mut total_rows_appended = 0;

        // Primary keys superseded by a LATER primary-key change in the same batch.
        // Appending such a row would resurrect a key MySQL has already moved away from.
        let stale_pks = self.compute_stale_pks();

        for table_name in self.coalescer.tables.keys() {
            if !self.layouts.contains_key(table_name) {
                warn!(
                    "No schema known for table '{table_name}' — rows for this table are dropped. \
                     This usually means the initial schema fetch failed."
                );
                self.metrics
                    .rows_dropped_unknown_table
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let tx = self.conn.transaction()?;

        for (table_name, pk_ops) in &self.coalescer.tables {
            affected_tables.insert(table_name.clone());
            row_counts.insert(table_name.clone(), pk_ops.len());

            let layout = match self.layouts.get(table_name) {
                Some(l) => l.clone(),
                None => {
                    warn!("Could not ensure table schema for '{}'", table_name);
                    continue;
                }
            };

            let schema = &layout.source;
            let pk_cols = schema.primary_keys();
            let pk_indices = schema.primary_key_indices();

            let mut deletes: Vec<Vec<Value>> = Vec::new();
            let mut appends: Vec<Vec<Value>> = Vec::new();

            for (pk_vals, op) in pk_ops {
                let op_str = match &op {
                    CoalescedRowOp::Insert { .. } => "insert",
                    CoalescedRowOp::Update { .. } => "update",
                    CoalescedRowOp::Delete { .. } => "delete",
                };

                // Skip appending a row whose primary key was already superseded by a
                // later primary-key change in the same batch (chained pk updates).
                let is_stale = stale_pks
                    .get(table_name)
                    .map(|keys| keys.contains(pk_vals))
                    .unwrap_or(false);

                if !is_stale && broadcast_deltas.len() < self.max_delta_broadcast_rows {
                    let pk_json: Vec<serde_json::Value> =
                        pk_vals.iter().map(|v| v.clone().into()).collect();
                    let row_json = op.after_row().map(|r| {
                        let row_map: serde_json::Map<String, serde_json::Value> = r
                            .iter()
                            .enumerate()
                            .map(|(idx, v)| {
                                let col_name = schema
                                    .columns
                                    .get(idx)
                                    .map(|c| c.name.clone())
                                    .unwrap_or_else(|| format!("col_{idx}"));
                                (col_name, v.clone().into())
                            })
                            .collect();
                        serde_json::Value::Object(row_map)
                    });

                    broadcast_deltas.push(TableDelta {
                        table: table_name.clone(),
                        op: op_str.into(),
                        pk: pk_json,
                        row: row_json,
                    });
                }

                match op {
                    CoalescedRowOp::Insert { after, .. } => {
                        if !is_stale {
                            // Delete any pre-existing row with the same PK first so that
                            // crash-recovery binlog replay and REPLACE INTO are idempotent
                            // against DuckDB PRIMARY KEY constraints.
                            deletes.push(pk_vals.clone());
                            appends.push(layout.project(after));
                        }
                    }
                    CoalescedRowOp::Update { before, after, .. } => {
                        deletes.push(pk_vals.clone());
                        // A primary key that changed within the window leaves the
                        // OLD row behind unless the before-pk is deleted as well.
                        if let Some(before_row) = before {
                            let before_pk: Vec<Value> = pk_indices
                                .iter()
                                .map(|&i| before_row.get(i).cloned().unwrap_or(Value::Null))
                                .collect();
                            if before_pk != *pk_vals {
                                deletes.push(before_pk);
                            }
                        }
                        if !is_stale {
                            appends.push(layout.project(after));
                        }
                    }
                    CoalescedRowOp::Delete { before, .. } => {
                        deletes.push(pk_vals.clone());
                        // Covers Update(pk-change)+Delete coalesced into a single
                        // Delete: the before image still references the OLD key.
                        if let Some(before_row) = before {
                            let before_pk: Vec<Value> = pk_indices
                                .iter()
                                .map(|&i| before_row.get(i).cloned().unwrap_or(Value::Null))
                                .collect();
                            if before_pk != *pk_vals {
                                deletes.push(before_pk);
                            }
                        }
                    }
                }
            }

            // 1. Execute batched DELETE for all keys in `deletes`
            if !deletes.is_empty() && !pk_cols.is_empty() {
                for chunk in deletes.chunks(500) {
                    if pk_cols.len() == 1 {
                        let pk_col = pk_cols[0];
                        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                        let delete_sql = format!(
                            "DELETE FROM \"{}\" WHERE \"{}\" IN ({})",
                            table_name, pk_col, placeholders
                        );
                        let mut params: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len());
                        for pk in chunk {
                            if let Some(first_val) = pk.first() {
                                params.push(first_val as &dyn ToSql);
                            }
                        }
                        tx.execute(&delete_sql, params.as_slice())?;
                    } else {
                        // Composite primary key
                        let mut or_clauses = Vec::new();
                        let mut params: Vec<&dyn ToSql> = Vec::new();
                        for pk in chunk {
                            let mut and_parts = Vec::new();
                            for (i, pk_col) in pk_cols.iter().enumerate() {
                                and_parts.push(format!("\"{}\" = ?", pk_col));
                                if let Some(v) = pk.get(i) {
                                    params.push(v as &dyn ToSql);
                                }
                            }
                            or_clauses.push(format!("({})", and_parts.join(" AND ")));
                        }
                        let delete_sql = format!(
                            "DELETE FROM \"{}\" WHERE {}",
                            table_name,
                            or_clauses.join(" OR ")
                        );
                        tx.execute(&delete_sql, params.as_slice())?;
                    }
                }
            }

            // 2. Append all Insert and Update.after rows using DuckDB Appender
            if !appends.is_empty() {
                total_rows_appended += appends.len();
                let mut appender = tx.appender(table_name.as_str())?;
                for row in &appends {
                    let refs: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
                    appender.append_row(refs.as_slice())?;
                }
                appender.flush()?;
                drop(appender);
            }
        }

        // 3. Atomically persist checkpoint in the same transaction
        if let Some(cp) = latest_checkpoint {
            tx.execute(
                "INSERT OR REPLACE INTO _xloka_checkpoint \
                 (source_id, binlog_file, binlog_pos, gtid_set, event_ts_ms, updated_at) \
                 VALUES (?, ?, ?, ?, ?, current_timestamp);",
                duckdb::params![
                    cp.source_id,
                    cp.binlog_file,
                    cp.binlog_pos,
                    cp.gtid_set,
                    cp.event_ts_ms
                ],
            )?;
        }

        tx.commit()?;

        // The batch is durable — now it is safe to drop it from the coalescer.
        self.coalescer.drain_tables();

        // Update metrics
        let elapsed_us = t0.elapsed().as_micros() as u64;
        self.metrics
            .duckdb_batches_committed_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .duckdb_rows_written_total
            .fetch_add(total_rows_appended as u64, Ordering::Relaxed);
        self.metrics
            .cdc_coalesced_ops_saved_total
            .store(self.coalescer.coalesced_ops_saved(), Ordering::Relaxed);
        self.metrics
            .last_batch_flush_duration_us
            .store(elapsed_us, Ordering::Relaxed);
        self.metrics.update_lag_from_ts(latest_ts);
        if let Some(ref tel) = self.telemetry {
            tel.add_rows_synced(total_rows_appended as u64);
            tel.update_lag_from_ts(latest_ts);
        }

        // Broadcast non-blocking commit notification to WebSocket subscribers
        let lag_for_notif = self
            .telemetry
            .as_ref()
            .map(|t| t.lag_ms())
            .unwrap_or_else(|| self.metrics.replication_lag_ms());
        let notif = CommitNotification {
            batch_id,
            committed_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            replication_lag_ms: lag_for_notif,
            account_id: self.account_id.clone(),
            db_path: self.settings.db_path.clone(),
            affected_tables: Arc::new(affected_tables),
            row_counts: Arc::new(row_counts),
            deltas: Arc::new(broadcast_deltas),
        };
        let _ = self.commit_tx.send(notif);

        self.coalescer.reset_window();
        Ok(())
    }

    /// Compute the primary keys that are superseded by a *later* primary-key change
    /// within the current batch, per table.
    ///
    /// The coalescer keys updates by their **after** primary key. So
    /// `UPDATE t SET id=2 WHERE id=1` followed by `UPDATE t SET id=3 WHERE id=2`
    /// produces two entries (keyed `2` and `3`). Appending both would resurrect the
    /// intermediate row `id=2`, which MySQL has already deleted. An entry is stale
    /// when a *later* entry consumed the key it produced.
    ///
    /// The same applies to an insert followed by a primary-key change: the insert
    /// is keyed by the old primary key, the update consumes it.
    ///
    /// Ordering uses the coalescer's insertion order (an `IndexMap`), not the
    /// event timestamp — binlog timestamps only have second granularity, so two
    /// updates within the same second are indistinguishable by time.
    fn compute_stale_pks(&self) -> HashMap<String, HashSet<Vec<Value>>> {
        let mut stale: HashMap<String, HashSet<Vec<Value>>> = HashMap::new();

        for (table_name, pk_ops) in &self.coalescer.tables {
            let pk_indices = self
                .layouts
                .get(table_name)
                .map(|l| l.source.primary_key_indices())
                .unwrap_or_default();

            // key produced by an entry -> position of that entry
            let mut produced: HashMap<Vec<Value>, usize> = HashMap::new();
            // key consumed by an entry -> position of that entry
            let mut consumed: HashMap<Vec<Value>, usize> = HashMap::new();

            for (position, (pk_vals, op)) in pk_ops.iter().enumerate() {
                produced.insert(pk_vals.clone(), position);
                if let Some(before_row) = op.before_row() {
                    let before_pk: Vec<Value> = pk_indices
                        .iter()
                        .map(|&i| before_row.get(i).cloned().unwrap_or(Value::Null))
                        .collect();
                    if before_pk != *pk_vals {
                        consumed
                            .entry(before_pk)
                            .and_modify(|pos| *pos = (*pos).max(position))
                            .or_insert(position);
                    }
                }
            }

            // An entry is stale when a LATER entry consumed the key it produced.
            let table_stale = stale.entry(table_name.clone()).or_default();
            for (key, produced_pos) in produced {
                if let Some(consumed_pos) = consumed.get(&key) {
                    if *consumed_pos > produced_pos {
                        table_stale.insert(key);
                    }
                }
            }
        }

        stale
    }
}
