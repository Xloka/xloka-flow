//! DuckDB writer — receives `RowOp`s from the CDC channel, applies schema
//! migrations, and batches upserts/deletes into DuckDB.
//!
//! ## Concurrency model
//! - This module owns the **single write connection** to DuckDB.
//! - API query threads open their own **read-only connections** independently.
//! - Writes are batched: flush when `batch_size` rows accumulate OR every
//!   `flush_interval` elapses — whichever comes first.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use duckdb::Connection;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::cdc::{ColumnDef, RowOp, TableSchema, Value};
use crate::error::FlowResult;
use crate::Settings;

// ─── Cached schema state ──────────────────────────────────────────────────────

/// What we know about the DuckDB side of one table.
#[derive(Debug, Clone)]
struct CachedSchema {
    columns: Vec<ColumnDef>,
}

// ─── Writer ───────────────────────────────────────────────────────────────────

pub struct StoreWriter {
    settings: Arc<Settings>,
}

impl StoreWriter {
    pub fn new(settings: Arc<Settings>) -> Self {
        StoreWriter { settings }
    }

    /// Open the write connection.
    fn open_write_conn(&self) -> FlowResult<Connection> {
        let conn = Connection::open(&self.settings.db_path)?;
        // Note: DuckDB does not support PRAGMA (SQLite-only). Busy timeout is
        // handled by DuckDB's internal locking. No extra config needed.
        Ok(conn)
    }

    /// Run the writer loop.  Consumes the receiver end of the CDC channel.
    ///
    /// This is intentionally `async` so we can use `tokio::time::interval` for
    /// flush timing, but the DuckDB calls themselves are synchronous blocking
    /// calls — they are cheap enough that `spawn_blocking` is not needed for
    /// batch sizes ≤ 10k rows.
    pub async fn run(self, mut rx: mpsc::Receiver<RowOp>) -> FlowResult<()> {
        info!(
            "StoreWriter starting — db={}, batch_size={}, flush_interval={}ms",
            self.settings.db_path,
            self.settings.batch_size,
            self.settings.flush_interval_ms
        );

        let conn = self.open_write_conn()?;

        // Cached table schemas so we can do schema diffing without querying DuckDB every time.
        let mut schema_cache: HashMap<String, CachedSchema> = HashMap::new();

        // Pending batch (table → list of ops).
        let mut batch: Vec<RowOp> = Vec::with_capacity(self.settings.batch_size);
        let flush_interval =
            std::time::Duration::from_millis(self.settings.flush_interval_ms);
        let mut last_flush = Instant::now();

        loop {
            // Try to receive the next op, but also honour the flush interval.
            let timeout = flush_interval.saturating_sub(last_flush.elapsed());
            let maybe_op = tokio::time::timeout(timeout, rx.recv()).await;

            match maybe_op {
                // Timeout elapsed — flush whatever we have.
                Err(_) => {
                    if !batch.is_empty() {
                        if let Err(e) = flush_batch(&conn, &mut batch, &mut schema_cache) {
                            error!("Flush error: {e}");
                        }
                        last_flush = Instant::now();
                    }
                }
                // Channel closed — drain and exit.
                Ok(None) => {
                    info!("CDC channel closed — flushing final batch and shutting down.");
                    if !batch.is_empty() {
                        if let Err(e) = flush_batch(&conn, &mut batch, &mut schema_cache) {
                            error!("Final flush error: {e}");
                        }
                    }
                    break;
                }
                // Got an op.
                Ok(Some(op)) => {
                    // SchemaChange ops are flushed immediately — they must be applied
                    // before any subsequent row ops for the same table.
                    if matches!(op, RowOp::SchemaChange(_)) {
                        // Flush any pending rows first.
                        if !batch.is_empty() {
                            if let Err(e) =
                                flush_batch(&conn, &mut batch, &mut schema_cache)
                            {
                                error!("Pre-schema flush error: {e}");
                            }
                            last_flush = Instant::now();
                        }
                        if let RowOp::SchemaChange(schema) = &op {
                            if let Err(e) =
                                reconcile_schema(&conn, schema, &mut schema_cache)
                            {
                                error!("Schema reconcile error for {}: {e}", schema.table_name);
                            }
                        }
                    } else {
                        batch.push(op);
                        if batch.len() >= self.settings.batch_size {
                            if let Err(e) =
                                flush_batch(&conn, &mut batch, &mut schema_cache)
                            {
                                error!("Batch flush error: {e}");
                            }
                            last_flush = Instant::now();
                        }
                    }
                }
            }
        }

        info!("StoreWriter shut down cleanly.");
        Ok(())
    }
}

// ─── Schema reconciliation ────────────────────────────────────────────────────

fn reconcile_schema(
    conn: &Connection,
    incoming: &TableSchema,
    cache: &mut HashMap<String, CachedSchema>,
) -> FlowResult<()> {
    let table = &incoming.table_name;

    match cache.get(table) {
        None => {
            // Table doesn't exist yet — CREATE TABLE.
            let ddl = incoming.create_ddl();
            debug!("Creating table:\n{ddl}");
            conn.execute_batch(&ddl)?;
            cache.insert(
                table.clone(),
                CachedSchema {
                    columns: incoming.columns.clone(),
                },
            );
            info!("Created table '{table}'");
        }
        Some(cached) => {
            // Table exists — find new columns and ALTER TABLE ADD COLUMN for each.
            let existing_names: std::collections::HashSet<&str> =
                cached.columns.iter().map(|c| c.name.as_str()).collect();

            let mut new_cols = Vec::new();
            for col in &incoming.columns {
                if !existing_names.contains(col.name.as_str()) {
                    new_cols.push(col);
                }
            }

            for col in &new_cols {
                let sql = format!(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {} {}",
                    col.name,
                    col.duck_type.to_ddl()
                );
                debug!("Schema evolution: {sql}");
                conn.execute_batch(&sql)?;
                info!("Added column '{}.{}'", table, col.name);
            }

            if !new_cols.is_empty() {
                // Update cache with merged column set.
                let mut merged = cached.columns.clone();
                for col in new_cols {
                    merged.push(col.clone());
                }
                cache.insert(table.clone(), CachedSchema { columns: merged });
            }
        }
    }

    Ok(())
}

// ─── Batch flushing ───────────────────────────────────────────────────────────

fn flush_batch(
    conn: &Connection,
    batch: &mut Vec<RowOp>,
    cache: &mut HashMap<String, CachedSchema>,
) -> FlowResult<()> {
    if batch.is_empty() {
        return Ok(());
    }

    debug!("Flushing {} ops", batch.len());

    // Group by table so we can use a single prepared statement per table.
    let ops = std::mem::take(batch);

    // Process sequentially (ordered CDC semantics matter).
    for op in ops {
        match op {
            RowOp::Insert { table, row } => {
                apply_upsert(conn, &table, row, cache)?;
            }
            RowOp::Update { table, after, .. } => {
                apply_upsert(conn, &table, after, cache)?;
            }
            RowOp::Delete { table, pk_values } => {
                apply_delete(conn, &table, pk_values, cache)?;
            }
            RowOp::SchemaChange(_) => {
                // Should never appear here — handled before batching.
                warn!("SchemaChange inside batch (bug): skipping");
            }
        }
    }

    Ok(())
}

// ─── Upsert ───────────────────────────────────────────────────────────────────

fn apply_upsert(
    conn: &Connection,
    table: &str,
    row: HashMap<String, Value>,
    cache: &HashMap<String, CachedSchema>,
) -> FlowResult<()> {
    if row.is_empty() {
        return Ok(());
    }

    // Get PK columns from cache (for ON CONFLICT clause).
    let pk_cols: Vec<String> = cache
        .get(table)
        .map(|s| {
            s.columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect()
        })
        .unwrap_or_default();

    let cols: Vec<&String> = row.keys().collect();
    let col_list = cols.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", ");

    let values_sql = cols
        .iter()
        .map(|k| row.get(*k).map(|v| v.to_string()).unwrap_or("NULL".into()))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = if !pk_cols.is_empty() {
        // Build the UPDATE SET part (all non-PK columns).
        let update_set = cols
            .iter()
            .filter(|c| !pk_cols.contains(*c))
            .map(|c| {
                let v = row.get(*c).map(|v| v.to_string()).unwrap_or("NULL".into());
                format!("{c} = {v}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        let conflict_cols = pk_cols.join(", ");
        if update_set.is_empty() {
            // Only PK columns — INSERT OR IGNORE
            format!(
                "INSERT INTO {table} ({col_list}) VALUES ({values_sql}) ON CONFLICT DO NOTHING"
            )
        } else {
            format!(
                "INSERT INTO {table} ({col_list}) VALUES ({values_sql}) \
                 ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        }
    } else {
        // No known PK — plain INSERT IGNORE
        format!("INSERT INTO {table} ({col_list}) VALUES ({values_sql}) ON CONFLICT DO NOTHING")
    };

    debug!("UPSERT: {sql}");
    conn.execute_batch(&sql)?;
    Ok(())
}

// ─── Delete ───────────────────────────────────────────────────────────────────

fn apply_delete(
    conn: &Connection,
    table: &str,
    pk_values: HashMap<String, Value>,
    _cache: &HashMap<String, CachedSchema>,
) -> FlowResult<()> {
    if pk_values.is_empty() {
        warn!("Delete on '{table}' with empty pk_values — skipping");
        return Ok(());
    }

    let where_clause = pk_values
        .iter()
        .map(|(k, v)| format!("{k} = {v}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    let sql = format!("DELETE FROM {table} WHERE {where_clause}");
    debug!("DELETE: {sql}");
    conn.execute_batch(&sql)?;
    Ok(())
}
