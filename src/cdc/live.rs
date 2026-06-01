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
use crate::error::FlowResult;
use crate::Checkpoint;

// ─── Config ───────────────────────────────────────────────────────────────────

pub struct LiveCdcConfig {
    /// MySQL connection URL.  E.g. `mysql://user:pass@host:3306/db`
    pub mysql_url: String,
    /// Optional: Read-only replica for the initial snapshot phase.
    pub snapshot_mysql_url: Option<String>,
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
    async fn run(self, tx: mpsc::Sender<RowOp>) -> FlowResult<()> {
        info!(
            "LiveCdc: connecting to MySQL at {}",
            mask_password(&self.config.mysql_url)
        );

        // ── 1. Verify connectivity and binlog settings ────────────────────────
        let pool = mysql_async::Pool::new(self.config.mysql_url.as_str());
        let mut conn = pool.get_conn().await?;
        conn.query_drop("SET NAMES utf8mb4").await?;

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
        let mut checkpoint = Checkpoint::load(&self.config.checkpoint_path);
        info!("LiveCdc: resuming from checkpoint: {:?}", checkpoint);

        // ── 1.5. Initial Schema Sync ─────────────────────────────────────────
        let filter_tables = if self.config.filter_tables.is_empty() {
            // Get all base tables in the database (exclude VIEWs)
            let rows: Vec<String> = conn.query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'").await?;
            rows
        } else {
            self.config.filter_tables.clone()
        };

        let mut table_schemas = HashMap::new();
        for table_name in &filter_tables {
            if let Some(schema) = fetch_table_schema(&pool, table_name).await? {
                table_schemas.insert(table_name.clone(), schema.clone());
                if let Err(e) = tx.send(RowOp::SchemaChange(schema)).await {
                    warn!("Failed to send SchemaChange for {}: {}", table_name, e);
                }
            } else {
                warn!("LiveCdc: schema for table '{}' not found in database.", table_name);
            }
        }

        // ── 1.8. Initial Snapshot Sync (if needed) ───────────────────────────
        if checkpoint.filename.is_none() {
            info!("LiveCdc: No checkpoint found. Starting Initial Snapshot Sync.");
            
            // Determine which connection to use for the snapshot
            let mut snap_conn = if let Some(ref snap_url) = self.config.snapshot_mysql_url {
                info!("LiveCdc: Using Snapshot Replica: {}", mask_password(snap_url));
                let snap_pool = mysql_async::Pool::new(snap_url.as_str());
                snap_pool.get_conn().await?
            } else {
                pool.get_conn().await?
            };
            
            snap_conn.query_drop("SET NAMES utf8mb4").await?;

            // Start a consistent snapshot transaction
            snap_conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;
            snap_conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT").await?;
            
            // Capture binlog coordinates for this exact snapshot
            let (target_file, target_pos) = if self.config.snapshot_mysql_url.is_some() {
                // If using a replica, we need to know where it is relative to the master
                // Try SHOW REPLICA STATUS (MySQL 8+) first, fallback to SHOW SLAVE STATUS (MySQL 5.7)
                let mut status_row: Option<mysql_async::Row> = snap_conn.query_first("SHOW REPLICA STATUS").await.unwrap_or(None);
                if status_row.is_none() {
                    status_row = snap_conn.query_first("SHOW SLAVE STATUS").await.unwrap_or(None);
                }
                
                if let Some(row) = status_row {
                    // Extract Relay_Master_Log_File and Exec_Master_Log_Pos
                    // These columns might be retrieved by index or name, but for SHOW SLAVE STATUS it's safer to use names.
                    // However, `mysql_async::Row` allows getting by column name.
                    let file: Option<String> = row.get("Relay_Master_Log_File").or_else(|| row.get(9)); // index 9 is traditionally Relay_Master_Log_File
                    let pos: Option<u64> = row.get("Exec_Master_Log_Pos").or_else(|| row.get(21)); // index 21 is traditionally Exec_Master_Log_Pos
                    if file.is_none() || pos.is_none() {
                        warn!("LiveCdc: Could not determine Master Log position from Replica Status. Check if it's actually a replicating slave!");
                    }
                    (file, pos)
                } else {
                    warn!("LiveCdc: SHOW REPLICA/SLAVE STATUS returned empty. Are you sure this is a replica?");
                    (None, None)
                }
            } else {
                // If using master directly, SHOW MASTER STATUS gives us the current master position
                let master_status: Option<mysql_async::Row> = snap_conn.query_first("SHOW MASTER STATUS").await?;
                if let Some(row) = master_status {
                    (row.get::<String, _>(0), row.get::<u64, _>(1))
                } else {
                    (None, None)
                }
            };
            
            info!("LiveCdc: Snapshot target binlog coordinates: file={:?} pos={:?}", target_file, target_pos);
            
            // Dump each table
            use futures::StreamExt;
            for table_name in &filter_tables {
                if let Some(schema) = table_schemas.get(table_name) {
                    info!("LiveCdc: Snapshotting table '{}'...", table_name);
                    
                    let temp_dir = std::path::Path::new("data");
                    std::fs::create_dir_all(temp_dir).unwrap_or_default();
                    let tmp = tempfile::Builder::new()
                        .prefix(&format!("{table_name}_snap_"))
                        .suffix(".csv")
                        .tempfile_in(temp_dir)
                        .expect("Failed to create temporary CSV file");
                    
                    let (tmp_file, tmp_path) = tmp.keep().expect("Failed to keep tempfile");
                    // Fix windows path separators for duckdb
                    let tmp_path_str = tmp_path.to_string_lossy().replace('\\', "/");
                    
                    let mut wtr = csv::WriterBuilder::new()
                        .has_headers(false) // We write our own header
                        .from_writer(std::io::BufWriter::new(tmp_file));
                    
                    let headers: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
                    wtr.write_record(&headers).unwrap();
                    
                    let mut stream = snap_conn.query_stream(format!("SELECT * FROM `{}`", table_name)).await?;
                    let mut row_count = 0;
                    
                    while let Some(row_result) = stream.next().await {
                        let row: mysql_async::Row = row_result?;
                        let mut record = Vec::with_capacity(schema.columns.len());
                        
                        for (idx, _) in schema.columns.iter().enumerate() {
                            let val = row.get::<mysql_async::Value, _>(idx).unwrap_or(mysql_async::Value::NULL);
                            use mysql_async::Value as MyVal;
                            let s = match val {
                                MyVal::NULL => "\\N".to_string(),
                                MyVal::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
                                MyVal::Int(i) => i.to_string(),
                                MyVal::UInt(u) => u.to_string(),
                                MyVal::Float(f) => f.to_string(),
                                MyVal::Double(d) => d.to_string(),
                                MyVal::Date(y, m, d, h, mn, s, _) => {
                                    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mn, s)
                                }
                                MyVal::Time(is_neg, d, h, m, s, _) => {
                                    let sign = if is_neg { "-" } else { "" };
                                    format!("{}{:02}:{:02}:{:02}", sign, d * 24 + h as u32, m, s)
                                }
                            };
                            record.push(s);
                        }
                        wtr.write_record(&record).unwrap();
                        row_count += 1;
                    }
                    
                    wtr.flush().unwrap();
                    info!("LiveCdc: Downloaded {} rows. Sending BulkLoadCsv to writer.", row_count);
                    
                    let op = RowOp::BulkLoadCsv {
                        table: table_name.clone(),
                        csv_path: tmp_path_str,
                    };
                    if let Err(e) = tx.send(op).await {
                        warn!("LiveCdc failed to send BulkLoadCsv during snapshot: {}", e);
                    }
                    info!("LiveCdc: Snapshot for '{}' complete ({} rows).", table_name, row_count);
                }
            }
            
            // Commit and save checkpoint
            snap_conn.query_drop("COMMIT").await?;
            drop(snap_conn); // Release connection back to pool / drop replica connection

            checkpoint.filename = target_file;
            checkpoint.position = target_pos;
            let _ = checkpoint.save(&self.config.checkpoint_path);
            info!("LiveCdc: Initial Snapshot Sync complete. Checkpoint saved.");
        }

        // ── 2. Full binlog streaming ──────────────────────────────────────────
        use mysql_async::BinlogStreamRequest;
        use mysql_common::binlog::events::{EventData, RowsEventData};
        use futures::StreamExt;

        let server_id = self.config.server_id;
        let mut request = BinlogStreamRequest::new(server_id);
        
        if let Some(ref file) = checkpoint.filename {
            let mut req = request.with_filename(file.as_bytes());
            if let Some(pos) = checkpoint.position {
                req = req.with_pos(pos);
            }
            request = req;
        }

        info!("LiveCdc: starting binlog stream...");
        let mut stream = conn.get_binlog_stream(request).await?;

        use std::collections::HashMap;
        let mut table_maps = HashMap::new();

        while let Some(event) = stream.next().await {
            let event = event?;
            
            // Checkpoint tracking
            let header = event.header();
            let pos = header.log_pos();

            if let Ok(Some(event_data)) = event.read_data() {
                match event_data {
                    EventData::TableMapEvent(tm) => {
                        table_maps.insert(tm.table_id(), tm.into_owned());
                    }
                    EventData::RowsEvent(RowsEventData::WriteRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let table_name = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if self.config.filter_tables.is_empty() || self.config.filter_tables.contains(&table_name) {
                                for row_result in ev.rows(tm) {
                                    if let Ok((_, Some(after_row))) = row_result {
                                        let schema = table_schemas.get(&table_name);
                                        let row_map = parse_row(after_row, schema);
                                        let op = RowOp::Insert {
                                            table: table_name.clone(),
                                            row: row_map,
                                        };
                                        if let Err(e) = tx.send(op).await {
                                            warn!("LiveCdc failed to send Insert: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    EventData::RowsEvent(RowsEventData::UpdateRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let table_name = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if self.config.filter_tables.is_empty() || self.config.filter_tables.contains(&table_name) {
                                for row_result in ev.rows(tm) {
                                    if let Ok((Some(before_row), Some(after_row))) = row_result {
                                        let schema = table_schemas.get(&table_name);
                                        let before_map = parse_row(before_row, schema);
                                        let after_map = parse_row(after_row, schema);
                                        let op = RowOp::Update {
                                            table: table_name.clone(),
                                            before: Some(before_map),
                                            after: after_map,
                                        };
                                        if let Err(e) = tx.send(op).await {
                                            warn!("LiveCdc failed to send Update: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    EventData::RowsEvent(RowsEventData::DeleteRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let table_name = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if self.config.filter_tables.is_empty() || self.config.filter_tables.contains(&table_name) {
                                for row_result in ev.rows(tm) {
                                    if let Ok((Some(before_row), _)) = row_result {
                                        let schema = table_schemas.get(&table_name);
                                        let row_map = parse_row(before_row, schema);
                                        // Delete requires pk_values, we just pass the full row and let StoreWriter filter it.
                                        let op = RowOp::Delete {
                                            table: table_name.clone(),
                                            pk_values: row_map,
                                        };
                                        if let Err(e) = tx.send(op).await {
                                            warn!("LiveCdc failed to send Delete: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    EventData::RotateEvent(ev) => {
                        let file_name = String::from_utf8_lossy(ev.name_raw()).into_owned();
                        checkpoint.filename = Some(file_name);
                        checkpoint.position = Some(pos as u64);
                        let _ = checkpoint.save(&self.config.checkpoint_path);
                    }
                    EventData::XidEvent(_) => {
                        checkpoint.position = Some(pos as u64);
                        let _ = checkpoint.save(&self.config.checkpoint_path);
                    }
                    EventData::QueryEvent(ev) => {
                        checkpoint.position = Some(pos as u64);
                        let _ = checkpoint.save(&self.config.checkpoint_path);
                        
                        let sql = ev.query().into_owned();
                        let sql_upper = sql.trim().to_uppercase();
                        if sql_upper.starts_with("ALTER TABLE") {
                            let parts: Vec<&str> = sql.split_whitespace().collect();
                            if parts.len() > 2 {
                                let raw_table = parts[2].trim_matches(|c| c == '`' || c == '"' || c == '\'');
                                if self.config.filter_tables.is_empty() || self.config.filter_tables.contains(&raw_table.to_string()) {
                                    info!("Detected ALTER TABLE for {}, refreshing schema...", raw_table);
                                    if let Ok(Some(schema)) = fetch_table_schema(&pool, raw_table).await {
                                        table_schemas.insert(raw_table.to_string(), schema.clone());
                                        if let Err(e) = tx.send(RowOp::SchemaChange(schema)).await {
                                            warn!("Failed to send SchemaChange for {}: {}", raw_table, e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn value_from_mysql(val: mysql_async::Value) -> crate::cdc::Value {
    use mysql_async::Value as MyVal;
    use crate::cdc::Value as OurVal;
    match val {
        MyVal::NULL => OurVal::Null,
        MyVal::Bytes(b) => {
            if let Ok(s) = String::from_utf8(b.clone()) {
                OurVal::Text(s)
            } else {
                OurVal::Bytes(b)
            }
        }
        MyVal::Int(i) => OurVal::Integer(i),
        MyVal::UInt(u) => OurVal::Integer(u as i64),
        MyVal::Float(f) => OurVal::Float(f as f64),
        MyVal::Double(d) => OurVal::Float(d),
        MyVal::Date(y, m, d, h, mn, s, _ms) => {
            let text = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mn, s);
            OurVal::Text(text)
        }
        MyVal::Time(is_neg, d, h, m, s, _ms) => {
            let sign = if is_neg { "-" } else { "" };
            let text = format!("{}{:02}:{:02}:{:02}", sign, d * 24 + h as u32, m, s);
            OurVal::Text(text)
        }
    }
}

fn parse_row(binlog_row: mysql_common::binlog::row::BinlogRow, schema: Option<&crate::cdc::TableSchema>) -> std::collections::HashMap<String, crate::cdc::Value> {
    use std::convert::TryInto;
    let row: mysql_async::Row = binlog_row.try_into().unwrap();
    let mut map = std::collections::HashMap::new();
    for (idx, _col) in row.columns_ref().iter().enumerate() {
        let name = if let Some(s) = schema {
            if idx < s.columns.len() {
                s.columns[idx].name.clone()
            } else {
                format!("@{}", idx)
            }
        } else {
            format!("@{}", idx)
        };
        let val = row.get::<mysql_async::Value, _>(idx).unwrap_or(mysql_async::Value::NULL);
        map.insert(name, value_from_mysql(val));
    }
    map
}

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

async fn fetch_table_schema(
    pool: &mysql_async::Pool,
    table_name: &str,
) -> FlowResult<Option<crate::cdc::TableSchema>> {
    use mysql_async::prelude::*;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("SET NAMES utf8mb4").await?;
    let sql = format!(
        "SELECT column_name, column_type, is_nullable, column_key \
         FROM information_schema.columns \
         WHERE table_name = '{}' AND table_schema = DATABASE() \
         ORDER BY ordinal_position",
        table_name
    );
    let rows: Vec<(String, String, String, String)> = conn.query(sql).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut columns = Vec::new();
    for (col_name, col_type, is_nullable, col_key) in rows {
        columns.push(crate::cdc::ColumnDef {
            name: col_name,
            duck_type: crate::cdc::DuckDbType::from_mysql(&col_type),
            nullable: is_nullable.eq_ignore_ascii_case("YES"),
            is_primary_key: col_key.eq_ignore_ascii_case("PRI"),
        });
    }
    Ok(Some(crate::cdc::TableSchema {
        table_name: table_name.to_string(),
        columns,
    }))
}
