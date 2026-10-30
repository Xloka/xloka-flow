//! Live MySQL CDC via `mysql_async` binlog replication.

use async_trait::async_trait;
use mysql_async::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::accounts::AccountTelemetry;
use crate::cdc::checkpoint::BinlogCheckpoint;
use crate::cdc::filter::{Projection, ReplicationFilter};
use crate::cdc::types::{CdcEvent, ColumnDef, DdlOp, DuckDbType, RowOp, TableSchema, Value};
use crate::cdc::CdcSource;
use crate::error::FlowResult;
use crate::store::writer::StoreWriter;
use crate::Checkpoint;

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LiveCdcConfig {
    /// MySQL connection URL. E.g. `mysql://user:pass@host:3306/db`
    pub mysql_url: String,
    /// Optional: Read-only replica for the initial snapshot phase.
    pub snapshot_mysql_url: Option<String>,
    /// Path where the JSON binlog checkpoint fallback is persisted.
    pub checkpoint_path: String,
    /// Optional DuckDB path to load atomic checkpoint from.
    pub db_path: Option<String>,
    /// Source ID for checkpoint lookup.
    pub source_id: Option<String>,
    /// If non-empty, only replicate rows from these databases.
    pub filter_databases: Vec<String>,
    /// If non-empty, only replicate rows from these tables (legacy shorthand for `filter.table_include`).
    pub filter_tables: Vec<String>,
    /// Optional full table/column replication filter.
    pub filter: Option<ReplicationFilter>,
    /// Unique replication server-id (must not clash with MySQL server's own id).
    pub server_id: u32,
    /// Pre-loaded checkpoint passed from startup (loaded before writer holds the DB lock).
    pub initial_checkpoint: Option<BinlogCheckpoint>,
    /// Maximum number of consecutive reconnect attempts before giving up.
    /// `None` means "retry forever" (daemon mode).
    pub max_reconnect_attempts: Option<u32>,
    /// Optional per-account telemetry collector.
    pub telemetry: Option<Arc<AccountTelemetry>>,
}

impl LiveCdcConfig {
    /// Default configuration used by the CLI and by account spawning.
    pub fn new(mysql_url: String) -> Self {
        Self {
            mysql_url,
            snapshot_mysql_url: None,
            checkpoint_path: "checkpoint.json".into(),
            db_path: None,
            source_id: None,
            filter_databases: Vec::new(),
            filter_tables: Vec::new(),
            filter: None,
            server_id: generate_server_id(),
            initial_checkpoint: None,
            max_reconnect_attempts: None,
            telemetry: None,
        }
    }

    /// Build the effective [`ReplicationFilter`] combining `filter` and legacy `filter_tables`.
    pub fn effective_filter(&self) -> ReplicationFilter {
        match &self.filter {
            Some(f) => {
                let mut f = f.clone();
                if f.table_include.is_empty() && !self.filter_tables.is_empty() {
                    f.table_include = self.filter_tables.clone();
                }
                f
            }
            None => ReplicationFilter::from_table_include(self.filter_tables.clone()),
        }
    }
}

pub struct LiveCdc {
    config: LiveCdcConfig,
}

impl LiveCdc {
    pub fn new(config: LiveCdcConfig) -> Self {
        LiveCdc { config }
    }
}

// ─── Server ID Allocation ────────────────────────────────────────────────────

/// Server-ids handed out by this process, to guarantee that two accounts
/// replicating from the same master never collide (MySQL kills the second
/// stream with `ERROR 1412` / "Duplicate server id").
static ALLOCATED_SERVER_IDS: Mutex<Option<HashSet<u32>>> = Mutex::new(None);

/// Allocate a random, process-unique replication server-id.
///
/// The previous implementation used `42 + rand % 1000`, i.e. only ~1000 distinct
/// values — two accounts on the same master had a realistic chance of colliding.
pub fn generate_server_id() -> u32 {
    let mut guard = ALLOCATED_SERVER_IDS
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let set = guard.get_or_insert_with(HashSet::new);
    loop {
        // Keep clear of the small ids commonly used by real replicas.
        let candidate = 1_000_000 + (rand::random::<u32>() % 3_000_000_000);
        if set.insert(candidate) {
            return candidate;
        }
    }
}

// ─── CdcSource impl ──────────────────────────────────────────────────────────

#[async_trait]
impl CdcSource for LiveCdc {
    async fn run(self, tx: mpsc::Sender<CdcEvent>) -> FlowResult<()> {
        let mut attempt: u32 = 0;
        loop {
            match self.run_once(&tx).await {
                Ok(()) => {
                    // The writer channel was closed — this is a clean shutdown.
                    info!("LiveCdc: replication stream finished, writer closed.");
                    return Ok(());
                }
                Err(e) => {
                    if e.is_fatal_config() {
                        error!("LiveCdc: fatal configuration error (not retrying): {e}");
                        if let Some(ref tel) = self.config.telemetry {
                            tel.set_status("failed");
                            tel.set_error(Some(e.to_string()));
                        }
                        return Err(e);
                    }
                    attempt += 1;
                    if let Some(max) = self.config.max_reconnect_attempts {
                        if attempt > max {
                            error!(
                                "LiveCdc: giving up after {} failed replication attempts: {e}",
                                attempt - 1
                            );
                            if let Some(ref tel) = self.config.telemetry {
                                tel.set_status("failed");
                                tel.set_error(Some(e.to_string()));
                            }
                            return Err(e);
                        }
                    }
                    if let Some(ref tel) = self.config.telemetry {
                        tel.set_status("reconnecting");
                        tel.set_error(Some(e.to_string()));
                    }
                    let delay = reconnect_delay(attempt);
                    warn!(
                        "LiveCdc: replication stream error ({e}) — reconnecting in {}s \
                         (attempt {attempt})",
                        delay.as_secs()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

impl LiveCdc {
    /// Run a single replication session. Reconnects are handled by [`CdcSource::run`].
    async fn run_once(&self, tx: &mpsc::Sender<CdcEvent>) -> FlowResult<()> {
        info!(
            "LiveCdc: connecting to MySQL at {}",
            mask_password(&self.config.mysql_url)
        );

        let source_id = self
            .config
            .source_id
            .clone()
            .unwrap_or_else(|| "default".into());

        // ── Checkpoint Recovery ───────────────────────────────────────────────
        let mut checkpoint = Checkpoint::load(&self.config.checkpoint_path);

        let recovered_cp = self.config.initial_checkpoint.clone().or_else(|| {
            self.config.db_path.as_ref().and_then(|p| {
                StoreWriter::load_checkpoint(Path::new(p), &source_id)
                    .ok()
                    .flatten()
            })
        });

        if let Some(persisted_cp) = recovered_cp {
            info!(
                "LiveCdc: Recovered atomic checkpoint from DuckDB: file={} pos={}",
                persisted_cp.binlog_file, persisted_cp.binlog_pos
            );
            checkpoint.filename = Some(persisted_cp.binlog_file);
            checkpoint.position = Some(persisted_cp.binlog_pos);
            checkpoint.gtid_set = persisted_cp.gtid_set;
        }

        // ── 1. Verify connectivity and binlog settings ────────────────────────
        let pool = mysql_async::Pool::new(self.config.mysql_url.as_str());
        let mut conn = pool.get_conn().await?;
        conn.query_drop("SET NAMES utf8mb4").await?;

        // The database this connection is attached to. Rows from other databases
        // on the same server must never be applied to our tables.
        let current_db: Option<String> = conn
            .query_first::<String, _>("SELECT DATABASE()")
            .await
            .ok()
            .flatten();

        // The set of databases we replicate from.
        let allowed_dbs: Vec<String> = if self.config.filter_databases.is_empty() {
            if let Some(ref db) = current_db {
                vec![db.clone()]
            } else {
                let all_dbs: Vec<String> = conn.query("SHOW DATABASES").await.unwrap_or_default();
                let user_dbs: Vec<String> = all_dbs
                    .into_iter()
                    .filter(|d| {
                        !matches!(
                            d.to_ascii_lowercase().as_str(),
                            "information_schema" | "performance_schema" | "mysql" | "sys"
                        )
                    })
                    .collect();
                if user_dbs.is_empty() {
                    warn!(
                        "LiveCdc: the connection has no default database (DATABASE() is NULL) and no user databases found — \
                         no tables will be replicated."
                    );
                    Vec::new()
                } else {
                    info!("LiveCdc: auto-discovered user databases: {:?}", user_dbs);
                    user_dbs
                }
            }
        } else {
            self.config.filter_databases.clone()
        };
        let multi_db = allowed_dbs.len() > 1;
        if !allowed_dbs.is_empty() {
            info!("LiveCdc: replicating databases: {:?}", allowed_dbs);
        }

        let binlog_format: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'binlog_format'")
            .await?;

        match binlog_format {
            Some((_, fmt)) if fmt.eq_ignore_ascii_case("ROW") => {
                info!("LiveCdc: binlog_format=ROW ✓");
            }
            Some((_, fmt)) => {
                warn!(
                    "LiveCdc: binlog_format={fmt} — must be ROW. Set `binlog_format = ROW` in my.cnf."
                );
            }
            None => {
                warn!("LiveCdc: binary logging not detected. Set `log_bin = ON` in my.cnf.");
            }
        }

        let row_image: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'binlog_row_image'")
            .await?;

        match row_image {
            Some((_, img)) if img.eq_ignore_ascii_case("FULL") => {
                info!("LiveCdc: binlog_row_image=FULL ✓");
            }
            Some((_, img)) => {
                warn!(
                    "LiveCdc: binlog_row_image={img} — FULL is required. MINIMAL row images \
                     omit unchanged columns and will silently MISALIGN positional row data."
                );
            }
            None => {
                warn!("LiveCdc: binlog_row_image variable not found (MySQL < 5.6?).");
            }
        }

        let master_status: Option<mysql_async::Row> =
            match conn.query_first("SHOW BINARY LOG STATUS").await {
                Ok(Some(row)) => Some(row),
                _ => conn.query_first("SHOW MASTER STATUS").await.unwrap_or(None),
            };

        let (master_file, master_pos): (Option<String>, Option<u64>) = match master_status {
            Some(row) => (row.get(0), row.get(1)),
            None => (None, None),
        };

        if let (Some(ref f), Some(p)) = (&master_file, master_pos) {
            info!("LiveCdc: current MySQL binlog position — file={f:?} pos={p:?}");
        }

        // ── 1.5. Initial Schema Sync ──────────────────────────────────────────
        let filter = self.config.effective_filter();
        let mut discovered_tables: Vec<(String, String)> = Vec::new();
        for db in &allowed_dbs {
            let rows: Vec<String> = conn
                .exec(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                    (db,),
                )
                .await?;
            for tbl in rows {
                discovered_tables.push((db.clone(), tbl));
            }
        }

        // Loud validation for non-existent include entries (warn, do not fail the account)
        for inc in &filter.table_include {
            if !ReplicationFilter::pattern_matches_any(inc, &discovered_tables) {
                warn!(
                    "LiveCdc: table_include entry '{}' was not found in source database(s) {:?}",
                    inc, allowed_dbs
                );
            }
        }

        let allowed_tables: Vec<(String, String)> = discovered_tables
            .into_iter()
            .filter(|(db, bare)| filter.table_allowed(db, bare))
            .collect();

        // Keep TWO maps:
        // - `table_schemas`: full source schema (required for positional binlog row parsing)
        // - `projections`: per-table Projection (projected schema + kept column indices + remapped PK indices)
        let mut table_schemas: HashMap<String, TableSchema> = HashMap::new();
        let mut projections: HashMap<String, Projection> = HashMap::new();

        for (db, bare) in &allowed_tables {
            if let Some(mut full_schema) = fetch_table_schema(&pool, db, bare).await? {
                let local_name = local_table_name(db, bare, multi_db);
                full_schema.table_name = local_name.clone();
                let proj = filter.project_table(db, bare, &full_schema)?;
                if proj.pk_indices.is_empty() && !filter.table_include.is_empty() {
                    warn!(
                        "LiveCdc: table '{}' is in table_include but has no primary key — \
                         it will be snapshot-only (binlog row mutations require a primary key).",
                        local_name
                    );
                }
                table_schemas.insert(local_name.clone(), full_schema);
                projections.insert(local_name.clone(), proj.clone());
                if tx.send(CdcEvent::Schema(proj.schema)).await.is_err() {
                    info!("LiveCdc: writer channel closed during schema sync — stopping.");
                    return Ok(());
                }
            } else {
                warn!(
                    "LiveCdc: schema for table '{}.{}' not found in database.",
                    db, bare
                );
            }
        }
        if let Some(ref tel) = self.config.telemetry {
            tel.set_tables_count(projections.len());
        }

        // ── 1.8. Initial Snapshot Sync (if no checkpoint exists) ──────────────
        if checkpoint.filename.is_none() {
            info!("LiveCdc: No checkpoint found. Starting Initial Snapshot Sync.");
            if let Some(ref tel) = self.config.telemetry {
                tel.set_status("snapshotting");
            }

            let mut snap_conn = if let Some(ref snap_url) = self.config.snapshot_mysql_url {
                info!(
                    "LiveCdc: Using Snapshot Replica: {}",
                    mask_password(snap_url)
                );
                let snap_pool = mysql_async::Pool::new(snap_url.as_str());
                snap_pool.get_conn().await?
            } else {
                pool.get_conn().await?
            };

            snap_conn.query_drop("SET NAMES utf8mb4").await?;
            snap_conn
                .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await?;
            snap_conn
                .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?;

            let (target_file, target_pos) = if self.config.snapshot_mysql_url.is_some() {
                let mut status_row: Option<mysql_async::Row> = snap_conn
                    .query_first("SHOW REPLICA STATUS")
                    .await
                    .unwrap_or(None);
                if status_row.is_none() {
                    status_row = snap_conn
                        .query_first("SHOW SLAVE STATUS")
                        .await
                        .unwrap_or(None);
                }

                if let Some(row) = status_row {
                    let file: Option<String> =
                        row.get("Relay_Master_Log_File").or_else(|| row.get(9));
                    let pos: Option<u64> = row.get("Exec_Master_Log_Pos").or_else(|| row.get(21));
                    if file.is_none() || pos.is_none() {
                        warn!(
                            "LiveCdc: Could not determine Master Log position from Replica Status."
                        );
                    }
                    (file.or(master_file), pos.or(master_pos))
                } else {
                    warn!("LiveCdc: SHOW REPLICA/SLAVE STATUS returned empty; falling back to master position.");
                    (master_file, master_pos)
                }
            } else {
                let snap_master: Option<mysql_async::Row> =
                    match snap_conn.query_first("SHOW BINARY LOG STATUS").await {
                        Ok(Some(row)) => Some(row),
                        _ => snap_conn
                            .query_first("SHOW MASTER STATUS")
                            .await
                            .unwrap_or(None),
                    };
                if let Some(row) = snap_master {
                    (
                        row.get::<String, _>(0).or(master_file),
                        row.get::<u64, _>(1).or(master_pos),
                    )
                } else {
                    (master_file, master_pos)
                }
            };

            info!(
                "LiveCdc: Snapshot target binlog coordinates: file={:?} pos={:?}",
                target_file, target_pos
            );

            let snapshot_cp = match (&target_file, target_pos) {
                (Some(f), Some(p)) if !f.is_empty() => {
                    Some(BinlogCheckpoint::new(&source_id, f, p, None, 0))
                }
                _ => None,
            };

            use futures::StreamExt;
            for (db, bare) in &allowed_tables {
                let local_name = local_table_name(db, bare, multi_db);
                if let Some(proj) = projections.get(&local_name) {
                    let schema = &proj.schema;
                    if schema.columns.is_empty() {
                        continue;
                    }
                    info!(
                        "LiveCdc: Snapshotting table '{}.{}' ({} columns)...",
                        db,
                        bare,
                        schema.columns.len()
                    );

                    let tmp = tempfile::Builder::new()
                        .prefix(&format!("{local_name}_snap_"))
                        .suffix(".csv")
                        .tempfile()?;
                    let (tmp_file, tmp_path) = tmp
                        .keep()
                        .map_err(|e| crate::error::FlowError::Io(e.error))?;
                    let tmp_path_str = tmp_path.to_string_lossy().replace('\\', "/");

                    let mut wtr = csv::WriterBuilder::new()
                        .has_headers(false)
                        .from_writer(std::io::BufWriter::new(tmp_file));

                    let headers: Vec<&str> =
                        schema.columns.iter().map(|c| c.name.as_str()).collect();
                    wtr.write_record(&headers)
                        .map_err(|e| crate::error::FlowError::other(e.to_string()))?;

                    let col_list = schema
                        .columns
                        .iter()
                        .map(|c| format!("`{}`", c.name.replace('`', "``")))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let query_sql = if db.is_empty() {
                        format!("SELECT {col_list} FROM `{}`", bare.replace('`', "``"))
                    } else {
                        format!(
                            "SELECT {col_list} FROM `{}`.`{}`",
                            db.replace('`', "``"),
                            bare.replace('`', "``")
                        )
                    };
                    let mut stream = snap_conn.query_stream(query_sql).await?;
                    let mut row_count = 0usize;

                    while let Some(row_result) = stream.next().await {
                        let row: mysql_async::Row = row_result?;
                        let mut record = Vec::with_capacity(schema.columns.len());

                        for idx in 0..schema.columns.len() {
                            let val = row
                                .get::<mysql_async::Value, _>(idx)
                                .unwrap_or(mysql_async::Value::NULL);
                            use mysql_async::Value as MyVal;
                            let s = match val {
                                MyVal::NULL => "\\N".to_string(),
                                MyVal::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
                                MyVal::Int(i) => i.to_string(),
                                MyVal::UInt(u) => u.to_string(),
                                MyVal::Float(f) => f.to_string(),
                                MyVal::Double(d) => d.to_string(),
                                MyVal::Date(y, m, d, h, mn, s, _) => {
                                    format!(
                                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                                        y, m, d, h, mn, s
                                    )
                                }
                                MyVal::Time(is_neg, d, h, m, s, _) => {
                                    let sign = if is_neg { "-" } else { "" };
                                    format!("{}{:02}:{:02}:{:02}", sign, d * 24 + h as u32, m, s)
                                }
                            };
                            record.push(s);
                        }
                        wtr.write_record(&record)
                            .map_err(|e| crate::error::FlowError::other(e.to_string()))?;
                        row_count += 1;
                    }

                    wtr.flush()?;
                    info!(
                        "LiveCdc: Downloaded {} rows for '{}'. Sending BulkLoadCsv to writer.",
                        row_count, local_name
                    );

                    if tx
                        .send(CdcEvent::BulkLoadCsv {
                            table: local_name.clone(),
                            csv_path: tmp_path_str,
                            checkpoint: snapshot_cp.clone(),
                        })
                        .await
                        .is_err()
                    {
                        info!("LiveCdc: writer channel closed during snapshot — stopping.");
                        return Ok(());
                    }
                    if let Some(ref tel) = self.config.telemetry {
                        tel.add_rows_synced(row_count as u64);
                    }
                    info!(
                        "LiveCdc: Snapshot for '{}' complete ({} rows).",
                        local_name, row_count
                    );
                }
            }

            snap_conn.query_drop("COMMIT").await?;
            drop(snap_conn);

            checkpoint.filename = target_file;
            checkpoint.position = target_pos;
            let _ = checkpoint.save(&self.config.checkpoint_path);
            info!("LiveCdc: Initial Snapshot Sync complete. Checkpoint saved.");
        }

        // ── 2. Full binlog streaming ──────────────────────────────────────────
        use futures::StreamExt;
        use mysql_async::BinlogStreamRequest;
        use mysql_common::binlog::events::{EventData, RowsEventData};

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
        if let Some(ref tel) = self.config.telemetry {
            tel.set_status("streaming");
            tel.set_error(None);
        }
        let mut table_maps = HashMap::new();
        let mut current_binlog_file = checkpoint.filename.clone().unwrap_or_default();
        let mut warned_unknown_tables: HashSet<String> = HashSet::new();

        while let Some(event) = stream.next().await {
            let event = event?;
            let header = event.header();
            let pos = header.log_pos() as u64;
            let event_ts_ms = (header.timestamp() as u64) * 1000;

            let make_cp = |src: &str, file: &str, p: u64| {
                if file.is_empty() {
                    None
                } else {
                    Some(BinlogCheckpoint::new(src, file, p, None, event_ts_ms))
                }
            };

            if let Ok(Some(event_data)) = event.read_data() {
                match event_data {
                    EventData::TableMapEvent(tm) => {
                        table_maps.insert(tm.table_id(), tm.into_owned());
                    }
                    EventData::RowsEvent(RowsEventData::WriteRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let db = String::from_utf8_lossy(tm.database_name_raw()).to_string();
                            if !allowed_dbs.iter().any(|d| d == &db) {
                                // Row belongs to a different database on the same
                                // server — it must never be applied to our tables.
                                continue;
                            }
                            let bare = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if !filter.table_allowed(&db, &bare) {
                                continue;
                            }
                            let table_name = local_table_name(&db, &bare, multi_db);
                            if let (Some(full_schema), Some(proj)) =
                                (table_schemas.get(&table_name), projections.get(&table_name))
                            {
                                if proj.pk_indices.is_empty() {
                                    warn!(
                                        "LiveCdc: table '{}' has no primary key — skipping row mutations. xloka-flow requires primary keys for batch coalescing.",
                                        table_name
                                    );
                                    continue;
                                }
                                for row_result in ev.rows(tm) {
                                    if let Ok((_, Some(after_row))) = row_result {
                                        // Invariant: always parse binlog rows with the FULL MySQL schema,
                                        // then project to the allowed columns.
                                        let full_vals =
                                            parse_row_vec(after_row, &full_schema.columns);
                                        let row_vals = proj.project_row(&full_vals);
                                        let op = RowOp::Insert {
                                            table: table_name.clone(),
                                            pk_indices: proj.pk_indices.clone(),
                                            row: row_vals,
                                            event_ts_ms,
                                            checkpoint: make_cp(
                                                &source_id,
                                                &current_binlog_file,
                                                pos,
                                            ),
                                        };
                                        if tx.send(CdcEvent::Row(op)).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            } else if warned_unknown_tables.insert(table_name.clone()) {
                                warn!(
                                    "LiveCdc: received rows for unknown table '{table_name}' — \
                                     dropping them. The table is not replicated."
                                );
                            }
                        }
                    }
                    EventData::RowsEvent(RowsEventData::UpdateRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let db = String::from_utf8_lossy(tm.database_name_raw()).to_string();
                            if !allowed_dbs.iter().any(|d| d == &db) {
                                continue;
                            }
                            let bare = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if !filter.table_allowed(&db, &bare) {
                                continue;
                            }
                            let table_name = local_table_name(&db, &bare, multi_db);
                            if let (Some(full_schema), Some(proj)) =
                                (table_schemas.get(&table_name), projections.get(&table_name))
                            {
                                if proj.pk_indices.is_empty() {
                                    warn!(
                                        "LiveCdc: table '{}' has no primary key — skipping row mutations. xloka-flow requires primary keys for batch coalescing.",
                                        table_name
                                    );
                                    continue;
                                }
                                for row_result in ev.rows(tm) {
                                    if let Ok((Some(before_row), Some(after_row))) = row_result {
                                        let full_before =
                                            parse_row_vec(before_row, &full_schema.columns);
                                        let full_after =
                                            parse_row_vec(after_row, &full_schema.columns);
                                        let before_vals = proj.project_row(&full_before);
                                        let after_vals = proj.project_row(&full_after);
                                        let op = RowOp::Update {
                                            table: table_name.clone(),
                                            pk_indices: proj.pk_indices.clone(),
                                            before: Some(before_vals),
                                            after: after_vals,
                                            event_ts_ms,
                                            checkpoint: make_cp(
                                                &source_id,
                                                &current_binlog_file,
                                                pos,
                                            ),
                                        };
                                        if tx.send(CdcEvent::Row(op)).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            } else if warned_unknown_tables.insert(table_name.clone()) {
                                warn!(
                                    "LiveCdc: received rows for unknown table '{table_name}' — \
                                     dropping them. The table is not replicated."
                                );
                            }
                        }
                    }
                    EventData::RowsEvent(RowsEventData::DeleteRowsEvent(ev)) => {
                        if let Some(tm) = table_maps.get(&ev.table_id()) {
                            let db = String::from_utf8_lossy(tm.database_name_raw()).to_string();
                            if !allowed_dbs.iter().any(|d| d == &db) {
                                continue;
                            }
                            let bare = String::from_utf8_lossy(tm.table_name_raw()).to_string();
                            if !filter.table_allowed(&db, &bare) {
                                continue;
                            }
                            let table_name = local_table_name(&db, &bare, multi_db);
                            if let (Some(full_schema), Some(proj)) =
                                (table_schemas.get(&table_name), projections.get(&table_name))
                            {
                                if proj.pk_indices.is_empty() {
                                    warn!(
                                        "LiveCdc: table '{}' has no primary key — skipping row mutations. xloka-flow requires primary keys for batch coalescing.",
                                        table_name
                                    );
                                    continue;
                                }
                                for row_result in ev.rows(tm) {
                                    if let Ok((Some(before_row), _)) = row_result {
                                        let full_before =
                                            parse_row_vec(before_row, &full_schema.columns);
                                        let before_vals = proj.project_row(&full_before);
                                        let pk_vals = proj.extract_pk_from_projected(&before_vals);
                                        let op = RowOp::Delete {
                                            table: table_name.clone(),
                                            pk_indices: proj.pk_indices.clone(),
                                            before: Some(before_vals),
                                            pk_values: pk_vals,
                                            event_ts_ms,
                                            checkpoint: make_cp(
                                                &source_id,
                                                &current_binlog_file,
                                                pos,
                                            ),
                                        };
                                        if tx.send(CdcEvent::Row(op)).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            } else if warned_unknown_tables.insert(table_name.clone()) {
                                warn!(
                                    "LiveCdc: received rows for unknown table '{table_name}' — \
                                     dropping them. The table is not replicated."
                                );
                            }
                        }
                    }
                    EventData::RotateEvent(ev) => {
                        let file_name = String::from_utf8_lossy(ev.name_raw()).into_owned();
                        current_binlog_file = file_name.clone();
                        checkpoint.filename = Some(file_name);
                        // Events in the new file start at byte 4 — resuming at the
                        // rotate event's own log_pos would skip the head of the file.
                        checkpoint.position = Some(4);
                        let _ = checkpoint.save(&self.config.checkpoint_path);
                    }
                    EventData::XidEvent(_) => {
                        checkpoint.position = Some(pos);
                        let _ = checkpoint.save(&self.config.checkpoint_path);
                    }
                    EventData::QueryEvent(ev) => {
                        checkpoint.position = Some(pos);
                        let _ = checkpoint.save(&self.config.checkpoint_path);

                        let event_db = String::from_utf8_lossy(ev.schema_raw()).to_string();
                        let sql = ev.query().into_owned();
                        let statements = extract_ddl_statements(&sql);

                        for stmt in statements {
                            let bare = stmt.table.clone();
                            let db = stmt
                                .db
                                .clone()
                                .or_else(|| (!event_db.is_empty()).then(|| event_db.clone()))
                                .or_else(|| current_db.clone())
                                .unwrap_or_default();

                            // Ignore DDL executed against non-replicated databases on
                            // the same MySQL server.
                            if !allowed_dbs.is_empty() && !allowed_dbs.iter().any(|d| d == &db) {
                                continue;
                            }

                            let table_name = local_table_name(&db, &bare, multi_db);

                            if !matches!(stmt.kind, DdlKind::Rename { .. })
                                && !filter.table_allowed(&db, &bare)
                            {
                                continue;
                            }

                            match stmt.kind {
                                DdlKind::Alter => {
                                    info!("Detected ALTER TABLE for {bare}, sending Ddl event...");
                                    // Fetch the authoritative post-DDL schema BEFORE
                                    // sending the event: the writer needs MySQL's
                                    // physical column order to bind row values by name.
                                    let new_schema = fetch_table_schema(&pool, &db, &bare).await?;
                                    if let Some(mut full_schema) = new_schema {
                                        full_schema.table_name = table_name.clone();
                                        let proj =
                                            filter.project_table(&db, &bare, &full_schema)?;
                                        table_schemas.insert(table_name.clone(), full_schema);
                                        projections.insert(table_name.clone(), proj.clone());
                                        let ddl_event = CdcEvent::Ddl {
                                            table: table_name.clone(),
                                            raw_sql: sql.clone(),
                                            op: DdlOp::Alter,
                                            new_schema: Some(proj.schema),
                                            event_ts_ms,
                                            checkpoint: make_cp(
                                                &source_id,
                                                &current_binlog_file,
                                                pos,
                                            ),
                                        };
                                        if tx.send(ddl_event).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
                                        }
                                    } else {
                                        warn!(
                                            "LiveCdc: could not re-read the schema of '{bare}' after \
                                             ALTER — the DDL is not applied and replication stops here."
                                        );
                                        return Err(anyhow::anyhow!(
                                            "unable to re-read schema of table '{bare}' after ALTER"
                                        )
                                        .into());
                                    }
                                }
                                DdlKind::Create => {
                                    info!("Detected CREATE TABLE for {bare}, fetching schema...");
                                    if let Some(mut full_schema) =
                                        fetch_table_schema(&pool, &db, &bare).await?
                                    {
                                        full_schema.table_name = table_name.clone();
                                        let proj =
                                            filter.project_table(&db, &bare, &full_schema)?;
                                        table_schemas.insert(table_name.clone(), full_schema);
                                        projections.insert(table_name.clone(), proj.clone());
                                        if let Some(ref tel) = self.config.telemetry {
                                            tel.set_tables_count(projections.len());
                                        }
                                        if tx.send(CdcEvent::Schema(proj.schema)).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
                                        }
                                    } else {
                                        warn!(
                                            "LiveCdc: could not read the schema of the newly created \
                                             table '{bare}'."
                                        );
                                    }
                                }
                                DdlKind::Drop => {
                                    info!("Detected DROP TABLE for {bare}, sending Ddl event...");
                                    table_schemas.remove(&table_name);
                                    projections.remove(&table_name);
                                    if let Some(ref tel) = self.config.telemetry {
                                        tel.set_tables_count(projections.len());
                                    }
                                    let ddl_event = CdcEvent::Ddl {
                                        table: table_name.clone(),
                                        raw_sql: sql.clone(),
                                        op: DdlOp::Drop,
                                        new_schema: None,
                                        event_ts_ms,
                                        checkpoint: make_cp(&source_id, &current_binlog_file, pos),
                                    };
                                    if tx.send(ddl_event).await.is_err() {
                                        info!(
                                            "LiveCdc: writer channel closed — stopping binlog stream."
                                        );
                                        return Ok(());
                                    }
                                }
                                DdlKind::Truncate => {
                                    info!(
                                        "Detected TRUNCATE TABLE for {bare}, sending Ddl event..."
                                    );
                                    let ddl_event = CdcEvent::Ddl {
                                        table: table_name.clone(),
                                        raw_sql: sql.clone(),
                                        op: DdlOp::Truncate,
                                        new_schema: None,
                                        event_ts_ms,
                                        checkpoint: make_cp(&source_id, &current_binlog_file, pos),
                                    };
                                    if tx.send(ddl_event).await.is_err() {
                                        info!(
                                            "LiveCdc: writer channel closed — stopping binlog stream."
                                        );
                                        return Ok(());
                                    }
                                }
                                DdlKind::Rename { from, to } => {
                                    info!("Detected RENAME TABLE {from} -> {to}");
                                    let (_, from_bare) = split_qualified(&from);
                                    let (to_db, to_bare) = split_qualified(&to);
                                    let target_db = to_db.as_deref().unwrap_or(&db);
                                    let from_name = local_table_name(&db, &from_bare, multi_db);
                                    let to_name = local_table_name(target_db, &to_bare, multi_db);
                                    table_schemas.remove(&from_name);
                                    projections.remove(&from_name);
                                    if !filter.table_allowed(target_db, &to_bare) {
                                        continue;
                                    }
                                    let new_schema =
                                        fetch_table_schema(&pool, target_db, &to_bare).await?;
                                    if let Some(mut full_schema) = new_schema {
                                        full_schema.table_name = to_name.clone();
                                        let proj = filter.project_table(
                                            target_db,
                                            &to_bare,
                                            &full_schema,
                                        )?;
                                        table_schemas.insert(to_name.clone(), full_schema);
                                        projections.insert(to_name.clone(), proj.clone());
                                        let ddl_event = CdcEvent::Ddl {
                                            table: from_name.clone(),
                                            raw_sql: sql.clone(),
                                            op: DdlOp::Rename {
                                                from: from_name,
                                                to: to_name,
                                            },
                                            new_schema: Some(proj.schema),
                                            event_ts_ms,
                                            checkpoint: make_cp(
                                                &source_id,
                                                &current_binlog_file,
                                                pos,
                                            ),
                                        };
                                        if tx.send(ddl_event).await.is_err() {
                                            info!(
                                                "LiveCdc: writer channel closed — stopping binlog stream."
                                            );
                                            return Ok(());
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

/// Exponential backoff for replication reconnects (1s, 2s, 4s, … capped at 60s).
fn reconnect_delay(attempt: u32) -> std::time::Duration {
    let secs = 1u64 << attempt.min(6);
    std::time::Duration::from_secs(secs.min(60))
}

/// Name of the local (DuckDB) table for a MySQL table.
///
/// When more than one database is replicated the database is prefixed with `__`
/// so that identically named tables from different databases do not collide and
/// DuckDB's `PRAGMA table_info('...')` does not misparse a dot as `schema.table`.
fn local_table_name(db: &str, table: &str, multi_db: bool) -> String {
    if multi_db && !db.is_empty() {
        format!("{db}__{table}")
    } else {
        table.to_string()
    }
}

/// Split a possibly qualified table name (`db`.`table`) into its parts.
fn split_qualified(name: &str) -> (Option<String>, String) {
    let cleaned = name.trim_matches(|c| c == '`' || c == '"' || c == '\'' || c == ';');
    match cleaned.split_once('.') {
        Some((db, table)) => (
            Some(db.trim_matches(|c| c == '`' || c == '"').to_string()),
            table.trim_matches(|c| c == '`' || c == '"').to_string(),
        ),
        None => (
            None,
            cleaned.trim_matches(|c| c == '`' || c == '"').to_string(),
        ),
    }
}

fn value_from_mysql(val: mysql_async::Value, unsigned64: bool) -> Value {
    use mysql_async::Value as MyVal;
    match val {
        MyVal::NULL => Value::Null,
        MyVal::Bytes(b) => {
            if let Ok(s) = String::from_utf8(b.clone()) {
                Value::Text(s)
            } else {
                Value::Bytes(b)
            }
        }
        MyVal::Int(i) => Value::Integer(i),
        MyVal::UInt(u) => {
            if unsigned64 {
                // `BIGINT UNSIGNED` — the full u64 range must be preserved,
                // otherwise distinct primary keys would collide.
                Value::Unsigned(u)
            } else if u > i64::MAX as u64 {
                warn!(
                    "LiveCdc: unsigned integer value {} exceeds i64::MAX ({}) in a signed \
                     column; clamping to i64::MAX to prevent overflow.",
                    u,
                    i64::MAX
                );
                Value::Integer(i64::MAX)
            } else {
                Value::Integer(u as i64)
            }
        }
        MyVal::Float(f) => Value::Float(f as f64),
        MyVal::Double(d) => Value::Float(d),
        MyVal::Date(y, m, d, h, mn, s, _ms) => {
            let text = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mn, s);
            Value::Text(text)
        }
        MyVal::Time(is_neg, d, h, m, s, _ms) => {
            let sign = if is_neg { "-" } else { "" };
            let text = format!("{}{:02}:{:02}:{:02}", sign, d * 24 + h as u32, m, s);
            Value::Text(text)
        }
    }
}

fn parse_row_vec(
    binlog_row: mysql_common::binlog::row::BinlogRow,
    columns: &[ColumnDef],
) -> Vec<Value> {
    use std::convert::TryInto;
    let row: mysql_async::Row = match binlog_row.try_into() {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut vec = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let val = row
            .get::<mysql_async::Value, _>(idx)
            .unwrap_or(mysql_async::Value::NULL);
        let unsigned64 = matches!(column.duck_type, DuckDbType::UBigInt);
        vec.push(value_from_mysql(val, unsigned64));
    }
    vec
}

/// Mask the password in a MySQL connection URL.
pub fn mask_password(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
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
    db: &str,
    table_name: &str,
) -> FlowResult<Option<TableSchema>> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop("SET NAMES utf8mb4").await?;
    // Parameterised query — table names are never interpolated into SQL.
    let rows: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT column_name, column_type, is_nullable, column_key \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
            (db, table_name),
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut columns = Vec::new();
    for (col_name, col_type, is_nullable, col_key) in rows {
        columns.push(ColumnDef {
            name: col_name,
            duck_type: DuckDbType::from_mysql(&col_type),
            nullable: is_nullable.eq_ignore_ascii_case("YES"),
            is_primary_key: col_key.eq_ignore_ascii_case("PRI"),
        });
    }
    Ok(Some(TableSchema {
        table_name: table_name.to_string(),
        columns,
    }))
}

// ─── DDL Extraction ───────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Clone)]
enum DdlKind {
    Alter,
    Create,
    Drop,
    Truncate,
    Rename { from: String, to: String },
}

#[derive(Debug)]
struct DdlStatement {
    kind: DdlKind,
    /// Optional explicit database qualifier from `db.table`.
    db: Option<String>,
    /// The bare table the statement applies to (for `Rename`: the source table).
    table: String,
}

/// Test-only view of the DDL extraction.
#[derive(Debug, PartialEq, Eq)]
pub struct ExtractedDdl {
    pub kind: &'static str,
    pub db: Option<String>,
    pub table: String,
}

/// Extract the DDL statements of a query event — exposed for tests.
pub fn extract_ddl_statements_for_test(sql: &str) -> Vec<ExtractedDdl> {
    extract_ddl_statements(sql)
        .into_iter()
        .map(|s| ExtractedDdl {
            kind: match s.kind {
                DdlKind::Alter => "alter",
                DdlKind::Create => "create",
                DdlKind::Drop => "drop",
                DdlKind::Truncate => "truncate",
                DdlKind::Rename { .. } => "rename",
            },
            db: s.db,
            table: s.table,
        })
        .collect()
}

fn split_object_name(name: &sqlparser::ast::ObjectName) -> Option<(Option<String>, String)> {
    let last = name.0.last()?.value.clone();
    let db = if name.0.len() >= 2 {
        Some(name.0[name.0.len() - 2].value.clone())
    } else {
        None
    };
    Some((db, last))
}

/// Extract every DDL statement contained in a binlog query event.
///
/// A single `DROP TABLE a, b, c` yields three statements — the previous
/// implementation only ever handled the first name.
fn extract_ddl_statements(sql: &str) -> Vec<DdlStatement> {
    use sqlparser::ast::{ObjectType, Statement};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    let dialect = MySqlDialect {};
    let mut out = Vec::new();

    if let Ok(ast) = Parser::parse_sql(&dialect, sql) {
        for stmt in ast {
            match stmt {
                Statement::AlterTable { name, .. } => {
                    if let Some((db, table)) = split_object_name(&name) {
                        out.push(DdlStatement {
                            kind: DdlKind::Alter,
                            db,
                            table,
                        });
                    }
                }
                Statement::CreateTable(create_table) => {
                    if let Some((db, table)) = split_object_name(&create_table.name) {
                        out.push(DdlStatement {
                            kind: DdlKind::Create,
                            db,
                            table,
                        });
                    }
                }
                Statement::Drop {
                    object_type: ObjectType::Table,
                    names,
                    ..
                } => {
                    for name in names {
                        if let Some((db, table)) = split_object_name(&name) {
                            out.push(DdlStatement {
                                kind: DdlKind::Drop,
                                db,
                                table,
                            });
                        }
                    }
                }
                Statement::Truncate { table_names, .. } => {
                    for target in table_names {
                        if let Some((db, table)) = split_object_name(&target.name) {
                            out.push(DdlStatement {
                                kind: DdlKind::Truncate,
                                db,
                                table,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if !out.is_empty() {
        return out;
    }

    // Fallback: token based detection for statements the parser rejects.
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() > 2 {
        let first = parts[0].to_uppercase();
        let second = parts[1].to_uppercase();
        if first == "RENAME" && second == "TABLE" {
            if let Some((from, to)) = parse_rename_table(trimmed) {
                let (db, bare_from) = split_qualified(&from);
                out.push(DdlStatement {
                    kind: DdlKind::Rename {
                        from: from.clone(),
                        to,
                    },
                    db,
                    table: bare_from,
                });
                return out;
            }
        }
        let kind = match (first.as_str(), second.as_str()) {
            ("ALTER", "TABLE") => Some(DdlKind::Alter),
            ("CREATE", "TABLE") => Some(DdlKind::Create),
            ("DROP", "TABLE") => Some(DdlKind::Drop),
            ("TRUNCATE", "TABLE") => Some(DdlKind::Truncate),
            _ => None,
        };
        if let Some(kind) = kind {
            let mut idx = 2;
            if parts.get(idx).is_some_and(|s| s.eq_ignore_ascii_case("IF"))
                && parts.get(idx + 1).is_some_and(|s| {
                    s.eq_ignore_ascii_case("EXISTS") || s.eq_ignore_ascii_case("NOT")
                })
            {
                idx += 2;
                if parts
                    .get(idx)
                    .is_some_and(|s| s.eq_ignore_ascii_case("EXISTS"))
                {
                    idx += 1;
                }
            }
            // A multi-table DROP lists several names — emit one statement each.
            let rest = parts[idx..].join(" ");
            let names: Vec<String> = rest
                .split(',')
                .map(|n| {
                    n.trim()
                        .trim_matches(|c| {
                            c == '`' || c == '"' || c == '\'' || c == ';' || c == '(' || c == ')'
                        })
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_string()
                })
                .filter(|n| !n.is_empty())
                .collect();
            for name in names {
                let (db, bare) = split_qualified(&name);
                out.push(DdlStatement {
                    kind: kind.clone(),
                    db,
                    table: bare,
                });
            }
        }
    }

    let _ = upper;
    out
}

/// Parse `RENAME TABLE old TO new` into its two table names.
fn parse_rename_table(sql: &str) -> Option<(String, String)> {
    let rest = sql.split_whitespace().skip(2).collect::<Vec<_>>().join(" ");
    let mut from = None;
    let mut to = None;
    let mut expect_to = false;
    for token in rest.split_whitespace() {
        let clean = token.trim_matches(|c| c == '`' || c == '"' || c == ',' || c == ';');
        if clean.eq_ignore_ascii_case("TO") {
            expect_to = true;
            continue;
        }
        if expect_to {
            to = Some(clean.to_string());
            expect_to = false;
        } else if from.is_none() {
            from = Some(clean.to_string());
        }
    }
    match (from, to) {
        (Some(f), Some(t)) => Some((f, t)),
        _ => None,
    }
}
