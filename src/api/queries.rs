//! Declarative Analytical SQL Manifest and parallel JoinSet executor.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::error::FlowError;

/// Maximum number of rows a single manifest statement may return.
///
/// Analytical manifests are written by operators; a missing `LIMIT` must not be
/// able to exhaust the daemon's memory.
pub const DEFAULT_MAX_QUERY_ROWS: usize = 100_000;

/// Declarative analytical query manifest containing named query definitions.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct QueryManifest {
    pub queries: HashMap<String, QueryDef>,
}

impl QueryManifest {
    /// Load a query manifest from a TOML string.
    pub fn load_from_str(toml_str: &str) -> anyhow::Result<Self> {
        let manifest: QueryManifest = toml::from_str(toml_str)?;
        Ok(manifest)
    }

    /// Load a query manifest from a TOML file on disk.
    pub fn load_from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::load_from_str(&content)
    }
}

/// A named analytical query definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryDef {
    /// Description of the query's business analytical purpose.
    pub description: String,
    /// Dependent tables whose mutations should invalidate and trigger query re-evaluation.
    pub tables: Vec<String>,
    /// One or more SQL statements to execute concurrently.
    pub statements: Vec<String>,
}

use std::sync::Mutex;

static READER_REGISTRY: std::sync::OnceLock<Mutex<HashMap<PathBuf, duckdb::Connection>>> =
    std::sync::OnceLock::new();

fn get_registry() -> &'static Mutex<HashMap<PathBuf, duckdb::Connection>> {
    READER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a DuckDB database connection so concurrent readers can clone isolated handles.
pub fn register_db_connection(path: &Path, conn: &duckdb::Connection) -> anyhow::Result<()> {
    let cloned1 = conn.try_clone()?;
    let cloned2 = conn.try_clone()?;
    let mut reg = get_registry().lock().unwrap();
    let abs_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    reg.insert(abs_path, cloned1);
    reg.insert(path.to_path_buf(), cloned2);
    Ok(())
}

/// Unregister a DuckDB database connection when the writer shuts down.
pub fn unregister_db_connection(path: &Path) {
    if let Ok(mut reg) = get_registry().lock() {
        let abs_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        reg.remove(&abs_path);
        reg.remove(path);
    }
}

/// Open an isolated read-only DuckDB connection handle.
///
/// Uses an existing in-process instance if registered (avoiding OS file locks on
/// Windows), or opens directly from disk with `AccessMode::ReadOnly`.
///
/// Note: the in-process handle is a clone of the writer's connection and is
/// therefore *not* enforced as read-only by DuckDB itself. The protection
/// against accidental writes is the SQL guard in [`guard_select_only`], which
/// rejects anything that is not a read-only statement.
pub fn open_read_only(db_path: &Path) -> anyhow::Result<duckdb::Connection> {
    if let Ok(reg) = get_registry().lock() {
        let abs_path = std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.to_path_buf());
        if let Some(conn) = reg.get(&abs_path).or_else(|| reg.get(db_path)) {
            if let Ok(cloned) = conn.try_clone() {
                return Ok(cloned);
            }
        }
    }

    let cfg = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
    let conn = duckdb::Connection::open_with_flags(db_path, cfg)?;
    Ok(conn)
}

/// Reject any statement that is not read-only.
///
/// The manifest is operator-authored, but a mistake in it must not be able to
/// destroy the replicated store: the analytical connection shares the writer's
/// DuckDB instance and can therefore execute DDL/DML. Only `SELECT` (and
/// `WITH ... SELECT`) statements are allowed.
pub fn guard_select_only(sql: &str) -> Result<(), FlowError> {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| FlowError::sql_guard(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(FlowError::sql_guard("Empty SQL statement"));
    }

    for stmt in statements {
        match stmt {
            Statement::Query(_) => continue,
            other => {
                return Err(FlowError::sql_guard(format!(
                    "Only read-only SELECT statements are allowed. Rejected: {}",
                    summarise_statement(&other)
                )));
            }
        }
    }

    Ok(())
}

/// Short, safe description of a rejected statement (no SQL text echoed back).
fn summarise_statement(stmt: &sqlparser::ast::Statement) -> String {
    use sqlparser::ast::Statement;
    match stmt {
        Statement::Insert(_) => "INSERT".into(),
        Statement::Update { .. } => "UPDATE".into(),
        Statement::Delete(_) => "DELETE".into(),
        Statement::CreateTable { .. } => "CREATE TABLE".into(),
        Statement::CreateView { .. } => "CREATE VIEW".into(),
        Statement::Drop { .. } => "DROP".into(),
        Statement::AlterTable { .. } => "ALTER TABLE".into(),
        Statement::Truncate { .. } => "TRUNCATE".into(),
        other => format!("{other:?}").chars().take(40).collect::<String>(),
    }
}

/// Execute a `QueryDef` in parallel across statements using a `tokio::task::JoinSet`.
///
/// Each statement gets its own DuckDB connection handle in `spawn_blocking`,
/// avoiding locks on the single DuckDB writer.
pub async fn execute_query_def(
    db_path: Arc<PathBuf>,
    def: Arc<QueryDef>,
    params: Vec<String>,
) -> anyhow::Result<serde_json::Value> {
    execute_query_def_with_limit(db_path, def, params, DEFAULT_MAX_QUERY_ROWS).await
}

/// Execute a `QueryDef`, capping the number of materialized rows per statement.
pub async fn execute_query_def_with_limit(
    db_path: Arc<PathBuf>,
    def: Arc<QueryDef>,
    params: Vec<String>,
    max_rows: usize,
) -> anyhow::Result<serde_json::Value> {
    let n = def.statements.len();
    let mut join_set: JoinSet<Result<(usize, JsonValue), String>> = JoinSet::new();

    for (i, stmt) in def.statements.iter().enumerate() {
        // Guard every statement before it reaches the database.
        if let Err(e) = guard_select_only(stmt) {
            return Err(anyhow::anyhow!(e));
        }

        let db_path_c = Arc::clone(&db_path);
        let single_sql = stmt.clone();
        let params_c = params.clone();

        join_set.spawn_blocking(move || {
            let conn =
                open_read_only(&db_path_c).map_err(|e| format!("stmt[{i}] open_read_only: {e}"))?;

            let mut prep = conn
                .prepare(&single_sql)
                .map_err(|e| format!("stmt[{i}] prepare error: {e}"))?;

            let param_refs: Vec<&dyn duckdb::ToSql> =
                params_c.iter().map(|p| p as &dyn duckdb::ToSql).collect();

            let mut rows = prep
                .query(param_refs.as_slice())
                .map_err(|e| format!("stmt[{i}] query error: {e}"))?;

            let col_names = rows
                .as_ref()
                .ok_or_else(|| format!("stmt[{i}] missing column metadata"))?
                .column_names();

            let mut results = Vec::new();
            let mut truncated = false;
            while let Some(row) = rows
                .next()
                .map_err(|e| format!("stmt[{i}] fetch error: {e}"))?
            {
                if results.len() >= max_rows {
                    truncated = true;
                    break;
                }
                let mut map = Map::new();
                for (j, col) in col_names.iter().enumerate() {
                    map.insert(col.clone(), duckdb_cell_to_json(row, j));
                }
                results.push(JsonValue::Object(map));
            }

            let stmt_result = json!({
                "statement_index": i,
                "columns": col_names,
                "rows": results,
                "count": results.len(),
                "truncated": truncated
            });

            Ok((i, stmt_result))
        });
    }

    // Preserve original statement order
    let mut ordered: Vec<Option<JsonValue>> = (0..n).map(|_| None).collect();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok((i, val))) => ordered[i] = Some(val),
            Ok(Err(e)) => return Err(anyhow::anyhow!(e)),
            Err(e) => return Err(anyhow::anyhow!("Join error: {e}")),
        }
    }

    let results: Vec<JsonValue> = ordered.into_iter().flatten().collect();
    Ok(json!({ "results": results }))
}

use duckdb::types::ValueRef;

/// Convert a DuckDB row column at index `idx` to `serde_json::Value`.
pub fn duckdb_cell_to_json(row: &duckdb::Row<'_>, idx: usize) -> JsonValue {
    match row.get_ref(idx) {
        Ok(ValueRef::Null) => JsonValue::Null,
        Ok(ValueRef::Boolean(b)) => JsonValue::Bool(b),
        Ok(ValueRef::TinyInt(i)) => JsonValue::Number((i as i64).into()),
        Ok(ValueRef::SmallInt(i)) => JsonValue::Number((i as i64).into()),
        Ok(ValueRef::Int(i)) => JsonValue::Number((i as i64).into()),
        Ok(ValueRef::BigInt(i)) => JsonValue::Number(i.into()),
        Ok(ValueRef::HugeInt(i)) => {
            if let Ok(i64_val) = i64::try_from(i) {
                JsonValue::Number(i64_val.into())
            } else {
                JsonValue::String(i.to_string())
            }
        }
        Ok(ValueRef::UTinyInt(u)) => JsonValue::Number((u as u64).into()),
        Ok(ValueRef::USmallInt(u)) => JsonValue::Number((u as u64).into()),
        Ok(ValueRef::UInt(u)) => JsonValue::Number((u as u64).into()),
        Ok(ValueRef::UBigInt(u)) => JsonValue::Number(u.into()),
        Ok(ValueRef::Float(f)) => serde_json::Number::from_f64(f as f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(f.to_string())),
        Ok(ValueRef::Double(d)) => serde_json::Number::from_f64(d)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(d.to_string())),
        Ok(ValueRef::Decimal(dec)) => JsonValue::String(dec.to_string()),
        Ok(ValueRef::Text(bytes)) => JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        Ok(ValueRef::Blob(bytes)) => JsonValue::String(format!("<blob len={}>", bytes.len())),
        Ok(ValueRef::Timestamp(_, ts)) => JsonValue::Number(ts.into()),
        Ok(_) => {
            if let Ok(Some(s)) = row.get::<_, Option<String>>(idx) {
                JsonValue::String(s)
            } else {
                JsonValue::Null
            }
        }
        Err(_) => JsonValue::Null,
    }
}
