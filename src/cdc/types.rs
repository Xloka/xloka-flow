//! Core CDC data types, representations, and schema definitions.

use duckdb::types::{ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

use super::checkpoint::BinlogCheckpoint;

// ─── Column / Schema types ────────────────────────────────────────────────────

/// MySQL → DuckDB type mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuckDbType {
    Integer,
    BigInt,
    /// Unsigned 64-bit integer (`BIGINT UNSIGNED`).
    UBigInt,
    Double,
    Decimal(u8, u8),
    Varchar,
    Timestamp,
    Date,
    Json,
    Blob,
    Boolean,
    Unknown(String),
}

impl DuckDbType {
    /// Emit the DDL fragment used in `CREATE TABLE` / `ALTER TABLE`.
    pub fn to_ddl(&self) -> String {
        match self {
            DuckDbType::Integer => "INTEGER".into(),
            DuckDbType::BigInt => "BIGINT".into(),
            DuckDbType::UBigInt => "UBIGINT".into(),
            DuckDbType::Double => "DOUBLE".into(),
            DuckDbType::Decimal(p, s) => format!("DECIMAL({p},{s})"),
            DuckDbType::Varchar => "VARCHAR".into(),
            DuckDbType::Timestamp => "TIMESTAMP".into(),
            DuckDbType::Date => "DATE".into(),
            DuckDbType::Json => "JSON".into(),
            DuckDbType::Blob => "BLOB".into(),
            DuckDbType::Boolean => "BOOLEAN".into(),
            DuckDbType::Unknown(t) => format!("VARCHAR /* {t} */"),
        }
    }

    /// Parse a MySQL type string into a DuckDB type.
    pub fn from_mysql(mysql_type: &str) -> Self {
        let raw = mysql_type.to_uppercase();
        let unsigned = raw.contains("UNSIGNED");
        // Strip MySQL modifiers that do not change the storage class:
        // `INT(11) UNSIGNED ZEROFILL` -> `INT`.
        let t = raw
            .replace("UNSIGNED", " ")
            .replace("ZEROFILL", " ")
            .trim()
            .to_string();
        let base = t.split('(').next().unwrap_or("").trim();
        match base {
            "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" => {
                if t.contains("(1)") && base == "TINYINT" {
                    DuckDbType::Boolean
                } else {
                    // All unsigned 32-bit values fit into a signed BIGINT.
                    DuckDbType::Integer
                }
            }
            "BIGINT" => {
                // `BIGINT UNSIGNED` spans the full u64 range — it must not be
                // stored in a signed BIGINT, or values above i64::MAX would be
                // clamped and collide with each other.
                if unsigned {
                    DuckDbType::UBigInt
                } else {
                    DuckDbType::BigInt
                }
            }
            "FLOAT" | "DOUBLE" | "REAL" => DuckDbType::Double,
            "DECIMAL" | "NUMERIC" => {
                let inner = t
                    .trim_start_matches(base)
                    .trim_matches(|c| c == '(' || c == ')')
                    .to_string();
                let parts: Vec<&str> = inner.split(',').collect();
                let p = parts
                    .first()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(18);
                let s = parts
                    .get(1)
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(4);
                DuckDbType::Decimal(p, s)
            }
            "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "CHAR" | "ENUM"
            | "SET" => DuckDbType::Varchar,
            "DATETIME" | "TIMESTAMP" => DuckDbType::Timestamp,
            "DATE" => DuckDbType::Date,
            "JSON" => DuckDbType::Json,
            "BLOB" | "BINARY" | "VARBINARY" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                DuckDbType::Blob
            }
            "BIT" if t.contains("(1)") => DuckDbType::Boolean,
            other => DuckDbType::Unknown(other.to_lowercase()),
        }
    }

    /// Parse a DuckDB catalog type string (as reported by `PRAGMA table_info`)
    /// back into a [`DuckDbType`].
    pub fn from_duckdb(duck_type: &str) -> Self {
        let t = duck_type.trim().to_uppercase();
        let base = t.split('(').next().unwrap_or("").trim();
        match base {
            "TINYINT" | "SMALLINT" | "INT" | "INT4" | "INTEGER" => DuckDbType::Integer,
            "BIGINT" | "INT8" | "HUGEINT" => DuckDbType::BigInt,
            "UBIGINT" => DuckDbType::UBigInt,
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT8" => DuckDbType::Double,
            "DECIMAL" | "NUMERIC" => {
                let inner = t
                    .trim_start_matches(base)
                    .trim_matches(|c| c == '(' || c == ')')
                    .to_string();
                let parts: Vec<&str> = inner.split(',').collect();
                DuckDbType::Decimal(
                    parts
                        .first()
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(18),
                    parts
                        .get(1)
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(4),
                )
            }
            "VARCHAR" | "TEXT" | "STRING" | "CHAR" => DuckDbType::Varchar,
            "TIMESTAMP" | "DATETIME" => DuckDbType::Timestamp,
            "DATE" => DuckDbType::Date,
            "JSON" => DuckDbType::Json,
            "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DuckDbType::Blob,
            "BOOLEAN" | "BOOL" => DuckDbType::Boolean,
            other => DuckDbType::Unknown(other.to_lowercase()),
        }
    }
}

/// A single column description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub duck_type: DuckDbType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// Schema for one table — ordered list of columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    /// Return the names of all primary key columns in order.
    pub fn primary_keys(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Return the 0-based column indices of all primary key columns.
    pub fn primary_key_indices(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    /// Generate a DuckDB-compatible `CREATE TABLE IF NOT EXISTS` statement.
    pub fn create_ddl(&self) -> String {
        let cols = self
            .columns
            .iter()
            .map(|c| {
                let nullable = if c.nullable { "" } else { " NOT NULL" };
                format!("    \"{}\" {}{}", c.name, c.duck_type.to_ddl(), nullable)
            })
            .collect::<Vec<_>>()
            .join(",\n");

        let pks = self.primary_keys();
        let pk_clause = if !pks.is_empty() {
            let quoted_pks = pks
                .iter()
                .map(|k| format!("\"{}\"", k))
                .collect::<Vec<_>>()
                .join(", ");
            format!(",\n    PRIMARY KEY ({})", quoted_pks)
        } else {
            String::new()
        };

        format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}{}\n)",
            self.table_name, cols, pk_clause
        )
    }
}

// ─── Value Representation ─────────────────────────────────────────────────────

/// A strongly typed value from MySQL row mutations.
///
/// Implements `Eq` and `Hash` using bit-level comparisons for floating-point values
/// so it can serve as a primary key or key component in composite indices and maps.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Integer(i64),
    /// Unsigned 64-bit integer (`BIGINT UNSIGNED`). Kept separate from
    /// [`Value::Integer`] so values above `i64::MAX` are stored exactly instead
    /// of being clamped (which would make distinct primary keys collide).
    Unsigned(u64),
    Float(f64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Unsigned(a), Value::Unsigned(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Integer(i) => i.hash(state),
            Value::Unsigned(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Text(s) => s.hash(state),
            Value::Bool(b) => b.hash(state),
            Value::Bytes(b) => b.hash(state),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Unsigned(i) => write!(f, "{i}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Text(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Value::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Value::Bytes(_) => write!(f, "NULL"),
        }
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        match self {
            Value::Null => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
            Value::Integer(i) => Ok(ToSqlOutput::Owned(duckdb::types::Value::BigInt(*i))),
            Value::Unsigned(i) => Ok(ToSqlOutput::Owned(duckdb::types::Value::UBigInt(*i))),
            Value::Float(f) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Double(*f))),
            Value::Text(s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes()))),
            Value::Bool(b) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Boolean(*b))),
            Value::Bytes(b) => Ok(ToSqlOutput::Borrowed(ValueRef::Blob(b.as_slice()))),
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(val: Value) -> Self {
        match val {
            Value::Null => serde_json::Value::Null,
            Value::Integer(i) => serde_json::Value::Number(i.into()),
            Value::Unsigned(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Text(s) => serde_json::Value::String(s),
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Bytes(b) => serde_json::Value::String(format!("<blob: {} bytes>", b.len())),
        }
    }
}

// ─── Row Operation ────────────────────────────────────────────────────────────

/// A row-level mutation event in the CDC pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RowOp {
    /// A row was inserted.
    Insert {
        table: String,
        pk_indices: Vec<usize>,
        row: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
    /// A row was updated. `before` is optional.
    Update {
        table: String,
        pk_indices: Vec<usize>,
        before: Option<Vec<Value>>,
        after: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
    /// A row was deleted.
    Delete {
        table: String,
        pk_indices: Vec<usize>,
        before: Option<Vec<Value>>,
        pk_values: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
}

impl RowOp {
    /// Target table name for this mutation.
    pub fn table(&self) -> &str {
        match self {
            RowOp::Insert { table, .. } => table,
            RowOp::Update { table, .. } => table,
            RowOp::Delete { table, .. } => table,
        }
    }

    /// Primary key column positions within the row.
    pub fn pk_indices(&self) -> &[usize] {
        match self {
            RowOp::Insert { pk_indices, .. } => pk_indices,
            RowOp::Update { pk_indices, .. } => pk_indices,
            RowOp::Delete { pk_indices, .. } => pk_indices,
        }
    }

    /// Origin event timestamp in milliseconds.
    pub fn event_ts_ms(&self) -> u64 {
        match self {
            RowOp::Insert { event_ts_ms, .. } => *event_ts_ms,
            RowOp::Update { event_ts_ms, .. } => *event_ts_ms,
            RowOp::Delete { event_ts_ms, .. } => *event_ts_ms,
        }
    }

    /// Optional binlog checkpoint attached to this operation.
    pub fn checkpoint(&self) -> Option<&BinlogCheckpoint> {
        match self {
            RowOp::Insert { checkpoint, .. } => checkpoint.as_ref(),
            RowOp::Update { checkpoint, .. } => checkpoint.as_ref(),
            RowOp::Delete { checkpoint, .. } => checkpoint.as_ref(),
        }
    }

    /// Extract the primary key values from this row mutation.
    pub fn extract_pk(&self) -> Vec<Value> {
        match self {
            RowOp::Insert {
                pk_indices, row, ..
            } => pk_indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect(),
            RowOp::Update {
                pk_indices, after, ..
            } => pk_indices
                .iter()
                .map(|&i| after.get(i).cloned().unwrap_or(Value::Null))
                .collect(),
            RowOp::Delete {
                pk_values,
                pk_indices,
                before,
                ..
            } => {
                if !pk_values.is_empty() {
                    pk_values.clone()
                } else if let Some(b) = before {
                    pk_indices
                        .iter()
                        .map(|&i| b.get(i).cloned().unwrap_or(Value::Null))
                        .collect()
                } else {
                    vec![]
                }
            }
        }
    }
}

// ─── CDC Pipeline Event ───────────────────────────────────────────────────────

/// The kind of DDL operation intercepted from the binlog.
///
/// The kind is determined by the *source* (MySQL) semantics, never by what
/// DuckDB happens to accept — the writer uses it to apply the correct
/// structural change to the local table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DdlOp {
    /// `ALTER TABLE ...` — column added/dropped/renamed/retyped.
    Alter,
    /// `CREATE TABLE ...`
    Create,
    /// `DROP TABLE ...`
    Drop,
    /// `TRUNCATE TABLE ...` — all rows removed.
    Truncate,
    /// `RENAME TABLE a TO b`
    Rename { from: String, to: String },
}

/// Top-level CDC pipeline event emitted by replication sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CdcEvent {
    /// Row-level mutation.
    Row(RowOp),
    /// Data Definition Language (DDL) statement intercepted from binlog or migration.
    ///
    /// `new_schema` carries the authoritative post-DDL schema as observed in
    /// MySQL (column order, types, primary keys). When present, the writer
    /// *reconciles* the local table to it — that is the only safe way to keep
    /// DuckDB's physical column order identical to MySQL's, which positional
    /// `Appender` binding depends on.
    Ddl {
        table: String,
        raw_sql: String,
        op: DdlOp,
        new_schema: Option<TableSchema>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
    /// Explicit table schema declaration (e.g. for pipeline initialization).
    Schema(TableSchema),
    /// Bulk load a snapshot CSV file into a local DuckDB table (`COPY ... FROM ...`).
    BulkLoadCsv {
        table: String,
        csv_path: String,
        checkpoint: Option<BinlogCheckpoint>,
    },
}

impl From<RowOp> for CdcEvent {
    fn from(op: RowOp) -> Self {
        CdcEvent::Row(op)
    }
}
