//! CDC abstractions: the `CdcSource` trait, `RowOp` event enum, and `TableSchema`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;

pub mod live;
pub mod mock;

// ─── Column / Schema types ────────────────────────────────────────────────────

/// MySQL → DuckDB type mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuckDbType {
    Integer,
    BigInt,
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
        let t = mysql_type.to_uppercase();
        let base = t.split('(').next().unwrap_or("").trim();
        match base {
            "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" => {
                // BIT(1) is a special case for BOOLEAN but TINYINT(1) is commonly used too
                if t.contains("(1)") && base == "TINYINT" {
                    DuckDbType::Boolean
                } else {
                    DuckDbType::Integer
                }
            }
            "BIGINT" => DuckDbType::BigInt,
            "FLOAT" | "DOUBLE" | "REAL" => DuckDbType::Double,
            "DECIMAL" | "NUMERIC" => {
                // Try to parse precision/scale
                let inner = t
                    .trim_start_matches(base)
                    .trim_matches(|c| c == '(' || c == ')')
                    .to_string();
                let parts: Vec<&str> = inner.split(',').collect();
                let p = parts.first().and_then(|s| s.trim().parse().ok()).unwrap_or(18);
                let s = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(4);
                DuckDbType::Decimal(p, s)
            }
            "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "CHAR" => {
                DuckDbType::Varchar
            }
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
}

/// A single column description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub duck_type: DuckDbType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// Schema for one table — list of columns in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    pub fn primary_keys(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect()
    }

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
            let quoted_pks = pks.iter().map(|k| format!("\"{}\"", k)).collect::<Vec<_>>().join(", ");
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

// ─── Row Operation ────────────────────────────────────────────────────────────

/// A value from a MySQL row, loosely typed for transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Text(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Value::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Value::Bytes(_) => write!(f, "NULL"), // blobs serialized separately
        }
    }
}

/// A CDC row operation emitted from any `CdcSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowOp {
    /// Full schema arrived / changed — writer must reconcile DDL.
    SchemaChange(TableSchema),
    /// A row was inserted.
    Insert {
        table: String,
        row: HashMap<String, Value>,
    },
    /// A row was updated.  `before` is optional (not all CDC sources provide it).
    Update {
        table: String,
        before: Option<HashMap<String, Value>>,
        after: HashMap<String, Value>,
    },
    /// A row was deleted.  `pk_values` maps PK column → value.
    Delete {
        table: String,
        pk_values: HashMap<String, Value>,
    },
    /// Bulk load data from a temporary CSV file into DuckDB natively via COPY
    BulkLoadCsv {
        table: String,
        csv_path: String,
    },
}

// ─── Trait ────────────────────────────────────────────────────────────────────

/// Every CDC source (live MySQL or mock) implements this trait.
#[async_trait]
pub trait CdcSource: Send + 'static {
    /// Stream row operations into `tx` until shutdown is signalled or an error
    /// occurs.  The implementation is responsible for checkpoint updates.
    async fn run(self, tx: mpsc::Sender<RowOp>) -> crate::error::FlowResult<()>;
}
