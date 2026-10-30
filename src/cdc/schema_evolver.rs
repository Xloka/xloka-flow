//! Live Schema Evolution: Translates MySQL DDL statements into DuckDB ALTER TABLE statements.
//!
//! # Why translation alone is not enough
//!
//! The writer binds row values to DuckDB **positionally** (native `Appender`). MySQL
//! and DuckDB do not agree on where a newly added column physically lives:
//!
//! * MySQL `ALTER TABLE t ADD COLUMN c FIRST` inserts the column *in the middle*.
//! * DuckDB `ALTER TABLE t ADD COLUMN c` always appends the column *at the end*.
//!
//! Translating the DDL therefore silently misaligns every subsequent row: the new
//! value lands in the wrong column and all trailing columns shift. The fix is not a
//! better translation — it is to keep the value ↔ column binding **by name**.
//!
//! See `StoreWriter` for the name-based binding and `TableSchema` for the
//! authoritative MySQL-side column order that row vectors follow.

use sqlparser::ast::{AlterTableOperation, ColumnOption, DataType, MySQLColumnPosition, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use tracing::{debug, warn};

use super::types::{ColumnDef, DuckDbType, TableSchema};

/// A single translated DDL operation against the local DuckDB table.
#[derive(Debug, Clone, PartialEq)]
pub struct DdlOperation {
    /// Table the operation applies to.
    pub table: String,
    /// DuckDB statement to execute.
    pub sql: String,
}

/// Translate a MySQL `ALTER TABLE` statement into DuckDB statements.
///
/// Unlike the previous implementation this never falls back to executing raw MySQL
/// SQL inside DuckDB: an unparsable or unsupported statement is reported as an error
/// so the caller can *fail closed* (refuse to advance the binlog checkpoint) instead
/// of silently diverging from the source schema.
///
/// `target` — when supplied — is the authoritative post-DDL schema observed in MySQL.
/// It is used to resolve the *exact* DuckDB type of the affected column, so that
/// `MODIFY COLUMN`/`CHANGE COLUMN` are translated faithfully.
pub fn translate_alter(
    raw_sql: &str,
    current: Option<&TableSchema>,
    target: Option<&TableSchema>,
) -> anyhow::Result<Vec<DdlOperation>> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, raw_sql)
        .map_err(|e| anyhow::anyhow!("failed to parse MySQL DDL '{raw_sql}': {e}"))?;

    let mut ops: Vec<DdlOperation> = Vec::new();

    for stmt in ast {
        match stmt {
            Statement::AlterTable {
                name, operations, ..
            } => {
                let table_name = target
                    .or(current)
                    .map(|s| s.table_name.clone())
                    .or_else(|| name.0.last().map(|ident| ident.value.clone()))
                    .unwrap_or_else(|| "unknown".into());

                for op in operations {
                    match op {
                        AlterTableOperation::AddColumn {
                            column_def,
                            if_not_exists,
                            column_position,
                            ..
                        } => {
                            let col_name = column_def.name.value.clone();
                            if let Some(tgt) = target {
                                if !tgt
                                    .columns
                                    .iter()
                                    .any(|c| c.name.eq_ignore_ascii_case(&col_name))
                                {
                                    debug!(
                                        "Skipping ADD COLUMN `{}` on `{}` because column is excluded from projected schema.",
                                        col_name, table_name
                                    );
                                    continue;
                                }
                            }
                            if let Some(pos) = column_position {
                                // Position is irrelevant for the local table: values are
                                // bound by column NAME, so DuckDB may keep its own
                                // physical order.
                                debug!(
                                    "ALTER TABLE `{}` ADD COLUMN `{}` has position ({:?}); \
                                     the local column order is irrelevant because rows are \
                                     bound by column name.",
                                    table_name, col_name, pos
                                );
                            }
                            let duck_type = resolve_type(&col_name, column_def.data_type, target);
                            let if_not = if if_not_exists { "IF NOT EXISTS " } else { "" };
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql: format!(
                                    "ALTER TABLE \"{}\" ADD COLUMN {}\"{}\" {}",
                                    table_name,
                                    if_not,
                                    col_name,
                                    duck_type.to_ddl()
                                ),
                            });
                        }
                        AlterTableOperation::DropColumn {
                            column_name,
                            if_exists,
                            ..
                        } => {
                            let col_name = column_name.value.clone();
                            if let Some(cur) = current {
                                if !cur
                                    .columns
                                    .iter()
                                    .any(|c| c.name.eq_ignore_ascii_case(&col_name))
                                {
                                    debug!(
                                        "Skipping DROP COLUMN `{}` on `{}` because column was not present in projected schema.",
                                        col_name, table_name
                                    );
                                    continue;
                                }
                            }
                            let if_ex = if if_exists { "IF EXISTS " } else { "" };
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql: format!(
                                    "ALTER TABLE \"{}\" DROP COLUMN {}\"{}\"",
                                    table_name, if_ex, col_name
                                ),
                            });
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        } => {
                            let in_cur = current
                                .map(|c| {
                                    c.columns.iter().any(|col| {
                                        col.name.eq_ignore_ascii_case(&old_column_name.value)
                                    })
                                })
                                .unwrap_or(true);
                            let in_tgt = target
                                .map(|t| {
                                    t.columns.iter().any(|col| {
                                        col.name.eq_ignore_ascii_case(&new_column_name.value)
                                    })
                                })
                                .unwrap_or(true);
                            if !in_cur || !in_tgt {
                                continue;
                            }
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql: format!(
                                    "ALTER TABLE \"{}\" RENAME COLUMN \"{}\" TO \"{}\"",
                                    table_name, old_column_name.value, new_column_name.value
                                ),
                            });
                        }
                        AlterTableOperation::ModifyColumn {
                            col_name,
                            data_type,
                            options,
                            ..
                        } => {
                            let name = col_name.value.clone();
                            if let Some(tgt) = target {
                                if !tgt
                                    .columns
                                    .iter()
                                    .any(|c| c.name.eq_ignore_ascii_case(&name))
                                {
                                    debug!(
                                        "Skipping MODIFY COLUMN `{}` on `{}` because column is excluded from projected schema.",
                                        name, table_name
                                    );
                                    continue;
                                }
                            }
                            let duck_type = resolve_type(&name, data_type, target);
                            let mut sql = format!(
                                "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" TYPE {}",
                                table_name,
                                name,
                                duck_type.to_ddl()
                            );
                            for opt in &options {
                                if let Some(extra) = column_option_to_duckdb(opt) {
                                    sql.push(' ');
                                    sql.push_str(&extra);
                                }
                            }
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql,
                            });
                        }
                        AlterTableOperation::ChangeColumn {
                            old_name,
                            new_name,
                            data_type,
                            options,
                            ..
                        } => {
                            let name = new_name.value.clone();
                            let in_cur = current
                                .map(|c| {
                                    c.columns
                                        .iter()
                                        .any(|col| col.name.eq_ignore_ascii_case(&old_name.value))
                                })
                                .unwrap_or(true);
                            let in_tgt = target
                                .map(|t| {
                                    t.columns
                                        .iter()
                                        .any(|col| col.name.eq_ignore_ascii_case(&name))
                                })
                                .unwrap_or(true);
                            if !in_cur || !in_tgt {
                                continue;
                            }
                            let duck_type = resolve_type(&name, data_type, target);
                            let mut sql = format!(
                                "ALTER TABLE \"{}\" RENAME COLUMN \"{}\" TO \"{}\"",
                                table_name, old_name.value, name
                            );
                            sql.push_str(&format!(
                                "; ALTER TABLE \"{}\" ALTER COLUMN \"{}\" TYPE {}",
                                table_name,
                                name,
                                duck_type.to_ddl()
                            ));
                            for opt in &options {
                                if let Some(extra) = column_option_to_duckdb(opt) {
                                    sql.push(' ');
                                    sql.push_str(&extra);
                                }
                            }
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql,
                            });
                        }
                        AlterTableOperation::RenameTable {
                            table_name: new_name,
                        } => {
                            let target_name = new_name
                                .0
                                .last()
                                .map(|i| i.value.clone())
                                .unwrap_or_default();
                            ops.push(DdlOperation {
                                table: table_name.clone(),
                                sql: format!(
                                    "ALTER TABLE \"{}\" RENAME TO \"{}\"",
                                    table_name, target_name
                                ),
                            });
                        }
                        // Index / constraint / table-option operations do not change the
                        // row layout, so they are irrelevant for the analytical mirror.
                        AlterTableOperation::AddConstraint(_)
                        | AlterTableOperation::DropConstraint { .. }
                        | AlterTableOperation::AlterColumn { .. } => {
                            debug!(
                                "Ignoring non-layout ALTER TABLE operation on '{}'",
                                table_name
                            );
                        }
                        other => {
                            debug!("Unhandled ALTER TABLE operation: {:?}", other);
                        }
                    }
                }
            }
            Statement::CreateTable { .. } => {
                // A brand new table is created from the authoritative MySQL schema by
                // the writer (`CdcEvent::Schema`), which keeps the column order and
                // types exactly right. Executing the raw MySQL CREATE here would be
                // both redundant and unsafe.
                debug!("Ignoring CREATE TABLE — the table is created from its schema.");
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                for name in names {
                    if let Some(tbl) = name.0.last() {
                        let if_ex = if if_exists { "IF EXISTS " } else { "" };
                        ops.push(DdlOperation {
                            table: tbl.value.clone(),
                            sql: format!("DROP TABLE {}\"{}\"", if_ex, tbl.value),
                        });
                    }
                }
            }
            other => {
                warn!("Unsupported DDL intercepted: {:?}", other);
                return Err(anyhow::anyhow!(
                    "unsupported DDL statement intercepted; refusing to advance the \
                     replication checkpoint"
                ));
            }
        }
    }

    // Safety net: if the ALTER produced no local operation at all (e.g. it only added
    // an index), fall back to a pure schema-diff against the authoritative target.
    if ops.is_empty() {
        if let (Some(cur), Some(tgt)) = (current, target) {
            ops = diff_schema(cur, tgt)?;
        }
    }

    Ok(ops)
}

/// Compute the DuckDB statements required to bring a local table from `current` to
/// `target` (both MySQL-side schemas, in MySQL physical column order).
pub fn diff_schema(
    current: &TableSchema,
    target: &TableSchema,
) -> anyhow::Result<Vec<DdlOperation>> {
    let mut ops = Vec::new();

    for col in &target.columns {
        if !current.columns.iter().any(|c| c.name == col.name) {
            ops.push(DdlOperation {
                table: target.table_name.clone(),
                sql: format!(
                    "ALTER TABLE \"{}\" ADD COLUMN IF NOT EXISTS \"{}\" {}",
                    target.table_name,
                    col.name,
                    col.duck_type.to_ddl()
                ),
            });
        }
    }

    for col in &current.columns {
        if !target.columns.iter().any(|c| c.name == col.name) {
            ops.push(DdlOperation {
                table: target.table_name.clone(),
                sql: format!(
                    "ALTER TABLE \"{}\" DROP COLUMN IF EXISTS \"{}\"",
                    target.table_name, col.name
                ),
            });
        }
    }

    Ok(ops)
}

/// Resolve the DuckDB type of a column, preferring the authoritative MySQL schema.
fn resolve_type(col_name: &str, ast_type: DataType, target: Option<&TableSchema>) -> DuckDbType {
    if let Some(tgt) = target {
        if let Some(col) = tgt.columns.iter().find(|c| c.name == col_name) {
            return col.duck_type.clone();
        }
    }
    sql_type_to_duckdb(&ast_type)
}

/// Translate the few MySQL column options that affect the local table.
fn column_option_to_duckdb(opt: &ColumnOption) -> Option<String> {
    match opt {
        ColumnOption::NotNull => Some("NOT NULL".to_string()),
        ColumnOption::Null => Some("NULL".to_string()),
        ColumnOption::Default(expr) => Some(format!("DEFAULT {}", expr)),
        ColumnOption::Unique { is_primary, .. } => {
            if *is_primary {
                Some("PRIMARY KEY".to_string())
            } else {
                Some("UNIQUE".to_string())
            }
        }
        _ => None,
    }
}

/// Map a `sqlparser` AST data type onto a DuckDB type.
///
/// Unknown MySQL types degrade to `VARCHAR` rather than to an opaque `Unknown`,
/// so a column never changes its storage class when the schema evolves.
pub fn sql_type_to_duckdb(data_type: &DataType) -> DuckDbType {
    match data_type {
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::MediumInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::Int2(_)
        | DataType::Int4(_)
        | DataType::Int64
        | DataType::Int32 => DuckDbType::Integer,
        DataType::BigInt(_) | DataType::Int8(_) | DataType::Int16 | DataType::Int128 => {
            DuckDbType::BigInt
        }
        DataType::Float(_) | DataType::Real | DataType::Double | DataType::DoublePrecision => {
            DuckDbType::Double
        }
        DataType::Decimal(exact_info) | DataType::Numeric(exact_info) => {
            let (p, s) = match exact_info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sqlparser::ast::ExactNumberInfo::None => (18, 4),
            };
            DuckDbType::Decimal(p, s)
        }
        DataType::Varchar(_)
        | DataType::Text
        | DataType::Char(_)
        | DataType::Character(_)
        | DataType::String(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Nvarchar(_) => DuckDbType::Varchar,
        DataType::Timestamp(_, _) | DataType::Datetime(_) => DuckDbType::Timestamp,
        DataType::Date => DuckDbType::Date,
        DataType::JSON => DuckDbType::Json,
        DataType::Blob(_) | DataType::Binary(_) | DataType::Varbinary(_) | DataType::Bytea => {
            DuckDbType::Blob
        }
        DataType::Boolean | DataType::Bool => DuckDbType::Boolean,
        other => {
            warn!("Unmapped MySQL type {:?} — storing as VARCHAR", other);
            DuckDbType::Varchar
        }
    }
}

/// Apply a MySQL DDL statement to a **source-side** (MySQL) schema.
///
/// The source schema describes the *physical* column order MySQL uses for row
/// images. It must be updated with MySQL semantics — including `FIRST`/`AFTER`
/// positioning — because the row values arriving from the binlog are laid out in
/// exactly that order.
pub fn apply_ddl_to_source_schema(raw_sql: &str, schema: &mut TableSchema) -> anyhow::Result<()> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, raw_sql)
        .map_err(|e| anyhow::anyhow!("failed to parse MySQL DDL '{raw_sql}': {e}"))?;

    for stmt in ast {
        if let Statement::AlterTable { operations, .. } = stmt {
            for op in operations {
                match op {
                    AlterTableOperation::AddColumn {
                        column_def,
                        column_position,
                        ..
                    } => {
                        let name = column_def.name.value.clone();
                        if schema.columns.iter().any(|c| c.name == name) {
                            continue;
                        }
                        let col = ColumnDef {
                            name: name.clone(),
                            duck_type: sql_type_to_duckdb(&column_def.data_type),
                            nullable: true,
                            is_primary_key: false,
                        };
                        let idx = match column_position {
                            Some(MySQLColumnPosition::First) => 0,
                            Some(MySQLColumnPosition::After(after)) => schema
                                .columns
                                .iter()
                                .position(|c| c.name == after.value)
                                .map(|p| p + 1)
                                .unwrap_or(schema.columns.len()),
                            None => schema.columns.len(),
                        };
                        schema.columns.insert(idx, col);
                    }
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        schema.columns.retain(|c| c.name != column_name.value);
                    }
                    AlterTableOperation::RenameColumn {
                        old_column_name,
                        new_column_name,
                    } => {
                        for c in schema.columns.iter_mut() {
                            if c.name == old_column_name.value {
                                c.name = new_column_name.value.clone();
                            }
                        }
                    }
                    AlterTableOperation::ModifyColumn {
                        col_name,
                        data_type,
                        ..
                    } => {
                        let duck_type = sql_type_to_duckdb(&data_type);
                        for c in schema.columns.iter_mut() {
                            if c.name == col_name.value {
                                c.duck_type = duck_type.clone();
                            }
                        }
                    }
                    AlterTableOperation::ChangeColumn {
                        old_name,
                        new_name,
                        data_type,
                        ..
                    } => {
                        let duck_type = sql_type_to_duckdb(&data_type);
                        for c in schema.columns.iter_mut() {
                            if c.name == old_name.value {
                                c.name = new_name.value.clone();
                                c.duck_type = duck_type.clone();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

/// Convenience re-export used by callers that only need the position enum.
pub use sqlparser::ast::MySQLColumnPosition as ColumnPosition;
