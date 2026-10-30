//! Table and column scoping (`ReplicationFilter` and `Projection`).
//!
//! Enforces the replication security contract:
//! - `table_exclude` always wins over `table_include`.
//! - `columns.<table>.exclude` always wins over `columns.<table>.include`.
//! - Matching is case-insensitive and supports both bare (`users`) and qualified
//!   (`app.users` / `app__users`) table names, while preserving source column casing.
//! - Primary-key columns must never be excluded: if a filter excludes any PK column
//!   of a replicated table, projection fails closed with [`FilterError::ExcludedPrimaryKey`].

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::cdc::types::{ColumnDef, TableSchema, Value};

/// Per-table column allow/deny rules.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnFilter {
    /// If non-empty, only these columns are replicated (minus `exclude`).
    #[serde(default)]
    pub include: Vec<String>,
    /// Columns that must never be replicated (always wins over `include`).
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl ColumnFilter {
    /// Returns `true` if `col_name` is allowed by this column filter.
    pub fn column_allowed(&self, col_name: &str) -> bool {
        let col_clean = clean_ident(col_name);
        if self
            .exclude
            .iter()
            .any(|ex| clean_ident(ex).eq_ignore_ascii_case(col_clean))
        {
            return false;
        }
        if self.include.is_empty() {
            return true;
        }
        self.include
            .iter()
            .any(|inc| clean_ident(inc).eq_ignore_ascii_case(col_clean))
    }
}

/// Account- or pipeline-level table and column filtering configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationFilter {
    /// If non-empty, only listed tables are replicated (minus `table_exclude`).
    #[serde(default)]
    pub table_include: Vec<String>,
    /// Tables that must never be replicated (always wins over `table_include`).
    #[serde(default)]
    pub table_exclude: Vec<String>,
    /// Per-table column rules keyed by `table` or `db.table`.
    #[serde(default)]
    pub columns: IndexMap<String, ColumnFilter>,
}

/// Errors produced when validating or projecting a table schema through a [`ReplicationFilter`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FilterError {
    #[error(
        "table '{table}' excludes primary key column(s): {}",
        excluded_pks.join(", ")
    )]
    ExcludedPrimaryKey {
        table: String,
        excluded_pks: Vec<String>,
    },
}

/// The result of projecting a full MySQL [`TableSchema`] through a [`ReplicationFilter`].
///
/// Binlog row images are positional over the **full** MySQL schema. Callers must parse
/// binlog rows using the full schema first and then call [`Projection::project_row`]
/// to strip excluded columns and remap primary-key indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    /// Projected schema containing only allowed columns in source order, sent to `StoreWriter`.
    pub schema: TableSchema,
    /// 0-based indices into the **full** source schema for every kept column.
    pub keep: Vec<usize>,
    /// 0-based primary-key column indices within the **projected** row (`schema.columns`).
    pub pk_indices: Vec<usize>,
}

impl Projection {
    /// Create an identity projection keeping all columns of `full`.
    pub fn identity(full: &TableSchema) -> Self {
        let keep: Vec<usize> = (0..full.columns.len()).collect();
        let pk_indices = full.primary_key_indices();
        Self {
            schema: full.clone(),
            keep,
            pk_indices,
        }
    }

    /// Project a full-schema row vector into a projected row vector containing only `keep` indices.
    pub fn project_row(&self, full_row: &[Value]) -> Vec<Value> {
        self.keep
            .iter()
            .map(|&idx| full_row.get(idx).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Extract the primary-key values from an already-projected row.
    pub fn extract_pk_from_projected(&self, projected_row: &[Value]) -> Vec<Value> {
        self.pk_indices
            .iter()
            .map(|&idx| projected_row.get(idx).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Returns `true` if every column of the source schema is kept.
    pub fn is_identity(&self, full_column_count: usize) -> bool {
        self.keep.len() == full_column_count && self.keep.iter().enumerate().all(|(i, &k)| i == k)
    }
}

impl ReplicationFilter {
    /// Create a filter with only an include list of tables (compatibility helper for `filter_tables`).
    pub fn from_table_include(tables: Vec<String>) -> Self {
        Self {
            table_include: tables,
            table_exclude: Vec::new(),
            columns: IndexMap::new(),
        }
    }

    /// Returns `true` if no table or column rules are configured.
    pub fn is_empty(&self) -> bool {
        self.table_include.is_empty() && self.table_exclude.is_empty() && self.columns.is_empty()
    }

    /// Check whether a table (`db`, `table`) is allowed to be replicated.
    ///
    /// Accepts bare (`users`) or qualified (`db.users` / `db__users`) names in both
    /// the filter rules and the `table` argument. `table_exclude` always wins over
    /// `table_include`.
    pub fn table_allowed(&self, db: &str, table: &str) -> bool {
        let (inferred_db, bare) = split_table_ref(db, table);

        if self
            .table_exclude
            .iter()
            .any(|pat| pattern_matches_table(pat, inferred_db, bare))
        {
            return false;
        }

        if self.table_include.is_empty() {
            return true;
        }

        self.table_include
            .iter()
            .any(|pat| pattern_matches_table(pat, inferred_db, bare))
    }

    /// Find the effective [`ColumnFilter`] for `(db, table)`, if any.
    ///
    /// If both a qualified (`db.table`) and bare (`table`) rule exist in `columns`,
    /// the qualified rule takes precedence.
    pub fn column_filter_for(&self, db: &str, table: &str) -> Option<&ColumnFilter> {
        let (inferred_db, bare) = split_table_ref(db, table);

        // 1. Check for an exact qualified match (`db.table` or `db__table`) first when `db` is known.
        if !inferred_db.is_empty() {
            for (key, rule) in &self.columns {
                let (key_db, key_bare) = split_pattern(key);
                if let Some(kdb) = key_db {
                    if kdb.eq_ignore_ascii_case(inferred_db) && key_bare.eq_ignore_ascii_case(bare)
                    {
                        return Some(rule);
                    }
                }
            }
        }

        // 2. Fall back to any matching pattern (bare or qualified).
        for (key, rule) in &self.columns {
            if pattern_matches_table(key, inferred_db, bare) {
                return Some(rule);
            }
        }

        None
    }

    /// Project a full [`TableSchema`] using its `table_name` (which may be bare, `db.table`, or `db__table`).
    pub fn project(&self, full: &TableSchema) -> Result<Projection, FilterError> {
        self.project_table("", &full.table_name, full)
    }

    /// Project a full [`TableSchema`] with explicit `(db, table)` context.
    ///
    /// Validates that no primary-key column is excluded. If any PK column is excluded
    /// (either via `exclude` or omitted from a non-empty `include`), returns
    /// [`FilterError::ExcludedPrimaryKey`] listing the exact offending column names.
    pub fn project_table(
        &self,
        db: &str,
        table: &str,
        full: &TableSchema,
    ) -> Result<Projection, FilterError> {
        let Some(col_filter) = self.column_filter_for(db, table) else {
            return Ok(Projection::identity(full));
        };

        let mut excluded_pks = Vec::new();
        let mut keep = Vec::with_capacity(full.columns.len());
        let mut projected_cols: Vec<ColumnDef> = Vec::with_capacity(full.columns.len());

        for (idx, col) in full.columns.iter().enumerate() {
            if col_filter.column_allowed(&col.name) {
                keep.push(idx);
                projected_cols.push(col.clone());
            } else if col.is_primary_key {
                excluded_pks.push(col.name.clone());
            }
        }

        if !excluded_pks.is_empty() {
            return Err(FilterError::ExcludedPrimaryKey {
                table: full.table_name.clone(),
                excluded_pks,
            });
        }

        let projected_schema = TableSchema {
            table_name: full.table_name.clone(),
            columns: projected_cols,
        };
        let pk_indices = projected_schema.primary_key_indices();

        Ok(Projection {
            schema: projected_schema,
            keep,
            pk_indices,
        })
    }

    /// Check whether a specific pattern in `table_include` matches any discovered `(db, bare)` pair.
    pub fn pattern_matches_any(pattern: &str, discovered: &[(String, String)]) -> bool {
        discovered
            .iter()
            .any(|(db, bare)| pattern_matches_table(pattern, db, bare))
    }
}

fn clean_ident(s: &str) -> &str {
    s.trim().trim_matches(|c| c == '`' || c == '"' || c == '\'')
}

fn split_pattern(pattern: &str) -> (Option<&str>, &str) {
    let cleaned = clean_ident(pattern);
    if let Some((db, tbl)) = cleaned.split_once('.') {
        (Some(clean_ident(db)), clean_ident(tbl))
    } else if let Some((db, tbl)) = cleaned.split_once("__") {
        (Some(clean_ident(db)), clean_ident(tbl))
    } else {
        (None, cleaned)
    }
}

fn split_table_ref<'a>(db: &'a str, table: &'a str) -> (&'a str, &'a str) {
    let db_clean = clean_ident(db);
    let tbl_clean = clean_ident(table);
    if !db_clean.is_empty() {
        if let Some((prefix, rest)) = tbl_clean.split_once('.') {
            if prefix.eq_ignore_ascii_case(db_clean) {
                return (db_clean, clean_ident(rest));
            }
        }
        if let Some((prefix, rest)) = tbl_clean.split_once("__") {
            if prefix.eq_ignore_ascii_case(db_clean) {
                return (db_clean, clean_ident(rest));
            }
        }
        return (db_clean, tbl_clean);
    }
    if let Some((d, t)) = tbl_clean.split_once('.') {
        (clean_ident(d), clean_ident(t))
    } else if let Some((d, t)) = tbl_clean.split_once("__") {
        (clean_ident(d), clean_ident(t))
    } else {
        ("", tbl_clean)
    }
}

fn pattern_matches_table(pattern: &str, db: &str, bare: &str) -> bool {
    let (pat_db, pat_bare) = split_pattern(pattern);
    if !pat_bare.eq_ignore_ascii_case(bare) {
        return false;
    }
    match pat_db {
        Some(p_db) => {
            // If the caller knows the DB, it must match. If the caller passed an
            // unqualified table name (db == ""), allow matching only if the table
            // reference itself was split above.
            !db.is_empty() && p_db.eq_ignore_ascii_case(db)
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::types::DuckDbType;

    fn col(name: &str, ty: DuckDbType, pk: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            duck_type: ty,
            nullable: !pk,
            is_primary_key: pk,
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            table_name: "users".to_string(),
            columns: vec![
                col("id", DuckDbType::BigInt, true),
                col("password_hash", DuckDbType::Varchar, false),
                col("email", DuckDbType::Varchar, false),
                col("secret_token", DuckDbType::Varchar, false),
                col("created_at", DuckDbType::Timestamp, false),
            ],
        }
    }

    #[test]
    fn test_table_include_exclude_precedence_and_qualified_names() {
        let filter = ReplicationFilter {
            table_include: vec!["users".into(), "app.orders".into(), "audit_log".into()],
            table_exclude: vec!["audit_log".into(), "other.users".into()],
            columns: IndexMap::new(),
        };

        // Bare include matches any DB unless excluded for that DB
        assert!(filter.table_allowed("app", "users"));
        assert!(filter.table_allowed("APP", "USERS")); // case-insensitive
        assert!(!filter.table_allowed("other", "users")); // excluded for `other`

        // Qualified include matches only `app.orders`
        assert!(filter.table_allowed("app", "orders"));
        assert!(filter.table_allowed("", "app.orders"));
        assert!(filter.table_allowed("", "app__orders"));
        assert!(!filter.table_allowed("other", "orders"));

        // Exclude always wins over include
        assert!(!filter.table_allowed("app", "audit_log"));
        assert!(!filter.table_allowed("", "audit_log"));

        // Unlisted table rejected when include is non-empty
        assert!(!filter.table_allowed("app", "products"));
    }

    #[test]
    fn test_empty_include_allows_all_except_excluded() {
        let filter = ReplicationFilter {
            table_include: vec![],
            table_exclude: vec!["sessions".into(), " billing.raw_events ".into()],
            columns: IndexMap::new(),
        };

        assert!(filter.table_allowed("app", "users"));
        assert!(filter.table_allowed("billing", "invoices"));
        assert!(!filter.table_allowed("app", "sessions"));
        assert!(!filter.table_allowed("billing", "raw_events"));
        assert!(filter.table_allowed("app", "raw_events"));
    }

    #[test]
    fn test_projection_excluded_middle_and_adjacent_columns_no_shift() {
        let mut columns = IndexMap::new();
        columns.insert(
            "users".to_string(),
            ColumnFilter {
                include: vec![],
                exclude: vec!["password_hash".into(), "secret_token".into()],
            },
        );
        let filter = ReplicationFilter {
            table_include: vec![],
            table_exclude: vec![],
            columns,
        };

        let full = sample_schema();
        let proj = filter.project(&full).expect("projection should succeed");

        assert_eq!(proj.keep, vec![0, 2, 4]);
        assert_eq!(proj.pk_indices, vec![0]);
        let col_names: Vec<&str> = proj
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "email", "created_at"]);

        let full_row = vec![
            Value::Integer(42),
            Value::Text(" hunter2 ".into()),
            Value::Text("alice@example.com".into()),
            Value::Text("tok_secret".into()),
            Value::Text("2026-10-30 00:00:00".into()),
        ];
        let projected_row = proj.project_row(&full_row);
        assert_eq!(
            projected_row,
            vec![
                Value::Integer(42),
                Value::Text("alice@example.com".into()),
                Value::Text("2026-10-30 00:00:00".into()),
            ]
        );
    }

    #[test]
    fn test_projection_excluded_first_and_last_with_middle_composite_pk() {
        let full = TableSchema {
            table_name: "order_items".into(),
            columns: vec![
                col("internal_note", DuckDbType::Varchar, false), // 0 - excluded
                col("order_id", DuckDbType::BigInt, true),        // 1 - PK 0
                col("secret_margin", DuckDbType::Double, false),  // 2 - excluded
                col("item_id", DuckDbType::BigInt, true),         // 3 - PK 1
                col("quantity", DuckDbType::Integer, false),      // 4 - kept
                col("raw_payload", DuckDbType::Json, false),      // 5 - excluded
            ],
        };

        let mut columns = IndexMap::new();
        columns.insert(
            "order_items".into(),
            ColumnFilter {
                include: vec!["ORDER_ID".into(), "item_id".into(), "quantity".into()],
                exclude: vec!["internal_note".into(), "raw_payload".into()],
            },
        );
        let filter = ReplicationFilter {
            table_include: vec![],
            table_exclude: vec![],
            columns,
        };

        let proj = filter.project(&full).unwrap();
        assert_eq!(proj.keep, vec![1, 3, 4]);
        // In the projected schema [order_id, item_id, quantity], the PKs are at 0 and 1!
        assert_eq!(proj.pk_indices, vec![0, 1]);

        let full_row = vec![
            Value::Text("note".into()),
            Value::Integer(100),
            Value::Float(0.45),
            Value::Integer(7),
            Value::Integer(3),
            Value::Text("{}".into()),
        ];
        let projected = proj.project_row(&full_row);
        assert_eq!(
            projected,
            vec![Value::Integer(100), Value::Integer(7), Value::Integer(3)]
        );
        assert_eq!(
            proj.extract_pk_from_projected(&projected),
            vec![Value::Integer(100), Value::Integer(7)]
        );
    }

    #[test]
    fn test_projection_fails_when_pk_excluded_explicitly_or_by_omission() {
        let full = sample_schema();

        // Case 1: PK explicitly in `exclude`
        let mut cols1 = IndexMap::new();
        cols1.insert(
            "users".into(),
            ColumnFilter {
                include: vec![],
                exclude: vec!["ID".into()],
            },
        );
        let f1 = ReplicationFilter {
            columns: cols1,
            ..Default::default()
        };
        let err1 = f1.project(&full).unwrap_err();
        assert_eq!(
            err1,
            FilterError::ExcludedPrimaryKey {
                table: "users".into(),
                excluded_pks: vec!["id".into()],
            }
        );

        // Case 2: Non-empty `include` omits PK
        let mut cols2 = IndexMap::new();
        cols2.insert(
            "users".into(),
            ColumnFilter {
                include: vec!["email".into(), "created_at".into()],
                exclude: vec![],
            },
        );
        let f2 = ReplicationFilter {
            columns: cols2,
            ..Default::default()
        };
        let err2 = f2.project(&full).unwrap_err();
        assert_eq!(
            err2,
            FilterError::ExcludedPrimaryKey {
                table: "users".into(),
                excluded_pks: vec!["id".into()],
            }
        );
    }
}
