//! Axum HTTP API for xloka-flow.
//!
//! Endpoints:
//!   GET  /health          → 200 OK + uptime
//!   GET  /tables          → JSON list of tables with row counts
//!   POST /query           → body = SQL SELECT; returns JSON array of rows
//!
//! Security: the SELECT-only guard rejects any non-SELECT statement with 400.

use axum::{
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use duckdb::{Connection, Row};
use serde_json::{json, Map, Value as JsonValue};
use sqlparser::{
    dialect::GenericDialect,
    parser::Parser,
    ast::Statement,
};
use std::time::Instant;
use tower_http::cors::CorsLayer;
use tracing::debug;

use crate::AppState;
use crate::error::FlowError;

// ─── Error helper ─────────────────────────────────────────────────────────────

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": self.1 }).to_string();
        Response::builder()
            .status(self.0)
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }
}

impl From<FlowError> for ApiError {
    fn from(e: FlowError) -> Self {
        match e {
            FlowError::SqlGuard { reason } => ApiError(StatusCode::BAD_REQUEST, reason),
            other => ApiError(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        }
    }
}

impl From<duckdb::Error> for ApiError {
    fn from(e: duckdb::Error) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

// ─── SELECT-only guard ────────────────────────────────────────────────────────

fn guard_select_only(sql: &str) -> Result<(), FlowError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| {
        FlowError::sql_guard(format!("SQL parse error: {e}"))
    })?;

    if stmts.is_empty() {
        return Err(FlowError::sql_guard("Empty SQL statement"));
    }

    if stmts.len() > 1 {
        return Err(FlowError::sql_guard(
            "Multi-statement SQL is not allowed",
        ));
    }

    match &stmts[0] {
        Statement::Query(_) => Ok(()),
        other => Err(FlowError::sql_guard(format!(
            "Only SELECT queries are allowed. Got: {}",
            other
        ))),
    }
}

// ─── Route builder ────────────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/tables", get(tables_handler))
        .route("/query", post(query_handler))
        .with_state(state)
        .layer(CorsLayer::permissive())
}

// ─── /health ─────────────────────────────────────────────────────────────────

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

async fn health_handler() -> impl IntoResponse {
    let uptime_secs = START.get_or_init(Instant::now).elapsed().as_secs();
    Json(json!({
        "status": "ok",
        "uptime_seconds": uptime_secs,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

// ─── /tables ─────────────────────────────────────────────────────────────────

async fn tables_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let db_path = state.db_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        // Open a fresh read connection for this request.
        // DuckDB supports multiple concurrent readers.
        let conn = Connection::open(&db_path)?;

        // List all non-system tables
        let mut stmt = conn.prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'main' ORDER BY table_name",
        )?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        let mut tables = Vec::new();
        for tbl in table_names {
            let count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {tbl}"),
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            tables.push(json!({ "table": tbl, "row_count": count }));
        }
        Ok::<_, duckdb::Error>(tables)
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e: duckdb::Error| ApiError::from(e))?;

    Ok(Json(json!({ "tables": result })))
}

// ─── /query ──────────────────────────────────────────────────────────────────

async fn query_handler(
    State(state): State<AppState>,
    body: String,
) -> Result<impl IntoResponse, ApiError> {
    let sql = body.trim().to_owned();
    debug!("POST /query: {sql}");

    // Security: reject anything that isn't a SELECT.
    guard_select_only(&sql).map_err(ApiError::from)?;

    let db_path = state.db_path.clone();

    let rows_json = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)?;

        let mut stmt = conn.prepare(&sql)?;
        let col_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();

        let rows: Vec<JsonValue> = stmt
            .query_map([], |row| {
                let mut map = Map::new();
                for (i, col) in col_names.iter().enumerate() {
                    let val = duckdb_val_to_json(row, i);
                    map.insert(col.clone(), val);
                }
                Ok(JsonValue::Object(map))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok::<_, duckdb::Error>(rows)
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e: duckdb::Error| ApiError::from(e))?;

    Ok(Json(json!({
        "rows": rows_json,
        "count": rows_json.len(),
    })))
}

// ─── DuckDB → JSON value coercion ─────────────────────────────────────────────

fn duckdb_val_to_json(row: &Row<'_>, idx: usize) -> JsonValue {
    // Try each type in preference order
    if let Ok(Some(v)) = row.get::<_, Option<i64>>(idx) {
        return JsonValue::Number(v.into());
    }
    if let Ok(Some(v)) = row.get::<_, Option<f64>>(idx) {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return JsonValue::Number(n);
        }
        return JsonValue::String(v.to_string());
    }
    if let Ok(Some(v)) = row.get::<_, Option<bool>>(idx) {
        return JsonValue::Bool(v);
    }
    if let Ok(Some(v)) = row.get::<_, Option<String>>(idx) {
        return JsonValue::String(v);
    }
    JsonValue::Null
}
