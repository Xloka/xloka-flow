use axum::{
    body::Body,
    extract::{State, Path},
    http::{StatusCode, HeaderMap},
    response::{IntoResponse, Response},
    routing::{get, post, delete},
    Json, Router,
};
use duckdb::Row;
use serde::Deserialize;
use serde_json::{json, Map, Value as JsonValue};
use sqlparser::{dialect::GenericDialect, parser::Parser, ast::Statement};
use std::time::Instant;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{AppState, accounts::Account};
use crate::error::FlowError;

// ─── Errors ───────────────────────────────────────────────────────────────────

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

// ─── Auth Helpers ────────────────────────────────────────────────────────────

fn check_admin(headers: &HeaderMap) -> Result<(), ApiError> {
    let token = std::env::var("XLOKA__ADMIN_TOKEN").unwrap_or_else(|_| "secret".into());
    if let Some(auth) = headers.get("authorization") {
        let auth_str = auth.to_str().unwrap_or("");
        if auth_str == format!("Bearer {}", token) {
            return Ok(());
        }
    }
    Err(ApiError(StatusCode::UNAUTHORIZED, "Invalid admin token".into()))
}

// ─── SQL Guard ───────────────────────────────────────────────────────────────

fn guard_select_only(sql: &str) -> Result<(), FlowError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| FlowError::sql_guard(format!("SQL parse error: {e}")))?;
    if stmts.is_empty() { return Err(FlowError::sql_guard("Empty SQL")); }
    if stmts.len() > 1 { return Err(FlowError::sql_guard("Multi-statement SQL not allowed")); }
    match &stmts[0] {
        Statement::Query(_) => Ok(()),
        other => Err(FlowError::sql_guard(format!("Only SELECT queries allowed. Got: {}", other))),
    }
}

// ─── Router ──────────────────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    let api_routes = Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler))
        .route("/admin/accounts", get(list_accounts).post(add_account))
        .route("/admin/accounts/:id", delete(remove_account))
        .route("/admin/accounts/:id/tables", get(account_tables))
        .route("/admin/accounts/:id/sync", post(account_sync));

    Router::new()
        .nest("/api", api_routes)
        .with_state(state)
        .layer(CorsLayer::permissive())
}

// ─── /health ─────────────────────────────────────────────────────────────────

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

async fn health_handler() -> impl IntoResponse {
    let uptime = START.get_or_init(Instant::now).elapsed().as_secs();
    Json(json!({ "status": "ok", "uptime_seconds": uptime }))
}

// ─── /query (Account API) ──────────────────────────────────────────────────────

async fn query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, ApiError> {
    let auth = headers.get("authorization").and_then(|h| h.to_str().ok()).unwrap_or("");
    let token = auth.strip_prefix("Bearer ").ok_or(ApiError(StatusCode::UNAUTHORIZED, "Missing Bearer token".into()))?;

    let conn = state.account_manager.get_account_conn_by_api_key(token)
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "Invalid API Key".into()))?;

    let sql = body.trim().to_owned();
    guard_select_only(&sql).map_err(ApiError::from)?;

    let rows_json = tokio::task::spawn_blocking(move || {
        let conn = conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let col_names: Vec<String> = rows.as_ref().unwrap().column_names().into_iter().map(String::from).collect();
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut map = Map::new();
            for (i, col) in col_names.iter().enumerate() {
                map.insert(col.clone(), duckdb_val_to_json(row, i));
            }
            results.push(JsonValue::Object(map));
        }
        Ok::<_, duckdb::Error>(results)
    }).await.unwrap()?;

    Ok(Json(json!({ "rows": rows_json, "count": rows_json.len() })))
}

// ─── Admin Endpoints ─────────────────────────────────────────────────────────

async fn list_accounts(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    check_admin(&headers)?;
    let accounts = state.account_manager.get_accounts();
    
    let mut response = Vec::new();
    for c in accounts {
        let db_size = std::fs::metadata(format!("data/{}.db", c.id)).map(|m| m.len()).unwrap_or(0);
        response.push(json!({
            "id": c.id,
            "name": c.name,
            "mysql_url": c.mysql_url,
            "api_key": c.api_key,
            "status": c.status,
            "db_size_bytes": db_size,
        }));
    }
    
    Ok(Json(response))
}

#[derive(Deserialize)]
struct AddAccountReq {
    name: String,
    mysql_url: String,
}

async fn add_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AddAccountReq>,
) -> Result<impl IntoResponse, ApiError> {
    check_admin(&headers)?;
    
    let id = Uuid::new_v4().to_string().replace("-", "")[..12].to_string();
    let api_key = format!("xk_{}", Uuid::new_v4().to_string().replace("-", ""));
    
    let account = Account {
        id: id.clone(),
        name: payload.name,
        mysql_url: payload.mysql_url,
        api_key,
        status: "active".into(),
    };
    
    state.account_manager.spawn_and_add_account(account.clone())
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("Spawn error: {}", e)))?;
        
    let accounts = state.account_manager.get_accounts();
    state.account_manager.save_persisted(&accounts)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("Save error: {}", e)))?;
        
    Ok(Json(account))
}

async fn remove_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    check_admin(&headers)?;
    
    state.account_manager.remove_account(&id);
    let accounts = state.account_manager.get_accounts();
    state.account_manager.save_persisted(&accounts)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("Save error: {}", e)))?;
        
    // Clean up files safely
    let _ = std::fs::remove_file(format!("data/{}.db", id));
    let _ = std::fs::remove_file(format!("data/{}_checkpoint.json", id));
        
    Ok(Json(json!({"status": "deleted"})))
}

async fn account_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    check_admin(&headers)?;
    
    let conn = state.account_manager.get_account_conn_by_id(&id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Account not found".into()))?;

    let result = tokio::task::spawn_blocking(move || {
        let conn = conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")?;
        let table_names: Vec<String> = stmt.query_map([], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;
        let mut tables = Vec::new();
        for tbl in table_names {
            let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{tbl}\""), [], |row| row.get(0)).unwrap_or(0);
            tables.push(json!({ "table": tbl, "row_count": count }));
        }
        Ok::<_, duckdb::Error>(tables)
    }).await.unwrap()?;

    Ok(Json(json!({ "tables": result })))
}

async fn account_sync(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    check_admin(&headers)?;
    
    let account = state.account_manager.get_account_by_id(&id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Account not found".into()))?;
    let conn = state.account_manager.get_account_conn_by_id(&id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Connection not found".into()))?;

    let mysql_url = account.mysql_url.clone();

    tokio::task::spawn_blocking(move || {
        let conn = conn.lock().unwrap();
        info!("Starting 1-Click Initial Sync for account {}", id);
        let _ = conn.execute_batch("INSTALL mysql; LOAD mysql;");
        let attach_sql = format!("ATTACH '{}' AS sync_mysql_db (TYPE mysql);", mysql_url);
        if let Err(e) = conn.execute_batch(&attach_sql) {
            warn!("Sync failed to attach: {}", e);
            return;
        }
        
        let mut stmt = match conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'sync_mysql_db'") {
            Ok(s) => s,
            Err(_) => return,
        };
        let tables: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().filter_map(|r| r.ok()).collect();
        
        for table in tables {
            let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{}'", table), [], |r| r.get(0)).unwrap_or(0);
            if count > 0 {
                info!("Syncing table '{}' for account {}...", table, id);
                let sync_sql = format!("INSERT INTO main.\"{}\" SELECT * FROM sync_mysql_db.\"{}\";", table, table);
                let _ = conn.execute_batch(&sync_sql);
            }
        }
        let _ = conn.execute_batch("DETACH sync_mysql_db;");
        info!("Sync completed for account {}", id);
    });

    Ok(Json(json!({ "status": "Sync started in background" })))
}

fn duckdb_val_to_json(row: &Row<'_>, idx: usize) -> JsonValue {
    if let Ok(Some(v)) = row.get::<_, Option<i64>>(idx) { return JsonValue::Number(v.into()); }
    if let Ok(Some(v)) = row.get::<_, Option<f64>>(idx) {
        if let Some(n) = serde_json::Number::from_f64(v) { return JsonValue::Number(n); }
        return JsonValue::String(v.to_string());
    }
    if let Ok(Some(v)) = row.get::<_, Option<bool>>(idx) { return JsonValue::Bool(v); }
    if let Ok(Some(v)) = row.get::<_, Option<String>>(idx) { return JsonValue::String(v); }
    JsonValue::Null
}
