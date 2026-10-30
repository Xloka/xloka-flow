//! REST & WebSocket routing for xloka-flow v0.2.0.

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::api::metrics::{health_handler, metrics_handler};
use crate::api::queries::execute_query_def_with_limit;
use crate::api::ws::ws_handler;
use crate::AppState;

/// Create the full Axum router mounting all REST endpoints, WebSocket streaming, and metrics.
pub fn create_router(state: AppState) -> Router {
    let mut cors = CorsLayer::permissive();
    if let Some(ref domains) = state.settings.allowed_domains {
        // An unset/empty value means "no restriction configured" — treat it the
        // same as `None` and keep the permissive default. Without this, an
        // empty string parses into a single empty `HeaderValue`, producing
        // `allow-origin: ""`, which no browser ever matches: every cross-origin
        // request would silently fail.
        let domains = domains.trim();
        if !domains.is_empty() {
            // tower-http treats a literal `*` as an origin string, not as a
            // wildcard. Browsers never send `Origin: *`, so passing it through
            // would silently disable CORS instead of allowing everything.
            if domains.split(',').any(|d| d.trim() == "*") {
                cors = CorsLayer::permissive();
            } else {
                let origins: Vec<HeaderValue> = domains
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| s.parse().ok())
                    .collect();
                if !origins.is_empty() {
                    cors = CorsLayer::new()
                        .allow_origin(origins)
                        .allow_methods(tower_http::cors::Any)
                        .allow_headers(tower_http::cors::Any);
                }
            }
        }
    }

    if state
        .settings
        .admin_token
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        tracing::warn!(
            "Admin API is disabled because XLOKA__ADMIN_TOKEN (or --admin-token) is not configured. Admin endpoints will return 503 Service Unavailable."
        );
    }

    let admin_routes = Router::new()
        .route(
            "/accounts",
            get(admin_list_accounts).post(admin_add_account),
        )
        .route(
            "/accounts/{id}",
            axum::routing::delete(admin_remove_account),
        )
        .route("/accounts/{id}/tables", get(admin_account_tables))
        .route(
            "/accounts/{id}/sync",
            axum::routing::post(admin_account_sync),
        )
        .route(
            "/accounts/{id}/resync",
            axum::routing::post(admin_account_sync),
        );

    let api_v1 = Router::new()
        .route("/queries", get(list_queries_endpoint))
        .route("/query", axum::routing::post(raw_query_handler))
        .route("/query/{query_name}", get(query_endpoint))
        .route("/queries/{query_name}", get(query_endpoint))
        .route("/ws", get(ws_handler))
        .nest("/admin", admin_routes.clone());

    Router::new()
        .route("/health", get(health_route))
        .route("/metrics", get(metrics_route))
        .route("/ws", get(ws_handler))
        .nest("/api/v1", api_v1)
        .nest("/api/admin", admin_routes)
        // Backwards compatibility routes (v0.1.0)
        .route("/api/query", axum::routing::post(raw_query_handler))
        .route("/api/health", get(health_route))
        .route("/api/v1/health", get(health_route))
        .route("/api/v1/metrics", get(metrics_route))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[derive(serde::Deserialize, Default)]
pub struct ListQueriesParams {
    pub account_id: Option<String>,
    pub api_key: Option<String>,
}

async fn list_queries_endpoint(
    State(state): State<AppState>,
    Query(params): Query<ListQueriesParams>,
    headers: axum::http::HeaderMap,
) -> Response {
    let api_key = extract_api_key(&headers, params.api_key.as_deref());
    let target_account_id = resolve_target(&state, params.account_id.as_deref(), api_key)
        .ok()
        .and_then(|(_, acc_id)| acc_id)
        .or(params.account_id);
    let queries = state.resolve_all_queries(target_account_id.as_deref());
    let json = serde_json::json!({
        "queries": queries
    });
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json.to_string(),
    )
        .into_response()
}

async fn health_route(State(state): State<AppState>) -> Response {
    health_handler(Arc::clone(&state.metrics)).await
}

async fn metrics_route(State(state): State<AppState>) -> Response {
    metrics_handler(Arc::clone(&state.metrics)).await
}

// ─── Query Endpoint ───────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct QueryOptions {
    #[serde(default = "default_format")]
    pub format: String,
    pub max_staleness_ms: Option<u64>,
    /// Optional account whose data should be queried.
    pub account_id: Option<String>,
    /// Optional API key passed via query string (useful for browser clients).
    pub api_key: Option<String>,
}

fn default_format() -> String {
    "json".into()
}

/// Extract a data-plane API key from headers (`x-api-key` or `Authorization: Bearer`)
/// or from the `?api_key=` query parameter.
fn extract_api_key<'a>(
    headers: &'a axum::http::HeaderMap,
    query_api_key: Option<&'a str>,
) -> Option<&'a str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
        })
        .or(query_api_key)
        .filter(|s| !s.is_empty())
}

/// Resolve which dataset a request targets.
///
/// * `x-api-key: <account_key>` (or `?api_key=`) selects the account owning that key.
/// * `x-api-key: <data_api_key>` authorizes the request and honors `?account_id=<id>` if present.
/// * `?account_id=<id>` selects a replicated account when the data plane is open.
/// * Otherwise the default (single-tenant) dataset is used.
fn resolve_target(
    state: &AppState,
    account_id: Option<&str>,
    api_key: Option<&str>,
) -> Result<(PathBuf, Option<String>), (StatusCode, String)> {
    if let Some(key) = api_key {
        // 1. Check if the key belongs to a specific replicated account.
        if let Some(account) = state.account_manager.get_account_by_api_key(key) {
            if let Some(requested_id) = account_id {
                if requested_id != account.id {
                    return Err((
                        StatusCode::FORBIDDEN,
                        "API key does not match the requested account_id".into(),
                    ));
                }
            }
            let db_path = state
                .account_manager
                .get_account_db_path_by_id(&account.id)
                .ok_or_else(|| (StatusCode::NOT_FOUND, "Account not found".into()))?;
            return Ok((PathBuf::from(db_path), Some(account.id)));
        }

        // 2. Otherwise the key must match the global data-plane key (if configured).
        let matches_global = state
            .settings
            .data_api_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .is_some_and(|expected| constant_time_eq(key, expected));

        if !matches_global {
            return Err((StatusCode::UNAUTHORIZED, "Invalid API key".into()));
        }
        // Global data-plane key matched — fall through so `?account_id=` is still honored.
    }

    if let Some(id) = account_id {
        let db_path = state
            .account_manager
            .get_account_db_path_by_id(id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Account '{id}' not found")))?;
        return Ok((PathBuf::from(db_path), Some(id.to_string())));
    }

    Ok((PathBuf::from(&state.settings.db_path), None::<String>))
}

/// Enforce the optional data-plane API key.
///
/// When `XLOKA__DATA_API_KEY` is configured, requests to the read-only data
/// plane must present either the global `data_api_key` or a valid per-account
/// API key. The admin API keeps its own token.
fn check_data_plane_auth(
    state: &AppState,
    provided: Option<&str>,
) -> Result<(), (StatusCode, String)> {
    let Some(expected) = state
        .settings
        .data_api_key
        .as_deref()
        .filter(|k| !k.is_empty())
    else {
        return Ok(());
    };

    match provided {
        Some(t)
            if constant_time_eq(t, expected)
                || state.account_manager.get_account_by_api_key(t).is_some() =>
        {
            Ok(())
        }
        _ => Err((
            StatusCode::UNAUTHORIZED,
            json!({ "error": "Missing or invalid API key" }).to_string(),
        )),
    }
}

async fn query_endpoint(
    State(state): State<AppState>,
    Path(query_name): Path<String>,
    Query(opts): Query<QueryOptions>,
    Query(all_params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let api_key = extract_api_key(&headers, opts.api_key.as_deref());
    check_data_plane_auth(&state, api_key)?;

    // 1. Resolve the target dataset (default store or a replicated account)
    let (db_path, target_account_id) = resolve_target(&state, opts.account_id.as_deref(), api_key)
        .map_err(|(s, e)| (s, json!({ "error": e }).to_string()))?;

    // 2. Check replication lag staleness guard (per-account when targeting an account)
    if let Some(max_lag) = opts.max_staleness_ms {
        let current_lag = match target_account_id.as_deref() {
            Some(acc_id) => state
                .account_manager
                .get_account_lag_ms(acc_id)
                .unwrap_or_else(|| state.metrics.replication_lag_ms()),
            None => state.metrics.replication_lag_ms(),
        };
        if current_lag > max_lag {
            let diag = json!({
                "error": "Replication lag exceeds max_staleness_ms",
                "current_replication_lag_ms": current_lag,
                "max_staleness_ms": max_lag
            });
            return Err((StatusCode::TOO_EARLY, diag.to_string()));
        }
    }

    // 3. Lookup query in per-account or global manifest
    let query_def = state
        .resolve_query(target_account_id.as_deref(), &query_name)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                json!({ "error": format!("Named query '{query_name}' not found") }).to_string(),
            )
        })?;

    // 4. Extract positional parameters p1, p2, ... pN
    let mut indexed_params: Vec<(usize, String)> = Vec::new();
    for (k, v) in all_params {
        if let Some(stripped) = k.strip_prefix('p') {
            if let Ok(idx) = stripped.parse::<usize>() {
                indexed_params.push((idx, v));
            }
        }
    }
    indexed_params.sort_by_key(|&(idx, _)| idx);
    let params: Vec<String> = indexed_params.into_iter().map(|(_, v)| v).collect();

    // 5. Execute query via JoinSet
    let res = execute_query_def_with_limit(
        Arc::new(db_path),
        Arc::new(query_def),
        params,
        state.settings.max_query_rows,
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Query error: {e}"),
        )
    })?;

    // 6. Format negotiation (json | ndjson | csv)
    match opts.format.to_lowercase().as_str() {
        "ndjson" => {
            let mut lines = Vec::new();
            if let Some(results) = res.get("results").and_then(|r| r.as_array()) {
                for stmt_res in results {
                    if let Some(rows) = stmt_res.get("rows").and_then(|r| r.as_array()) {
                        for row in rows {
                            if let Ok(line) = serde_json::to_string(row) {
                                lines.push(line);
                            }
                        }
                    }
                }
            }
            let body = lines.join("\n");
            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/x-ndjson")],
                body,
            )
                .into_response())
        }
        "csv" => {
            let mut csv_writer = csv::Writer::from_writer(vec![]);
            if let Some(results) = res.get("results").and_then(|r| r.as_array()) {
                if let Some(first_stmt) = results.first() {
                    if let Some(rows) = first_stmt.get("rows").and_then(|r| r.as_array()) {
                        let explicit_cols: Option<Vec<&str>> = first_stmt
                            .get("columns")
                            .and_then(|c| c.as_array())
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect());
                        let fallback_cols: Option<Vec<&str>> = rows
                            .first()
                            .and_then(|r| r.as_object())
                            .map(|obj| obj.keys().map(|k| k.as_str()).collect());
                        if let Some(headers) = explicit_cols.or(fallback_cols) {
                            if !headers.is_empty() {
                                let _ = csv_writer.write_record(&headers);
                            }

                            for row in rows {
                                if let Some(map) = row.as_object() {
                                    let record: Vec<String> = headers
                                        .iter()
                                        .map(|&h| {
                                            map.get(h)
                                                .map(|v| match v {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    serde_json::Value::Null => "".into(),
                                                    other => other.to_string(),
                                                })
                                                .unwrap_or_default()
                                        })
                                        .collect();
                                    let _ = csv_writer.write_record(&record);
                                }
                            }
                        }
                    }
                }
            }
            let csv_bytes = csv_writer.into_inner().unwrap_or_default();
            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
                csv_bytes,
            )
                .into_response())
        }
        _ => Ok(Json(res).into_response()),
    }
}

// ─── Admin Endpoints ─────────────────────────────────────────────────────────

/// Constant-time string comparison to avoid leaking the admin token via timing.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    let max = a.len().max(b.len());
    for i in 0..max {
        let ba = a.get(i).copied().unwrap_or(0);
        let bb = b.get(i).copied().unwrap_or(0);
        diff |= (ba ^ bb) as usize;
    }
    diff == 0
}

fn check_admin(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), (StatusCode, String)> {
    // Fail closed: without a configured token, admin routes are disabled entirely.
    let expected = state
        .settings
        .admin_token
        .as_deref()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Admin API disabled: set XLOKA__ADMIN_TOKEN (or --admin-token) to enable it".into(),
            )
        })?;

    let token = headers
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
        });

    match token {
        Some(t) if constant_time_eq(t, expected) => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, "Unauthorized admin access".into())),
    }
}

async fn admin_list_accounts(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    check_admin(&state, &headers)?;
    let accounts = state.account_manager.get_accounts();
    let mut response = Vec::with_capacity(accounts.len());
    for a in accounts {
        let db_size = state
            .account_manager
            .get_account_db_path_by_id(&a.id)
            .and_then(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .unwrap_or(0);
        let tel = state.account_manager.get_telemetry(&a.id);
        let runtime_status = tel
            .as_ref()
            .map(|t| t.status())
            .unwrap_or_else(|| a.status.clone());
        let lag_ms = tel.as_ref().map(|t| t.lag_ms()).unwrap_or(0);
        let last_error = tel.as_ref().and_then(|t| t.last_error());
        let started_at = tel.as_ref().map(|t| t.started_at.clone());
        let rows_synced = tel.as_ref().map(|t| t.rows_synced()).unwrap_or(0);
        let tables_count = tel.as_ref().map(|t| t.tables_count()).unwrap_or(0);
        let redacted = a.redacted();
        response.push(serde_json::json!({
            "id": redacted.id,
            "name": redacted.name,
            "mysql_url": redacted.mysql_url,
            "snapshot_mysql_url": redacted.snapshot_mysql_url,
            "api_key": redacted.api_key,
            "status": runtime_status,
            "lag_ms": lag_ms,
            "last_error": last_error,
            "started_at": started_at,
            "rows_synced": rows_synced,
            "tables": tables_count,
            "db_size_bytes": db_size,
            "filters": redacted.filters,
        }));
    }
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&response).unwrap_or_else(|_| "[]".into()),
    )
        .into_response())
}

#[derive(serde::Deserialize)]
struct AdminAddAccountReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    mysql_url: String,
    snapshot_mysql_url: Option<String>,
    #[serde(default)]
    filters: Option<crate::cdc::filter::ReplicationFilter>,
}

async fn admin_add_account(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<AdminAddAccountReq>,
) -> Result<Response, (StatusCode, String)> {
    check_admin(&state, &headers)?;
    if state.settings.accounts_read_only {
        return Err((
            StatusCode::CONFLICT,
            json!({ "error": "managed by accounts.json" }).to_string(),
        ));
    }
    let id = payload
        .id
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string().replace('-', "")[..12].to_string());
    let api_key = format!("xk_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    let account = crate::accounts::Account {
        id: id.clone(),
        name: payload.name,
        mysql_url: payload.mysql_url,
        snapshot_mysql_url: payload.snapshot_mysql_url,
        api_key,
        status: "active".into(),
        filters: payload.filters,
    };

    state
        .account_manager
        .spawn_and_add_account(account.clone())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to spawn account: {e}"),
            )
        })?;

    let all_accounts = state.account_manager.get_accounts();
    let _ = state.account_manager.save_persisted(&all_accounts);

    Ok((
        StatusCode::CREATED,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&account).unwrap_or_else(|_| "{}".into()),
    )
        .into_response())
}

#[derive(serde::Deserialize, Default)]
struct AdminRemoveAccountQuery {
    prune: Option<bool>,
}

async fn admin_remove_account(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<AdminRemoveAccountQuery>,
) -> Result<Response, (StatusCode, String)> {
    check_admin(&state, &headers)?;
    if state.settings.accounts_read_only {
        return Err((
            StatusCode::CONFLICT,
            json!({ "error": "managed by accounts.json" }).to_string(),
        ));
    }
    let prune = query.prune.unwrap_or(true);
    state.account_manager.stop_account(&id, prune);
    let all_accounts = state.account_manager.get_accounts();
    let _ = state.account_manager.save_persisted(&all_accounts);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({ "status": "deleted", "id": id, "pruned": prune }).to_string(),
    )
        .into_response())
}

async fn admin_account_tables(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    check_admin(&state, &headers)?;
    let conn = state
        .account_manager
        .get_account_conn_by_id(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Account not found".into()))?;

    let tables = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<serde_json::Value>> {
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_name != '_xloka_checkpoint' \
             ORDER BY table_name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut list = Vec::new();
        for name in rows.flatten() {
            let escaped = name.replace('"', "\"\"");
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM \"{escaped}\""), [], |r| {
                    r.get(0)
                })
                .unwrap_or(0);
            list.push(serde_json::json!({
                "table": name,
                "row_count": count,
            }));
        }
        Ok(list)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({ "tables": tables }).to_string(),
    )
        .into_response())
}

async fn admin_account_sync(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    check_admin(&state, &headers)?;

    let account = state
        .account_manager
        .get_account_by_id(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Account not found".into()))?;
    let conn = state
        .account_manager
        .get_account_conn_by_id(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Connection not found".into()))?;

    let raw_sync_url = account
        .snapshot_mysql_url
        .as_deref()
        .unwrap_or(&account.mysql_url)
        .to_string();
    let sync_mysql_url = crate::accounts::interpolate_env(&raw_sync_url).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": e.to_string() }).to_string(),
        )
    })?;
    let filter = account.filters.clone().unwrap_or_default();

    tokio::task::spawn_blocking(move || {
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        tracing::info!(
            "Starting 1-Click Initial Sync for account {} using {}",
            id,
            crate::cdc::live::mask_password(&sync_mysql_url)
        );
        let _ = conn.execute_batch("INSTALL mysql; LOAD mysql;");
        let escaped_url = sync_mysql_url.replace('\'', "''");
        let attach_sql = format!("ATTACH '{}' AS sync_mysql_db (TYPE mysql);", escaped_url);
        if let Err(e) = conn.execute_batch(&attach_sql) {
            tracing::warn!("Sync failed to attach: {}", e);
            return;
        }

        let mut stmt = match conn.prepare(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'sync_mysql_db'",
        ) {
            Ok(s) => s,
            Err(_) => {
                let _ = conn.execute_batch("DETACH sync_mysql_db;");
                return;
            }
        };
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();

        for table in tables {
            if !filter.table_allowed("", &table) {
                continue;
            }
            let escaped_tbl = table.replace('\'', "''");
            let count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM information_schema.tables \
                         WHERE table_schema = 'main' AND table_name = '{}'",
                        escaped_tbl
                    ),
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if count > 0 {
                // Build the explicit allowed column list from the local projected table
                let pragma_sql = format!("PRAGMA table_info('{}');", escaped_tbl);
                let allowed_cols: Vec<String> = conn
                    .prepare(&pragma_sql)
                    .ok()
                    .and_then(|mut s| {
                        s.query_map([], |r| r.get::<_, String>(1)).ok().map(|rows| {
                            rows.flatten()
                                .filter(|col_name| {
                                    filter
                                        .column_filter_for("", &table)
                                        .map(|cf| cf.column_allowed(col_name))
                                        .unwrap_or(true)
                                })
                                .collect()
                        })
                    })
                    .unwrap_or_default();

                if allowed_cols.is_empty() {
                    continue;
                }
                let quoted_cols = allowed_cols
                    .iter()
                    .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(", ");
                tracing::info!(
                    "Syncing table '{}' ({} columns) for account {}...",
                    table,
                    allowed_cols.len(),
                    id
                );
                let quoted = table.replace('"', "\"\"");
                let sync_sql = format!(
                    "INSERT OR REPLACE INTO main.\"{}\" ({}) SELECT {} FROM sync_mysql_db.\"{}\";",
                    quoted, quoted_cols, quoted_cols, quoted
                );
                let _ = conn.execute_batch(&sync_sql);
            }
        }
        let _ = conn.execute_batch("DETACH sync_mysql_db;");
        tracing::info!("Sync completed for account {}", id);
    });

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({ "status": "Sync started in background" }).to_string(),
    )
        .into_response())
}

// ─── Ad-Hoc Read-Only SQL Endpoint (POST /api/query) ─────────────────────────

async fn raw_query_handler(
    State(state): State<AppState>,
    Query(opts): Query<QueryOptions>,
    headers: axum::http::HeaderMap,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let api_key = extract_api_key(&headers, opts.api_key.as_deref());
    check_data_plane_auth(&state, api_key)?;

    let (db_path, _account_id) = resolve_target(&state, opts.account_id.as_deref(), api_key)
        .map_err(|(s, e)| (s, json!({ "error": e }).to_string()))?;

    let trimmed = body.trim();
    let sql = if trimmed.starts_with('{') {
        serde_json::from_str::<serde_json::Value>(trimmed)
            .ok()
            .and_then(|v| v.get("sql").and_then(|s| s.as_str()).map(String::from))
            .unwrap_or_else(|| trimmed.to_owned())
    } else {
        trimmed.to_owned()
    };
    crate::api::queries::guard_select_only(&sql).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            json!({ "error": e.to_string() }).to_string(),
        )
    })?;

    let dialect = sqlparser::dialect::GenericDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &sql).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            json!({ "error": format!("SQL parse error: {e}") }).to_string(),
        )
    })?;

    let is_multi = stmts.len() > 1;
    let n = stmts.len();
    let max_rows = state.settings.max_query_rows;
    let db_path = Arc::new(db_path);

    let mut join_set: tokio::task::JoinSet<Result<(usize, serde_json::Value), String>> =
        tokio::task::JoinSet::new();

    for (i, stmt) in stmts.into_iter().enumerate() {
        let db_path_c = Arc::clone(&db_path);
        let single_sql = stmt.to_string();

        join_set.spawn_blocking(move || {
            let conn = crate::api::queries::open_read_only(&db_path_c)
                .map_err(|e| format!("stmt[{i}] open: {e}"))?;
            let mut prep = conn
                .prepare(&single_sql)
                .map_err(|e| format!("stmt[{i}] prepare: {e}"))?;
            let mut rows = prep
                .query([])
                .map_err(|e| format!("stmt[{i}] query: {e}"))?;
            let col_names = rows
                .as_ref()
                .ok_or_else(|| format!("stmt[{i}] missing column metadata"))?
                .column_names();

            let mut results = Vec::new();
            let mut truncated = false;
            while let Some(row) = rows.next().map_err(|e| format!("stmt[{i}] fetch: {e}"))? {
                if results.len() >= max_rows {
                    truncated = true;
                    break;
                }
                let mut map = serde_json::Map::new();
                for (j, col) in col_names.iter().enumerate() {
                    map.insert(
                        col.clone(),
                        crate::api::queries::duckdb_cell_to_json(row, j),
                    );
                }
                results.push(serde_json::Value::Object(map));
            }

            let value = json!({
                "columns": col_names,
                "rows": results,
                "count": results.len(),
                "truncated": truncated,
            });
            Ok((i, value))
        });
    }

    let mut ordered: Vec<Option<serde_json::Value>> = (0..n).map(|_| None).collect();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok((i, val))) => ordered[i] = Some(val),
            Ok(Err(e)) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({ "error": e }).to_string(),
                ))
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({ "error": e.to_string() }).to_string(),
                ))
            }
        }
    }
    let final_json: Vec<serde_json::Value> = ordered.into_iter().flatten().collect();

    if is_multi {
        Ok(Json(json!(final_json)).into_response())
    } else {
        Ok(Json(
            final_json
                .into_iter()
                .next()
                .unwrap_or(serde_json::Value::Null),
        )
        .into_response())
    }
}
