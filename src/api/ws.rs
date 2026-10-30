//! Real-Time WebSocket Multiplexed Server & Live Query Subscriptions.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::api::metrics::FlowMetrics;
use crate::api::queries::{execute_query_def_with_limit, QueryDef};
use crate::store::writer::TableDelta;

// ─── Wire Protocol ────────────────────────────────────────────────────────────

/// Client commands received over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    /// Subscribe to live re-evaluations of a named analytical query when its source tables mutate.
    SubscribeQuery {
        sub_id: String,
        query_name: String,
        #[serde(default)]
        params: Vec<String>,
        #[serde(default = "default_throttle_ms")]
        throttle_ms: u64,
        /// Optional account whose data should be queried.
        #[serde(default)]
        account_id: Option<String>,
    },
    /// Subscribe to real-time row-level CDC deltas for specific tables.
    SubscribeTableDeltas {
        sub_id: String,
        tables: Vec<String>,
        #[serde(default)]
        account_id: Option<String>,
    },
    /// Cancel an active subscription on this WebSocket connection.
    Unsubscribe { sub_id: String },
    /// Application-level keepalive ping.
    Ping { ts: u64 },
}

fn default_throttle_ms() -> u64 {
    250
}

/// Server responses and live push notifications sent over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsServerMessage {
    Subscribed {
        sub_id: String,
        mode: String,
    },
    Unsubscribed {
        sub_id: String,
    },
    QuerySnapshot {
        sub_id: String,
        query_name: String,
        batch_id: u64,
        execution_time_ms: u64,
        replication_lag_ms: u64,
        data: serde_json::Value,
    },
    TableDeltaBatch {
        sub_id: String,
        batch_id: u64,
        committed_at_ms: u64,
        replication_lag_ms: u64,
        deltas: Vec<TableDelta>,
    },
    Lagged {
        missed_batches: u64,
        message: String,
    },
    Error {
        sub_id: Option<String>,
        error: String,
    },
    Pong {
        ts: u64,
    },
}

// ─── Connection Guard ─────────────────────────────────────────────────────────

struct WsConnectionGuard(Arc<FlowMetrics>);

impl WsConnectionGuard {
    fn new(metrics: Arc<FlowMetrics>) -> Self {
        metrics
            .active_ws_connections
            .fetch_add(1, Ordering::Relaxed);
        Self(metrics)
    }
}

impl Drop for WsConnectionGuard {
    fn drop(&mut self) {
        self.0.active_ws_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

// ─── Active Subscription State ────────────────────────────────────────────────

struct ActiveQuerySub {
    query_name: String,
    query_def: Arc<QueryDef>,
    params: Vec<String>,
    throttle_ms: u64,
    last_pushed: Instant,
    dirty: bool,
    pending_batch_id: u64,
    /// Dataset this subscription reads from.
    db_path: Arc<PathBuf>,
    /// Account this subscription belongs to (`None` = default dataset).
    account_id: Option<String>,
    /// Maximum number of rows a re-evaluation may materialize.
    max_rows: usize,
    /// Number of consecutive failed re-evaluations.
    consecutive_errors: u32,
    /// Earliest time at which the next re-evaluation may be attempted.
    next_retry_at: Option<Instant>,
    /// A re-evaluation is currently in flight.
    in_flight: bool,
}

struct ActiveDeltaSub {
    tables: HashSet<String>,
    account_id: Option<String>,
}

/// Result of an off-loop query evaluation.
struct EvalOutcome {
    sub_id: String,
    batch_id: u64,
    /// `Ok((data, execution_time_ms))` or the error message.
    result: Result<(serde_json::Value, u64), String>,
}

/// Backoff schedule for failing query re-evaluations (1s, 2s, 4s, … 30s).
fn error_backoff(consecutive_errors: u32) -> Duration {
    let secs = 1u64 << consecutive_errors.clamp(1, 5);
    Duration::from_secs(secs.min(30))
}

/// Query parameters accepted on the WebSocket upgrade handshake (`GET /ws?api_key=...`).
///
/// Browser `WebSocket` clients cannot attach custom HTTP headers, so `?api_key=`
/// provides a first-class way to authenticate browser sessions when
/// `XLOKA__DATA_API_KEY` or per-account API keys are used.
#[derive(Debug, Default, Deserialize)]
pub struct WsUpgradeQuery {
    pub api_key: Option<String>,
    pub account_id: Option<String>,
}

// ─── WebSocket Upgrade Handler ────────────────────────────────────────────────

/// Handler for `GET /api/v1/ws` and `GET /ws`.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<crate::AppState>,
    axum::extract::Query(ws_query): axum::extract::Query<WsUpgradeQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    let provided = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
        })
        .or(ws_query.api_key.as_deref())
        .filter(|s| !s.is_empty());

    // Resolve authentication and optional account scope for this WebSocket session.
    // `locked_account_id` is `Some(id)` when the client authenticated with a
    // tenant-specific API key (restricting the session to that account).
    let (default_account_id, locked_account_id) = match provided {
        Some(key) => {
            if let Some(account) = state.account_manager.get_account_by_api_key(key) {
                if let Some(ref req_id) = ws_query.account_id {
                    if req_id != &account.id {
                        return Err(StatusCode::FORBIDDEN);
                    }
                }
                (Some(account.id.clone()), Some(account.id))
            } else if state
                .settings
                .data_api_key
                .as_deref()
                .filter(|k| !k.is_empty())
                .is_some_and(|expected| constant_time_eq(key, expected))
            {
                (ws_query.account_id, None)
            } else {
                return Err(StatusCode::UNAUTHORIZED);
            }
        }
        None => {
            if state
                .settings
                .data_api_key
                .as_deref()
                .is_some_and(|k| !k.is_empty())
            {
                return Err(StatusCode::UNAUTHORIZED);
            }
            (ws_query.account_id, None)
        }
    };

    let current_conns = state.metrics.active_ws_connections.load(Ordering::Relaxed);
    if current_conns >= state.settings.max_ws_connections {
        warn!(
            "WebSocket rejected: current connections ({}) exceeds limit ({})",
            current_conns, state.settings.max_ws_connections
        );
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    Ok(ws.on_upgrade(move |socket| {
        handle_ws_session(socket, state, default_account_id, locked_account_id)
    }))
}

/// Constant-time string comparison for API keys.
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

async fn handle_ws_session(
    socket: WebSocket,
    state: crate::AppState,
    default_account_id: Option<String>,
    locked_account_id: Option<String>,
) {
    let _guard = WsConnectionGuard::new(Arc::clone(&state.metrics));
    let (mut ws_sender, mut ws_receiver) = socket.split();

    let mut commit_rx = state.commit_tx.subscribe();
    let mut debounce_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(50),
        Duration::from_millis(50),
    );
    let mut heartbeat_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(30),
        Duration::from_secs(30),
    );

    let mut active_query_subs: HashMap<String, ActiveQuerySub> = HashMap::new();
    let mut active_delta_subs: HashMap<String, ActiveDeltaSub> = HashMap::new();

    // Query evaluations run on their own task so a slow analytical query can
    // never delay the delta fan-out to other subscribers on this connection.
    let (eval_tx, mut eval_rx) = mpsc::unbounded_channel::<EvalOutcome>();

    let metrics = Arc::clone(&state.metrics);

    debug!("WebSocket client connected");

    loop {
        tokio::select! {
            // ── Branch A: Client Messages ────────────────────────────────────
            client_msg = ws_receiver.next() => {
                match client_msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<WsClientMessage>(&text) {
                            Ok(cmd) => {
                                match cmd {
                                    WsClientMessage::SubscribeQuery {
                                        sub_id,
                                        query_name,
                                        params,
                                        throttle_ms,
                                        account_id,
                                    } => {
                                        let effective_account_id =
                                            account_id.or_else(|| default_account_id.clone());
                                        if let Some(ref locked) = locked_account_id {
                                            if effective_account_id.as_deref() != Some(locked.as_str()) {
                                                let err_msg = WsServerMessage::Error {
                                                    sub_id: Some(sub_id),
                                                    error: "API key is not authorized for the requested account_id".into(),
                                                };
                                                let _ = send_ws_message(&mut ws_sender, &metrics, err_msg).await;
                                                continue;
                                            }
                                        }

                                        if let Some(def) = state.resolve_query(effective_account_id.as_deref(), &query_name) {
                                            let query_def = Arc::new(def);

                                            // Resolve the dataset this subscription
                                            // reads from. Account subscriptions read
                                            // the account's own DuckDB file.
                                            let db_path = match resolve_subscription_db(
                                                &state,
                                                effective_account_id.as_deref(),
                                            ) {
                                                Ok(p) => p,
                                                Err(msg) => {
                                                    let err_msg = WsServerMessage::Error {
                                                        sub_id: Some(sub_id),
                                                        error: msg,
                                                    };
                                                    let _ = send_ws_message(&mut ws_sender, &metrics, err_msg).await;
                                                    continue;
                                                }
                                            };

                                            // Acknowledge subscription
                                            let sub_ack = WsServerMessage::Subscribed {
                                                sub_id: sub_id.clone(),
                                                mode: "query".into(),
                                            };
                                            if send_ws_message(&mut ws_sender, &metrics, sub_ack).await.is_err() {
                                                break;
                                            }

                                            let db_path = db_path.clone();
                                            let eval_params = params.clone();
                                            let eval_query_def = Arc::clone(&query_def);
                                            active_query_subs.insert(
                                                sub_id.clone(),
                                                ActiveQuerySub {
                                                    query_name: query_name.clone(),
                                                    query_def,
                                                    params,
                                                    throttle_ms: throttle_ms.max(10),
                                                    last_pushed: Instant::now(),
                                                    dirty: false,
                                                    pending_batch_id: 0,
                                                    db_path: db_path.clone(),
                                                    account_id: effective_account_id,
                                                    max_rows: state.settings.max_query_rows,
                                                    consecutive_errors: 0,
                                                    next_retry_at: None,
                                                    in_flight: false,
                                                },
                                            );

                                            // Push initial snapshot (batch_id: 0)
                                            spawn_evaluation(
                                                &eval_tx,
                                                &sub_id,
                                                &db_path,
                                                &eval_query_def,
                                                &eval_params,
                                                state.settings.max_query_rows,
                                                0,
                                            );
                                        } else {
                                            let err_msg = WsServerMessage::Error {
                                                sub_id: Some(sub_id),
                                                error: format!("Named query '{query_name}' not found in manifest"),
                                            };
                                            let _ = send_ws_message(&mut ws_sender, &metrics, err_msg).await;
                                        }
                                    }
                                    WsClientMessage::SubscribeTableDeltas { sub_id, tables, account_id } => {
                                        let effective_account_id =
                                            account_id.or_else(|| default_account_id.clone());
                                        if let Some(ref locked) = locked_account_id {
                                            if effective_account_id.as_deref() != Some(locked.as_str()) {
                                                let err_msg = WsServerMessage::Error {
                                                    sub_id: Some(sub_id),
                                                    error: "API key is not authorized for the requested account_id".into(),
                                                };
                                                let _ = send_ws_message(&mut ws_sender, &metrics, err_msg).await;
                                                continue;
                                            }
                                        }

                                        let sub_ack = WsServerMessage::Subscribed {
                                            sub_id: sub_id.clone(),
                                            mode: "table_deltas".into(),
                                        };
                                        if send_ws_message(&mut ws_sender, &metrics, sub_ack).await.is_err() {
                                            break;
                                        }

                                        active_delta_subs.insert(
                                            sub_id,
                                            ActiveDeltaSub {
                                                tables: tables.into_iter().collect(),
                                                account_id: effective_account_id,
                                            },
                                        );
                                    }
                                    WsClientMessage::Unsubscribe { sub_id } => {
                                        active_query_subs.remove(&sub_id);
                                        active_delta_subs.remove(&sub_id);
                                        let ack = WsServerMessage::Unsubscribed { sub_id };
                                        let _ = send_ws_message(&mut ws_sender, &metrics, ack).await;
                                    }
                                    WsClientMessage::Ping { ts } => {
                                        let pong = WsServerMessage::Pong { ts };
                                        let _ = send_ws_message(&mut ws_sender, &metrics, pong).await;
                                    }
                                }
                            }
                            Err(e) => {
                                let err_msg = WsServerMessage::Error {
                                    sub_id: None,
                                    error: format!("Invalid JSON message: {e}"),
                                };
                                let _ = send_ws_message(&mut ws_sender, &metrics, err_msg).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        let _ = ws_sender.send(Message::Pong(bytes)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("WebSocket client disconnected cleanly");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket receive error: {e}");
                        break;
                    }
                    _ => {}
                }
            }

            // ── Branch B: Store Commit Notifications ─────────────────────────
            commit_res = commit_rx.recv() => {
                match commit_res {
                    Ok(notif) => {
                        // 1. Fan out table deltas (only to subscriptions of the
                        //    account the notification belongs to).
                        for (sub_id, delta_sub) in &active_delta_subs {
                            if delta_sub.account_id != notif.account_id {
                                continue;
                            }
                            if notif.affected_tables.iter().any(|t| delta_sub.tables.contains(t)) {
                                let matched_deltas: Vec<TableDelta> = notif
                                    .deltas
                                    .iter()
                                    .filter(|d| delta_sub.tables.contains(&d.table))
                                    .cloned()
                                    .collect();

                                if !matched_deltas.is_empty() {
                                    let msg = WsServerMessage::TableDeltaBatch {
                                        sub_id: sub_id.clone(),
                                        batch_id: notif.batch_id,
                                        committed_at_ms: notif.committed_at_ms,
                                        replication_lag_ms: notif.replication_lag_ms,
                                        deltas: matched_deltas,
                                    };
                                    if send_ws_message(&mut ws_sender, &metrics, msg).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }

                        // 2. Trigger or mark dirty for query subscriptions
                        for (sub_id, query_sub) in &mut active_query_subs {
                            if query_sub.account_id != notif.account_id {
                                continue;
                            }
                            if !query_sub.query_def.tables.iter().any(|t| notif.affected_tables.contains(t)) {
                                continue;
                            }
                            if query_sub.in_flight {
                                // An evaluation is already running — just remember
                                // that newer data arrived.
                                query_sub.dirty = true;
                                query_sub.pending_batch_id = notif.batch_id;
                                continue;
                            }
                            if query_sub.last_pushed.elapsed() < Duration::from_millis(query_sub.throttle_ms) {
                                query_sub.dirty = true;
                                query_sub.pending_batch_id = notif.batch_id;
                                continue;
                            }
                            if let Some(next) = query_sub.next_retry_at {
                                if Instant::now() < next {
                                    query_sub.dirty = true;
                                    query_sub.pending_batch_id = notif.batch_id;
                                    continue;
                                }
                            }
                            start_evaluation(&eval_tx, sub_id, query_sub, notif.batch_id);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        warn!("WebSocket client lagged by {missed} commit batches");
                        let lagged_msg = WsServerMessage::Lagged {
                            missed_batches: missed,
                            message: "Client fell behind; refreshing query snapshots".into(),
                        };
                        let _ = send_ws_message(&mut ws_sender, &metrics, lagged_msg).await;

                        // Force refresh all active query subscriptions
                        for (sub_id, query_sub) in &mut active_query_subs {
                            query_sub.dirty = true;
                            if query_sub.in_flight {
                                continue;
                            }
                            start_evaluation(&eval_tx, sub_id, query_sub, query_sub.pending_batch_id);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!("Commit notification broadcast channel closed");
                        break;
                    }
                }
            }

            // ── Branch C: Completed Query Evaluations ─────────────────────────
            Some(outcome) = eval_rx.recv() => {
                let Some(sub) = active_query_subs.get_mut(&outcome.sub_id) else {
                    continue;
                };
                sub.in_flight = false;

                match outcome.result {
                    Ok((data, exec_ms)) => {
                        let replication_lag_ms = sub
                            .account_id
                            .as_deref()
                            .and_then(|id| state.account_manager.get_account_lag_ms(id))
                            .unwrap_or_else(|| metrics.replication_lag_ms());
                        let snapshot = WsServerMessage::QuerySnapshot {
                            sub_id: outcome.sub_id.clone(),
                            query_name: sub.query_name.clone(),
                            batch_id: outcome.batch_id,
                            execution_time_ms: exec_ms,
                            replication_lag_ms,
                            data,
                        };
                        if send_ws_message(&mut ws_sender, &metrics, snapshot).await.is_err() {
                            return;
                        }
                        sub.last_pushed = Instant::now();
                        sub.dirty = false;
                        sub.consecutive_errors = 0;
                        sub.next_retry_at = None;
                    }
                    Err(err) => {
                        // Tell the client, and back off so a permanently failing
                        // query does not spin the event loop ~20x/second.
                        sub.consecutive_errors = sub.consecutive_errors.saturating_add(1);
                        let backoff = error_backoff(sub.consecutive_errors);
                        sub.next_retry_at = Some(Instant::now() + backoff);
                        let err_msg = WsServerMessage::Error {
                            sub_id: Some(outcome.sub_id.clone()),
                            error: format!("Query evaluation failed: {err}"),
                        };
                        if send_ws_message(&mut ws_sender, &metrics, err_msg).await.is_err() {
                            return;
                        }
                    }
                }
            }

            // ── Branch D: Debounce Interval (every 50ms) ─────────────────────
            _ = debounce_interval.tick() => {
                for (sub_id, query_sub) in &mut active_query_subs {
                    if !query_sub.dirty || query_sub.in_flight {
                        continue;
                    }
                    if query_sub.last_pushed.elapsed() < Duration::from_millis(query_sub.throttle_ms) {
                        continue;
                    }
                    if let Some(next) = query_sub.next_retry_at {
                        if Instant::now() < next {
                            continue;
                        }
                    }
                    start_evaluation(&eval_tx, sub_id, query_sub, query_sub.pending_batch_id);
                }
            }

            // ── Branch E: Heartbeat Keepalive (every 30s) ────────────────────
            _ = heartbeat_interval.tick() => {
                if ws_sender.send(Message::Ping(vec![].into())).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Start an off-loop evaluation of a subscription's query.
fn start_evaluation(
    eval_tx: &mpsc::UnboundedSender<EvalOutcome>,
    sub_id: &str,
    sub: &mut ActiveQuerySub,
    batch_id: u64,
) {
    sub.in_flight = true;
    spawn_evaluation(
        eval_tx,
        sub_id,
        &sub.db_path,
        &sub.query_def,
        &sub.params,
        sub.max_rows,
        batch_id,
    );
}

/// Spawn the actual query execution on its own task.
fn spawn_evaluation(
    eval_tx: &mpsc::UnboundedSender<EvalOutcome>,
    sub_id: &str,
    db_path: &Arc<PathBuf>,
    query_def: &Arc<QueryDef>,
    params: &[String],
    max_rows: usize,
    batch_id: u64,
) {
    let tx = eval_tx.clone();
    let sub_id = sub_id.to_string();
    let db_path = Arc::clone(db_path);
    let query_def = Arc::clone(query_def);
    let params = params.to_vec();

    tokio::spawn(async move {
        let t0 = Instant::now();
        let result = execute_query_def_with_limit(db_path, query_def, params, max_rows)
            .await
            .map(|data| (data, t0.elapsed().as_millis() as u64))
            .map_err(|e| e.to_string());

        let _ = tx.send(EvalOutcome {
            sub_id,
            batch_id,
            result,
        });
    });
}

/// Resolve the DuckDB file a subscription should read from.
fn resolve_subscription_db(
    state: &crate::AppState,
    account_id: Option<&str>,
) -> Result<Arc<PathBuf>, String> {
    match account_id {
        Some(id) => {
            let db_path = state
                .account_manager
                .get_account_db_path_by_id(id)
                .ok_or_else(|| format!("Account '{id}' not found"))?;
            Ok(Arc::new(PathBuf::from(db_path)))
        }
        None => Ok(Arc::new(PathBuf::from(&state.settings.db_path))),
    }
}

async fn send_ws_message(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    metrics: &FlowMetrics,
    msg: WsServerMessage,
) -> Result<(), axum::Error> {
    let json_str = serde_json::to_string(&msg).unwrap_or_else(|_| "{}".into());
    sender.send(Message::Text(json_str.into())).await?;
    metrics
        .ws_messages_sent_total
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}
