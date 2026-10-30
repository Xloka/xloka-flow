//! Lock-free metrics collectors and Prometheus exposition.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Atomic counters and gauges tracking real-time pipeline performance and health.
#[derive(Debug, Default)]
pub struct FlowMetrics {
    /// Total count of CDC events received by the pipeline.
    pub cdc_events_received_total: AtomicU64,
    /// Total redundant operations eliminated via in-memory coalescing.
    pub cdc_coalesced_ops_saved_total: AtomicU64,
    /// Total number of transactions committed to DuckDB.
    pub duckdb_batches_committed_total: AtomicU64,
    /// Total rows written (appended) to DuckDB.
    pub duckdb_rows_written_total: AtomicU64,
    /// Duration of the most recent batch flush in microseconds.
    pub last_batch_flush_duration_us: AtomicU64,
    /// Current estimated replication lag in milliseconds.
    pub replication_lag_ms: AtomicU64,
    /// Active concurrent WebSocket connections.
    pub active_ws_connections: AtomicUsize,
    /// Total number of messages sent over WebSocket connections.
    pub ws_messages_sent_total: AtomicU64,
    /// Set to 1 once the store writer has halted (fail-closed).
    pub writer_halted: AtomicU64,
    /// Number of row batches dropped because the table schema was unknown.
    pub rows_dropped_unknown_table: AtomicU64,
}

impl FlowMetrics {
    /// Create a new `FlowMetrics` instance with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the current replication lag in milliseconds.
    pub fn replication_lag_ms(&self) -> u64 {
        self.replication_lag_ms.load(Ordering::Relaxed)
    }

    /// Update replication lag given origin timestamp in milliseconds.
    pub fn update_lag_from_ts(&self, origin_ts_ms: u64) {
        if origin_ts_ms > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(origin_ts_ms);
            let lag = now_ms.saturating_sub(origin_ts_ms);
            self.replication_lag_ms.store(lag, Ordering::Relaxed);
        }
    }

    /// Generate Prometheus text format exposition (v0.0.4).
    pub fn prometheus_export(&self) -> String {
        format!(
            "# HELP xloka_cdc_events_received_total Total number of CDC events received\n\
             # TYPE xloka_cdc_events_received_total counter\n\
             xloka_cdc_events_received_total {}\n\
             # HELP xloka_cdc_coalesced_ops_saved_total Redundant CDC operations eliminated by coalescing\n\
             # TYPE xloka_cdc_coalesced_ops_saved_total counter\n\
             xloka_cdc_coalesced_ops_saved_total {}\n\
             # HELP xloka_duckdb_batches_committed_total Total batches committed to DuckDB\n\
             # TYPE xloka_duckdb_batches_committed_total counter\n\
             xloka_duckdb_batches_committed_total {}\n\
             # HELP xloka_duckdb_rows_written_total Total rows written to DuckDB\n\
             # TYPE xloka_duckdb_rows_written_total counter\n\
             xloka_duckdb_rows_written_total {}\n\
             # HELP xloka_last_batch_flush_duration_us Duration of last batch flush in microseconds\n\
             # TYPE xloka_last_batch_flush_duration_us gauge\n\
             xloka_last_batch_flush_duration_us {}\n\
             # HELP xloka_replication_lag_ms Replication lag in milliseconds\n\
             # TYPE xloka_replication_lag_ms gauge\n\
             xloka_replication_lag_ms {}\n\
             # HELP xloka_active_ws_connections Current active WebSocket connections\n\
             # TYPE xloka_active_ws_connections gauge\n\
             xloka_active_ws_connections {}\n\
             # HELP xloka_ws_messages_sent_total Total WebSocket messages sent\n\
             # TYPE xloka_ws_messages_sent_total counter\n\
             xloka_ws_messages_sent_total {}\n\
             # HELP xloka_writer_halted 1 when the store writer has halted and replication stopped\n\
             # TYPE xloka_writer_halted gauge\n\
             xloka_writer_halted {}\n\
             # HELP xloka_rows_dropped_unknown_table Row batches dropped because the table schema was unknown\n\
             # TYPE xloka_rows_dropped_unknown_table counter\n\
             xloka_rows_dropped_unknown_table {}\n",
            self.cdc_events_received_total.load(Ordering::Relaxed),
            self.cdc_coalesced_ops_saved_total.load(Ordering::Relaxed),
            self.duckdb_batches_committed_total.load(Ordering::Relaxed),
            self.duckdb_rows_written_total.load(Ordering::Relaxed),
            self.last_batch_flush_duration_us.load(Ordering::Relaxed),
            self.replication_lag_ms.load(Ordering::Relaxed),
            self.active_ws_connections.load(Ordering::Relaxed),
            self.ws_messages_sent_total.load(Ordering::Relaxed),
            self.writer_halted.load(Ordering::Relaxed),
            self.rows_dropped_unknown_table.load(Ordering::Relaxed),
        )
    }
}

/// Handler for `GET /metrics`.
pub async fn metrics_handler(metrics: Arc<FlowMetrics>) -> Response {
    let body = metrics.prometheus_export();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

/// Handler for `GET /health`.
pub async fn health_handler(metrics: Arc<FlowMetrics>) -> Response {
    let halted = metrics.writer_halted.load(Ordering::Relaxed) == 1;
    let status_json = json!({
        "status": if halted { "degraded" } else { "ok" },
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "replication_lag_ms": metrics.replication_lag_ms.load(Ordering::Relaxed),
        "active_ws_connections": metrics.active_ws_connections.load(Ordering::Relaxed),
        "batches_committed": metrics.duckdb_batches_committed_total.load(Ordering::Relaxed),
        "writer_halted": halted,
    });

    let code = if halted {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        status_json.to_string(),
    )
        .into_response()
}
