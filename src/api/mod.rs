//! REST and Real-Time WebSocket analytical API server.

pub mod metrics;
pub mod queries;
pub mod router;
pub mod ws;

pub use metrics::FlowMetrics;
pub use queries::{
    execute_query_def, execute_query_def_with_limit, guard_select_only, open_read_only, QueryDef,
    QueryManifest,
};
pub use router::create_router;
pub use ws::{WsClientMessage, WsServerMessage};

use crate::AppState;
use axum::Router;

/// Helper function providing backwards compatibility with `api::router(state)`.
pub fn router(state: AppState) -> Router {
    create_router(state)
}
