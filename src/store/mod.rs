//! DuckDB ingestion engine: Single-writer concurrency model, native `Appender`
//! ingestion, primary-key batch coalescing, and non-blocking commit fan-out.
//!
//! # Concurrency Model
//! - `StoreWriter` owns the exclusive write handle (`duckdb::Connection`) to the
//!   DuckDB file.
//! - Analytical API queries and WebSocket sessions open isolated read-only
//!   handles (`duckdb::Connection::open_with_flags` with `AccessMode::ReadOnly`)
//!   or query in-memory channels.
//! - Commit notifications are published over `tokio::sync::broadcast` so that
//!   slow WebSocket subscribers never stall the ingestion pipeline.

pub mod coalescer;
pub mod writer;

pub use coalescer::{BatchCoalescer, CoalescedRowOp};
pub use writer::{CommitNotification, StoreWriter, TableDelta};
