//! CDC abstractions: `CdcSource` trait, `CdcEvent` pipeline enum, `RowOp`,
//! primary-key coalescing types, and schema evolution.

pub mod checkpoint;
pub mod filter;
pub mod live;
pub mod mock;
pub mod schema_evolver;
pub mod types;

pub use checkpoint::BinlogCheckpoint;
pub use filter::{ColumnFilter, FilterError, Projection, ReplicationFilter};
pub use schema_evolver::{apply_ddl_to_source_schema, diff_schema, translate_alter, DdlOperation};
pub use types::{CdcEvent, ColumnDef, DdlOp, DuckDbType, RowOp, TableSchema, Value};

use crate::error::FlowResult;
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Every CDC source (live MySQL or mock) implements this trait.
#[async_trait]
pub trait CdcSource: Send + 'static {
    /// Stream CDC events into `tx` until shutdown is signalled or an error occurs.
    async fn run(self, tx: mpsc::Sender<CdcEvent>) -> FlowResult<()>;
}
