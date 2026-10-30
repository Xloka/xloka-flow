//! Binlog and GTID checkpoint structures for crash recovery.

use serde::{Deserialize, Serialize};

/// Represents a persistent CDC checkpoint containing binlog coordinates and/or GTID sets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinlogCheckpoint {
    /// Identifier for the CDC source (e.g., "default" or an account/tenant ID).
    pub source_id: String,
    /// MySQL binary log file name (e.g., "mysql-bin.000001").
    pub binlog_file: String,
    /// Byte offset within the binlog file.
    pub binlog_pos: u64,
    /// Optional Global Transaction Identifier (GTID) set string.
    pub gtid_set: Option<String>,
    /// Origin timestamp of the event in milliseconds since UNIX epoch.
    pub event_ts_ms: u64,
}

impl BinlogCheckpoint {
    /// Create a new binlog checkpoint.
    pub fn new(
        source_id: impl Into<String>,
        binlog_file: impl Into<String>,
        binlog_pos: u64,
        gtid_set: Option<String>,
        event_ts_ms: u64,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            binlog_file: binlog_file.into(),
            binlog_pos,
            gtid_set,
            event_ts_ms,
        }
    }
}
