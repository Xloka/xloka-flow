//! In-memory primary-key batch coalescer for deduplicating row mutations.
//!
//! Collapses multiple mutations affecting the same primary key within a single
//! flush window before interacting with DuckDB.
//!
//! # Examples
//!
//! ```rust
//! use xloka_flow::store::coalescer::{BatchCoalescer, CoalescedRowOp};
//! use xloka_flow::cdc::types::{RowOp, Value};
//!
//! let mut coalescer = BatchCoalescer::new();
//! let pk = vec![Value::Integer(1)];
//!
//! // 1. Initial Insert
//! coalescer.push(RowOp::Insert {
//!     table: "users".into(),
//!     pk_indices: vec![0],
//!     row: vec![Value::Integer(1), Value::Text("Alice".into())],
//!     event_ts_ms: 1000,
//!     checkpoint: None,
//! });
//!
//! // 2. Update to same row coalesces into updated Insert
//! coalescer.push(RowOp::Update {
//!     table: "users".into(),
//!     pk_indices: vec![0],
//!     before: Some(vec![Value::Integer(1), Value::Text("Alice".into())]),
//!     after: vec![Value::Integer(1), Value::Text("Alice Cooper".into())],
//!     event_ts_ms: 1050,
//!     checkpoint: None,
//! });
//!
//! assert_eq!(coalescer.coalesced_ops_saved(), 1);
//! let table = coalescer.tables.get("users").unwrap();
//! assert_eq!(table.len(), 1);
//! match table.get(&pk).unwrap() {
//!     CoalescedRowOp::Insert { after, .. } => {
//!         assert_eq!(after[1], Value::Text("Alice Cooper".into()));
//!     }
//!     _ => panic!("Expected coalesced Insert"),
//! }
//! ```

use crate::cdc::checkpoint::BinlogCheckpoint;
use crate::cdc::types::{RowOp, Value};
use indexmap::IndexMap;

/// A deduplicated row operation stored within a `BatchCoalescer` window.
#[derive(Debug, Clone, PartialEq)]
pub enum CoalescedRowOp {
    /// Net-insert operation to be executed with DuckDB `Appender`.
    Insert {
        after: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
    /// Net-update operation (deletes old row, then appends `after`).
    Update {
        before: Option<Vec<Value>>,
        after: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
    /// Net-delete operation (deletes row by primary key).
    Delete {
        before: Option<Vec<Value>>,
        pk_values: Vec<Value>,
        event_ts_ms: u64,
        checkpoint: Option<BinlogCheckpoint>,
    },
}

impl CoalescedRowOp {
    /// Get the event timestamp in milliseconds.
    pub fn event_ts_ms(&self) -> u64 {
        match self {
            CoalescedRowOp::Insert { event_ts_ms, .. } => *event_ts_ms,
            CoalescedRowOp::Update { event_ts_ms, .. } => *event_ts_ms,
            CoalescedRowOp::Delete { event_ts_ms, .. } => *event_ts_ms,
        }
    }

    /// Get the attached checkpoint, if any.
    pub fn checkpoint(&self) -> Option<&BinlogCheckpoint> {
        match self {
            CoalescedRowOp::Insert { checkpoint, .. } => checkpoint.as_ref(),
            CoalescedRowOp::Update { checkpoint, .. } => checkpoint.as_ref(),
            CoalescedRowOp::Delete { checkpoint, .. } => checkpoint.as_ref(),
        }
    }

    /// Return the mutated row data if this operation contains an `after` image.
    pub fn after_row(&self) -> Option<&Vec<Value>> {
        match self {
            CoalescedRowOp::Insert { after, .. } => Some(after),
            CoalescedRowOp::Update { after, .. } => Some(after),
            CoalescedRowOp::Delete { .. } => None,
        }
    }

    /// Return the prior row data if this operation contains a `before` image.
    pub fn before_row(&self) -> Option<&Vec<Value>> {
        match self {
            CoalescedRowOp::Insert { .. } => None,
            CoalescedRowOp::Update { before, .. } => before.as_ref(),
            CoalescedRowOp::Delete { before, .. } => before.as_ref(),
        }
    }
}

/// In-memory batch coalescer that deduplicates mutations keyed by `(table, pk_values)`.
#[derive(Debug, Clone, Default)]
pub struct BatchCoalescer {
    /// Grouped operations: table_name -> primary_key_values -> coalesced_op.
    pub tables: IndexMap<String, IndexMap<Vec<Value>, CoalescedRowOp>>,
    coalesced_ops_saved: u64,
    latest_checkpoint: Option<BinlogCheckpoint>,
    latest_event_ts_ms: u64,
}

impl BatchCoalescer {
    /// Create a new, empty `BatchCoalescer`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of redundant CDC operations saved/eliminated by in-memory deduplication.
    pub fn coalesced_ops_saved(&self) -> u64 {
        self.coalesced_ops_saved
    }

    /// Total count of net pending operations across all tables.
    pub fn len(&self) -> usize {
        self.tables.values().map(|t| t.len()).sum()
    }

    /// True if there are no pending operations.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Latest checkpoint observed in the current batch.
    pub fn latest_checkpoint(&self) -> Option<&BinlogCheckpoint> {
        self.latest_checkpoint.as_ref()
    }

    /// Origin timestamp of the most recent event coalesced in this batch.
    pub fn latest_event_ts_ms(&self) -> u64 {
        self.latest_event_ts_ms
    }

    /// Drain and reset all accumulated table operations.
    pub fn drain_tables(&mut self) -> IndexMap<String, IndexMap<Vec<Value>, CoalescedRowOp>> {
        std::mem::take(&mut self.tables)
    }

    /// Reset checkpoint and event timestamp tracking after a flush.
    pub fn reset_window(&mut self) {
        self.latest_checkpoint = None;
        self.latest_event_ts_ms = 0;
    }

    /// Push a new `RowOp` into the coalescer, applying state transition rules.
    pub fn push(&mut self, op: RowOp) {
        let table = op.table().to_string();
        let pk = op.extract_pk();
        let ts = op.event_ts_ms();
        let cp = op.checkpoint().cloned();

        if cp.is_some() {
            self.latest_checkpoint = cp.clone();
        }
        if ts > self.latest_event_ts_ms {
            self.latest_event_ts_ms = ts;
        }

        let table_map = self.tables.entry(table.clone()).or_default();

        // If this is a primary-key-changing Update (`before_pk != after_pk`) and
        // `before_pk` already exists in the current batch window as an `Insert` or
        // `Update`, collapse the intermediate `before_pk` entry directly into the
        // new `after_pk` entry so no stale intermediate key remains in the batch.
        let op = match op {
            RowOp::Update {
                table: tbl,
                pk_indices,
                before: Some(before_row),
                after,
                event_ts_ms,
                checkpoint,
            } => {
                let before_pk: Vec<Value> = pk_indices
                    .iter()
                    .map(|&i| before_row.get(i).cloned().unwrap_or(Value::Null))
                    .collect();
                if before_pk != pk {
                    match table_map.get(&before_pk) {
                        Some(CoalescedRowOp::Insert { .. }) => {
                            table_map.shift_remove(&before_pk);
                            self.coalesced_ops_saved += 1;
                            RowOp::Insert {
                                table: tbl,
                                pk_indices,
                                row: after,
                                event_ts_ms,
                                checkpoint,
                            }
                        }
                        Some(CoalescedRowOp::Update { .. }) => {
                            let orig_before = match table_map.shift_remove(&before_pk) {
                                Some(CoalescedRowOp::Update { before: b, .. }) => b,
                                _ => Some(before_row),
                            };
                            self.coalesced_ops_saved += 1;
                            RowOp::Update {
                                table: tbl,
                                pk_indices,
                                before: orig_before,
                                after,
                                event_ts_ms,
                                checkpoint,
                            }
                        }
                        _ => RowOp::Update {
                            table: tbl,
                            pk_indices,
                            before: Some(before_row),
                            after,
                            event_ts_ms,
                            checkpoint,
                        },
                    }
                } else {
                    RowOp::Update {
                        table: tbl,
                        pk_indices,
                        before: Some(before_row),
                        after,
                        event_ts_ms,
                        checkpoint,
                    }
                }
            }
            other => other,
        };

        match table_map.entry(pk.clone()) {
            indexmap::map::Entry::Vacant(vac) => match op {
                RowOp::Insert { row, .. } => {
                    vac.insert(CoalescedRowOp::Insert {
                        after: row,
                        event_ts_ms: ts,
                        checkpoint: cp,
                    });
                }
                RowOp::Update { before, after, .. } => {
                    vac.insert(CoalescedRowOp::Update {
                        before,
                        after,
                        event_ts_ms: ts,
                        checkpoint: cp,
                    });
                }
                RowOp::Delete {
                    before, pk_values, ..
                } => {
                    let actual_pk = if pk_values.is_empty() { pk } else { pk_values };
                    vac.insert(CoalescedRowOp::Delete {
                        before,
                        pk_values: actual_pk,
                        event_ts_ms: ts,
                        checkpoint: cp,
                    });
                }
            },
            indexmap::map::Entry::Occupied(mut occ) => {
                self.coalesced_ops_saved += 1;
                let prev = occ.get_mut();

                match (prev, op) {
                    // Transition 4: Insert(prev) + Update { after, .. } -> Insert(after)
                    (
                        CoalescedRowOp::Insert {
                            after,
                            event_ts_ms,
                            checkpoint,
                        },
                        RowOp::Update {
                            after: new_after, ..
                        },
                    ) => {
                        *after = new_after;
                        *event_ts_ms = ts;
                        *checkpoint = cp;
                    }

                    // Transition 5: Insert(prev) + Delete(_) -> Remove key entirely (net no-op)
                    (CoalescedRowOp::Insert { .. }, RowOp::Delete { .. }) => {
                        occ.swap_remove();
                    }

                    // Transition 6: Update { before, .. } + Update { after, .. } -> Update { before, after }
                    (
                        CoalescedRowOp::Update {
                            after,
                            event_ts_ms,
                            checkpoint,
                            ..
                        },
                        RowOp::Update {
                            after: new_after, ..
                        },
                    ) => {
                        *after = new_after;
                        *event_ts_ms = ts;
                        *checkpoint = cp;
                    }

                    // Transition 7: Update { before, .. } + Delete(_) -> Delete(before)
                    (CoalescedRowOp::Update { before, .. }, RowOp::Delete { .. }) => {
                        let orig_before = before.take();
                        *occ.get_mut() = CoalescedRowOp::Delete {
                            before: orig_before,
                            pk_values: pk,
                            event_ts_ms: ts,
                            checkpoint: cp,
                        };
                    }

                    // Transition 8: Delete(before) + Insert(after) -> Update { before, after }
                    (
                        CoalescedRowOp::Delete { before, .. },
                        RowOp::Insert { row: new_after, .. },
                    ) => {
                        let orig_before = before.take();
                        *occ.get_mut() = CoalescedRowOp::Update {
                            before: orig_before,
                            after: new_after,
                            event_ts_ms: ts,
                            checkpoint: cp,
                        };
                    }

                    // Fallbacks for edge cases
                    (
                        CoalescedRowOp::Delete { before, .. },
                        RowOp::Update {
                            after: new_after, ..
                        },
                    ) => {
                        let orig_before = before.take();
                        *occ.get_mut() = CoalescedRowOp::Update {
                            before: orig_before,
                            after: new_after,
                            event_ts_ms: ts,
                            checkpoint: cp,
                        };
                    }
                    (
                        CoalescedRowOp::Insert {
                            after,
                            event_ts_ms,
                            checkpoint,
                        },
                        RowOp::Insert { row: new_after, .. },
                    ) => {
                        *after = new_after;
                        *event_ts_ms = ts;
                        *checkpoint = cp;
                    }
                    (
                        CoalescedRowOp::Update {
                            after,
                            event_ts_ms,
                            checkpoint,
                            ..
                        },
                        RowOp::Insert { row: new_after, .. },
                    ) => {
                        *after = new_after;
                        *event_ts_ms = ts;
                        *checkpoint = cp;
                    }
                    (
                        CoalescedRowOp::Delete {
                            event_ts_ms,
                            checkpoint,
                            ..
                        },
                        RowOp::Delete { .. },
                    ) => {
                        *event_ts_ms = ts;
                        *checkpoint = cp;
                    }
                }
            }
        }
    }
}
