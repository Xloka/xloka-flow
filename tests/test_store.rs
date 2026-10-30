//! Integration test suite for `xloka-flow`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use duckdb::Connection;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;

use xloka_flow::accounts::AccountManager;
use xloka_flow::api::metrics::FlowMetrics;
use xloka_flow::api::queries::{QueryDef, QueryManifest};
use xloka_flow::api::router::create_router;
use xloka_flow::api::ws::{WsClientMessage, WsServerMessage};
use xloka_flow::cdc::checkpoint::BinlogCheckpoint;
use xloka_flow::cdc::types::{CdcEvent, ColumnDef, DdlOp, DuckDbType, RowOp, TableSchema, Value};
use xloka_flow::store::coalescer::{BatchCoalescer, CoalescedRowOp};
use xloka_flow::store::StoreWriter;
use xloka_flow::{AppState, Settings};

fn column(name: &str, ty: DuckDbType, pk: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        duck_type: ty,
        nullable: !pk,
        is_primary_key: pk,
    }
}

fn schema(table: &str, columns: Vec<ColumnDef>) -> TableSchema {
    TableSchema {
        table_name: table.into(),
        columns,
    }
}

#[tokio::test]
async fn test_store_writer_integration() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test_store.db");
    let conn = Connection::open(&db_path).unwrap();
    let (commit_tx, _) = broadcast::channel(100);
    let metrics = Arc::new(FlowMetrics::new());

    let settings = Settings {
        db_path: db_path.to_str().unwrap().to_string(),
        batch_size: 2,
        ..Default::default()
    };
    let writer = StoreWriter::new(Arc::new(settings), conn, commit_tx, metrics).unwrap();

    let (tx, rx) = mpsc::channel(100);
    let writer_handle = tokio::spawn(async move {
        writer.run(rx).await.unwrap();
    });

    let schema = TableSchema {
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::Integer,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "name".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "org_id".into(),
                duck_type: DuckDbType::Integer,
                nullable: true,
                is_primary_key: false,
            },
        ],
    };
    tx.send(CdcEvent::Schema(schema)).await.unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "users".into(),
        pk_indices: vec![0],
        row: vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Integer(10),
        ],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "users".into(),
        pk_indices: vec![0],
        row: vec![
            Value::Integer(2),
            Value::Text("Bob".into()),
            Value::Integer(20),
        ],
        event_ts_ms: 1010,
        checkpoint: None,
    }))
    .await
    .unwrap();

    // Test BulkLoadCsv initial snapshot loading (csv_path string conversion)
    let csv_path = tmp.path().join("users_snapshot.csv");
    std::fs::write(&csv_path, "id,name,org_id\n3,\"Charlie\",\\N\n").unwrap();
    let cp = BinlogCheckpoint::new("default", "mysql-bin.000001", 999, None, 1020);
    tx.send(CdcEvent::BulkLoadCsv {
        table: "users".into(),
        csv_path: csv_path.to_string_lossy().to_string(),
        checkpoint: Some(cp),
    })
    .await
    .unwrap();

    drop(tx);
    writer_handle.await.unwrap();

    // Temp CSV file must be cleaned up after BulkLoadCsv
    assert!(!csv_path.exists(), "temporary snapshot CSV must be removed");

    let reader = Connection::open(&db_path).unwrap();
    let mut stmt = reader
        .prepare("SELECT id, name, org_id FROM users ORDER BY id")
        .unwrap();
    let rows: Vec<(i32, String, Option<i32>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string(), Some(10)),
            (2, "Bob".to_string(), Some(20)),
            (3, "Charlie".to_string(), None),
        ]
    );

    // Foreign-key auto-index `idx_users_org_id` must exist
    let idx_count: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name = 'idx_users_org_id'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(idx_count, 1, "expected auto-created index idx_users_org_id");
}

#[tokio::test]
async fn test_pk_changing_update_leaves_no_ghost_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ghost_row_test.db");
    let conn = Connection::open(&db_path).unwrap();
    let (commit_tx, _) = broadcast::channel(100);
    let metrics = Arc::new(FlowMetrics::new());

    let settings = Settings {
        db_path: db_path.to_str().unwrap().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    };
    let writer = StoreWriter::new(Arc::new(settings), conn, commit_tx, metrics).unwrap();

    let (tx, rx) = mpsc::channel(100);
    let writer_handle = tokio::spawn(async move {
        writer.run(rx).await.unwrap();
    });

    let schema = TableSchema {
        table_name: "items".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::Integer,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "title".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
        ],
    };
    tx.send(CdcEvent::Schema(schema)).await.unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![0],
        row: vec![Value::Integer(1), Value::Text("Original Title".into())],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![
            Value::Integer(1),
            Value::Text("Original Title".into()),
        ]),
        after: vec![Value::Integer(100), Value::Text("New Title".into())],
        event_ts_ms: 1020,
        checkpoint: None,
    }))
    .await
    .unwrap();

    drop(tx);
    writer_handle.await.unwrap();

    let reader = Connection::open(&db_path).unwrap();
    let mut stmt = reader
        .prepare("SELECT id, title FROM items ORDER BY id")
        .unwrap();
    let rows: Vec<(i32, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(
        rows,
        vec![(100, "New Title".to_string())],
        "Old primary key row (id=1) must be deleted when PK changes; no ghost rows should remain"
    );
}

#[tokio::test]
async fn test_checkpoint_recovery_and_ddl_evolution() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("recovery_test.db");

    let source_id = "default";
    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 50,
        flush_interval_ms: 20,
        ..Default::default()
    });

    let (tx, rx) = mpsc::channel(500);
    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();

    let writer_task = tokio::spawn(async move {
        writer.run(rx).await.unwrap();
    });

    let schema = TableSchema {
        table_name: "accounts".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::BigInt,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "balance".into(),
                duck_type: DuckDbType::Double,
                nullable: false,
                is_primary_key: false,
            },
        ],
    };
    tx.send(CdcEvent::Schema(schema)).await.unwrap();

    for i in 1..=100 {
        let cp =
            BinlogCheckpoint::new(source_id, "mysql-bin.000001", 1000 + i * 10, None, 1000 + i);
        let op = RowOp::Insert {
            table: "accounts".into(),
            pk_indices: vec![0],
            row: vec![Value::Integer(i as i64), Value::Float(100.0 + i as f64)],
            event_ts_ms: 1000 + i,
            checkpoint: Some(cp),
        };
        tx.send(CdcEvent::Row(op)).await.unwrap();
    }

    let ddl_cp = BinlogCheckpoint::new(source_id, "mysql-bin.000001", 2050, None, 2000);
    tx.send(CdcEvent::Ddl {
        table: "accounts".into(),
        raw_sql: "ALTER TABLE accounts ADD COLUMN tier VARCHAR;".into(),
        op: DdlOp::Alter,
        new_schema: None,
        event_ts_ms: 2000,
        checkpoint: Some(ddl_cp),
    })
    .await
    .unwrap();

    for i in 101..=250 {
        let cp = BinlogCheckpoint::new(
            source_id,
            "mysql-bin.000001",
            2050 + (i - 100) * 10,
            None,
            2000 + i,
        );
        let op = RowOp::Insert {
            table: "accounts".into(),
            pk_indices: vec![0],
            row: vec![
                Value::Integer(i as i64),
                Value::Float(200.0 + i as f64),
                Value::Text("platinum".into()),
            ],
            event_ts_ms: 2000 + i,
            checkpoint: Some(cp),
        };
        tx.send(CdcEvent::Row(op)).await.unwrap();
    }

    drop(tx);
    writer_task.await.unwrap();

    let recovered_cp = StoreWriter::load_checkpoint(&db_path, source_id)
        .unwrap()
        .expect("Expected persisted checkpoint in DuckDB");

    assert_eq!(recovered_cp.source_id, source_id);
    assert_eq!(recovered_cp.binlog_file, "mysql-bin.000001");
    assert_eq!(recovered_cp.binlog_pos, 3550);

    let read_conn = duckdb::Connection::open(&db_path).unwrap();
    let total_rows: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total_rows, 250);

    let tier_before: Option<String> = read_conn
        .query_row("SELECT tier FROM accounts WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(tier_before, None);

    let tier_after: Option<String> = read_conn
        .query_row("SELECT tier FROM accounts WHERE id = 101", [], |r| r.get(0))
        .unwrap();
    assert_eq!(tier_after, Some("platinum".into()));
}

#[test]
fn test_all_coalescer_state_transitions() {
    let mut coalescer = BatchCoalescer::new();
    let table = "accounts";
    let pk_indices = vec![0];

    let row = |id: i64, balance: f64| vec![Value::Integer(id), Value::Float(balance)];

    // Transition 1: None + Insert(after) -> Insert(after)
    coalescer.push(RowOp::Insert {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        row: row(1, 100.0),
        event_ts_ms: 1000,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 0);
    assert_eq!(coalescer.len(), 1);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(1)])
        .unwrap();
    match op {
        CoalescedRowOp::Insert { after, .. } => assert_eq!(after[1], Value::Float(100.0)),
        _ => panic!("Expected Insert"),
    }

    // Transition 2: None + Update { before, after } -> Update { before, after }
    coalescer.push(RowOp::Update {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(2, 200.0)),
        after: row(2, 250.0),
        event_ts_ms: 1010,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 0);
    assert_eq!(coalescer.len(), 2);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(2)])
        .unwrap();
    match op {
        CoalescedRowOp::Update { before, after, .. } => {
            assert_eq!(before.as_ref().unwrap()[1], Value::Float(200.0));
            assert_eq!(after[1], Value::Float(250.0));
        }
        _ => panic!("Expected Update"),
    }

    // Transition 3: None + Delete(before) -> Delete(before)
    coalescer.push(RowOp::Delete {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(3, 300.0)),
        pk_values: vec![Value::Integer(3)],
        event_ts_ms: 1020,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 0);
    assert_eq!(coalescer.len(), 3);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(3)])
        .unwrap();
    match op {
        CoalescedRowOp::Delete {
            before, pk_values, ..
        } => {
            assert_eq!(before.as_ref().unwrap()[1], Value::Float(300.0));
            assert_eq!(pk_values[0], Value::Integer(3));
        }
        _ => panic!("Expected Delete"),
    }

    // Transition 4: Insert(prev) + Update { after, .. } -> Insert(after)
    coalescer.push(RowOp::Update {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(1, 100.0)),
        after: row(1, 150.0),
        event_ts_ms: 1030,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 1);
    assert_eq!(coalescer.len(), 3);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(1)])
        .unwrap();
    match op {
        CoalescedRowOp::Insert { after, .. } => assert_eq!(after[1], Value::Float(150.0)),
        _ => panic!("Expected coalesced Insert"),
    }

    // Transition 5: Insert(prev) + Delete(_) -> Remove key entirely (net no-op)
    coalescer.push(RowOp::Insert {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        row: row(4, 400.0),
        event_ts_ms: 1040,
        checkpoint: None,
    });
    assert_eq!(coalescer.len(), 4);
    coalescer.push(RowOp::Delete {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(4, 400.0)),
        pk_values: vec![Value::Integer(4)],
        event_ts_ms: 1050,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 2);
    assert_eq!(coalescer.len(), 3);
    assert!(coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(4)])
        .is_none());

    // Transition 6: Update { before, .. } + Update { after, .. } -> Update { before, after }
    coalescer.push(RowOp::Update {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(2, 250.0)),
        after: row(2, 275.0),
        event_ts_ms: 1060,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 3);
    assert_eq!(coalescer.len(), 3);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(2)])
        .unwrap();
    match op {
        CoalescedRowOp::Update { before, after, .. } => {
            assert_eq!(before.as_ref().unwrap()[1], Value::Float(200.0));
            assert_eq!(after[1], Value::Float(275.0));
        }
        _ => panic!("Expected coalesced Update"),
    }

    // Transition 7: Update { before, .. } + Delete(_) -> Delete(before)
    coalescer.push(RowOp::Insert {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        row: row(5, 500.0),
        event_ts_ms: 1070,
        checkpoint: None,
    });
    coalescer.push(RowOp::Update {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(5, 500.0)),
        after: row(5, 550.0),
        event_ts_ms: 1080,
        checkpoint: None,
    });
    let mut coalescer2 = BatchCoalescer::new();
    coalescer2.push(RowOp::Update {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: Some(row(5, 500.0)),
        after: row(5, 550.0),
        event_ts_ms: 1090,
        checkpoint: None,
    });
    coalescer2.push(RowOp::Delete {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        before: None,
        pk_values: vec![Value::Integer(5)],
        event_ts_ms: 1100,
        checkpoint: None,
    });
    assert_eq!(coalescer2.coalesced_ops_saved(), 1);
    let op = coalescer2
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(5)])
        .unwrap();
    match op {
        CoalescedRowOp::Delete {
            before, pk_values, ..
        } => {
            assert_eq!(before.as_ref().unwrap()[1], Value::Float(500.0));
            assert_eq!(pk_values[0], Value::Integer(5));
        }
        _ => panic!("Expected coalesced Delete"),
    }

    // Transition 8: Delete(before) + Insert(after) -> Update { before, after }
    coalescer.push(RowOp::Insert {
        table: table.into(),
        pk_indices: pk_indices.clone(),
        row: row(3, 350.0),
        event_ts_ms: 1110,
        checkpoint: None,
    });
    assert_eq!(coalescer.coalesced_ops_saved(), 5);
    let op = coalescer
        .tables
        .get(table)
        .unwrap()
        .get(&vec![Value::Integer(3)])
        .unwrap();
    match op {
        CoalescedRowOp::Update { before, after, .. } => {
            assert_eq!(before.as_ref().unwrap()[1], Value::Float(300.0));
            assert_eq!(after[1], Value::Float(350.0));
        }
        _ => panic!("Expected coalesced Update from Delete + Insert"),
    }
}

#[tokio::test]
async fn test_add_column_first_does_not_shift_values() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("add_first.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer = StoreWriter::new(settings.clone(), conn, commit_tx, metrics).unwrap();
    let (tx, rx) = mpsc::channel(100);
    let writer_task = tokio::spawn(async move { writer.run(rx).await.unwrap() });

    tx.send(CdcEvent::Schema(schema(
        "items",
        vec![
            column("id", DuckDbType::BigInt, true),
            column("name", DuckDbType::Varchar, false),
        ],
    )))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![0],
        row: vec![Value::Integer(1), Value::Text("first".into())],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();

    let after_ddl = schema(
        "items",
        vec![
            column("score", DuckDbType::Double, false),
            column("id", DuckDbType::BigInt, true),
            column("name", DuckDbType::Varchar, false),
        ],
    );
    tx.send(CdcEvent::Ddl {
        table: "items".into(),
        raw_sql: "ALTER TABLE items ADD COLUMN score DOUBLE FIRST;".into(),
        op: DdlOp::Alter,
        new_schema: Some(after_ddl),
        event_ts_ms: 2000,
        checkpoint: None,
    })
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![1],
        row: vec![
            Value::Float(42.5),
            Value::Integer(2),
            Value::Text("second".into()),
        ],
        event_ts_ms: 2100,
        checkpoint: None,
    }))
    .await
    .unwrap();

    drop(tx);
    writer_task.await.unwrap();

    let reader = duckdb::Connection::open(&db_path).unwrap();
    let mut stmt = reader
        .prepare("SELECT id, name, score FROM items ORDER BY id")
        .unwrap();
    let rows: Vec<(i64, String, Option<f64>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(
        rows,
        vec![
            (1, "first".to_string(), None),
            (2, "second".to_string(), Some(42.5)),
        ],
        "values must land in the column they belong to, regardless of physical order"
    );
}

#[tokio::test]
async fn test_failed_ddl_does_not_advance_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("failed_ddl.db");
    let source_id = "default";

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 10,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer = StoreWriter::new(settings.clone(), conn, commit_tx, metrics).unwrap();
    let (tx, rx) = mpsc::channel(100);
    let writer_task = tokio::spawn(async move { writer.run(rx).await });

    tx.send(CdcEvent::Schema(schema(
        "items",
        vec![column("id", DuckDbType::BigInt, true)],
    )))
    .await
    .unwrap();

    tx.send(CdcEvent::Ddl {
        table: "does_not_exist".into(),
        raw_sql: "ALTER TABLE does_not_exist ADD COLUMN x INTEGER;".into(),
        op: DdlOp::Alter,
        new_schema: None,
        event_ts_ms: 1000,
        checkpoint: Some(BinlogCheckpoint::new(
            source_id,
            "mysql-bin.000001",
            4242,
            None,
            1000,
        )),
    })
    .await
    .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), writer_task)
        .await
        .expect("writer did not halt")
        .expect("writer task panicked");
    assert!(result.is_err(), "writer must fail closed on a broken DDL");

    let recovered = StoreWriter::load_checkpoint(&db_path, source_id).unwrap();
    assert!(
        recovered.is_none(),
        "checkpoint must not advance past a failed DDL"
    );
}

#[tokio::test]
async fn test_chained_pk_updates_leave_no_ghost_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("chained_pk.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1_000,
        flush_interval_ms: 500,
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer = StoreWriter::new(settings.clone(), conn, commit_tx, metrics).unwrap();
    let (tx, rx) = mpsc::channel(100);
    let writer_task = tokio::spawn(async move { writer.run(rx).await.unwrap() });

    tx.send(CdcEvent::Schema(schema(
        "items",
        vec![
            column("id", DuckDbType::BigInt, true),
            column("title", DuckDbType::Varchar, false),
        ],
    )))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(1), Value::Text("original".into())]),
        after: vec![Value::Integer(2), Value::Text("moved".into())],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(2), Value::Text("moved".into())]),
        after: vec![Value::Integer(3), Value::Text("moved again".into())],
        event_ts_ms: 1001,
        checkpoint: None,
    }))
    .await
    .unwrap();

    drop(tx);
    writer_task.await.unwrap();

    let reader = duckdb::Connection::open(&db_path).unwrap();
    let mut stmt = reader.prepare("SELECT id FROM items ORDER BY id").unwrap();
    let ids: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(
        ids,
        vec![3],
        "only the final primary key may exist — no ghost row for id=2"
    );
}

#[tokio::test]
async fn test_chained_pk_update_then_delete_leaves_no_ghost_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("chained_pk_delete.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1_000,
        flush_interval_ms: 500,
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, title VARCHAR);
         INSERT INTO items VALUES (1, 'seeded');",
    )
    .unwrap();

    let writer = StoreWriter::new(settings.clone(), conn, commit_tx, metrics).unwrap();
    let (tx, rx) = mpsc::channel(100);
    let writer_task = tokio::spawn(async move { writer.run(rx).await.unwrap() });

    tx.send(CdcEvent::Schema(schema(
        "items",
        vec![
            column("id", DuckDbType::BigInt, true),
            column("title", DuckDbType::Varchar, false),
        ],
    )))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(1), Value::Text("seeded".into())]),
        after: vec![Value::Integer(2), Value::Text("step2".into())],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();
    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(2), Value::Text("step2".into())]),
        after: vec![Value::Integer(3), Value::Text("step3".into())],
        event_ts_ms: 1001,
        checkpoint: None,
    }))
    .await
    .unwrap();
    tx.send(CdcEvent::Row(RowOp::Delete {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(3), Value::Text("step3".into())]),
        pk_values: vec![Value::Integer(3)],
        event_ts_ms: 1002,
        checkpoint: None,
    }))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![0],
        row: vec![Value::Integer(10), Value::Text("ephemeral".into())],
        event_ts_ms: 1003,
        checkpoint: None,
    }))
    .await
    .unwrap();
    tx.send(CdcEvent::Row(RowOp::Update {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(10), Value::Text("ephemeral".into())]),
        after: vec![Value::Integer(20), Value::Text("ephemeral2".into())],
        event_ts_ms: 1004,
        checkpoint: None,
    }))
    .await
    .unwrap();
    tx.send(CdcEvent::Row(RowOp::Delete {
        table: "items".into(),
        pk_indices: vec![0],
        before: Some(vec![Value::Integer(20), Value::Text("ephemeral2".into())]),
        pk_values: vec![Value::Integer(20)],
        event_ts_ms: 1005,
        checkpoint: None,
    }))
    .await
    .unwrap();

    drop(tx);
    writer_task.await.unwrap();

    let reader = duckdb::Connection::open(&db_path).unwrap();
    let mut stmt = reader.prepare("SELECT id FROM items ORDER BY id").unwrap();
    let ids: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert!(
        ids.is_empty(),
        "no ghost rows may survive chained PK updates followed by delete, got: {ids:?}"
    );
}

#[tokio::test]
async fn test_idempotent_insert_replay_across_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("idempotent_insert.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer = StoreWriter::new(settings.clone(), conn, commit_tx, metrics.clone()).unwrap();
    let (tx, rx) = mpsc::channel(100);
    let writer_task = tokio::spawn(async move { writer.run(rx).await.unwrap() });

    tx.send(CdcEvent::Schema(schema(
        "items",
        vec![
            column("id", DuckDbType::BigInt, true),
            column("title", DuckDbType::Varchar, false),
        ],
    )))
    .await
    .unwrap();

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![0],
        row: vec![Value::Integer(1), Value::Text("initial".into())],
        event_ts_ms: 1000,
        checkpoint: None,
    }))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    tx.send(CdcEvent::Row(RowOp::Insert {
        table: "items".into(),
        pk_indices: vec![0],
        row: vec![Value::Integer(1), Value::Text("replayed".into())],
        event_ts_ms: 1001,
        checkpoint: None,
    }))
    .await
    .unwrap();

    drop(tx);
    writer_task.await.unwrap();

    let reader = duckdb::Connection::open(&db_path).unwrap();
    let mut stmt = reader
        .prepare("SELECT id, title FROM items ORDER BY id")
        .unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(rows, vec![(1, "replayed".to_string())]);
}

#[test]
fn test_sql_guard_rejects_writes() {
    use xloka_flow::api::queries::guard_select_only;

    assert!(guard_select_only("SELECT id FROM users").is_ok());
    assert!(guard_select_only("WITH t AS (SELECT 1 AS x) SELECT * FROM t").is_ok());

    for bad in [
        "DELETE FROM users",
        "UPDATE users SET id = 2",
        "INSERT INTO users VALUES (1)",
        "DROP TABLE users",
        "CREATE TABLE x (a INT)",
        "ALTER TABLE users ADD COLUMN x INT",
        "TRUNCATE users",
    ] {
        assert!(guard_select_only(bad).is_err(), "guard must reject: {bad}");
    }
}

async fn allow_origin_for(allowed_domains: Option<&str>, request_origin: &str) -> Option<String> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("cors.db");

    let (commit_tx, _) = broadcast::channel(64);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        allowed_domains: allowed_domains.map(|s| s.to_string()),
        ..Default::default()
    });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(50);
    tokio::spawn(async move {
        let _ = writer.run(cdc_rx).await;
    });
    cdc_tx
        .send(CdcEvent::Schema(TableSchema {
            table_name: "t".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::Integer,
                nullable: false,
                is_primary_key: true,
            }],
        }))
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone());
    let state = AppState::new(
        (*settings).clone(),
        metrics.clone(),
        Arc::new(xloka_flow::api::queries::QueryManifest {
            queries: HashMap::new(),
        }),
        commit_tx.clone(),
        account_manager,
    );
    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let res = reqwest::Client::new()
        .get(format!("http://{addr}/health"))
        .header("Origin", request_origin)
        .send()
        .await
        .unwrap();

    res.headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap_or("<binary>").to_string())
}

#[tokio::test]
async fn test_empty_allowed_domains_does_not_break_cors() {
    let header = allow_origin_for(Some(""), "https://example.com").await;
    assert_ne!(
        header.as_deref(),
        Some(""),
        "an empty ALLOWED_DOMAINS must not emit `allow-origin: \"\"`"
    );
    assert_eq!(
        header.as_deref(),
        Some("*"),
        "an empty ALLOWED_DOMAINS must fall back to the permissive default"
    );
}

#[tokio::test]
async fn test_wildcard_allowed_domains_allows_any_origin() {
    let header = allow_origin_for(Some("*"), "https://example.com").await;
    assert_eq!(
        header.as_deref(),
        Some("*"),
        "`*` must behave as a wildcard, not as a literal origin"
    );
}

#[tokio::test]
async fn test_specific_allowed_domains_restricts_origins() {
    let allowed =
        allow_origin_for(Some("https://allowed.example"), "https://allowed.example").await;
    assert_eq!(
        allowed.as_deref(),
        Some("https://allowed.example"),
        "a configured origin must be echoed back"
    );

    let other = allow_origin_for(Some("https://allowed.example"), "https://evil.example").await;
    assert_eq!(
        other, None,
        "an origin outside the configured list must not receive a CORS header"
    );
}

#[test]
fn test_account_files_live_next_to_the_registry() {
    let dir = tempfile::tempdir().unwrap();
    let registry = dir.path().join("accounts.json");

    let settings = Arc::new(Settings::default());
    let (commit_tx, _) = broadcast::channel(16);
    let manager = AccountManager::new(registry.to_string_lossy().to_string(), settings, commit_tx);

    let base = std::path::Path::new(&manager.file_path)
        .parent()
        .unwrap()
        .to_path_buf();
    assert_eq!(base, dir.path());
}

#[test]
fn test_compose_file_publishes_only_the_served_port() {
    let compose = std::fs::read_to_string("docker-compose.yml")
        .expect("docker-compose.yml must be readable from the crate root");
    let dockerfile = std::fs::read_to_string("Dockerfile")
        .expect("Dockerfile must be readable from the crate root");

    assert!(
        dockerfile.contains("EXPOSE 3000"),
        "the Dockerfile must expose the port the daemon listens on"
    );

    let mut in_daemon_service = false;
    for line in compose.lines() {
        let trimmed = line.trim();
        if !line.starts_with(' ') && trimmed.ends_with(':') {
            in_daemon_service = trimmed == "xloka-flow:";
            continue;
        }
        if !in_daemon_service {
            continue;
        }
        let Some(rest) = trimmed.strip_prefix("- \"") else {
            continue;
        };
        let Some(spec) = rest.strip_suffix('"') else {
            continue;
        };
        let parts: Vec<&str> = spec.split(':').collect();
        let (host_port, container_port) = match parts.as_slice() {
            [host, container] => (*host, *container),
            [container] => ("", *container),
            _ => continue,
        };
        let host_port: u16 = host_port
            .parse()
            .unwrap_or_else(|_| panic!("unparseable port mapping: {spec}"));
        let container_port: u16 = container_port
            .parse()
            .unwrap_or_else(|_| panic!("unparseable port mapping: {spec}"));
        assert_eq!(
            container_port, 3000,
            "container port {container_port} is published but the daemon only serves 3000"
        );
        assert_eq!(
            host_port, 3000,
            "host port {host_port} is published but the daemon only serves 3000"
        );
    }

    assert!(
        compose.contains("127.0.0.1:3306:3306"),
        "the evaluation MySQL must be published on 127.0.0.1 only"
    );

    assert!(
        compose.contains("XLOKA__ACCOUNTS_FILE=/app/data/accounts.json"),
        "the account registry must be written into the mounted data volume"
    );
}

#[tokio::test]
async fn test_rest_api_e2e_and_format_negotiation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("e2e_api.db");

    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());

    let mut queries = HashMap::new();
    queries.insert(
        "category_breakdown".into(),
        QueryDef {
            description: "Live expenditure breakdown".into(),
            tables: vec!["transactions".into()],
            statements: vec![
                "SELECT category, COUNT(*) AS tx_count, ROUND(SUM(amount), 2) AS total_amount FROM transactions GROUP BY category ORDER BY total_amount DESC".into(),
            ],
        },
    );
    queries.insert(
        "filtered_category".into(),
        QueryDef {
            description: "Category filtered by name".into(),
            tables: vec!["transactions".into()],
            statements: vec![
                "SELECT category, amount FROM transactions WHERE category = ? ORDER BY amount DESC"
                    .into(),
            ],
        },
    );
    let query_manifest = Arc::new(QueryManifest { queries });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = writer.run(cdc_rx).await;
    });

    let schema = TableSchema {
        table_name: "transactions".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::BigInt,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "category".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "amount".into(),
                duck_type: DuckDbType::Double,
                nullable: false,
                is_primary_key: false,
            },
        ],
    };
    cdc_tx.send(CdcEvent::Schema(schema)).await.unwrap();

    cdc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "transactions".into(),
            pk_indices: vec![0],
            row: vec![
                Value::Integer(1),
                Value::Text("Groceries".into()),
                Value::Float(45.50),
            ],
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    cdc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "transactions".into(),
            pk_indices: vec![0],
            row: vec![
                Value::Integer(2),
                Value::Text("Electronics".into()),
                Value::Float(299.99),
            ],
            event_ts_ms: 1005,
            checkpoint: None,
        }))
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone());
    let state = AppState::new(
        (*settings).clone(),
        metrics.clone(),
        query_manifest.clone(),
        commit_tx.clone(),
        account_manager,
    );
    let app = create_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    let base_url = format!("http://{}", local_addr);
    let client = reqwest::Client::new();

    let health_res = client
        .get(format!("{}/health", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(health_res.status(), reqwest::StatusCode::OK);
    let health_json: serde_json::Value = health_res.json().await.unwrap();
    assert_eq!(health_json["status"], "ok");
    assert_eq!(health_json["version"], env!("CARGO_PKG_VERSION"));

    let metrics_res = client
        .get(format!("{}/metrics", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(metrics_res.status(), reqwest::StatusCode::OK);
    let metrics_text = metrics_res.text().await.unwrap();
    assert!(metrics_text.contains("xloka_cdc_events_received_total"));
    assert!(metrics_text.contains("xloka_cdc_coalesced_ops_saved_total"));
    assert!(metrics_text.contains("xloka_replication_lag_ms"));

    let queries_res = client
        .get(format!("{}/api/v1/queries", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(queries_res.status(), reqwest::StatusCode::OK);
    let queries_json: serde_json::Value = queries_res.json().await.unwrap();
    assert!(queries_json["queries"]["category_breakdown"].is_object());

    let json_res = client
        .get(format!(
            "{}/api/v1/queries/category_breakdown?format=json",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(json_res.status(), reqwest::StatusCode::OK);
    let json_val: serde_json::Value = json_res.json().await.unwrap();
    assert!(json_val.to_string().contains("Electronics"));
    assert!(json_val.to_string().contains("Groceries"));

    let csv_res = client
        .get(format!(
            "{}/api/v1/queries/category_breakdown?format=csv",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(csv_res.status(), reqwest::StatusCode::OK);
    let csv_text = csv_res.text().await.unwrap();
    assert!(csv_text.contains("category"));
    assert!(csv_text.contains("tx_count"));
    assert!(csv_text.contains("total_amount"));
    assert!(csv_text.contains("Electronics"));
    assert!(csv_text.contains("299.99"));

    let ndjson_res = client
        .get(format!(
            "{}/api/v1/queries/category_breakdown?format=ndjson",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(ndjson_res.status(), reqwest::StatusCode::OK);
    let ndjson_text = ndjson_res.text().await.unwrap();
    let lines: Vec<&str> = ndjson_text.trim().lines().collect();
    assert_eq!(lines.len(), 2);

    let param_res = client
        .get(format!(
            "{}/api/v1/queries/filtered_category?p1=Groceries",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(param_res.status(), reqwest::StatusCode::OK);
    let param_val: serde_json::Value = param_res.json().await.unwrap();
    let param_str = param_val.to_string();
    assert!(param_str.contains("Groceries"));
    assert!(!param_str.contains("Electronics"));

    // Test restored POST /api/query raw read-only SQL endpoint (plain-text SQL body & JSON body)
    let raw_query_res = client
        .post(format!("{}/api/query", base_url))
        .body("SELECT category, amount FROM transactions ORDER BY id ASC")
        .send()
        .await
        .unwrap();
    assert_eq!(raw_query_res.status(), reqwest::StatusCode::OK);
    let raw_query_json: serde_json::Value = raw_query_res.json().await.unwrap();
    assert_eq!(raw_query_json["rows"][0]["category"], "Groceries");
    assert_eq!(raw_query_json["rows"][1]["category"], "Electronics");

    let json_query_res = client
        .post(format!("{}/api/v1/query", base_url))
        .json(&serde_json::json!({
            "sql": "SELECT category, amount FROM transactions ORDER BY id ASC"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(json_query_res.status(), reqwest::StatusCode::OK);

    // Mutating SQL on POST /api/query must be rejected with 400 Bad Request
    let bad_raw_res = client
        .post(format!("{}/api/query", base_url))
        .body("DELETE FROM transactions")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_raw_res.status(), reqwest::StatusCode::BAD_REQUEST);

    metrics
        .replication_lag_ms
        .store(10, std::sync::atomic::Ordering::Relaxed);
    let fresh_res = client
        .get(format!(
            "{}/api/v1/queries/category_breakdown?max_staleness_ms=5000",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(fresh_res.status(), reqwest::StatusCode::OK);

    let stale_res = client
        .get(format!(
            "{}/api/v1/queries/category_breakdown?max_staleness_ms=5",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(stale_res.status(), reqwest::StatusCode::TOO_EARLY);

    let not_found_res = client
        .get(format!("{}/api/v1/queries/does_not_exist", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(not_found_res.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_account_data_is_queryable_via_api() {
    let temp_dir = tempfile::tempdir().unwrap();
    let main_db = temp_dir.path().join("main.db");
    let account_db = temp_dir.path().join("account.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: main_db.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let mut queries = HashMap::new();
    queries.insert(
        "account_totals".into(),
        QueryDef {
            description: "Totals per account".into(),
            tables: vec!["totals".into()],
            statements: vec!["SELECT name, amount FROM totals ORDER BY name".into()],
        },
    );
    let query_manifest = Arc::new(QueryManifest { queries });

    let conn = duckdb::Connection::open(&main_db).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = writer.run(cdc_rx).await;
    });

    let account_conn = duckdb::Connection::open(&account_db).unwrap();
    let mut account_settings = (*settings).clone();
    account_settings.db_path = account_db.to_string_lossy().to_string();
    let account_writer = StoreWriter::new(
        Arc::new(account_settings),
        account_conn,
        commit_tx.clone(),
        Arc::new(FlowMetrics::new()),
    )
    .unwrap();
    let (acc_tx, acc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = account_writer.run(acc_rx).await;
    });

    acc_tx
        .send(CdcEvent::Schema(TableSchema {
            table_name: "totals".into(),
            columns: vec![
                ColumnDef {
                    name: "name".into(),
                    duck_type: DuckDbType::Varchar,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "amount".into(),
                    duck_type: DuckDbType::Double,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        }))
        .await
        .unwrap();
    acc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "totals".into(),
            pk_indices: vec![0],
            row: vec![Value::Text("acme".into()), Value::Float(42.0)],
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    cdc_tx
        .send(CdcEvent::Schema(TableSchema {
            table_name: "totals".into(),
            columns: vec![
                ColumnDef {
                    name: "name".into(),
                    duck_type: DuckDbType::Varchar,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "amount".into(),
                    duck_type: DuckDbType::Double,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        }))
        .await
        .unwrap();
    cdc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "totals".into(),
            pk_indices: vec![0],
            row: vec![Value::Text("default".into()), Value::Float(1.0)],
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone());
    let account = xloka_flow::accounts::Account {
        id: "acct1".into(),
        name: "Acme".into(),
        mysql_url: "mysql://localhost/db".into(),
        snapshot_mysql_url: None,
        api_key: "xk_secret".into(),
        status: "active".into(),
        filters: None,
    };
    account_manager.add_account_state(xloka_flow::accounts::AccountState {
        account: account.clone(),
        db_path: account_db.to_string_lossy().to_string(),
        reader_conn: Arc::new(std::sync::Mutex::new(
            xloka_flow::api::queries::open_read_only(&account_db).unwrap(),
        )),
        cdc_task: None,
        writer_task: None,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state = AppState::new(
        (*settings).clone(),
        metrics.clone(),
        query_manifest.clone(),
        commit_tx.clone(),
        account_manager,
    );
    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://{}", local_addr);

    let default_res = client
        .get(format!("{base_url}/api/v1/query/account_totals"))
        .send()
        .await
        .unwrap();
    assert_eq!(default_res.status(), reqwest::StatusCode::OK);
    let default_json: serde_json::Value = default_res.json().await.unwrap();
    assert!(
        default_json.to_string().contains("default"),
        "default dataset must be served"
    );
    assert!(
        !default_json.to_string().contains("acme"),
        "the default dataset must not leak account data"
    );

    let account_res = client
        .get(format!(
            "{base_url}/api/v1/query/account_totals?account_id=acct1"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(account_res.status(), reqwest::StatusCode::OK);
    let account_json: serde_json::Value = account_res.json().await.unwrap();
    assert!(
        account_json.to_string().contains("acme"),
        "the account's own data must be served"
    );

    let key_res = client
        .get(format!("{base_url}/api/v1/query/account_totals"))
        .header("x-api-key", "xk_secret")
        .send()
        .await
        .unwrap();
    assert_eq!(key_res.status(), reqwest::StatusCode::OK);
    let key_json: serde_json::Value = key_res.json().await.unwrap();
    assert!(
        key_json.to_string().contains("acme"),
        "the API key must select the account's dataset"
    );

    let missing = client
        .get(format!(
            "{base_url}/api/v1/query/account_totals?account_id=nope"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    let bad_key = client
        .get(format!("{base_url}/api/v1/query/account_totals"))
        .header("x-api-key", "xk_wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_key.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[test]
fn test_ddl_extraction_covers_all_statement_kinds() {
    use xloka_flow::cdc::live::extract_ddl_statements_for_test;

    let alters = extract_ddl_statements_for_test("ALTER TABLE t ADD COLUMN c INT");
    assert_eq!(alters.len(), 1);
    assert_eq!(alters[0].kind, "alter");
    assert_eq!(alters[0].db, None);
    assert_eq!(alters[0].table, "t");

    let qualified_alters =
        extract_ddl_statements_for_test("ALTER TABLE other_db.t ADD COLUMN c INT");
    assert_eq!(qualified_alters.len(), 1);
    assert_eq!(qualified_alters[0].kind, "alter");
    assert_eq!(qualified_alters[0].db.as_deref(), Some("other_db"));
    assert_eq!(qualified_alters[0].table, "t");

    let truncates = extract_ddl_statements_for_test("TRUNCATE TABLE t");
    assert_eq!(truncates.len(), 1);
    assert_eq!(truncates[0].kind, "truncate");
    assert_eq!(truncates[0].table, "t");

    let renames = extract_ddl_statements_for_test("RENAME TABLE a TO b");
    assert_eq!(renames.len(), 1);
    assert_eq!(renames[0].kind, "rename");
    assert_eq!(renames[0].table, "a");

    let drops = extract_ddl_statements_for_test("DROP TABLE db1.a, b, c");
    assert_eq!(drops.len(), 3);
    assert_eq!(drops[0].db.as_deref(), Some("db1"));
    assert_eq!(drops[0].table, "a");
    assert_eq!(drops[1].db, None);
    assert_eq!(drops[1].table, "b");
    assert_eq!(drops[2].db, None);
    assert_eq!(drops[2].table, "c");
}

#[tokio::test]
async fn test_data_api_key_and_account_api_key_coexist() {
    let temp_dir = tempfile::tempdir().unwrap();
    let main_db = temp_dir.path().join("main_auth.db");
    let account_db = temp_dir.path().join("account_auth.db");

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: main_db.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        data_api_key: Some("global_secret".into()),
        ..Default::default()
    });

    let mut queries = HashMap::new();
    queries.insert(
        "account_totals".into(),
        QueryDef {
            description: "Totals per account".into(),
            tables: vec!["totals".into()],
            statements: vec!["SELECT name, amount FROM totals ORDER BY name".into()],
        },
    );
    let query_manifest = Arc::new(QueryManifest { queries });

    let conn = duckdb::Connection::open(&main_db).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = writer.run(cdc_rx).await;
    });

    let account_conn = duckdb::Connection::open(&account_db).unwrap();
    let mut account_settings = (*settings).clone();
    account_settings.db_path = account_db.to_string_lossy().to_string();
    let account_writer = StoreWriter::new(
        Arc::new(account_settings),
        account_conn,
        commit_tx.clone(),
        metrics.clone(),
    )
    .unwrap();
    let (acc_tx, acc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = account_writer.run(acc_rx).await;
    });

    let table_schema = TableSchema {
        table_name: "totals".into(),
        columns: vec![
            ColumnDef {
                name: "name".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "amount".into(),
                duck_type: DuckDbType::Double,
                nullable: false,
                is_primary_key: false,
            },
        ],
    };

    acc_tx
        .send(CdcEvent::Schema(table_schema.clone()))
        .await
        .unwrap();
    acc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "totals".into(),
            pk_indices: vec![0],
            row: vec![Value::Text("acme".into()), Value::Float(42.0)],
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    cdc_tx.send(CdcEvent::Schema(table_schema)).await.unwrap();
    cdc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "totals".into(),
            pk_indices: vec![0],
            row: vec![Value::Text("default".into()), Value::Float(1.0)],
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone())
        .with_metrics(metrics.clone());
    let account = xloka_flow::accounts::Account {
        id: "acct1".into(),
        name: "Acme".into(),
        mysql_url: "mysql://localhost/db".into(),
        snapshot_mysql_url: None,
        api_key: "xk_secret".into(),
        status: "active".into(),
        filters: None,
    };
    account_manager.add_account_state(xloka_flow::accounts::AccountState {
        account,
        db_path: account_db.to_string_lossy().to_string(),
        reader_conn: Arc::new(std::sync::Mutex::new(
            xloka_flow::api::queries::open_read_only(&account_db).unwrap(),
        )),
        cdc_task: None,
        writer_task: None,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state = AppState::new(
        (*settings).clone(),
        metrics.clone(),
        query_manifest.clone(),
        commit_tx.clone(),
        account_manager,
    );
    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://{}", local_addr);

    let unauth = client
        .get(format!("{base_url}/api/v1/query/account_totals"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let global_acct_res = client
        .get(format!(
            "{base_url}/api/v1/query/account_totals?account_id=acct1"
        ))
        .header("x-api-key", "global_secret")
        .send()
        .await
        .unwrap();
    assert_eq!(global_acct_res.status(), reqwest::StatusCode::OK);
    let global_acct_json: serde_json::Value = global_acct_res.json().await.unwrap();
    assert!(
        global_acct_json.to_string().contains("acme"),
        "global data_api_key must honor ?account_id=acct1"
    );

    let acct_key_res = client
        .get(format!(
            "{base_url}/api/v1/query/account_totals?api_key=xk_secret"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(acct_key_res.status(), reqwest::StatusCode::OK);
    let acct_key_json: serde_json::Value = acct_key_res.json().await.unwrap();
    assert!(
        acct_key_json.to_string().contains("acme"),
        "per-account ?api_key= must authenticate and select the account dataset"
    );

    let forbidden_res = client
        .get(format!(
            "{base_url}/api/v1/query/account_totals?account_id=other"
        ))
        .header("x-api-key", "xk_secret")
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden_res.status(), reqwest::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_ws_realtime_streaming_and_subscriptions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ws_realtime.db");

    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());

    let mut queries = HashMap::new();
    queries.insert(
        "user_balances".into(),
        QueryDef {
            description: "Live user balances".into(),
            tables: vec!["users".into()],
            statements: vec!["SELECT id, name, balance FROM users ORDER BY id ASC".into()],
        },
    );
    let query_manifest = Arc::new(QueryManifest { queries });

    let conn = duckdb::Connection::open(&db_path).unwrap();
    let writer =
        StoreWriter::new(settings.clone(), conn, commit_tx.clone(), metrics.clone()).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = writer.run(cdc_rx).await;
    });

    let schema = TableSchema {
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::BigInt,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "name".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "balance".into(),
                duck_type: DuckDbType::Double,
                nullable: false,
                is_primary_key: false,
            },
        ],
    };
    cdc_tx.send(CdcEvent::Schema(schema)).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone());
    let state = AppState::new(
        (*settings).clone(),
        metrics.clone(),
        query_manifest.clone(),
        commit_tx.clone(),
        account_manager,
    );
    let app = create_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{}/api/v1/ws", local_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("WebSocket handshake failed");

    let sub_query_cmd = WsClientMessage::SubscribeQuery {
        sub_id: "sub_q1".into(),
        query_name: "user_balances".into(),
        params: vec![],
        throttle_ms: 20,
        account_id: None,
    };
    let json_cmd = serde_json::to_string(&sub_query_cmd).unwrap();
    ws_stream
        .send(Message::Text(json_cmd.into()))
        .await
        .unwrap();

    let first_text = next_text_frame(&mut ws_stream).await;
    let parsed: WsServerMessage = serde_json::from_str(&first_text).unwrap();
    match parsed {
        WsServerMessage::Subscribed { sub_id, mode } => {
            assert_eq!(sub_id, "sub_q1");
            assert_eq!(mode, "query");
        }
        other => panic!("Expected Subscribed, got: {:?}", other),
    }

    let snapshot_text = next_text_frame(&mut ws_stream).await;
    let parsed: WsServerMessage = serde_json::from_str(&snapshot_text).unwrap();
    match parsed {
        WsServerMessage::QuerySnapshot {
            sub_id,
            batch_id,
            query_name,
            ..
        } => {
            assert_eq!(sub_id, "sub_q1");
            assert_eq!(query_name, "user_balances");
            assert_eq!(batch_id, 0);
        }
        other => panic!("Expected QuerySnapshot, got: {:?}", other),
    }

    let insert_alice = RowOp::Insert {
        table: "users".into(),
        pk_indices: vec![0],
        row: vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Float(150.0),
        ],
        event_ts_ms: 1000,
        checkpoint: None,
    };
    cdc_tx.send(CdcEvent::Row(insert_alice)).await.unwrap();

    let updated_text = next_text_frame(&mut ws_stream).await;
    let parsed: WsServerMessage = serde_json::from_str(&updated_text).unwrap();
    match parsed {
        WsServerMessage::QuerySnapshot {
            sub_id,
            batch_id,
            data,
            ..
        } => {
            assert_eq!(sub_id, "sub_q1");
            assert!(batch_id >= 1);
            let data_str = data.to_string();
            assert!(data_str.contains("Alice"));
            assert!(data_str.contains("150"));
        }
        other => panic!("Expected updated QuerySnapshot, got: {:?}", other),
    }

    let sub_deltas_cmd = WsClientMessage::SubscribeTableDeltas {
        sub_id: "sub_d1".into(),
        tables: vec!["users".into()],
        account_id: None,
    };
    let json_delta_cmd = serde_json::to_string(&sub_deltas_cmd).unwrap();
    ws_stream
        .send(Message::Text(json_delta_cmd.into()))
        .await
        .unwrap();

    let delta_text = next_text_frame(&mut ws_stream).await;
    let parsed: WsServerMessage = serde_json::from_str(&delta_text).unwrap();
    match parsed {
        WsServerMessage::Subscribed { sub_id, mode } => {
            assert_eq!(sub_id, "sub_d1");
            assert_eq!(mode, "table_deltas");
        }
        other => panic!("Expected Subscribed delta, got: {:?}", other),
    }

    let insert_bob = RowOp::Insert {
        table: "users".into(),
        pk_indices: vec![0],
        row: vec![
            Value::Integer(2),
            Value::Text("Bob".into()),
            Value::Float(250.0),
        ],
        event_ts_ms: 1010,
        checkpoint: None,
    };
    cdc_tx.send(CdcEvent::Row(insert_bob)).await.unwrap();

    let mut got_delta_batch = false;
    for _ in 0..3 {
        let text = next_text_frame(&mut ws_stream).await;
        let parsed: WsServerMessage = serde_json::from_str(&text).unwrap();
        if let WsServerMessage::TableDeltaBatch { sub_id, deltas, .. } = parsed {
            assert_eq!(sub_id, "sub_d1");
            assert_eq!(deltas.len(), 1);
            assert_eq!(deltas[0].op, "insert");
            assert_eq!(deltas[0].table, "users");
            got_delta_batch = true;
            break;
        }
    }
    assert!(got_delta_batch, "Expected TableDeltaBatch frame");
}

async fn next_text_frame<S>(stream: &mut S) -> String
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let msg = tokio::time::timeout(Duration::from_millis(1500), stream.next())
            .await
            .expect("timeout waiting for frame")
            .expect("stream ended")
            .expect("stream error");
        match msg {
            Message::Text(t) => return t.to_string(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("Expected text frame, got: {:?}", other),
        }
    }
}

#[tokio::test]
async fn test_ws_query_param_api_key_auth() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ws_auth.db");

    let settings = Arc::new(Settings {
        db_path: db_path.to_string_lossy().to_string(),
        data_api_key: Some("ws_secret_key".into()),
        ..Default::default()
    });

    let (commit_tx, _) = broadcast::channel(64);
    let metrics = Arc::new(FlowMetrics::new());
    let query_manifest = Arc::new(QueryManifest {
        queries: HashMap::new(),
    });
    let account_manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state = AppState::new(
        (*settings).clone(),
        metrics,
        query_manifest,
        commit_tx,
        account_manager,
    );
    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let unauth_url = format!("ws://{}/api/v1/ws", local_addr);
    let unauth_res = tokio_tungstenite::connect_async(&unauth_url).await;
    assert!(
        unauth_res.is_err(),
        "WebSocket upgrade without api_key must be rejected"
    );

    let auth_url = format!("ws://{}/api/v1/ws?api_key=ws_secret_key", local_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&auth_url)
        .await
        .expect("WebSocket handshake with ?api_key= should succeed");

    let ping_cmd = serde_json::to_string(&WsClientMessage::Ping { ts: 42 }).unwrap();
    ws_stream
        .send(Message::Text(ping_cmd.into()))
        .await
        .unwrap();

    let reply = next_text_frame(&mut ws_stream).await;
    let parsed: WsServerMessage = serde_json::from_str(&reply).unwrap();
    assert!(matches!(parsed, WsServerMessage::Pong { ts: 42 }));
}

#[tokio::test]
async fn test_account_snapshot_and_bulk_load() {
    let temp_dir = tempfile::tempdir().unwrap();
    let registry = temp_dir.path().join("accounts.json");
    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: temp_dir
            .path()
            .join("global.db")
            .to_string_lossy()
            .to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });

    let manager = AccountManager::new(
        registry.to_string_lossy().to_string(),
        settings.clone(),
        commit_tx.clone(),
    )
    .with_metrics(metrics.clone());

    let account = xloka_flow::accounts::Account {
        id: "tenant_snap_1".into(),
        name: "Tenant Snapshot Inc".into(),
        mysql_url: "mysql://user:pass@primary.host:3306/db".into(),
        snapshot_mysql_url: Some("mysql://replica_user:replica_pass@replica.host:3306/db".into()),
        api_key: "xk_snap_test_key".into(),
        status: "active".into(),
        filters: None,
    };

    // Verify redaction masks both primary and snapshot replica passwords
    let redacted = account.redacted();
    assert_eq!(redacted.mysql_url, "mysql://user:***@primary.host:3306/db");
    assert_eq!(
        redacted.snapshot_mysql_url,
        Some("mysql://replica_user:***@replica.host:3306/db".into())
    );

    // Save and load persistence
    manager.save_persisted(&[account.clone()]).unwrap();
    let loaded = manager.load_persisted();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "tenant_snap_1");
    assert_eq!(
        loaded[0].snapshot_mysql_url,
        Some("mysql://replica_user:replica_pass@replica.host:3306/db".into())
    );

    // Spawn the account
    manager.spawn_and_add_account(account.clone()).unwrap();

    let account_db_path = manager.get_account_db_path_by_id("tenant_snap_1").unwrap();
    assert!(std::path::Path::new(&account_db_path).exists());

    // Connect and verify schema + bulk load into account store
    let conn = manager.get_account_conn_by_id("tenant_snap_1").unwrap();

    // Verify writer registered and reader connection is functional
    let count: i64 = {
        let conn_guard = conn.lock().unwrap();
        conn_guard
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_xloka_checkpoint'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(count, 1, "_xloka_checkpoint table should be initialized");

    // Clean up: drop reader connection so file locks are released on Windows
    drop(conn);
    manager.shutdown().await;
    manager.remove_account("tenant_snap_1");
    assert!(!std::path::Path::new(&account_db_path).exists());
}

#[tokio::test]
async fn test_reconcile_hot_reload_and_read_only_admin_guard() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let registry = temp_dir.path().join("accounts.json");

    // 1. Write minimal accounts.json (missing id, api_key, status + extra unknown key)
    std::fs::write(
        &registry,
        r#"[
          {
            "name": "Acme Hosted",
            "mysql_url": "mysql://user:pass@127.0.0.1:65534/shop",
            "unknown_extra_field": true
          },
          {
            "id": "acc_second",
            "name": "Second Corp",
            "mysql_url": "mysql://user:pass@127.0.0.1:65534/second",
            "api_key": "xk_second"
          }
        ]"#,
    )
    .unwrap();

    let (commit_tx, _) = broadcast::channel(16);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: temp_dir
            .path()
            .join("global.db")
            .to_string_lossy()
            .to_string(),
        admin_token: Some("adm_secret".into()),
        accounts_read_only: true,
        max_accounts: 1, // Only allow 1 active account; second should be marked failed
        ..Default::default()
    });

    let manager = AccountManager::new(
        registry.to_string_lossy().to_string(),
        settings.clone(),
        commit_tx.clone(),
    )
    .with_metrics(metrics.clone());

    // Initial reconcile loads accounts.json
    let outcome = manager.reconcile().unwrap();
    assert_eq!(
        outcome,
        xloka_flow::accounts::ReconcileOutcome::Applied {
            added: 2,
            removed: 0,
            updated: 0,
        },
        "First reconcile should process 2 accounts from accounts.json"
    );
    let outcome2 = manager.reconcile().unwrap();
    assert_eq!(
        outcome2,
        xloka_flow::accounts::ReconcileOutcome::Unchanged,
        "Second reconcile with unchanged file hash should be a no-op"
    );

    let views = manager.get_accounts();
    assert_eq!(views.len(), 2);
    assert_eq!(
        manager.active_account_count(),
        1,
        "1 account should be admitted"
    );
    let failed_count = views
        .iter()
        .filter(|v| {
            let t = manager.get_telemetry(&v.id).unwrap();
            t.status() == "failed"
                && t.last_error()
                    .unwrap_or_default()
                    .contains("max_accounts limit")
        })
        .count();
    assert_eq!(
        failed_count, 1,
        "1 account should be rejected by max_accounts"
    );

    // 2. Corrupt accounts.json -> reconcile returns Err and preserves existing accounts
    std::fs::write(&registry, "{ invalid json ").unwrap();
    assert!(manager.reconcile().is_err());
    assert_eq!(
        manager.get_accounts().len(),
        2,
        "Corrupt reload must preserve running accounts"
    );

    // 3. Verify 409 Conflict on POST and DELETE when accounts_read_only = true
    let state = AppState::new(
        (*settings).clone(),
        metrics,
        Arc::new(QueryManifest::default()),
        commit_tx,
        manager.clone(),
    );
    let app = create_router(state);

    let post_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/accounts")
                .header("Authorization", "Bearer adm_secret")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name":"New","mysql_url":"mysql://root:pw@localhost:3306/db"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_resp.status(), StatusCode::CONFLICT);
    let body_bytes = axum::body::to_bytes(post_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body_json["error"], "managed by accounts.json");

    let del_resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/admin/accounts/acc_second")
                .header("Authorization", "Bearer adm_secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::CONFLICT);

    manager.shutdown().await;
}

#[tokio::test]
async fn test_column_projection_and_ddl_with_excluded_columns() {
    use xloka_flow::cdc::filter::{ColumnFilter, FilterError, ReplicationFilter};

    let full_schema = schema(
        "shop__users",
        vec![
            column("id", DuckDbType::Integer, true),
            column("email", DuckDbType::Varchar, false),
            column("password_hash", DuckDbType::Varchar, false),
            column("role", DuckDbType::Varchar, true),
        ],
    );

    // Excluding a PK column fails closed
    let mut bad_filter = ReplicationFilter::default();
    bad_filter.columns.insert(
        "shop.users".into(),
        ColumnFilter {
            include: vec![],
            exclude: vec!["role".into()],
        },
    );
    let err = bad_filter.project(&full_schema).unwrap_err();
    assert!(matches!(err, FilterError::ExcludedPrimaryKey { .. }));

    // Excluding password_hash keeps [id, email, role] and remaps pk_indices [0, 3] -> [0, 2]
    let mut good_filter = ReplicationFilter::default();
    good_filter.columns.insert(
        "users".into(),
        ColumnFilter {
            include: vec![],
            exclude: vec!["password_hash".into()],
        },
    );
    let proj = good_filter.project(&full_schema).unwrap();
    assert_eq!(proj.schema.columns.len(), 3);
    assert_eq!(proj.pk_indices, vec![0, 2]);

    // StoreWriter applies projected schema and handles DDL on excluded column cleanly
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("proj.db")
        .to_string_lossy()
        .to_string();
    let conn = Connection::open(&db_path).unwrap();
    let (commit_tx, _) = broadcast::channel(16);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: db_path.clone(),
        batch_size: 1,
        flush_interval_ms: 10,
        ..Default::default()
    });
    let writer = StoreWriter::new(settings, conn, commit_tx, metrics).unwrap();
    let (cdc_tx, cdc_rx) = mpsc::channel(32);

    cdc_tx
        .send(CdcEvent::Schema(proj.schema.clone()))
        .await
        .unwrap();

    // Full MySQL row has 4 values; projected row has 3
    let full_row = vec![
        Value::Integer(1),
        Value::Text("alice@example.com".into()),
        Value::Text("secret_bcrypt".into()),
        Value::Text("admin".into()),
    ];
    cdc_tx
        .send(CdcEvent::Row(RowOp::Insert {
            table: "shop__users".into(),
            pk_indices: proj.pk_indices.clone(),
            row: proj.project_row(&full_row),
            event_ts_ms: 1000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    // MySQL executes: ALTER TABLE users ADD COLUMN reset_token VARCHAR(255)
    // Where reset_token is also excluded -> projected schema does not change -> clean no-op in DuckDB
    cdc_tx
        .send(CdcEvent::Ddl {
            table: "shop__users".into(),
            raw_sql: "ALTER TABLE `users` ADD COLUMN `reset_token` VARCHAR(255)".into(),
            op: DdlOp::Alter,
            new_schema: Some(proj.schema.clone()),
            event_ts_ms: 1500,
            checkpoint: None,
        })
        .await
        .unwrap();

    // Update row after excluded-column DDL
    let updated_full_row = vec![
        Value::Integer(1),
        Value::Text("alice_new@example.com".into()),
        Value::Text("secret_bcrypt_v2".into()),
        Value::Text("admin".into()),
    ];
    cdc_tx
        .send(CdcEvent::Row(RowOp::Update {
            table: "shop__users".into(),
            pk_indices: proj.pk_indices.clone(),
            before: Some(proj.project_row(&full_row)),
            after: proj.project_row(&updated_full_row),
            event_ts_ms: 2000,
            checkpoint: None,
        }))
        .await
        .unwrap();

    drop(cdc_tx);
    writer.run(cdc_rx).await.unwrap();

    let verify_conn = Connection::open(&db_path).unwrap();
    let (email, role): (String, String) = verify_conn
        .query_row(
            "SELECT email, role FROM shop__users WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(email, "alice_new@example.com");
    assert_eq!(role, "admin");
}

#[tokio::test]
async fn test_per_account_query_manifest_and_health_lag() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let acct_db = temp_dir.path().join("acct_custom.duckdb");
    let manifest_dir = temp_dir.path().join("queries");
    std::fs::create_dir_all(&manifest_dir).unwrap();

    // Write per-account query manifest with a custom query and a staleness-guarded query
    std::fs::write(
        manifest_dir.join("acct_custom.toml"),
        r#"
[queries.tenant_only_metric]
description = "Per-tenant custom metric"
tables = ["tenant_metrics"]
statements = ["SELECT label, total FROM tenant_metrics WHERE label = ?1"]

[queries.fresh_metric]
description = "Staleness-checked query"
tables = ["tenant_metrics"]
statements = ["SELECT COUNT(*) AS cnt FROM tenant_metrics"]
"#,
    )
    .unwrap();

    let (commit_tx, _) = broadcast::channel(16);
    let metrics = Arc::new(FlowMetrics::new());
    let settings = Arc::new(Settings {
        db_path: temp_dir
            .path()
            .join("default.db")
            .to_string_lossy()
            .to_string(),
        batch_size: 1,
        flush_interval_ms: 10,
        admin_token: Some("adm_token".into()),
        query_manifest_dir: Some(manifest_dir.to_string_lossy().to_string()),
        ..Default::default()
    });

    let manager = AccountManager::new("".into(), settings.clone(), commit_tx.clone())
        .with_metrics(metrics.clone());
    let telemetry = Arc::new(xloka_flow::accounts::AccountTelemetry::new("streaming"));
    manager
        .telemetry
        .write()
        .unwrap()
        .insert("acct_custom".into(), telemetry.clone());

    // Populate acct_custom.duckdb via StoreWriter with telemetry attached
    {
        let conn = Connection::open(&acct_db).unwrap();
        let mut acct_settings = (*settings).clone();
        acct_settings.db_path = acct_db.to_string_lossy().to_string();
        let writer = StoreWriter::new(
            Arc::new(acct_settings),
            conn,
            commit_tx.clone(),
            metrics.clone(),
        )
        .unwrap()
        .with_telemetry(telemetry.clone());
        let (tx, rx) = mpsc::channel(16);
        tx.send(CdcEvent::Schema(schema(
            "tenant_metrics",
            vec![
                column("label", DuckDbType::Varchar, true),
                column("total", DuckDbType::Integer, false),
            ],
        )))
        .await
        .unwrap();
        tx.send(CdcEvent::Row(RowOp::Insert {
            table: "tenant_metrics".into(),
            pk_indices: vec![0],
            row: vec![Value::Text("orders".into()), Value::Integer(42)],
            event_ts_ms: 1000, // Very old timestamp -> high lag_ms
            checkpoint: None,
        }))
        .await
        .unwrap();
        drop(tx);
        writer.run(rx).await.unwrap();
    }

    let account = xloka_flow::accounts::Account {
        id: "acct_custom".into(),
        name: "Custom Tenant".into(),
        mysql_url: "mysql://user:secret@localhost:3306/db".into(),
        snapshot_mysql_url: None,
        api_key: "xk_custom".into(),
        status: "active".into(),
        filters: None,
    };
    manager.add_account_state(xloka_flow::accounts::AccountState {
        account,
        db_path: acct_db.to_string_lossy().to_string(),
        reader_conn: Arc::new(std::sync::Mutex::new(
            xloka_flow::api::queries::open_read_only(&acct_db).unwrap(),
        )),
        cdc_task: None,
        writer_task: None,
    });

    let global_manifest = QueryManifest::load_from_str(
        r#"
[queries.global_ping]
description = "Global query"
tables = ["tenant_metrics"]
statements = ["SELECT 1 AS ok"]
"#,
    )
    .unwrap();

    let state = AppState::new(
        (*settings).clone(),
        metrics,
        Arc::new(global_manifest),
        commit_tx,
        manager.clone(),
    );
    let app = create_router(state);

    // 1. Execute per-account custom query via account API key
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/tenant_only_metric?p1=orders")
                .header("Authorization", "Bearer xk_custom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["results"][0]["rows"][0]["total"], 42);

    // 2. Fallback to global query works for the account
    let resp_global = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/global_ping")
                .header("Authorization", "Bearer xk_custom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_global.status(), StatusCode::OK);

    // 3. Per-account staleness check triggers 425 Too Early because event_ts_ms=1000 is stale
    let resp_stale = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/fresh_metric?max_staleness_ms=50")
                .header("Authorization", "Bearer xk_custom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_stale.status(), StatusCode::TOO_EARLY);

    // 4. GET /api/admin/accounts exposes per-account telemetry (rows_synced, tables, db_size_bytes, lag_ms)
    let admin_resp = app
        .oneshot(
            Request::builder()
                .uri("/api/admin/accounts")
                .header("Authorization", "Bearer adm_token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_resp.status(), StatusCode::OK);
    let admin_bytes = axum::body::to_bytes(admin_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let admin_json: serde_json::Value = serde_json::from_slice(&admin_bytes).unwrap();
    let acct_view = &admin_json[0];
    assert_eq!(acct_view["id"], "acct_custom");
    assert_eq!(acct_view["mysql_url"], "mysql://user:***@localhost:3306/db");
    assert_eq!(acct_view["rows_synced"], 1);
    assert_eq!(acct_view["tables"], 1);
    assert!(acct_view["db_size_bytes"].as_u64().unwrap() > 0);
    assert!(acct_view["lag_ms"].as_u64().unwrap() > 1000);
}
