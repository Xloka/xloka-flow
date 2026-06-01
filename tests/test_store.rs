use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use duckdb::Connection;

use xloka_flow::Settings;
use xloka_flow::store::StoreWriter;
use xloka_flow::cdc::{ColumnDef, DuckDbType, RowOp, TableSchema, Value};

#[tokio::test]
async fn test_store_writer_integration() {
    // 1. Setup in-memory DuckDB connection
    let conn = Connection::open_in_memory().unwrap();
    let shared_conn = Arc::new(Mutex::new(conn));

    // 2. Create StoreWriter
    let settings = Settings {
        batch_size: 2, // small batch size to test auto-flush
        ..Default::default()
    };
    let writer = StoreWriter::new(Arc::new(settings), Arc::clone(&shared_conn));

    // 3. Spawn StoreWriter
    let (tx, rx) = mpsc::channel(100);
    let writer_handle = tokio::spawn(async move {
        writer.run(rx).await.unwrap();
    });

    // 4. Send SchemaChange
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
        ],
    };
    tx.send(RowOp::SchemaChange(schema)).await.unwrap();

    // 5. Send Insert
    let mut row1 = HashMap::new();
    row1.insert("id".into(), Value::Integer(1));
    row1.insert("name".into(), Value::Text("Alice".into()));
    tx.send(RowOp::Insert { table: "users".into(), row: row1 }).await.unwrap();

    // Send another insert to trigger batch flush
    let mut row2 = HashMap::new();
    row2.insert("id".into(), Value::Integer(2));
    row2.insert("name".into(), Value::Text("Bob".into()));
    tx.send(RowOp::Insert { table: "users".into(), row: row2 }).await.unwrap();

    // 6. Close channel (triggers final flush and shutdown)
    drop(tx);

    // 7. Wait for writer to finish
    writer_handle.await.unwrap();

    // 8. Verify data in DuckDB
    let conn = shared_conn.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name FROM users ORDER BY id ASC").unwrap();
    let mut rows = stmt.query([]).unwrap();

    let row_1 = rows.next().unwrap().unwrap();
    assert_eq!(row_1.get::<_, i32>(0).unwrap(), 1);
    assert_eq!(row_1.get::<_, String>(1).unwrap(), "Alice");

    let row_2 = rows.next().unwrap().unwrap();
    assert_eq!(row_2.get::<_, i32>(0).unwrap(), 2);
    assert_eq!(row_2.get::<_, String>(1).unwrap(), "Bob");
    
    assert!(rows.next().unwrap().is_none());
}
