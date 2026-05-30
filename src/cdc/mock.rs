//! Simulated CDC engine — generates synthetic INSERT/UPDATE/DELETE events
//! without requiring a live MySQL connection.  Useful for end-to-end pipeline
//! testing and demos.

use async_trait::async_trait;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, debug};

use crate::cdc::{
    CdcSource, ColumnDef, DuckDbType, RowOp, TableSchema, Value,
};
use crate::error::FlowResult;

/// Configuration for the mock CDC engine.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// How many events to emit per second (approximate).
    pub events_per_second: u64,
    /// When set, stop after emitting this many events total.
    pub max_events: Option<u64>,
}

impl Default for MockConfig {
    fn default() -> Self {
        MockConfig {
            events_per_second: 50,
            max_events: None,
        }
    }
}

pub struct MockCdc {
    pub config: MockConfig,
}

impl MockCdc {
    pub fn new(config: MockConfig) -> Self {
        MockCdc { config }
    }
}

// ─── Schemas ─────────────────────────────────────────────────────────────────

fn users_schema() -> TableSchema {
    TableSchema {
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
                name: "email".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "created_at".into(),
                duck_type: DuckDbType::Timestamp,
                nullable: false,
                is_primary_key: false,
            },
        ],
    }
}

fn transactions_schema() -> TableSchema {
    TableSchema {
        table_name: "transactions".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::BigInt,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "user_id".into(),
                duck_type: DuckDbType::Integer,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "amount".into(),
                duck_type: DuckDbType::Double,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "category".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "ts".into(),
                duck_type: DuckDbType::Timestamp,
                nullable: false,
                is_primary_key: false,
            },
        ],
    }
}

fn events_schema() -> TableSchema {
    TableSchema {
        table_name: "events".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                duck_type: DuckDbType::BigInt,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "event_type".into(),
                duck_type: DuckDbType::Varchar,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "payload".into(),
                duck_type: DuckDbType::Json,
                nullable: true,
                is_primary_key: false,
            },
            ColumnDef {
                name: "ts".into(),
                duck_type: DuckDbType::Timestamp,
                nullable: false,
                is_primary_key: false,
            },
        ],
    }
}

// ─── Generator helpers ────────────────────────────────────────────────────────

fn now_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn random_name(rng: &mut impl Rng) -> String {
    const FIRST: &[&str] = &[
        "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi",
        "Ivan", "Judy", "Mallory", "Oscar", "Peggy", "Trent", "Victor", "Walter",
    ];
    const LAST: &[&str] = &[
        "Smith", "Jones", "Brown", "Garcia", "Davis", "Martinez", "Wilson",
        "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris", "Martin",
    ];
    format!(
        "{} {}",
        FIRST[rng.random_range(0..FIRST.len())],
        LAST[rng.random_range(0..LAST.len())]
    )
}

fn random_category(rng: &mut impl Rng) -> &'static str {
    const CATS: &[&str] = &[
        "food", "transport", "entertainment", "utilities", "health",
        "shopping", "travel", "education",
    ];
    CATS[rng.random_range(0..CATS.len())]
}

fn random_event_type(rng: &mut impl Rng) -> &'static str {
    const TYPES: &[&str] = &[
        "page_view", "click", "signup", "login", "logout",
        "purchase", "refund", "search",
    ];
    TYPES[rng.random_range(0..TYPES.len())]
}

// ─── CdcSource impl ──────────────────────────────────────────────────────────

#[async_trait]
impl CdcSource for MockCdc {
    async fn run(self, tx: mpsc::Sender<RowOp>) -> FlowResult<()> {
        info!("MockCdc starting — emitting ~{} events/s", self.config.events_per_second);

        // Emit schema events first so the writer can create tables.
        tx.send(RowOp::SchemaChange(users_schema())).await.ok();
        tx.send(RowOp::SchemaChange(transactions_schema())).await.ok();
        tx.send(RowOp::SchemaChange(events_schema())).await.ok();

        let interval = tokio::time::Duration::from_micros(
            1_000_000 / self.config.events_per_second.max(1),
        );
        let mut interval_timer = tokio::time::interval(interval);
        // StdRng is Send + Sync — safe to hold across .await points.
        // ThreadRng is NOT Send, so we can't use rand::rng() here.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(42);
        let mut rng = StdRng::seed_from_u64(seed);

        let mut user_counter: i64 = 1;
        let mut tx_counter: i64 = 1;
        let mut ev_counter: i64 = 1;
        let mut total_sent: u64 = 0;

        // Simulate a schema evolution after 500 events — adds a `score` column to users.
        let mut schema_evolved = false;

        loop {
            interval_timer.tick().await;

            if let Some(max) = self.config.max_events {
                if total_sent >= max {
                    info!("MockCdc reached max_events={}, stopping.", max);
                    break;
                }
            }

            // Trigger schema evolution at event 500
            if !schema_evolved && total_sent >= 500 {
                schema_evolved = true;
                let mut evolved = users_schema();
                evolved.columns.push(ColumnDef {
                    name: "score".into(),
                    duck_type: DuckDbType::Double,
                    nullable: true,
                    is_primary_key: false,
                });
                info!("MockCdc: emitting schema evolution for 'users' table (adding 'score' column)");
                if tx.send(RowOp::SchemaChange(evolved)).await.is_err() {
                    break;
                }
            }

            // Pick a random operation across the three tables.
            let op_choice: u32 = rng.random_range(0..10u32);

            let op = match op_choice {
                // 60% chance: insert into one of the three tables
                0..=5 => {
                    let table_choice: u32 = rng.random_range(0..3u32);
                    match table_choice % 3 {
                        0 => {
                            let id = user_counter;
                            user_counter += 1;
                            let mut row = HashMap::new();
                            row.insert("id".into(), Value::Integer(id));
                            row.insert("name".into(), Value::Text(random_name(&mut rng)));
                            row.insert(
                                "email".into(),
                                Value::Text(format!("user{}@example.com", id)),
                            );
                            row.insert("created_at".into(), Value::Text(now_str()));
                            if schema_evolved {
                                row.insert(
                                    "score".into(),
                                    Value::Float(rng.random_range(0.0_f64..100.0)),
                                );
                            }
                            RowOp::Insert {
                                table: "users".into(),
                                row,
                            }
                        }
                        1 => {
                            let id = tx_counter;
                            tx_counter += 1;
                            let mut row = HashMap::new();
                            row.insert("id".into(), Value::Integer(id));
                            row.insert(
                                "user_id".into(),
                                Value::Integer(rng.random_range(1..user_counter.max(2))),
                            );
                            row.insert(
                                "amount".into(),
                                Value::Float(
                                    (rng.random_range(1_i32..=100_000) as f64) / 100.0,
                                ),
                            );
                            row.insert(
                                "category".into(),
                                Value::Text(random_category(&mut rng).into()),
                            );
                            row.insert("ts".into(), Value::Text(now_str()));
                            RowOp::Insert {
                                table: "transactions".into(),
                                row,
                            }
                        }
                        _ => {
                            let id = ev_counter;
                            ev_counter += 1;
                            let mut row = HashMap::new();
                            row.insert("id".into(), Value::Integer(id));
                            row.insert(
                                "event_type".into(),
                                Value::Text(random_event_type(&mut rng).into()),
                            );
                            row.insert(
                                "payload".into(),
                                Value::Text(
                                    serde_json::json!({ "seq": id }).to_string(),
                                ),
                            );
                            row.insert("ts".into(), Value::Text(now_str()));
                            RowOp::Insert {
                                table: "events".into(),
                                row,
                            }
                        }
                    }
                }
                // 20% chance: update a user's email
                6..=7 => {
                    let id = rng.random_range(1..user_counter.max(2));
                    let mut after = HashMap::new();
                    after.insert("id".into(), Value::Integer(id));
                    after.insert(
                        "email".into(),
                        Value::Text(format!("updated{}@example.com", id)),
                    );
                    after.insert("name".into(), Value::Text(random_name(&mut rng)));
                    after.insert("created_at".into(), Value::Text(now_str()));
                    if schema_evolved {
                        after.insert(
                            "score".into(),
                            Value::Float(rng.random_range(0.0_f64..100.0)),
                        );
                    }
                    RowOp::Update {
                        table: "users".into(),
                        before: None,
                        after,
                    }
                }
                // 20% chance: delete a transaction
                _ => {
                    let id = rng.random_range(1..tx_counter.max(2));
                    let mut pk = HashMap::new();
                    pk.insert("id".into(), Value::Integer(id));
                    RowOp::Delete {
                        table: "transactions".into(),
                        pk_values: pk,
                    }
                }
            };

            debug!("MockCdc: {:?}", op);

            if tx.send(op).await.is_err() {
                // Receiver dropped — pipeline is shutting down.
                break;
            }
            total_sent += 1;
        }

        info!("MockCdc stopped after {} events.", total_sent);
        Ok(())
    }
}
