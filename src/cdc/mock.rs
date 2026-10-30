//! Simulated CDC engine — generates synthetic INSERT/UPDATE/DELETE events
//! without requiring a live MySQL connection. Useful for pipeline testing and demos.

use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tracing::info;

use std::collections::HashMap;

use crate::cdc::filter::{Projection, ReplicationFilter};
use crate::cdc::types::{CdcEvent, ColumnDef, DdlOp, DuckDbType, RowOp, TableSchema, Value};
use crate::cdc::CdcSource;
use crate::error::FlowResult;

/// Configuration for the mock CDC engine.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// How many events to emit per second (approximate).
    pub events_per_second: u64,
    /// When set, stop after emitting this many events total.
    pub max_events: Option<u64>,
    /// Optional table and column filter applied to synthetic events.
    pub filter: Option<ReplicationFilter>,
}

impl Default for MockConfig {
    fn default() -> Self {
        MockConfig {
            events_per_second: 50,
            max_events: None,
            filter: None,
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

pub fn users_schema() -> TableSchema {
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

pub fn transactions_schema() -> TableSchema {
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

pub fn events_schema() -> TableSchema {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn now_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn random_name(rng: &mut impl Rng) -> String {
    const FIRST: &[&str] = &[
        "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
        "Mallory", "Oscar", "Peggy", "Trent", "Victor", "Walter",
    ];
    const LAST: &[&str] = &[
        "Smith", "Jones", "Brown", "Garcia", "Davis", "Martinez", "Wilson", "Anderson", "Taylor",
        "Thomas", "Jackson", "White", "Harris", "Martin",
    ];
    format!(
        "{} {}",
        FIRST[rng.random_range(0..FIRST.len())],
        LAST[rng.random_range(0..LAST.len())]
    )
}

fn random_category(rng: &mut impl Rng) -> &'static str {
    const CATS: &[&str] = &[
        "food",
        "transport",
        "entertainment",
        "utilities",
        "health",
        "shopping",
        "travel",
        "education",
    ];
    CATS[rng.random_range(0..CATS.len())]
}

fn random_event_type(rng: &mut impl Rng) -> &'static str {
    const TYPES: &[&str] = &[
        "page_view",
        "click",
        "signup",
        "login",
        "logout",
        "purchase",
        "refund",
        "search",
    ];
    TYPES[rng.random_range(0..TYPES.len())]
}

// ─── CdcSource impl ──────────────────────────────────────────────────────────

#[async_trait]
impl CdcSource for MockCdc {
    async fn run(self, tx: mpsc::Sender<CdcEvent>) -> FlowResult<()> {
        info!(
            "MockCdc starting — emitting ~{} events/s",
            self.config.events_per_second
        );

        let filter = self.config.filter.clone().unwrap_or_default();
        let mut projections: HashMap<String, Projection> = HashMap::new();

        // Emit table schemas first so writer initializes DuckDB tables (respecting filters)
        for full_schema in [users_schema(), transactions_schema(), events_schema()] {
            if filter.table_allowed("", &full_schema.table_name) {
                let proj = filter.project(&full_schema)?;
                let _ = tx.send(CdcEvent::Schema(proj.schema.clone())).await;
                projections.insert(full_schema.table_name.clone(), proj);
            }
        }

        let interval =
            tokio::time::Duration::from_micros(1_000_000 / self.config.events_per_second.max(1));
        let mut interval_timer = tokio::time::interval(interval);

        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(42);
        let mut rng = StdRng::seed_from_u64(seed);

        let mut user_counter: i64 = 1;
        let mut tx_counter: i64 = 1;
        let mut ev_counter: i64 = 1;
        let mut total_sent: u64 = 0;
        let mut schema_evolved = false;

        loop {
            interval_timer.tick().await;

            if let Some(max) = self.config.max_events {
                if total_sent >= max {
                    info!("MockCdc reached max_events={}, stopping.", max);
                    break;
                }
            }

            // Simulate live schema evolution at event 500
            if !schema_evolved && total_sent >= 500 {
                schema_evolved = true;
                if filter.table_allowed("", "users") {
                    info!("MockCdc: emitting DDL schema evolution for 'users' (adding 'score' column)");
                    let mut evolved_full = users_schema();
                    evolved_full.columns.push(ColumnDef {
                        name: "score".into(),
                        duck_type: DuckDbType::Double,
                        nullable: true,
                        is_primary_key: false,
                    });
                    let proj = filter.project(&evolved_full)?;
                    projections.insert("users".into(), proj.clone());
                    let ddl_event = CdcEvent::Ddl {
                        table: "users".into(),
                        raw_sql: "ALTER TABLE users ADD COLUMN score DOUBLE;".into(),
                        op: DdlOp::Alter,
                        new_schema: Some(proj.schema),
                        event_ts_ms: now_ms(),
                        checkpoint: None,
                    };
                    if tx.send(ddl_event).await.is_err() {
                        break;
                    }
                }
            }

            let op_choice: u32 = rng.random_range(0..10u32);
            let ts = now_ms();

            let cdc_event = match op_choice {
                0..=5 => {
                    let table_choice: u32 = rng.random_range(0..3u32);
                    match table_choice % 3 {
                        0 => {
                            let Some(proj) = projections.get("users") else {
                                total_sent += 1;
                                continue;
                            };
                            let id = user_counter;
                            user_counter += 1;
                            let mut full_row = vec![
                                Value::Integer(id),
                                Value::Text(random_name(&mut rng)),
                                Value::Text(format!("user{}@example.com", id)),
                                Value::Text(now_str()),
                            ];
                            if schema_evolved {
                                full_row.push(Value::Float(rng.random_range(0.0_f64..100.0)));
                            }
                            CdcEvent::Row(RowOp::Insert {
                                table: "users".into(),
                                pk_indices: proj.pk_indices.clone(),
                                row: proj.project_row(&full_row),
                                event_ts_ms: ts,
                                checkpoint: None,
                            })
                        }
                        1 => {
                            let Some(proj) = projections.get("transactions") else {
                                total_sent += 1;
                                continue;
                            };
                            let id = tx_counter;
                            tx_counter += 1;
                            let user_id = rng.random_range(1..user_counter.max(2));
                            let amount = (rng.random_range(1_i32..=100_000) as f64) / 100.0;
                            let full_row = vec![
                                Value::Integer(id),
                                Value::Integer(user_id),
                                Value::Float(amount),
                                Value::Text(random_category(&mut rng).into()),
                                Value::Text(now_str()),
                            ];
                            CdcEvent::Row(RowOp::Insert {
                                table: "transactions".into(),
                                pk_indices: proj.pk_indices.clone(),
                                row: proj.project_row(&full_row),
                                event_ts_ms: ts,
                                checkpoint: None,
                            })
                        }
                        _ => {
                            let Some(proj) = projections.get("events") else {
                                total_sent += 1;
                                continue;
                            };
                            let id = ev_counter;
                            ev_counter += 1;
                            let full_row = vec![
                                Value::Integer(id),
                                Value::Text(random_event_type(&mut rng).into()),
                                Value::Text(serde_json::json!({ "seq": id }).to_string()),
                                Value::Text(now_str()),
                            ];
                            CdcEvent::Row(RowOp::Insert {
                                table: "events".into(),
                                pk_indices: proj.pk_indices.clone(),
                                row: proj.project_row(&full_row),
                                event_ts_ms: ts,
                                checkpoint: None,
                            })
                        }
                    }
                }
                6..=7 => {
                    let Some(proj) = projections.get("users") else {
                        total_sent += 1;
                        continue;
                    };
                    let id = rng.random_range(1..user_counter.max(2));
                    let mut full_after = vec![
                        Value::Integer(id),
                        Value::Text(random_name(&mut rng)),
                        Value::Text(format!("updated{}@example.com", id)),
                        Value::Text(now_str()),
                    ];
                    if schema_evolved {
                        full_after.push(Value::Float(rng.random_range(0.0_f64..100.0)));
                    }
                    CdcEvent::Row(RowOp::Update {
                        table: "users".into(),
                        pk_indices: proj.pk_indices.clone(),
                        before: None,
                        after: proj.project_row(&full_after),
                        event_ts_ms: ts,
                        checkpoint: None,
                    })
                }
                _ => {
                    let Some(proj) = projections.get("transactions") else {
                        total_sent += 1;
                        continue;
                    };
                    let id = rng.random_range(1..tx_counter.max(2));
                    CdcEvent::Row(RowOp::Delete {
                        table: "transactions".into(),
                        pk_indices: proj.pk_indices.clone(),
                        before: None,
                        pk_values: vec![Value::Integer(id)],
                        event_ts_ms: ts,
                        checkpoint: None,
                    })
                }
            };

            if tx.send(cdc_event).await.is_err() {
                break;
            }
            total_sent += 1;
        }

        info!("MockCdc stopped after emitting {} events.", total_sent);
        Ok(())
    }
}
