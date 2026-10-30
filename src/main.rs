use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use xloka_flow::{
    accounts::{redact_secret_url, AccountManager},
    api::{self, FlowMetrics, QueryManifest},
    cdc::{
        filter::ReplicationFilter,
        live::{generate_server_id, LiveCdc, LiveCdcConfig},
        mock::{MockCdc, MockConfig},
        CdcSource,
    },
    store::StoreWriter,
    AppState, Settings,
};

// ─── CLI ─────────────────────────────────────────────────────────────────────

/// Command-line interface definition for `xloka-flow`.
#[derive(Parser, Debug)]
#[command(
    name = "xloka-flow",
    version,
    about = "MySQL CDC → DuckDB OLAP fast replicator & Real-Time Analytical Engine",
    long_about = None,
)]
struct Cli {
    /// Path to the configuration file (defaults to `config.toml` in the
    /// current working directory).
    #[arg(long, global = true, env = "XLOKA__CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// CLI Mode: Sync a single MySQL database to a local DuckDB file natively.
    Sync {
        /// The MySQL connection URL (e.g. mysql://user:pass@localhost:3306/db)
        #[arg(long)]
        mysql: String,

        /// Optional: Read-only MySQL replica to use for the initial snapshot phase
        #[arg(long)]
        snapshot_mysql: Option<String>,

        /// The local DuckDB file path to create/update (defaults to `db_path` in config or `xloka-flow.db`)
        #[arg(long, env = "XLOKA__DB_PATH")]
        db: Option<String>,

        /// Comma-separated list of MySQL databases to replicate.
        /// Defaults to the database of the connection URL.
        #[arg(long, env = "XLOKA__FILTER_DATABASES", value_delimiter = ',')]
        databases: Vec<String>,

        /// Comma-separated list of tables to include (bare `orders` or qualified `shop.orders`).
        #[arg(long, env = "XLOKA__FILTER_TABLES", value_delimiter = ',')]
        tables: Vec<String>,

        /// Comma-separated list of tables to exclude (always wins over `--tables`).
        #[arg(long, env = "XLOKA__EXCLUDE_TABLES", value_delimiter = ',')]
        exclude_tables: Vec<String>,
    },
    /// SaaS Mode: Run the Analytical REST API, WebSocket server, and ingestion engine.
    Serve {
        /// Use the built-in event simulator instead of a live MySQL connection.
        #[arg(long)]
        mock: bool,

        /// Path to the DuckDB database file (defaults to `db_path` in config or `xloka-flow.db`)
        #[arg(long, env = "XLOKA__DB_PATH")]
        db: Option<String>,

        /// Path to the declarative query manifest TOML file
        #[arg(long)]
        queries: Option<String>,

        /// Admin API token to secure endpoints
        #[arg(long, env = "XLOKA__ADMIN_TOKEN")]
        admin_token: Option<String>,

        /// API key for the read-only data plane (`/api/v1/query/*`, `/ws`).
        /// When unset the data plane is open.
        #[arg(long, env = "XLOKA__DATA_API_KEY")]
        data_api_key: Option<String>,

        /// Comma-separated list of allowed CORS domains
        #[arg(long, env = "XLOKA__ALLOWED_DOMAINS")]
        allowed_domains: Option<String>,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("xloka_flow=info,query_timing=info")),
        )
        .with_target(true)
        .compact()
        .init();

    let cli = Cli::parse();

    let settings = match Settings::load_from(cli.config.as_deref().map(std::path::Path::new)) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to load configuration: {e}");
            std::process::exit(1);
        }
    };

    let command = cli.command;

    let result = match command {
        Command::Sync {
            mysql,
            snapshot_mysql,
            db,
            databases,
            tables,
            exclude_tables,
        } => {
            let db_path = db.unwrap_or_else(|| settings.db_path.clone());
            run_sync(
                settings,
                mysql,
                snapshot_mysql,
                db_path,
                databases,
                tables,
                exclude_tables,
            )
            .await
        }
        Command::Serve {
            mock,
            db,
            queries,
            admin_token,
            data_api_key,
            allowed_domains,
        } => {
            let mut settings = settings;
            if let Some(db) = db {
                settings.db_path = db;
            }
            if admin_token.is_some() {
                settings.admin_token = admin_token;
            }
            if data_api_key.is_some() {
                settings.data_api_key = data_api_key;
            }
            if allowed_domains.is_some() {
                settings.allowed_domains = allowed_domains;
            }
            if queries.is_some() {
                settings.query_manifest_path = queries;
            }
            run_serve(settings, mock).await
        }
    };

    if let Err(e) = result {
        error!("xloka-flow exited with error: {e}");
        std::process::exit(1);
    }
}

// ─── Subcommand implementations ───────────────────────────────────────────────

async fn run_sync(
    settings: Settings,
    mysql_url: String,
    snapshot_mysql: Option<String>,
    db_path: String,
    filter_databases: Vec<String>,
    filter_tables: Vec<String>,
    exclude_tables: Vec<String>,
) -> xloka_flow::error::FlowResult<()> {
    info!("Starting xloka-flow in SYNC mode...");
    info!("Source: {}", redact_secret_url(&mysql_url));
    if let Some(ref sm) = snapshot_mysql {
        info!("Snapshot Replica: {}", redact_secret_url(sm));
    }
    info!("Destination: {}", db_path);

    // Pre-load checkpoint before StoreWriter acquires the exclusive read-write lock
    let initial_checkpoint =
        StoreWriter::load_checkpoint(std::path::Path::new(&db_path), "default")
            .ok()
            .flatten();

    let conn = duckdb::Connection::open(&db_path)?;
    if !settings.account_memory_limit.trim().is_empty() {
        let escaped = settings.account_memory_limit.replace('\'', "''");
        let _ = conn.execute_batch(&format!("PRAGMA memory_limit='{escaped}';"));
    }
    if settings.duckdb_threads > 0 {
        let _ = conn.execute_batch(&format!("PRAGMA threads={};", settings.duckdb_threads));
    }
    let (tx, rx) = mpsc::channel(1_000);
    let (commit_tx, _) = broadcast::channel(1_024);
    let metrics = Arc::new(FlowMetrics::new());

    // Spawn writer
    let mut writer_settings = settings.clone();
    writer_settings.db_path = db_path.clone();
    let writer_settings = Arc::new(writer_settings);
    let writer = StoreWriter::new(writer_settings, conn, commit_tx, metrics)?;
    let writer_task = tokio::spawn(async move {
        if let Err(e) = writer.run(rx).await {
            error!("Store writer halted: {e}");
        }
    });

    let filter = if !filter_tables.is_empty() || !exclude_tables.is_empty() {
        Some(ReplicationFilter {
            table_include: filter_tables.clone(),
            table_exclude: exclude_tables,
            columns: Default::default(),
        })
    } else {
        None
    };

    // Spawn CDC
    let cdc = LiveCdc::new(LiveCdcConfig {
        mysql_url,
        snapshot_mysql_url: snapshot_mysql,
        checkpoint_path: format!("{}_checkpoint.json", db_path),
        db_path: Some(db_path),
        source_id: Some("default".into()),
        filter_databases,
        filter_tables,
        filter,
        server_id: generate_server_id(),
        initial_checkpoint,
        // In sync mode a persistent failure should surface to the operator
        // instead of retrying silently forever.
        max_reconnect_attempts: Some(10),
        telemetry: None,
    });

    let cdc_task = tokio::spawn(async move {
        if let Err(e) = cdc.run(tx).await {
            error!("LiveCdc task failed: {e:?}");
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl-C received, shutting down…");
        }
        res = cdc_task => {
            error!("CDC task unexpectedly exited: {res:?}");
        }
        res = writer_task => {
            error!("Writer task unexpectedly exited: {res:?}");
        }
    }

    info!("xloka-flow shut down cleanly.");
    Ok(())
}

async fn run_serve(settings: Settings, mock: bool) -> xloka_flow::error::FlowResult<()> {
    info!("Starting xloka-flow in SaaS SERVE mode...");
    let db_path = settings.db_path.clone();
    let settings_arc = Arc::new(settings.clone());

    // Ensure data dir exists
    std::fs::create_dir_all("data").unwrap_or_default();

    let manifest = if let Some(ref path) = settings.query_manifest_path {
        QueryManifest::load_from_file(path).unwrap_or_default()
    } else if std::path::Path::new("flow_queries.toml").exists() {
        QueryManifest::load_from_file("flow_queries.toml").unwrap_or_default()
    } else if std::path::Path::new("flow_queries.toml.example").exists() {
        QueryManifest::load_from_file("flow_queries.toml.example").unwrap_or_default()
    } else {
        QueryManifest::default()
    };
    let manifest_arc = Arc::new(manifest);
    let metrics_arc = Arc::new(FlowMetrics::new());
    let (commit_tx, _) = broadcast::channel(1_024);

    // The account manager publishes account commits on the same channel as the
    // default store, so API/WebSocket subscribers observe every dataset.
    let accounts_file = settings
        .accounts_file
        .clone()
        .unwrap_or_else(|| "accounts.json".to_string());
    let manager = AccountManager::new(accounts_file, Arc::clone(&settings_arc), commit_tx.clone())
        .with_metrics(Arc::clone(&metrics_arc));

    // Spawn any persisted accounts
    match manager.try_load_persisted() {
        Ok(persisted_accounts) => {
            if !persisted_accounts.is_empty() {
                info!("Loading {} persisted accounts...", persisted_accounts.len());
                for acc in persisted_accounts {
                    let id = acc.id.clone();
                    if let Err(e) = manager.spawn_and_add_account(acc.clone()) {
                        tracing::warn!("Failed to spawn persisted account {}: {e}", id);
                        manager.register_inactive_account(acc, "failed", Some(e.to_string()));
                    }
                }
            }
        }
        Err(e) => {
            error!("{e}");
            if settings.accounts_reload_secs == 0 {
                return Err(xloka_flow::error::FlowError::other(e.to_string()));
            }
        }
    }

    // Periodic hot-reload of accounts.json when configured.
    let reconcile_handle = if settings.accounts_reload_secs > 0 {
        let reconcile_mgr = manager.clone();
        let interval_secs = settings.accounts_reload_secs;
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            // Skip the immediate first tick since we just loaded above.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let _ = reconcile_mgr.reconcile();
            }
        }))
    } else {
        None
    };

    // Default `--db` writer + MockCdc only run in `--mock` mode.
    // In live `serve` mode, each account manages its own `data/<id>.duckdb`
    // writer and CDC pipeline, so we do not open an idle default DuckDB handle.
    let (writer_task, cdc_task) = if mock {
        let (tx, rx) = mpsc::channel(10_000);
        let write_conn = duckdb::Connection::open(&db_path)?;
        if !settings.account_memory_limit.trim().is_empty() {
            let escaped = settings.account_memory_limit.replace('\'', "''");
            let _ = write_conn.execute_batch(&format!("PRAGMA memory_limit='{escaped}';"));
        }
        if settings.duckdb_threads > 0 {
            let _ =
                write_conn.execute_batch(&format!("PRAGMA threads={};", settings.duckdb_threads));
        }
        let writer = StoreWriter::new(
            Arc::clone(&settings_arc),
            write_conn,
            commit_tx.clone(),
            Arc::clone(&metrics_arc),
        )?;
        let w_task = tokio::spawn(async move {
            if let Err(e) = writer.run(rx).await {
                error!("Store writer halted: {e}");
            }
        });
        let c_task = tokio::spawn(async move {
            let mock_cdc = MockCdc::new(MockConfig::default());
            let _ = mock_cdc.run(tx).await;
        });
        (Some(w_task), Some(c_task))
    } else {
        info!(
            "Live serve mode active (per-account DuckDB isolation; default --db writer skipped)."
        );
        (None, None)
    };

    // ── API task ──
    let api_settings = Arc::clone(&settings_arc);
    let api_metrics = Arc::clone(&metrics_arc);
    let api_manifest = Arc::clone(&manifest_arc);
    let api_commit_tx = commit_tx.clone();
    // Keep a handle so the accounts can be shut down gracefully on exit.
    let shutdown_manager = manager.clone();

    let api_handle = tokio::spawn(async move {
        let addr: SocketAddr = api_settings
            .listen
            .parse()
            .unwrap_or_else(|_| "0.0.0.0:3000".parse().unwrap());
        let state = AppState::new(
            (*api_settings).clone(),
            api_metrics,
            api_manifest,
            api_commit_tx,
            manager,
        );
        let router = api::create_router(state);

        info!("xloka-flow API listening on http://{addr}");
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, router).await {
                    error!("Axum error: {e}");
                }
            }
            Err(e) => error!("Failed to bind API socket: {e}"),
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down…");
    api_handle.abort();
    if let Some(h) = reconcile_handle {
        h.abort();
    }

    // Graceful shutdown: aborting the CDC task drops the writer's event
    // channel, which makes the writer drain its final batch and close the
    // DuckDB connection cleanly instead of losing the last rows.
    if let Some(c_task) = cdc_task {
        c_task.abort();
    }
    if let Some(w_task) = writer_task {
        info!("Draining the store writer…");
        if tokio::time::timeout(std::time::Duration::from_secs(10), w_task)
            .await
            .is_err()
        {
            tracing::warn!("Store writer did not shut down in time.");
        }
    }

    // Stop the replicated accounts and let their writers drain.
    shutdown_manager.shutdown().await;

    info!("xloka-flow shut down cleanly.");
    Ok(())
}
