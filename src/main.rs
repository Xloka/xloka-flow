use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use xloka_flow::{
    api,
    cdc::{live::{LiveCdc, LiveCdcConfig}, CdcSource},
    store::StoreWriter,
    AppState, Settings,
    accounts::{AccountManager, AccountState},
};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "xloka-flow",
    version,
    about = "MySQL CDC → DuckDB OLAP fast replicator",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// OSS CLI Mode: Sync a single MySQL database to a local DuckDB file natively.
    Sync {
        /// The MySQL connection URL (e.g. mysql://user:pass@localhost:3306/db)
        #[arg(long)]
        mysql: String,
        
        /// The local DuckDB file path to create/update
        #[arg(long, default_value = "local.db")]
        db: String,
    },
    /// SaaS Mode: Run the Multi-Tenant REST API server and Account Manager.
    Serve {
        /// Use the built-in event simulator instead of a live MySQL connection.
        #[arg(long)]
        mock: bool,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("xloka_flow=info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();

    let settings = match Settings::load() {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to load configuration: {e}");
            std::process::exit(1);
        }
    };

    let result = match cli.command {
        Command::Sync { mysql, db } => run_sync(settings, mysql, db).await,
        Command::Serve { mock } => run_serve(settings, mock).await,
    };

    if let Err(e) = result {
        error!("xloka-flow exited with error: {e}");
        std::process::exit(1);
    }
}

// ─── Subcommand implementations ───────────────────────────────────────────────

async fn run_sync(settings: Settings, mysql_url: String, db_path: String) -> xloka_flow::error::FlowResult<()> {
    info!("Starting xloka-flow in OSS SYNC mode...");
    info!("Source: {}", mysql_url);
    info!("Destination: {}", db_path);

    let conn = duckdb::Connection::open(&db_path)?;
    let shared_conn = Arc::new(Mutex::new(conn));
    let (tx, rx) = mpsc::channel(1_000);
    
    // Spawn writer
    let writer_settings = Arc::new(settings);
    let writer_conn = Arc::clone(&shared_conn);
    let writer_task = tokio::spawn(async move {
        let writer = StoreWriter::new(writer_settings, writer_conn);
        let _ = writer.run(rx).await;
    });
    
    // Spawn CDC
    let cdc = LiveCdc::new(LiveCdcConfig {
        mysql_url,
        checkpoint_path: format!("{}_checkpoint.json", db_path),
        filter_databases: vec![],
        filter_tables: vec![],
        server_id: 42 + rand::random::<u32>() % 1000,
    });
    
    let cdc_task = tokio::spawn(async move { let _ = cdc.run(tx).await; });

    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down…");
    cdc_task.abort();
    writer_task.abort();
    
    info!("xloka-flow shut down cleanly.");
    Ok(())
}

async fn run_serve(settings: Settings, _mock: bool) -> xloka_flow::error::FlowResult<()> {
    info!("Starting xloka-flow in SaaS SERVE mode...");
    let settings = Arc::new(settings);
    let manager = AccountManager::new("accounts.json".to_string(), Arc::clone(&settings));
    
    // Ensure data dir exists
    std::fs::create_dir_all("data").unwrap_or_default();
    
    let persisted_accounts = manager.load_persisted();
    info!("Loaded {} accounts from accounts.json", persisted_accounts.len());
    
    for account in persisted_accounts {
        info!("Initializing account {} ({})", account.id, account.name);
        if let Err(e) = manager.spawn_and_add_account(account) {
            error!("Failed to spawn account: {}", e);
        }
    }

    // ── API task ──
    let api_settings = Arc::clone(&settings);
    let api_handle = tokio::spawn(async move {
        let addr: SocketAddr = api_settings.listen.parse().unwrap_or_else(|_| {
            "0.0.0.0:3000".parse().unwrap()
        });
        let state = AppState::new((*api_settings).clone(), manager);
        let router = api::router(state);

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
    
    info!("xloka-flow shut down cleanly.");
    Ok(())
}
