//! xloka-flow — MySQL CDC → DuckDB OLAP daemon.
//!
//! Usage:
//!   xloka-flow sync            # Live MySQL CDC → DuckDB
//!   xloka-flow sync --mock     # Simulated CDC → DuckDB (no MySQL needed)
//!   xloka-flow api             # Axum HTTP API over existing DuckDB file
//!   xloka-flow run-all         # CDC + API concurrently (live MySQL)
//!   xloka-flow run-all --mock  # CDC + API concurrently (simulated)

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use xloka_flow::{
    api,
    cdc::{mock::MockCdc, mock::MockConfig, CdcSource},
    store::StoreWriter,
    AppState, Settings,
};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "xloka-flow",
    version,
    about = "MySQL CDC → DuckDB OLAP offload daemon",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Stream CDC events into DuckDB.
    Sync {
        /// Use the built-in event simulator instead of a live MySQL connection.
        #[arg(long)]
        mock: bool,
    },
    /// Serve the Axum HTTP query API.
    Api,
    /// Run CDC + API concurrently under one tokio runtime.
    RunAll {
        /// Use the built-in event simulator instead of a live MySQL connection.
        #[arg(long)]
        mock: bool,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Bootstrap logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("xloka_flow=debug,info")),
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
        Command::Sync { mock } => run_sync(settings, mock).await,
        Command::Api => run_api(settings).await,
        Command::RunAll { mock } => run_all(settings, mock).await,
    };

    if let Err(e) = result {
        error!("xloka-flow exited with error: {e}");
        std::process::exit(1);
    }
}

// ─── Subcommand implementations ───────────────────────────────────────────────

async fn run_sync(settings: Settings, mock: bool) -> xloka_flow::error::FlowResult<()> {
    let settings = Arc::new(settings);
    let (tx, rx) = mpsc::channel(1_000);

    // Spawn writer.
    let writer = StoreWriter::new(Arc::clone(&settings));
    let writer_task = tokio::spawn(async move { writer.run(rx).await });

    // Run CDC source.
    if mock {
        info!("Starting MOCK CDC source");
        let cdc = MockCdc::new(MockConfig::default());
        cdc.run(tx).await?;
    } else {
        info!("Starting LIVE MySQL CDC source (url={})", settings.mysql_url);
        use xloka_flow::cdc::live::{LiveCdc, LiveCdcConfig};
        let cdc = LiveCdc::new(LiveCdcConfig {
            mysql_url: settings.mysql_url.clone(),
            checkpoint_path: settings.checkpoint_path.clone(),
            filter_databases: vec![],
            filter_tables: vec![],
            server_id: 42,
        });
        cdc.run(tx).await?;
    }

    // Wait for writer to finish draining.
    writer_task.await.map_err(|e| {
        xloka_flow::error::FlowError::other(format!("Writer task panicked: {e}"))
    })??;

    Ok(())
}

async fn run_api(settings: Settings) -> xloka_flow::error::FlowResult<()> {
    let addr: SocketAddr = settings.listen.parse().map_err(|e| {
        xloka_flow::error::FlowError::other(format!("Invalid listen address: {e}"))
    })?;

    let state = AppState::new(settings);
    let router = api::router(state);

    info!("xloka-flow API listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        xloka_flow::error::FlowError::other(format!("Failed to bind {addr}: {e}"))
    })?;

    axum::serve(listener, router).await.map_err(|e| {
        xloka_flow::error::FlowError::other(format!("Axum serve error: {e}"))
    })?;

    Ok(())
}

async fn run_all(settings: Settings, mock: bool) -> xloka_flow::error::FlowResult<()> {
    let settings = Arc::new(settings);
    let (tx, rx) = mpsc::channel(1_000);

    // ── Writer task ──
    let writer_settings = Arc::clone(&settings);
    let writer_handle = tokio::spawn(async move {
        let writer = StoreWriter::new(writer_settings);
        writer.run(rx).await
    });

    // ── API task ──
    let api_settings = Arc::clone(&settings);
    let api_handle = tokio::spawn(async move {
        let addr: SocketAddr = api_settings.listen.parse().unwrap_or_else(|_| {
            "0.0.0.0:3000".parse().unwrap()
        });
        let state = AppState::new((*api_settings).clone());
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

    // ── CDC task ──
    if mock {
        info!("Starting MOCK CDC source (run-all mode)");
        let cdc = MockCdc::new(MockConfig::default());
        tokio::select! {
            res = cdc.run(tx) => {
                if let Err(e) = res {
                    error!("MockCdc error: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, shutting down…");
            }
        }
    } else {
        info!("Starting LIVE MySQL CDC source (run-all mode, url={})", settings.mysql_url);
        use xloka_flow::cdc::live::{LiveCdc, LiveCdcConfig};
        let cdc = LiveCdc::new(LiveCdcConfig {
            mysql_url: settings.mysql_url.clone(),
            checkpoint_path: settings.checkpoint_path.clone(),
            filter_databases: vec![],
            filter_tables: vec![],
            server_id: 42,
        });
        tokio::select! {
            res = cdc.run(tx) => {
                if let Err(e) = res {
                    error!("LiveCdc error: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, shutting down…");
            }
        }
    }

    // Let writer drain then wait.
    match writer_handle.await {
        Ok(Ok(())) => info!("StoreWriter finished cleanly."),
        Ok(Err(e)) => error!("StoreWriter exited with error: {e}"),
        Err(e) => error!("StoreWriter task panicked: {e}"),
    }
    api_handle.abort();

    info!("xloka-flow shut down.");
    Ok(())
}
