//! Unified error types for xloka-flow.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlowError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("MySQL error: {0}")]
    Mysql(#[from] mysql_async::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("SQL guard rejected query: {reason}")]
    SqlGuard { reason: String },

    #[error("Channel send error: {0}")]
    Send(String),

    #[error("{0}")]
    Other(String),
}

impl FlowError {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    pub fn sql_guard(reason: impl Into<String>) -> Self {
        Self::SqlGuard {
            reason: reason.into(),
        }
    }
}

pub type FlowResult<T> = Result<T, FlowError>;
