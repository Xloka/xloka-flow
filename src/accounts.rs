use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::api::queries::QueryManifest;
use crate::cdc::filter::ReplicationFilter;
use crate::cdc::live::{generate_server_id, LiveCdc, LiveCdcConfig};
use crate::cdc::CdcSource;
use crate::store::writer::CommitNotification;
use crate::store::StoreWriter;
use crate::Settings;

const KNOWN_ACCOUNT_KEYS: &[&str] = &[
    "id",
    "name",
    "mysql_url",
    "snapshot_mysql_url",
    "api_key",
    "status",
    "filters",
];

const KNOWN_FILTER_KEYS: &[&str] = &["table_include", "table_exclude", "columns"];

fn default_account_status() -> String {
    "active".into()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub mysql_url: String,
    #[serde(default)]
    pub snapshot_mysql_url: Option<String>,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_account_status")]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<ReplicationFilter>,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            mysql_url: String::new(),
            snapshot_mysql_url: None,
            api_key: String::new(),
            status: default_account_status(),
            filters: None,
        }
    }
}

impl Account {
    /// Fill missing `id`, `api_key`, and `status` with deterministic or default values.
    pub fn normalize_defaults(&mut self) {
        if self.id.trim().is_empty() {
            self.id = deterministic_hex_id(&self.name, &self.mysql_url);
        }
        if self.api_key.trim().is_empty() {
            self.api_key = format!(
                "xk_{}{}",
                deterministic_hex_id(&self.id, &self.name),
                deterministic_hex_id(&self.mysql_url, "xk_salt")
            );
        }
        if self.status.trim().is_empty() {
            self.status = default_account_status();
        }
    }

    /// Returns `true` unless the account is marked `disabled` or `inactive`.
    pub fn is_active(&self) -> bool {
        let s = self.status.trim();
        !s.eq_ignore_ascii_case("disabled") && !s.eq_ignore_ascii_case("inactive")
    }

    /// Return a copy of the account with passwords and API keys masked.
    ///
    /// Also resolves or masks `${ENV}` placeholders so neither plaintext passwords
    /// nor raw unresolved secret templates are ever exposed in logs or API responses.
    pub fn redacted(&self) -> Account {
        Account {
            id: self.id.clone(),
            name: self.name.clone(),
            mysql_url: redact_secret_url(&self.mysql_url),
            snapshot_mysql_url: self
                .snapshot_mysql_url
                .as_ref()
                .map(|u| redact_secret_url(u)),
            api_key: redact_key(&self.api_key),
            status: self.status.clone(),
            filters: self.filters.clone(),
        }
    }
}

/// Mask an API key, keeping only a short prefix/suffix for identification.
fn redact_key(key: &str) -> String {
    if key.len() <= 8 {
        "***".into()
    } else {
        format!("{}***{}", &key[..4], &key[key.len() - 4..])
    }
}

/// Resolve `${ENV_NAME}` placeholders in a connection string from environment variables.
pub fn interpolate_env(input: &str) -> Result<String, anyhow::Error> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('}') else {
            return Err(anyhow::anyhow!(
                "unclosed environment variable placeholder in configuration"
            ));
        };
        let var_name = after_start[..end].trim();
        if var_name.is_empty() {
            return Err(anyhow::anyhow!(
                "empty environment variable placeholder '${{}}' in configuration"
            ));
        }
        let val = std::env::var(var_name).map_err(|_| {
            anyhow::anyhow!(
                "environment variable '{}' referenced in configuration is not set",
                var_name
            )
        })?;
        out.push_str(&val);
        rest = &after_start[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Redact a URL that may contain either a literal password or `${ENV_VAR}` placeholders.
pub fn redact_secret_url(url: &str) -> String {
    if url.contains("${") {
        if let Ok(resolved) = interpolate_env(url) {
            return crate::cdc::live::mask_password(&resolved);
        }
        // Replace any unresolved ${...} with *** so the raw template never leaks in logs
        let mut out = String::new();
        let mut rest = url;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            out.push_str("***");
            let after_start = &rest[start + 2..];
            if let Some(end) = after_start.find('}') {
                rest = &after_start[end + 1..];
            } else {
                rest = "";
                break;
            }
        }
        out.push_str(rest);
        return crate::cdc::live::mask_password(&out);
    }
    crate::cdc::live::mask_password(url)
}

/// Generate a deterministic 12-hex-character ID from two strings (FNV-1a 64-bit).
fn deterministic_hex_id(a: &str, b: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in a.as_bytes().iter().chain(b"\0").chain(b.as_bytes()) {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:012x}", hash & 0xffffffffffff)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ─── Per-Account Telemetry ────────────────────────────────────────────────────

/// Live runtime health, lag, and progress metrics for a single replicated account.
#[derive(Debug)]
pub struct AccountTelemetry {
    status: RwLock<String>,
    last_error: RwLock<Option<String>>,
    pub lag_ms: AtomicU64,
    pub rows_synced: AtomicU64,
    pub tables_count: AtomicUsize,
    pub started_at: String,
}

impl AccountTelemetry {
    pub fn new(initial_status: &str) -> Self {
        Self {
            status: RwLock::new(initial_status.to_string()),
            last_error: RwLock::new(None),
            lag_ms: AtomicU64::new(0),
            rows_synced: AtomicU64::new(0),
            tables_count: AtomicUsize::new(0),
            started_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn set_status(&self, status: impl Into<String>) {
        if let Ok(mut guard) = self.status.write() {
            *guard = status.into();
        }
    }

    pub fn status(&self) -> String {
        self.status
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| "unknown".into())
    }

    pub fn set_error(&self, err: Option<String>) {
        if let Ok(mut guard) = self.last_error.write() {
            *guard = err;
        }
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.read().ok().and_then(|g| g.clone())
    }

    pub fn update_lag_from_ts(&self, origin_ts_ms: u64) {
        if origin_ts_ms > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(origin_ts_ms);
            let lag = now_ms.saturating_sub(origin_ts_ms);
            self.lag_ms.store(lag, Ordering::Relaxed);
        }
    }

    pub fn lag_ms(&self) -> u64 {
        self.lag_ms.load(Ordering::Relaxed)
    }

    pub fn add_rows_synced(&self, count: u64) {
        self.rows_synced.fetch_add(count, Ordering::Relaxed);
    }

    pub fn rows_synced(&self) -> u64 {
        self.rows_synced.load(Ordering::Relaxed)
    }

    pub fn set_tables_count(&self, count: usize) {
        self.tables_count.store(count, Ordering::Relaxed);
    }

    pub fn tables_count(&self) -> usize {
        self.tables_count.load(Ordering::Relaxed)
    }
}

/// Summary of a reconciliation pass over `accounts.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    Unchanged,
    Applied {
        added: usize,
        removed: usize,
        updated: usize,
    },
}

pub struct AccountState {
    pub account: Account,
    pub db_path: String,
    pub reader_conn: Arc<Mutex<duckdb::Connection>>,
    pub cdc_task: Option<JoinHandle<()>>,
    pub writer_task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct AccountManager {
    pub states: Arc<RwLock<HashMap<String, AccountState>>>,
    pub telemetry: Arc<RwLock<HashMap<String, Arc<AccountTelemetry>>>>,
    pub account_manifests: Arc<RwLock<HashMap<String, Arc<QueryManifest>>>>,
    pub file_path: String,
    pub settings: Arc<Settings>,
    /// Shared commit-notification channel used by every account writer, so that
    /// API/WebSocket subscribers see the commits of all accounts (tagged with
    /// their `account_id`).
    pub commit_tx: broadcast::Sender<CommitNotification>,
    /// Shared metrics collector updated by account writers.
    pub metrics: Arc<crate::api::metrics::FlowMetrics>,
    last_applied_hash: Arc<Mutex<Option<u64>>>,
    last_error_hash: Arc<Mutex<Option<u64>>>,
}

impl AccountManager {
    pub fn new(
        file_path: String,
        settings: Arc<Settings>,
        commit_tx: broadcast::Sender<CommitNotification>,
    ) -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            telemetry: Arc::new(RwLock::new(HashMap::new())),
            account_manifests: Arc::new(RwLock::new(HashMap::new())),
            file_path,
            settings,
            commit_tx,
            metrics: Arc::new(crate::api::metrics::FlowMetrics::new()),
            last_applied_hash: Arc::new(Mutex::new(None)),
            last_error_hash: Arc::new(Mutex::new(None)),
        }
    }

    /// Share the server's `FlowMetrics` instance with account writers so that
    /// `/metrics`, `/health`, and `max_staleness_ms` reflect account activity.
    pub fn with_metrics(mut self, metrics: Arc<crate::api::metrics::FlowMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Parse and validate an accounts JSON string, logging line/column on syntax errors
    /// and warning on unknown top-level or filter keys.
    pub fn parse_accounts_str(
        contents: &str,
        source_path: &str,
    ) -> Result<Vec<Account>, anyhow::Error> {
        let raw_val: serde_json::Value = match serde_json::from_str(contents) {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to parse accounts file '{}' at line {}, column {}: {}",
                    source_path,
                    e.line(),
                    e.column(),
                    e
                );
                return Err(anyhow::anyhow!(
                    "failed to parse '{}' at line {}, column {}: {}",
                    source_path,
                    e.line(),
                    e.column(),
                    e
                ));
            }
        };

        let Some(arr) = raw_val.as_array() else {
            error!(
                "Invalid accounts file '{}': top-level JSON value must be an array",
                source_path
            );
            return Err(anyhow::anyhow!(
                "invalid accounts file '{}': top-level JSON value must be an array",
                source_path
            ));
        };

        let mut accounts = Vec::with_capacity(arr.len());
        for (idx, item) in arr.iter().enumerate() {
            if let Some(obj) = item.as_object() {
                for key in obj.keys() {
                    if !KNOWN_ACCOUNT_KEYS.contains(&key.as_str()) {
                        warn!(
                            "Accounts file '{}' entry [{}]: unknown top-level key '{}' (ignored)",
                            source_path, idx, key
                        );
                    }
                }
                if let Some(filters_obj) = obj.get("filters").and_then(|f| f.as_object()) {
                    for fkey in filters_obj.keys() {
                        if !KNOWN_FILTER_KEYS.contains(&fkey.as_str()) {
                            warn!(
                                "Accounts file '{}' entry [{}].filters: unknown key '{}' (ignored)",
                                source_path, idx, fkey
                            );
                        }
                    }
                }
            }

            let mut acc: Account = serde_json::from_value(item.clone()).map_err(|e| {
                error!(
                    "Invalid account entry [{}] in '{}': {}",
                    idx, source_path, e
                );
                anyhow::anyhow!(
                    "invalid account entry [{}] in '{}': {}",
                    idx,
                    source_path,
                    e
                )
            })?;
            acc.normalize_defaults();
            accounts.push(acc);
        }

        Ok(accounts)
    }

    /// Load and validate persisted accounts from `self.file_path`, returning `Err` on
    /// malformed JSON or invalid account definitions.
    ///
    /// Returns `Ok(Vec::new())` if the file does not exist yet.
    pub fn try_load_persisted(&self) -> Result<Vec<Account>, anyhow::Error> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return Ok(Vec::new());
        }

        self.warn_if_world_readable(path);

        let contents = std::fs::read_to_string(path).map_err(|e| {
            error!("Failed to read accounts file '{}': {}", self.file_path, e);
            anyhow::anyhow!("failed to read accounts file '{}': {}", self.file_path, e)
        })?;

        let hash = hash_bytes(contents.as_bytes());
        let accounts = Self::parse_accounts_str(&contents, &self.file_path)?;
        if let Ok(mut guard) = self.last_applied_hash.lock() {
            *guard = Some(hash);
        }
        Ok(accounts)
    }

    /// Load persisted accounts from `self.file_path`, logging any parse errors and
    /// returning an empty list on failure. Use [`Self::try_load_persisted`] when the
    /// caller needs to distinguish parse errors from an empty file.
    pub fn load_persisted(&self) -> Vec<Account> {
        self.try_load_persisted().unwrap_or_default()
    }

    #[cfg(unix)]
    fn warn_if_world_readable(&self, path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mode = meta.permissions().mode();
            if mode & 0o004 != 0 {
                warn!(
                    "Accounts file '{}' is world-readable (mode {:04o}); restrict permissions to 0600",
                    self.file_path,
                    mode & 0o777
                );
            }
        }
    }

    #[cfg(not(unix))]
    fn warn_if_world_readable(&self, _path: &Path) {}

    pub fn save_persisted(&self, accounts: &[Account]) -> Result<(), anyhow::Error> {
        if self.settings.accounts_read_only {
            return Err(anyhow::anyhow!(
                "accounts file '{}' is configured as read-only",
                self.file_path
            ));
        }
        let json = serde_json::to_string_pretty(accounts)?;
        let hash = hash_bytes(json.as_bytes());

        let path = Path::new(&self.file_path);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).ok();
        }

        std::fs::write(path, &json)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }

        if let Ok(mut guard) = self.last_applied_hash.lock() {
            *guard = Some(hash);
        }
        Ok(())
    }

    pub fn get_accounts(&self) -> Vec<Account> {
        let map = self.states.read().unwrap();
        map.values().map(|s| s.account.clone()).collect()
    }

    pub fn get_telemetry(&self, id: &str) -> Option<Arc<AccountTelemetry>> {
        let map = self.telemetry.read().unwrap();
        map.get(id).cloned()
    }

    pub fn add_account_state(&self, state: AccountState) {
        let id = state.account.id.clone();
        let initial_status = if state.account.is_active() {
            if state.cdc_task.is_some() {
                "starting"
            } else {
                &state.account.status
            }
        } else {
            "disabled"
        };
        {
            let mut tmap = self.telemetry.write().unwrap();
            tmap.entry(id.clone())
                .or_insert_with(|| Arc::new(AccountTelemetry::new(initial_status)));
        }
        self.load_account_manifest(&id);
        let mut map = self.states.write().unwrap();
        map.insert(id, state);
    }

    /// Look up an account by its data-plane API key.
    pub fn get_account_by_api_key(&self, api_key: &str) -> Option<Account> {
        let map = self.states.read().unwrap();
        map.values()
            .find(|s| constant_time_eq(&s.account.api_key, api_key))
            .map(|s| s.account.clone())
    }

    pub fn get_account_conn_by_api_key(
        &self,
        api_key: &str,
    ) -> Option<Arc<Mutex<duckdb::Connection>>> {
        let map = self.states.read().unwrap();
        map.values()
            .find(|s| constant_time_eq(&s.account.api_key, api_key))
            .map(|s| Arc::clone(&s.reader_conn))
    }

    pub fn get_account_db_path_by_api_key(&self, api_key: &str) -> Option<String> {
        let map = self.states.read().unwrap();
        map.values()
            .find(|s| constant_time_eq(&s.account.api_key, api_key))
            .map(|s| s.db_path.clone())
    }

    pub fn get_account_conn_by_id(&self, id: &str) -> Option<Arc<Mutex<duckdb::Connection>>> {
        let map = self.states.read().unwrap();
        map.get(id).map(|s| Arc::clone(&s.reader_conn))
    }

    pub fn get_account_db_path_by_id(&self, id: &str) -> Option<String> {
        let map = self.states.read().unwrap();
        map.get(id).map(|s| s.db_path.clone())
    }

    pub fn get_account_by_id(&self, id: &str) -> Option<Account> {
        let map = self.states.read().unwrap();
        map.get(id).map(|s| s.account.clone())
    }

    /// Return the replication lag in milliseconds for a specific account.
    pub fn get_account_lag_ms(&self, id: &str) -> Option<u64> {
        self.get_telemetry(id).map(|t| t.lag_ms())
    }

    /// Load or reload the optional per-account query manifest (`<query_manifest_dir>/<id>.toml`
    /// or `queries/<id>.toml`). Errors for one account never affect other accounts.
    pub fn load_account_manifest(&self, account_id: &str) -> Option<Arc<QueryManifest>> {
        let candidate = if let Some(ref dir) = self.settings.query_manifest_dir {
            Path::new(dir).join(format!("{account_id}.toml"))
        } else {
            Path::new("queries").join(format!("{account_id}.toml"))
        };

        if !candidate.exists() {
            let mut map = self.account_manifests.write().unwrap();
            map.remove(account_id);
            return None;
        }

        match QueryManifest::load_from_file(&candidate) {
            Ok(manifest) => {
                let arc = Arc::new(manifest);
                let mut map = self.account_manifests.write().unwrap();
                map.insert(account_id.to_string(), Arc::clone(&arc));
                Some(arc)
            }
            Err(e) => {
                warn!(
                    "Failed to load per-account query manifest '{}' for account '{}': {}",
                    candidate.display(),
                    account_id,
                    e
                );
                None
            }
        }
    }

    pub fn get_account_manifest(&self, account_id: &str) -> Option<Arc<QueryManifest>> {
        {
            let map = self.account_manifests.read().unwrap();
            if let Some(m) = map.get(account_id) {
                return Some(Arc::clone(m));
            }
        }
        self.load_account_manifest(account_id)
    }

    pub fn get_account_query(
        &self,
        account_id: &str,
        query_name: &str,
    ) -> Option<crate::api::queries::QueryDef> {
        self.get_account_manifest(account_id)
            .and_then(|m| m.queries.get(query_name).cloned())
    }

    /// Reload per-account query manifests for all known accounts.
    pub fn reload_account_manifests(&self) {
        let ids: Vec<String> = {
            let map = self.states.read().unwrap();
            map.keys().cloned().collect()
        };
        for id in ids {
            self.load_account_manifest(&id);
        }
    }

    fn base_dir(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.file_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("data"))
    }

    /// Count currently active (running) accounts.
    pub fn active_account_count(&self) -> usize {
        let map = self.states.read().unwrap();
        map.values()
            .filter(|s| s.cdc_task.is_some() || s.writer_task.is_some())
            .count()
    }

    /// Register an account that is disabled or failed so it remains visible in `GET /accounts`.
    pub fn register_inactive_account(
        &self,
        account: Account,
        status: &str,
        last_error: Option<String>,
    ) {
        let base_dir = self.base_dir();
        std::fs::create_dir_all(&base_dir).ok();
        let db_path = base_dir
            .join(format!("{}.db", account.id))
            .to_string_lossy()
            .to_string();

        let telemetry = Arc::new(AccountTelemetry::new(status));
        telemetry.set_error(last_error);
        {
            let mut tmap = self.telemetry.write().unwrap();
            tmap.insert(account.id.clone(), telemetry);
        }

        if let Ok(conn) = duckdb::Connection::open_in_memory() {
            let state = AccountState {
                account,
                db_path,
                reader_conn: Arc::new(Mutex::new(conn)),
                cdc_task: None,
                writer_task: None,
            };
            let mut map = self.states.write().unwrap();
            map.insert(state.account.id.clone(), state);
        }
    }

    pub fn spawn_and_add_account(&self, mut account: Account) -> Result<(), anyhow::Error> {
        account.normalize_defaults();

        if !account.is_active() {
            info!(
                "Account '{}' has status '{}' — registering without starting replication.",
                account.id, account.status
            );
            self.register_inactive_account(account, "disabled", None);
            return Ok(());
        }

        // Enforce `max_accounts` admission control (excluding this account if it is being replaced)
        let active_others = {
            let map = self.states.read().unwrap();
            map.values()
                .filter(|s| {
                    s.account.id != account.id && (s.cdc_task.is_some() || s.writer_task.is_some())
                })
                .count()
        };
        if active_others >= self.settings.max_accounts {
            warn!(
                "Refusing to start account '{}': max_accounts limit ({}) reached",
                account.id, self.settings.max_accounts
            );
            return Err(anyhow::anyhow!(
                "max_accounts limit ({}) reached",
                self.settings.max_accounts
            ));
        }

        // Resolve `${ENV_NAME}` secrets before connecting (never log the unresolved or raw secret)
        let resolved_mysql_url = interpolate_env(&account.mysql_url)?;
        let resolved_snapshot_url = match account.snapshot_mysql_url.as_deref() {
            Some(u) => Some(interpolate_env(u)?),
            None => None,
        };

        // Account data lives next to the account registry, so that a single
        // mounted directory keeps the registry, the DuckDB files and the
        // checkpoints together (and so they survive a container restart).
        let base_dir = self.base_dir();
        std::fs::create_dir_all(&base_dir).ok();

        let db_path = base_dir.join(format!("{}.db", account.id));
        let db_path = db_path.to_string_lossy().to_string();
        let checkpoint_path = base_dir.join(format!("{}_checkpoint.json", account.id));
        let checkpoint_path = checkpoint_path.to_string_lossy().to_string();

        if let Some(ref sm) = resolved_snapshot_url {
            info!(
                "Account '{}' using Snapshot Replica: {}",
                account.id,
                crate::cdc::live::mask_password(sm)
            );
        }

        // Pre-load checkpoint before StoreWriter acquires the exclusive read-write lock
        let initial_checkpoint =
            StoreWriter::load_checkpoint(std::path::Path::new(&db_path), &account.id)
                .ok()
                .flatten();

        let conn = duckdb::Connection::open(&db_path)?;

        // Execute PRAGMAs to enforce per-account memory ceiling and worker thread cap
        let mem_limit = self.settings.account_memory_limit.replace('\'', "");
        let threads = self.settings.duckdb_threads.max(1);
        conn.execute_batch(&format!(
            "PRAGMA memory_limit='{mem_limit}'; PRAGMA threads={threads};"
        ))?;

        let reader_conn = conn.try_clone()?;
        let shared_reader_conn = Arc::new(Mutex::new(reader_conn));
        let (tx, rx) = mpsc::channel(1_000);

        let telemetry = Arc::new(AccountTelemetry::new("starting"));
        {
            let mut tmap = self.telemetry.write().unwrap();
            tmap.insert(account.id.clone(), Arc::clone(&telemetry));
        }

        // The account writer publishes to the SHARED commit channel and metrics
        // collector so that API, WebSocket subscribers, and /health observe its commits.
        let commit_tx = self.commit_tx.clone();
        let metrics = Arc::clone(&self.metrics);

        // Spawn writer. The writer registers reader clones in the shared connection
        // registry keyed by db_path, so it must carry THIS account's db_path — not
        // the global settings path — or it would clobber the main database's entry.
        let mut writer_settings = (*self.settings).clone();
        writer_settings.db_path = db_path.clone();
        let writer_settings = Arc::new(writer_settings);
        let writer = StoreWriter::new(writer_settings, conn, commit_tx, metrics)?
            .with_account_id(account.id.clone())
            .with_telemetry(Arc::clone(&telemetry));
        let writer_account_id = account.id.clone();
        let writer_telemetry = Arc::clone(&telemetry);
        let writer_task = tokio::spawn(async move {
            if let Err(e) = writer.run(rx).await {
                error!("Account '{}' writer stopped: {e}", writer_account_id);
                writer_telemetry.set_status("failed");
                writer_telemetry.set_error(Some(e.to_string()));
            }
        });

        // Spawn CDC
        let account_id = account.id.clone();
        let filter = account.filters.clone().unwrap_or_default();
        let cdc_config = LiveCdcConfig {
            mysql_url: resolved_mysql_url,
            snapshot_mysql_url: resolved_snapshot_url,
            checkpoint_path,
            db_path: Some(db_path.clone()),
            source_id: Some(account.id.clone()),
            filter_databases: vec![],
            filter_tables: filter.table_include.clone(),
            filter: Some(filter),
            server_id: generate_server_id(),
            initial_checkpoint,
            max_reconnect_attempts: None,
            telemetry: Some(Arc::clone(&telemetry)),
        };
        let cdc = LiveCdc::new(cdc_config.clone());
        let cdc_telemetry = Arc::clone(&telemetry);

        let cdc_task = tokio::spawn(async move {
            // `LiveCdc::run` reconnects with backoff on stream errors; it only
            // returns when the writer channel is closed (clean shutdown) or when
            // a fatal config error occurs / the retry budget is exhausted.
            if let Err(e) = cdc.run(tx).await {
                error!("Account '{account_id}' CDC task failed: {e:?}");
                cdc_telemetry.set_status("failed");
                cdc_telemetry.set_error(Some(e.to_string()));
            }
        });

        info!(
            "Account '{}' started (db={}, server_id={})",
            account.id, db_path, cdc_config.server_id
        );

        let state = AccountState {
            account,
            db_path: db_path.clone(),
            reader_conn: shared_reader_conn,
            cdc_task: Some(cdc_task),
            writer_task: Some(writer_task),
        };

        self.add_account_state(state);
        Ok(())
    }

    /// Stop an account's CDC and writer tasks, optionally deleting its DuckDB file, WAL,
    /// and checkpoint from disk.
    pub fn stop_account(&self, id: &str, prune_data: bool) {
        let state = {
            let mut map = self.states.write().unwrap();
            map.remove(id)
        };
        {
            let mut tmap = self.telemetry.write().unwrap();
            tmap.remove(id);
        }
        {
            let mut mmap = self.account_manifests.write().unwrap();
            mmap.remove(id);
        }
        if let Some(mut state) = state {
            if let Some(cdc) = state.cdc_task.take() {
                cdc.abort();
            }
            if let Some(writer) = state.writer_task.take() {
                writer.abort();
            }
            let db_path = state.db_path.clone();
            // Release registered reader clones and the account's reader handle
            // before unlinking or reopening the database file (required on Windows).
            crate::api::queries::unregister_db_connection(std::path::Path::new(&db_path));
            drop(state.reader_conn);

            if prune_data {
                let base_dir = self.base_dir();
                let checkpoint_path = base_dir.join(format!("{id}_checkpoint.json"));
                for path in [
                    db_path.clone(),
                    format!("{db_path}.wal"),
                    checkpoint_path.to_string_lossy().to_string(),
                ] {
                    // On Windows, background tasks may take tens of milliseconds to release
                    // file locks upon task termination. Retry briefly.
                    for _ in 0..20 {
                        if let Err(e) = std::fs::remove_file(&path) {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        } else {
                            break;
                        }
                    }
                }
            } else {
                // Give aborted writer task a brief moment to release the DuckDB lock
                std::thread::sleep(std::time::Duration::from_millis(30));
            }
        }
    }

    /// Stop an account and remove its on-disk data.
    pub fn remove_account(&self, id: &str) {
        self.stop_account(id, true);
    }

    /// Reconcile running accounts against `self.file_path`.
    ///
    /// - Uses content hashing to skip unchanged files.
    /// - Keeps the last known-good running state if the file is unreadable or malformed.
    /// - Starts new accounts, stops removed accounts, and respawns accounts whose
    ///   connection URL, status, or filters changed.
    /// - Never lets one bad account abort the reconciliation pass.
    pub fn reconcile(&self) -> Result<ReconcileOutcome, anyhow::Error> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return Ok(ReconcileOutcome::Unchanged);
        }

        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "Reconcile: could not read accounts file '{}' ({}) — keeping current state",
                    self.file_path, e
                );
                return Err(e.into());
            }
        };

        let hash = hash_bytes(&bytes);
        {
            let applied = self.last_applied_hash.lock().unwrap();
            if *applied == Some(hash) {
                self.reload_account_manifests();
                return Ok(ReconcileOutcome::Unchanged);
            }
        }

        let contents = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                let mut err_hash = self.last_error_hash.lock().unwrap();
                if *err_hash != Some(hash) {
                    *err_hash = Some(hash);
                    error!(
                        "Reconcile: accounts file '{}' is not valid UTF-8: {}",
                        self.file_path, e
                    );
                }
                return Err(e.into());
            }
        };

        let parsed = match Self::parse_accounts_str(&contents, &self.file_path) {
            Ok(a) => a,
            Err(e) => {
                let mut err_hash = self.last_error_hash.lock().unwrap();
                if *err_hash != Some(hash) {
                    *err_hash = Some(hash);
                    error!(
                        "Reconcile: keeping last known-good state because '{}' failed validation: {}",
                        self.file_path, e
                    );
                }
                return Err(e);
            }
        };

        // Deduplicate by `id`, warning on duplicates
        let mut seen_ids = HashSet::new();
        let mut desired: Vec<Account> = Vec::with_capacity(parsed.len());
        for acc in parsed {
            if !seen_ids.insert(acc.id.clone()) {
                warn!(
                    "Reconcile: duplicate account id '{}' in '{}' — ignoring duplicate entry",
                    acc.id, self.file_path
                );
                continue;
            }
            desired.push(acc);
        }

        let desired_map: HashMap<String, Account> =
            desired.iter().map(|a| (a.id.clone(), a.clone())).collect();

        let existing_accounts: HashMap<String, Account> = {
            let map = self.states.read().unwrap();
            map.iter()
                .map(|(k, v)| (k.clone(), v.account.clone()))
                .collect()
        };

        let mut added = 0usize;
        let mut removed = 0usize;
        let mut updated = 0usize;

        // 1. Remove accounts that disappeared from the file
        for existing_id in existing_accounts.keys() {
            if !desired_map.contains_key(existing_id) {
                info!(
                    "Reconcile: stopping removed account '{}' (prune_data={})",
                    existing_id, self.settings.accounts_prune_data
                );
                self.stop_account(existing_id, self.settings.accounts_prune_data);
                removed += 1;
            }
        }

        // 2. Add or update desired accounts in file order
        for acc in desired {
            let id = acc.id.clone();
            if let Some(prev) = existing_accounts.get(&id) {
                let config_changed = prev.mysql_url != acc.mysql_url
                    || prev.snapshot_mysql_url != acc.snapshot_mysql_url
                    || prev.filters != acc.filters
                    || prev.is_active() != acc.is_active();

                if config_changed {
                    info!(
                        "Reconcile: configuration changed for account '{}' — restarting account",
                        id
                    );
                    self.stop_account(&id, false);
                    if let Err(e) = self.spawn_and_add_account(acc.clone()) {
                        error!("Reconcile: failed to respawn account '{}': {}", id, e);
                        self.register_inactive_account(acc, "failed", Some(e.to_string()));
                    }
                    updated += 1;
                } else if prev.name != acc.name || prev.api_key != acc.api_key {
                    let mut map = self.states.write().unwrap();
                    if let Some(state) = map.get_mut(&id) {
                        state.account.name = acc.name;
                        state.account.api_key = acc.api_key;
                    }
                    updated += 1;
                }
            } else {
                info!("Reconcile: starting new account '{}'", id);
                if let Err(e) = self.spawn_and_add_account(acc.clone()) {
                    error!("Reconcile: failed to start new account '{}': {}", id, e);
                    self.register_inactive_account(acc, "failed", Some(e.to_string()));
                }
                added += 1;
            }
        }

        self.reload_account_manifests();

        if let Ok(mut guard) = self.last_applied_hash.lock() {
            *guard = Some(hash);
        }
        if let Ok(mut err_guard) = self.last_error_hash.lock() {
            *err_guard = None;
        }

        Ok(ReconcileOutcome::Applied {
            added,
            removed,
            updated,
        })
    }

    /// Stop every account gracefully.
    ///
    /// Aborting the CDC task drops the event channel, which lets the writer
    /// drain its final batch and exit on its own instead of losing data.
    pub async fn shutdown(&self) {
        let mut tasks = Vec::new();
        {
            let mut map = self.states.write().unwrap();
            for state in map.values_mut() {
                if let Some(cdc) = state.cdc_task.take() {
                    cdc.abort();
                }
                if let Some(writer) = state.writer_task.take() {
                    tasks.push(writer);
                }
            }
        }

        for writer in tasks {
            if tokio::time::timeout(std::time::Duration::from_secs(5), writer)
                .await
                .is_err()
            {
                warn!("Account writer did not shut down in time.");
            }
        }
    }
}

/// Constant-time comparison for API keys.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    let max = a.len().max(b.len());
    for i in 0..max {
        let ba = a.get(i).copied().unwrap_or(0);
        let bb = b.get(i).copied().unwrap_or(0);
        diff |= (ba ^ bb) as usize;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Settings;
    use std::sync::Arc;

    #[test]
    fn test_account_persistence() {
        let temp_file = format!("test_accounts_{}.json", uuid::Uuid::new_v4());
        let settings = Arc::new(Settings::default());
        let (commit_tx, _) = broadcast::channel(16);
        let manager = AccountManager::new(temp_file.clone(), settings, commit_tx);

        // Should load empty if file doesn't exist
        let initial = manager.load_persisted();
        assert!(initial.is_empty());

        let test_account = Account {
            id: "123".into(),
            name: "Test Corp".into(),
            mysql_url: "mysql://localhost".into(),
            snapshot_mysql_url: None,
            api_key: "xk_test".into(),
            status: "active".into(),
            filters: None,
        };

        // Save account
        manager
            .save_persisted(std::slice::from_ref(&test_account))
            .unwrap();

        // Load account
        let loaded = manager.load_persisted();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "123");
        assert_eq!(loaded[0].name, "Test Corp");

        // Clean up temp file
        let _ = std::fs::remove_file(temp_file);
    }

    #[test]
    fn test_minimal_account_json_and_loud_parse_errors() {
        let minimal_json = r#"[
            { "name": "Acme", "mysql_url": "mysql://repl:secret@db1:3306/app" }
        ]"#;
        let parsed = AccountManager::parse_accounts_str(minimal_json, "accounts.json").unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "Acme");
        assert_eq!(parsed[0].id.len(), 12);
        assert!(parsed[0].api_key.starts_with("xk_"));
        assert_eq!(parsed[0].status, "active");

        // Parsing again yields the exact same deterministic id and api_key
        let parsed2 = AccountManager::parse_accounts_str(minimal_json, "accounts.json").unwrap();
        assert_eq!(parsed[0].id, parsed2[0].id);
        assert_eq!(parsed[0].api_key, parsed2[0].api_key);

        // Malformed JSON must fail loudly with line and column info
        let bad_json = "[\n  { \"name\": \"Broken\" \n]";
        let err = AccountManager::parse_accounts_str(bad_json, "accounts.json").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line"), "error should report line: {msg}");
        assert!(msg.contains("column"), "error should report column: {msg}");
    }

    #[test]
    fn test_env_secret_interpolation_and_redaction() {
        std::env::set_var(
            "XLOKA_TEST_ACME_MYSQL_URL",
            "mysql://repl:top_secret_pw@db1.internal:3306/app",
        );
        let acct = Account {
            id: "acme".into(),
            name: "Acme".into(),
            mysql_url: "${XLOKA_TEST_ACME_MYSQL_URL}".into(),
            snapshot_mysql_url: Some("${UNSET_XLOKA_REPLICA_VAR}".into()),
            api_key: "xk_1234567890abcdef".into(),
            status: "active".into(),
            filters: None,
        };

        let resolved = interpolate_env(&acct.mysql_url).unwrap();
        assert_eq!(resolved, "mysql://repl:top_secret_pw@db1.internal:3306/app");

        let redacted = acct.redacted();
        assert_eq!(redacted.mysql_url, "mysql://repl:***@db1.internal:3306/app");
        assert!(!redacted.mysql_url.contains("top_secret_pw"));
        assert!(!redacted.mysql_url.contains("XLOKA_TEST_ACME_MYSQL_URL"));
        assert_eq!(redacted.snapshot_mysql_url.as_deref(), Some("***"));
    }
}
