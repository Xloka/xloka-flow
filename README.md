<div align="center">
  <h1>xloka-flow 🌊</h1>
  <p><strong>Zero-Bloat Database Replication & Analytical Engine</strong></p>
  <p>Replicate transactional data to a local analytical engine in 30 seconds. No heavy JVMs. No message queues. Just pure Rust performance.</p>
</div>

---

**xloka-flow** is a high-performance Rust daemon that offloads analytical queries from OLTP databases (MySQL) to an embedded OLAP engine (DuckDB) via real-time Change Data Capture (CDC), exposing concurrent analytical queries over HTTP REST and real-time WebSockets.

It is built for developers who require enterprise-grade data synchronization but refuse to tolerate enterprise bloat. It runs as a single, statically compiled binary utilizing a fraction of the memory footprint of traditional replication systems.

## Why xloka-flow?
- **Raw Speed**: Written in pure Rust with primary-key batch coalescing and native DuckDB `Appender` bulk ingestion.
- **Local-First Architecture**: Data flows directly from your transactional database to a local analytical file on your machine or server. It never transits through third-party proprietary clouds.
- **Zero OLTP Load**: By querying the analytical replica locally, your production transactional database experiences 0% analytical read load.
- **Real-Time Push Streaming**: Multiplexed WebSocket subscriptions push live analytical query snapshots and row-level table deltas as transactions commit.

---

## Quick Start (CLI Mode)

The **Sync** mode is a CLI tool that pipes binary logs directly into a local analytical database file.

### 1. Install

**Via Cargo (Recommended for Rust Developers)**
```bash
cargo install xloka-flow
```

**Pre-compiled Binaries (No Rust required)**

*Linux (x86_64)*
```bash
curl -LsSf https://github.com/xloka/xloka-flow/releases/latest/download/xloka-flow-x86_64-unknown-linux-gnu.tar.xz | tar -xJ && sudo mv xloka-flow /usr/local/bin/
```

*macOS (Apple Silicon)*
```bash
curl -LsSf https://github.com/xloka/xloka-flow/releases/latest/download/xloka-flow-aarch64-apple-darwin.tar.xz | tar -xJ && sudo mv xloka-flow /usr/local/bin/
```

*macOS (Intel)*
```bash
curl -LsSf https://github.com/xloka/xloka-flow/releases/latest/download/xloka-flow-x86_64-apple-darwin.tar.xz | tar -xJ && sudo mv xloka-flow /usr/local/bin/
```

*Windows (x86_64)*
```powershell
Invoke-WebRequest https://github.com/xloka/xloka-flow/releases/latest/download/xloka-flow-x86_64-pc-windows-msvc.zip -OutFile xloka-flow.zip; Expand-Archive xloka-flow.zip; Move-Item xloka-flow\xloka-flow.exe $env:USERPROFILE\.cargo\bin\
```

**Via Docker**
```bash
docker compose up -d
```

### 2. Run
```bash
xloka-flow sync \
  --mysql "mysql://user:pass@host:3306/mydb" \
  --db "analytics.db" \
  --tables "orders,users" \
  --exclude-tables "audit_logs"

# Optional: Offload the initial consistent-snapshot backfill onto a Read Replica!
xloka-flow sync \
  --mysql "mysql://user:pass@primary:3306/mydb" \
  --snapshot-mysql "mysql://user:pass@read-replica:3306/mydb" \
  --db "analytics.db"
```

That is it. The daemon will continuously replicate your transactional database into `analytics.db` until the process is terminated. You can immediately open `analytics.db` in your preferred SQL client and execute analytical queries.

### Database Prerequisites
Ensure MySQL has row-based binary logging enabled (`binlog_format = ROW`, `binlog_row_image = FULL`) and grant replication permissions to your user:
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'youruser'@'%';
```

---

## Server Mode

If you are running a multi-tenant application or want to expose your replicas via a secure HTTP REST and WebSocket API, utilize the **Serve** mode:

```bash
xloka-flow serve \
  --db "analytics.db" \
  --queries "flow_queries.toml.example" \
  --admin-token "your_secret" \
  --allowed-domains "https://my-dashboard.com"
```

For local testing without MySQL, run with `--mock` to enable the built-in synthetic CDC generator:
```bash
xloka-flow serve --mock --db "analytics.db" --queries "flow_queries.toml.example"
```

### Querying Analytical Endpoints
Named read-only SQL queries defined in your `flow_queries.toml` manifest are executed concurrently across isolated read-only DuckDB handles:

```bash
# JSON output (default)
curl -s "http://localhost:3000/api/v1/query/account_summary"

# Newline-Delimited JSON (NDJSON) or CSV output
curl -s "http://localhost:3000/api/v1/query/account_summary?format=ndjson"
curl -s "http://localhost:3000/api/v1/query/account_summary?format=csv"

# Reject with HTTP 425 if replication lag exceeds 500ms
curl -s "http://localhost:3000/api/v1/query/account_summary?max_staleness_ms=500"

# Target a specific replicated account via query param, x-api-key, or Authorization: Bearer
curl -s "http://localhost:3000/api/v1/query/account_summary?account_id=<id>"
curl -s -H "x-api-key: xk_..." "http://localhost:3000/api/v1/query/account_summary"
curl -s -H "Authorization: Bearer xk_..." "http://localhost:3000/api/v1/query/account_summary"

# Ad-hoc read-only SQL (v0.1.0 compatible POST /api/query or POST /api/v1/query)
curl -s -X POST "http://localhost:3000/api/query" \
  -H "Authorization: Bearer xk_..." \
  -d "SELECT COUNT(*) AS total FROM orders;"
```

### Real-Time WebSocket Streaming
Connect to `ws://localhost:3000/api/v1/ws` (or `/ws`; pass `?api_key=...` or `x-api-key` when data-plane authentication is enabled):

```json
{"type": "subscribe_query", "sub_id": "sub_1", "query_name": "account_summary", "throttle_ms": 100}
{"type": "subscribe_table_deltas", "sub_id": "sub_2", "tables": ["users", "transactions"]}
```

### Observability & Health
- `GET /health` — JSON health check (`status`, `version`, `timestamp`, `replication_lag_ms`, `active_ws_connections`, `batches_committed`, `writer_halted`).
- `GET /metrics` — Prometheus text exposition metrics.
- `GET /api/admin/accounts` — Per-account replication health (`status`, `lag_ms`, `last_error`, `started_at`, `rows_synced`, `tables`, `db_size_bytes`, and `filters`).

### Hosted Multi-Account & Scoped Replication (`accounts.json`)

For hosted deployments (Railway, Fly, Render, Kubernetes, or a single VM), mount or inject `accounts.json` and enable hot-reloading (`XLOKA__ACCOUNTS_RELOAD_SECS=5`, `XLOKA__ACCOUNTS_READ_ONLY=true`). Each account replicates into its own isolated `data/<id>.duckdb` file with bounded memory (`XLOKA__ACCOUNT_MEMORY_LIMIT="256MB"`, `XLOKA__DUCKDB_THREADS=2`), secret `${ENV_NAME}` interpolation, and optional table/column scoping:

```json
[
  {
    "id": "acc_acme",
    "name": "Acme Production",
    "mysql_url": "mysql://rep:${ACME_MYSQL_PW}@db.acme.internal:3306/shop",
    "snapshot_mysql_url": "mysql://rep:${ACME_MYSQL_PW}@replica.acme.internal:3306/shop",
    "api_key": "xk_live_acme",
    "status": "active",
    "filters": {
      "table_include": ["orders", "users"],
      "table_exclude": ["audit_logs"],
      "columns": {
        "users": {
          "exclude": ["password_hash", "reset_token", "totp_secret"]
        },
        "orders": {
          "include": ["id", "user_id", "total_cents", "status", "created_at"]
        }
      }
    }
  }
]
```

- **Sizing rule of thumb:** `RAM ≈ 150 MB base + N_accounts × account_memory_limit` (e.g., 8 accounts × `256MB` fits comfortably inside a 2 GB container).
- **Per-account query manifests:** Drop `queries/<account_id>.toml` (or set `XLOKA__QUERY_MANIFEST_DIR`) to give each tenant its own analytical SQL endpoints alongside the global manifest.

### Technical Details & Consistent Snapshot Backfill

- **Zero-Lock Initial Snapshot (`CONSISTENT SNAPSHOT`)**: On first run (when no `_xloka_checkpoint` entry exists yet), `xloka-flow` opens a `START TRANSACTION WITH CONSISTENT SNAPSHOT`, records the exact `(File, Position)` coordinates via `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS`, streams the initial backfill via DuckDB `COPY ... FROM (FORMAT CSV)`, and commits the initial checkpoint atomically. Passing `--snapshot-mysql` (or `snapshot_mysql_url` per account) offloads this entire snapshot phase to a read-only MySQL replica so your primary experiences zero backfill load.
- **Row-Level Binlog CDC & Primary-Key Coalescing**: After the snapshot completes, `xloka-flow` connects to the binary log stream at the recorded coordinates and coalesces `INSERT`, `UPDATE` (including chained primary-key updates), and `DELETE` events per primary key before flushing via DuckDB transactions and native `Appender` batches.

### Upgrading from v0.1.0 to v0.2.x (Migration Notes)

1. **Query Endpoints (`POST /api/query` + Declarative `GET /api/v1/query/<name>`)**:
   - `v0.1.0`'s ad-hoc `POST /api/query` (and `POST /api/v1/query`) accepting raw `SELECT` SQL or `{"sql": "SELECT ..."}` with `Authorization: Bearer xk_...` remains **fully supported**.
   - `v0.2.x` adds declarative named queries (`GET /api/v1/query/<name>`) defined in `flow_queries.toml` (with JSON, NDJSON, and CSV format negotiation and `?max_staleness_ms=` guards) plus real-time WebSocket subscriptions (`/api/v1/ws`).
2. **Authentication Headers**:
   - All data-plane endpoints (`/api/query`, `/api/v1/query/*`, `/api/v1/ws`) accept **`Authorization: Bearer <key>`**, **`x-api-key: <key>`**, or **`?api_key=<key>`**.
   - Admin endpoints (`/api/admin/*`) accept `Authorization: Bearer <admin_token>` or `x-admin-token: <admin_token>`, and now **fail closed** (`503 Service Unavailable`) if `XLOKA__ADMIN_TOKEN` / `--admin-token` is unset (whereas `v0.1.0` fell back to `"secret"`).

### Deploying the Server
Because the daemon is compiled into a single static binary, deployment is straightforward with `systemd` or Docker:

```ini
[Unit]
Description=xloka-flow daemon
After=network.target

[Service]
ExecStart=/usr/local/bin/xloka-flow serve --db /var/lib/xloka-flow/analytics.db --queries /etc/xloka-flow/flow_queries.toml
Environment="XLOKA__ADMIN_TOKEN=your_secure_token"
Restart=always
User=xloka

[Install]
WantedBy=multi-user.target
```

---

## License

This project is licensed under the **MIT License**.

You are free to use, modify, distribute, and build commercial products on top of this software without any restrictions. We believe in true open-source freedom.

See the [LICENSE](LICENSE) file for details.
