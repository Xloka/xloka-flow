<div align="center">
  <h1>xloka-flow 🌊</h1>
  <p><strong>Zero-Bloat Database Replication & Analytical Engine</strong></p>
  <p>Replicate transactional data to a local analytical engine in 30 seconds. No heavy JVMs. No message queues. Just pure Rust performance.</p>
</div>

---

**xloka-flow** is a high-performance Rust daemon that offloads analytical queries from OLTP databases to an embedded OLAP engine via real-time Change Data Capture (CDC). 

It is built for developers who require enterprise-grade data synchronization but refuse to tolerate enterprise bloat. It runs as a single, statically compiled binary utilizing a fraction of the memory footprint of traditional replication systems.

## Why xloka-flow?
- **Raw Speed**: Written in pure Rust. It operates with minimal latency and minimal memory overhead.
- **Local-First Architecture**: Data flows directly from your transactional database to a local analytical file on your machine or server. It never transits through third-party proprietary clouds.
- **Zero OLTP Load**: By querying the analytical replica locally, your production transactional database experiences 0% analytical read load.

---

## 🚀 Quick Start (CLI Mode)

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

### 2. Run
```bash
xloka-flow sync --mysql "mysql://user:pass@host:3306/mydb" --db "analytics.db"

# Optional: Safely execute the initial snapshot backfill against a Read Replica!
# xloka-flow sync --mysql "mysql://master..." --snapshot-mysql "mysql://replica..." --db "analytics.db"
```

That is it. The daemon will continuously replicate your transactional database into `analytics.db` until the process is terminated. You can immediately open `analytics.db` in your preferred SQL client and execute massive analytical queries.

### Database Prerequisites
Your database user must have replication permissions:
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'youruser'@'%';
```

---

## 🏢 Server Mode

If you are running a Multi-Tenant application or want to expose your replicas via a secure HTTP REST API, utilize the **Serve** mode.

```bash
xloka-flow serve --admin-token "your_secret" --allowed-domains "https://my-dashboard.com"
```

### Features:
- Bootstraps a high-performance web server.
- Loads multiple isolated database environments from an `accounts.json` manifest.
- Exposes secure, authenticated `/api/query` endpoints.
- Configurable CORS origin allow-lists for secure web dashboard integrations.

### Authentication & Tokens

In Server Mode, security and isolation are handled by two distinct types of tokens:

1. **Admin Token**: Required to perform administrative tasks, such as creating or deleting isolated database accounts. You define this token yourself using the `--admin-token` CLI flag or by setting the `XLOKA__ADMIN_TOKEN` environment variable.
2. **Client API Key**: Required to execute SQL queries. When you create a new account via the `/api/admin/accounts` endpoint using your Admin Token, the server automatically generates a secure, unique API key (starting with `xk_`) and returns it in the JSON response.

To query a specific database, the client simply provides their assigned API key in the `Authorization` header:
```http
POST /api/query
Authorization: Bearer xk_your_client_api_key_here

SELECT COUNT(*) FROM your_table;
```

### Deploying the Server
Because the daemon is compiled into a single static binary, deployment is exceptionally simple. We recommend using `systemd` on your Linux server:

```ini
[Unit]
Description=xloka-flow daemon
After=network.target

[Service]
ExecStart=/usr/local/bin/xloka-flow serve
Environment="XLOKA__ADMIN_TOKEN=your_secure_token"
Restart=always
User=xloka

[Install]
WantedBy=multi-user.target
```
---

## Technical Details

- **Initial Snapshot Sync**: When running for the very first time, the daemon executes a `CONSISTENT SNAPSHOT` transaction to seamlessly backfill existing database records into the analytical engine without locking your production tables.
- **CDC Engine**: Once the snapshot completes, it instantly transitions to utilizing native binary log protocols to stream exact real-time row-level inserts, updates, and deletes natively.
- **Analytical Appender**: It batches and flushes operations via an asynchronous MPSC channel, ensuring maximum I/O throughput and consistency.

## License

This project is licensed under the **MIT License**. 

You are free to use, modify, distribute, and build commercial products on top of this software without any restrictions. We believe in true open-source freedom.

See the [LICENSE](LICENSE) file for details.
