# ==============================================================================
# xloka-flow Multi-Stage Containerfile
# High-performance MySQL CDC -> DuckDB Analytical Engine & API Daemon
# ==============================================================================

# ── Stage 1: Build binary ─────────────────────────────────────────────────────
# The MSRV is dictated by the locked dependency graph (e.g. `time 0.3.55` needs
# 1.88, `duckdb`/`arrow` need 1.85). Bumping this without bumping the lock file
# breaks the image build.
FROM rust:1.88-bookworm AS builder
WORKDIR /app

# Install build prerequisites for DuckDB bundled C++ compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests and build script
COPY Cargo.toml Cargo.lock build.rs ./

# Create dummy source tree to cache compiled dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    cargo build --release --bin xloka-flow && \
    rm -rf src target/release/deps/xloka_flow* target/release/deps/libxloka_flow* target/release/.fingerprint/xloka-flow* target/release/xloka-flow*

# Copy real source code
COPY src ./src

# Build the release binary with LTO
RUN cargo build --release --bin xloka-flow && \
    strip target/release/xloka-flow

# ── Stage 2: Minimal Runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Install CA certificates for TLS MySQL/HTTPS connections and curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Security: Run as dedicated non-root user
RUN groupadd -g 10001 xloka && \
    useradd -u 10001 -g xloka -s /bin/sh -m xloka

WORKDIR /app

# Create persistent data and configuration directories
RUN mkdir -p /app/data && chown -R xloka:xloka /app

# Copy compiled binary from builder
COPY --from=builder /app/target/release/xloka-flow /usr/local/bin/xloka-flow

# Copy default query manifest example
COPY flow_queries.toml.example /app/flow_queries.toml.example

USER xloka:xloka

# Expose API and WebSocket port
EXPOSE 3000

# Volume for DuckDB database files and account state persistence
VOLUME ["/app/data"]

# Standard container healthcheck against xloka-flow /health endpoint
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://127.0.0.1:3000/health || exit 1

ENTRYPOINT ["/usr/local/bin/xloka-flow"]
CMD ["serve", "--db", "/app/data/xloka-flow.db", "--queries", "/app/flow_queries.toml.example"]
