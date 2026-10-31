# ==============================================================================
# xloka-flow Containerfile
# High-performance MySQL CDC -> DuckDB Analytical Engine & API Daemon
#
# Uses the prebuilt release binary published to GitHub Releases.
# Override the release tag at build time with:
#   docker build --build-arg VERSION=v0.2.2 -t xloka-flow .
# ==============================================================================

FROM debian:bookworm-slim

ARG VERSION=latest
LABEL org.opencontainers.image.title="xloka-flow" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.source="https://github.com/Xloka/xloka-flow" \
    org.opencontainers.image.licenses="MIT"

# Install CA certificates, timezone data, curl (for healthchecks + download),
# and xz-utils (to extract the release archive).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    curl \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Download and install the prebuilt xloka-flow binary from GitHub Releases
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
    x86_64)  target="x86_64-unknown-linux-gnu" ;; \
    aarch64|arm64) target="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    if [ "$VERSION" = "latest" ]; then \
    url="https://github.com/Xloka/xloka-flow/releases/latest/download/xloka-flow-${target}.tar.xz"; \
    else \
    url="https://github.com/Xloka/xloka-flow/releases/download/${VERSION}/xloka-flow-${target}.tar.xz"; \
    fi; \
    curl -fsSL "$url" -o /tmp/xloka-flow.tar.xz; \
    tar -xJf /tmp/xloka-flow.tar.xz --strip-components=1 -C /usr/local/bin "xloka-flow-${target}/xloka-flow"; \
    chmod +x /usr/local/bin/xloka-flow; \
    rm -f /tmp/xloka-flow.tar.xz

# Security: Run as dedicated non-root user
RUN groupadd -g 10001 xloka && \
    useradd -u 10001 -g xloka -s /bin/sh -m xloka

WORKDIR /app

# Create persistent data and configuration directories
RUN mkdir -p /app/data && chown -R xloka:xloka /app

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
