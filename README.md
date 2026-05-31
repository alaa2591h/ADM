# APEX Download Manager (ADM)

A high-performance, cross-platform download manager built in Rust. ADM supports HTTP/S, FTP, SFTP, S3, WebDAV, BitTorrent, and media extraction (HLS, DASH, YouTube, Twitch, and more).

---

## Architecture

ADM is a Rust workspace with a Clean Architecture layout:

```
ADM/
├── apps/                    # Runnable binaries
│   ├── daemon/              # Core daemon — composition root, JSON-RPC over WebSocket
│   ├── server/              # Headless REST/WebSocket/SSE API server
│   ├── desktop-ui/          # Slint-based desktop GUI
│   └── native-host/         # Browser extension native messaging host
│
└── crates/                  # Library crates
    ├── adm-core/            # Shared models, utilities, i18n strings
    ├── adm-engine/          # Core download logic & event bus
    ├── adm-network/         # HTTP/3, FTP, SFTP, S3, WebDAV, Torrent clients
    ├── adm-storage/         # SQLite persistence (tasks, chunks, history)
    ├── adm-cache/           # In-memory caching layer
    ├── adm-observability/   # Metrics, tracing, runtime snapshots
    ├── adm-runtime/         # Scheduler diagnostics bridge
    ├── adm-gateway/         # Axum REST + SSE + WebSocket gateway
    ├── extractor/           # Media extractors (HLS, DASH, YouTube, etc.)
    ├── ipc/                 # JSON-RPC contract types
    ├── jsonrpc/             # JSON-RPC 2.0 dispatcher
    ├── settings-core/       # Settings manager (load/save/watch)
    ├── settings-schema/     # Settings data structures
    ├── config-storage/      # Persistent config store
    └── utils/               # Common helpers & validation
```

---

## Building

**Prerequisites:** Rust stable (see `rust-toolchain.toml`), SQLite (bundled via `rusqlite`).

```bash
# Build everything
cargo build --workspace

# Build only the daemon
cargo build -p adm-daemon

# Release build
cargo build --workspace --release
```

---

## Running

### Daemon (primary IPC backend)

```bash
# Default — listens on ws://127.0.0.1:9001 (WS) and http://127.0.0.1:57423 (REST)
cargo run -p adm-daemon

# With custom bind addresses
WS_BIND=127.0.0.1:9001 API_BIND=127.0.0.1:57423 cargo run -p adm-daemon

# With API authentication
API_TOKEN=my-secret-token cargo run -p adm-daemon
```

### Headless server (REST/WebSocket/SSE only)

```bash
# Default — listens on http://0.0.0.0:57423
cargo run -p adm-server

# Custom bind
ADM_BIND=0.0.0.0:8080 cargo run -p adm-server
```

### Desktop UI

```bash
cargo run -p adm-ui
```

---

## Environment Variables

| Variable         | Default              | Description                                      |
|------------------|----------------------|--------------------------------------------------|
| `WS_BIND`        | `127.0.0.1:9001`     | WebSocket gateway bind address (daemon)          |
| `API_BIND`       | `127.0.0.1:57423`    | REST/WebSocket API bind address (daemon)         |
| `ADM_BIND`       | `0.0.0.0:57423`      | API bind address (server binary)                 |
| `API_TOKEN`      | *(none)*             | Bearer token for REST API authentication         |
| `DATABASE_PATH`  | `adm.db`             | SQLite database file path                        |
| `SETTINGS_PATH`  | `settings.toml`      | Settings file path                               |
| `DOWNLOAD_PATH`  | *(home dir)*         | Default download directory                       |
| `ADM_LOG`        | `info`               | Tracing filter (e.g. `debug`, `adm_engine=trace`)|

---

## Testing

```bash
# Run all tests
cargo test --workspace

# Run a specific crate's tests
cargo test -p adm-engine

# Run integration tests
cargo test -p adm-daemon
```

---

## License

APEX Download Manager is licensed under the [Business Source License 1.1](LICENSE).  
© APEX Download Manager Team
