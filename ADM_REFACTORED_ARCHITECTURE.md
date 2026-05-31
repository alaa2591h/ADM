# ADM Refactor Architecture

## Final Simplified Architecture

The new ADM architecture consolidates the download manager into a small number of crates and a single server binary.

### Target workspace structure

```
adm/
├── Cargo.toml
├── ADM_REFACTORED_ARCHITECTURE.md
├── crates/
│   ├── adm-types/          # shared types, DTOs, event enum, config
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── config.rs
│   │       ├── download.rs
│   │       └── event.rs
│   ├── adm-storage/        # SQLite persistence, task/chunk repositories, settings storage
│   ├── adm-network/        # protocol clients and network abstractions
│   ├── adm-engine/         # core download engine and scheduler
│   ├── adm-extractor/      # media extractor modules
│   ├── adm-cache/          # in-memory TTL cache
│   └── adm-gateway/        # Axum REST + WebSocket API
├── apps/
│   ├── adm-server/         # single production binary
│   │   └── src/main.rs
│   └── desktop-ui/         # Slint desktop UI (unchanged)
```

## Before / After Comparison

### Before

- 27 workspace crates
- CQRS with `CommandBus` and `QueryBus`
- `MessagingHub`, `EventBus`, `ProgressBus`, `ipc`, `jsonrpc`
- Multiple composition wrappers: `headless-core`, `engine-bridge`, `application`
- Two separate server binaries: `apps/daemon` and `apps/server`
- Domain crate with unrelated security/audit modules

### After

- 7 library crates + 2 binaries + UI app
- Direct engine API surface with `DownloadEngine`
- No command/query/message buses for internal coordination
- `broadcast::Sender` for push notifications when needed
- Single production binary: `apps/adm-server`
- `adm-types` owns shared DTOs, events, and configuration
- `adm-storage` owns SQLite persistence and settings storage

## Core principles

- direct calls over message buses
- one process, one binary
- async channels only where notification is required
- state owned by engine with `Arc` handles exposed to handlers
- modules over crates: only true isolation stays as crates

## Key simplified components

- `adm-types`
- `adm-storage`
- `adm-network`
- `adm-engine`
- `adm-extractor`
- `adm-cache`
- `adm-gateway`
- `apps/adm-server`
- `apps/desktop-ui`

## Architecture diagram

```
Client
  │
  ▼
[adm-gateway]  -- direct REST/WebSocket API -->  [adm-engine]
                        │                              │
                        │                              └─ [adm-storage] SQLite state
                        │
                        └─ broadcast::Sender -> websocket / SSE
```

## Implementation decision summary

- `crates/api-gateway` renamed to `crates/adm-gateway`
- `crates/storage` renamed to `crates/adm-storage`
- `crates/network` renamed to `crates/adm-network`
- `crates/engine` renamed to `crates/adm-engine`
- `crates/extractor` renamed to `crates/adm-extractor`
- `crates/cache` renamed to `crates/adm-cache`
- new shared crate: `crates/adm-types`
- `apps/server` renamed to `apps/adm-server`
- root `Cargo.toml` workspace now targets the simplified ADM architecture

## Notes

This refactor is the architectural foundation for the IDM-style engine:
- direct engine methods instead of runtime-typed command/query buses
- event enum and broadcast channels replace topic-based EventBus
- simplified workspace membership removes the old CQRS/messaging crates from the active workspace

Further migration steps should focus on moving business logic from `application`, `messaging`, `ipc`, `jsonrpc`, and `headless-core` into the new `adm-engine` / `adm-gateway` surfaces.
