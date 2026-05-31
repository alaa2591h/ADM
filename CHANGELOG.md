# Changelog

All notable changes to APEX Download Manager are documented here.

## [Unreleased]

### Fixed
- Permanently removed the CLI component (`apps/adm-cli`) from the project architecture and documentation
- Removed duplicate `apps/ws-gateway` skeleton (WebSocket gateway is inlined in `apps/daemon`)
- Removed duplicate `apps/headless-engine` binary (superseded by `apps/server`)
- Removed thin-wrapper crates `models`, `utils`, `i18n` (re-exports of `adm-core` with no added value)
- Removed temporary `scripts/` directory (auto-fix and CI scripts not suitable for production)
- Removed duplicate `apps/desktop-ui/src/app_icon.ico` (canonical copy lives in `installer/assets/`)
- Removed `#[allow(dead_code)]` from `apps/daemon/src/main.rs`
- Removed dead `load_client_config()` from daemon (logic lives in `engine-bridge`)

### Changed
- Renamed `core` crate to `adm-core` (avoids collision with Rust's built-in `core`)
- Renamed `engine` → `adm-engine`, `storage` → `adm-storage`, `network` → `adm-network`
- Renamed `application` → `adm-application`, `messaging` → `adm-messaging`
- Renamed `domain` → `adm-domain`, `cache` → `adm-cache`, `commands` → `adm-commands`
- Renamed `queries` → `adm-queries`, `events` → `adm-events`, `observability` → `adm-observability`
- `engine.status` IPC method now reports `worker_count` from the live `WorkerPool` instead of a hardcoded `4`

### Added
- `README.md` with architecture overview, build instructions, and environment variable reference
- `CHANGELOG.md`
- `TaskSupervisor::recover_crashed_tasks()` is now wired into `main()` on daemon startup — tasks interrupted by a crash are automatically re-queued

## [0.1.0] — Initial release
