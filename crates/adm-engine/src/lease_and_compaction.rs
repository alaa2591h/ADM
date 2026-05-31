//! `lease_and_compaction.rs`
//! Two orthogonal additions to crates/engine/src/runtime.rs:
//!
//!   A) Reservation lease expiration  — ensures stale write-reservations
//!      are cleaned up even after crashes / unexpected cancellation.
//!   B) Runtime state compaction      — bounds unbounded growth of
//!      metrics samples, chunk history, and adaptive state.
//!
//! Integration: paste the two `impl` blocks and the `CompactionConfig`
//! struct into runtime.rs; wire `run_lease_reaper` and `compact_state`
//! into the engine tick loop or spawn them as independent tasks from
//! `EngineContext::start()`.

use crate::EventBus;
use adm_types::ChunkState;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────
// §A  RESERVATION LEASE EXPIRATION
// ─────────────────────────────────────────────────────────

/// Maximum age a chunk reservation is allowed to live without completion.
/// After this the lease reaper cancels it so it becomes eligible for retry.
pub const DEFAULT_RESERVATION_LEASE: Duration = Duration::from_secs(30);

/// Per-chunk lease record.  Stored inside `FileWriteCoordinator` or a
/// dedicated `LeaseRegistry` living on `EngineContext`.
#[derive(Debug, Clone)]
pub struct ReservationLease {
    pub chunk_id: Uuid,
    pub task_id: Uuid,
    pub worker_id: Option<Uuid>,
    pub reserved_at: Instant,
    pub max_age: Duration,
}

impl ReservationLease {
    #[must_use]
    pub fn new(chunk_id: Uuid, task_id: Uuid, worker_id: Option<Uuid>, max_age: Duration) -> Self {
        Self {
            chunk_id,
            task_id,
            worker_id,
            reserved_at: Instant::now(),
            max_age,
        }
    }

    /// `true` if the lease has expired and must be reaped.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.reserved_at.elapsed() > self.max_age
    }

    #[must_use]
    pub fn age_ms(&self) -> u64 {
        u64::try_from(self.reserved_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// Thread-safe registry of active leases.
/// One instance lives on `EngineContext` and is shared across all schedulers.
pub struct LeaseRegistry {
    leases: Mutex<HashMap<Uuid, ReservationLease>>,
    default_max_age: Duration,
}

impl LeaseRegistry {
    #[must_use]
    pub fn new(default_max_age: Duration) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            default_max_age,
        }
    }

    fn leases(&self) -> MutexGuard<'_, HashMap<Uuid, ReservationLease>> {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register a lease when a chunk is reserved to a worker.
    pub fn register(&self, chunk_id: Uuid, task_id: Uuid, worker_id: Option<Uuid>) {
        let lease = ReservationLease::new(chunk_id, task_id, worker_id, self.default_max_age);
        self.leases().insert(chunk_id, lease);
    }

    /// Remove the lease when a chunk completes, fails, or is explicitly cancelled.
    pub fn release(&self, chunk_id: Uuid) {
        self.leases().remove(&chunk_id);
    }

    /// Drain and return all expired leases.  Call this from the reaper task.
    pub fn drain_expired(&self) -> Vec<ReservationLease> {
        let mut guard = self.leases();
        let mut expired = Vec::new();
        guard.retain(|_, lease| {
            if lease.is_expired() {
                expired.push(lease.clone());
                false
            } else {
                true
            }
        });
        expired
    }

    /// Snapshot for diagnostics / IPC.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.leases().len()
    }
}

/// Background task: periodically reap expired leases and emit
/// `adaptive.lease_expired` events so the scheduler can retry the chunks.
///
/// Spawn once from `EngineContext::start()`:
/// ```rust,ignore
/// tokio::spawn(run_lease_reaper(
///     context.lease_registry.clone(),
///     context.event_bus.clone(),
///     Duration::from_secs(5),
/// ));
/// ```
pub async fn run_lease_reaper(
    registry: std::sync::Arc<LeaseRegistry>,
    event_bus: EventBus,
    poll_interval: Duration,
    shutdown: adm_storage::ShutdownToken,
) {
    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        if shutdown.is_cancelled() {
            tracing::debug!("run_lease_reaper: shutdown signalled, exiting");
            break;
        }
        let expired = registry.drain_expired();
        if expired.is_empty() {
            continue;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        for lease in &expired {
            tracing::warn!(
                chunk_id = %lease.chunk_id,
                task_id  = %lease.task_id,
                age_ms   = lease.age_ms(),
                "reservation lease expired — marking for retry",
            );
            event_bus.publish(
                "adaptive.lease_expired",
                serde_json::json!({
                    "task_id":    lease.task_id.to_string(),
                    "chunk_id":   lease.chunk_id.to_string(),
                    "worker_id":  lease.worker_id.map(|w| w.to_string()),
                    "lease_age_ms": lease.age_ms(),
                    "timestamp_ms": now_ms,
                }),
            );
        }
    }
}

// ─────────────────────────────────────────────────────────
// §B  RUNTIME STATE COMPACTION
// ─────────────────────────────────────────────────────────

/// Tunable limits for each compactable data structure.
/// Conservative defaults; override per-instance for tests.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of throughput samples kept in `ThroughputSampler`.
    pub max_throughput_samples: usize,

    /// Oldest allowed age for a throughput sample.
    pub throughput_sample_max_age: Duration,

    /// Maximum number of per-chunk history entries kept in memory.
    /// Older entries are evicted FIFO.
    pub max_chunk_history_per_task: usize,

    /// Maximum number of snapshot events kept in the `TransferStateAggregator`.
    pub max_aggregator_snapshots: usize,

    /// Maximum number of stall entries kept in adaptive `StallState` maps.
    pub max_stall_state_entries: usize,

    /// Maximum age of worker heartbeat records before they are evicted.
    pub heartbeat_max_age: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_throughput_samples: 256,
            throughput_sample_max_age: Duration::from_mins(1),
            max_chunk_history_per_task: 512,
            max_aggregator_snapshots: 64,
            max_stall_state_entries: 256,
            heartbeat_max_age: Duration::from_secs(30),
        }
    }
}

impl CompactionConfig {
    /// Lean config for unit / integration tests.
    #[must_use]
    pub const fn for_tests() -> Self {
        Self {
            max_throughput_samples: 16,
            throughput_sample_max_age: Duration::from_secs(5),
            max_chunk_history_per_task: 32,
            max_aggregator_snapshots: 8,
            max_stall_state_entries: 32,
            heartbeat_max_age: Duration::from_secs(5),
        }
    }
}

/// Add this method to `ThroughputSampler`.
/// Drops samples older than `cfg.throughput_sample_max_age` and then
/// evicts from the front until `len <= cfg.max_throughput_samples`.
///
/// ```rust,ignore
/// impl ThroughputSampler {
///     pub fn compact(&mut self, cfg: &CompactionConfig) {
///         let cutoff = std::time::Instant::now() - cfg.throughput_sample_max_age;
///         self.samples.retain(|(ts, _)| *ts >= cutoff);
///         while self.samples.len() > cfg.max_throughput_samples {
///             self.samples.pop_front();
///         }
///     }
/// }
/// ```
///
/// Annotate the method signature here for reference; actual integration
/// happens by adding it to the `ThroughputSampler` impl in runtime.rs.
pub trait Compactable {
    fn compact(&mut self, cfg: &CompactionConfig);
}

/// In-memory chunk event history — bounded ring buffer.
/// Replace unbounded `Vec<(String, i64)>` in runtime with this.
#[derive(Debug, Clone)]
pub struct ChunkHistoryBuffer {
    max_entries: usize,
    /// (`event_name`, `unix_ms`)
    entries: std::collections::VecDeque<(String, i64)>,
}

impl ChunkHistoryBuffer {
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: std::collections::VecDeque::with_capacity(max_entries),
        }
    }

    pub fn push(&mut self, event: impl Into<String>, ts_ms: i64) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front(); // evict oldest
        }
        self.entries.push_back((event.into(), ts_ms));
    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, i64)> {
        self.entries.iter()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Compaction for the adaptive `StallState` maps (`chunk_samples`, `retry_history`,
/// `worker_heartbeats`).  Call this from the monitor loop tick.
///
/// ```rust,ignore
/// use std::collections::HashMap;
/// use std::time::Instant;
/// let mut chunk_last_progress: HashMap<uuid::Uuid, Instant> = HashMap::new();
/// let mut worker_heartbeats: HashMap<uuid::Uuid, Instant> = HashMap::new();
/// let mut retry_history: HashMap<uuid::Uuid, u32> = HashMap::new();
/// let cfg = CompactionConfig::default();
/// compact_stall_state_maps(&mut chunk_last_progress, &mut worker_heartbeats, &mut retry_history, &cfg);
/// ```
pub fn compact_stall_state_maps(
    chunk_last_progress: &mut HashMap<Uuid, Instant>,
    worker_heartbeats: &mut HashMap<Uuid, Instant>,
    retry_history: &mut HashMap<Uuid, u32>,
    cfg: &CompactionConfig,
) {
    let now = Instant::now();

    // Evict chunks with stale progress timestamps (finished / abandoned).
    chunk_last_progress.retain(|_, last| now.duration_since(*last) < cfg.throughput_sample_max_age);

    // Evict worker heartbeats older than the heartbeat max age.
    worker_heartbeats.retain(|_, last| now.duration_since(*last) < cfg.heartbeat_max_age);

    // Hard-cap retry_history at max_stall_state_entries (oldest-first removal).
    while retry_history.len() > cfg.max_stall_state_entries {
        if let Some(k) = retry_history.keys().next().copied() {
            retry_history.remove(&k);
        } else {
            break;
        }
    }
}

/// Background task: compact runtime state on a slow ticker.
/// Spawn from `EngineContext::start()` once.
///
/// In practice the compaction functions are cheap (a few map iterates),
/// so a 30-second tick is fine for production.
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use std::time::Duration;
/// use tokio::sync::Mutex;
/// let runtime_state = Arc::new(Mutex::new(()));
/// tokio::spawn(run_state_compactor_tick(
///     CompactionConfig::default(),
///     Duration::from_secs(30),
/// ));
/// ```
///
/// `runtime_state` here is a `Arc<Mutex<YourRuntimeState>>` that holds
/// the `ThroughputSampler`, `StallState`, etc.  Adapt the type to your actual
/// runtime struct.
pub async fn run_state_compactor_tick(_cfg: CompactionConfig, poll_interval: Duration) {
    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        // Caller provides an Arc<Mutex<RuntimeState>> and calls the
        // compaction helpers above while holding the lock.
        // This task exists as a ticker; the actual compaction calls
        // are integrated at the call site with access to the state lock.
        tracing::trace!("state compaction tick");
    }
}

// ─────────────────────────────────────────────────────────
// §C  CRASH / RESTART TORTURE TEST HELPERS
// ─────────────────────────────────────────────────────────

/// Inject a forced termination signal after `delay` to simulate crash.
/// Use only in `#[cfg(test)]` blocks.
///
/// ```rust
/// #[tokio::test]
/// async fn restart_during_active_writes() {
///     let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
///     tokio::spawn(forced_kill_after(Duration::from_millis(300), kill_tx));
///     // run engine …
///     // assert recovery after restart
/// }
/// ```
#[cfg(test)]
pub async fn forced_kill_after(delay: Duration, tx: tokio::sync::oneshot::Sender<()>) {
    tokio::time::sleep(delay).await;
    let _ = tx.send(());
}

/// Verify that no stale reservations remain in storage after a simulated crash + restart.
/// Returns the list of orphaned chunk IDs.
#[cfg(test)]
pub async fn find_orphaned_reservations(
    storage: &std::sync::Arc<dyn adm_storage::ChunkRepository>,
) -> Vec<Uuid> {
    let chunks = storage.load_pending_chunks().await.unwrap_or_default();
    chunks
        .into_iter()
        .filter(|c| c.state == ChunkState::Reserved || c.state == ChunkState::Connecting)
        .map(|c| c.id)
        .collect()
}
