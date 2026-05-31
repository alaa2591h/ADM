//! Segmented download runtime: orchestration, coordination, and state management.

use crate::lease_and_compaction::ChunkHistoryBuffer;
use crate::ChunkState;
use crate::EventBus;
use adm_storage::{PersistedWriteRange, PersistedWriteReservation, SnapshotRepository, Storage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;

/// Authority registry for all active download tasks currently managed by the runtime.
///
/// Unlike the `QueueManager`'s legacy `active_tasks` list, the `ActiveTaskRegistry`
/// provides granular access to each task's `CancellationToken` and `JoinHandle`,
/// enabling precise pause/cancel operations and preventing "zombie tasks".
#[derive(Clone, Default)]
pub struct ActiveTaskRegistry {
    tasks: Arc<RwLock<HashMap<Uuid, Arc<TaskHandle>>>>,
}

impl ActiveTaskRegistry {
    pub async fn register(&self, handle: Arc<TaskHandle>) {
        let mut tasks = self.tasks.write().await;
        tasks.insert(handle.task_id, handle);
    }

    pub async fn unregister(&self, task_id: Uuid) {
        let mut tasks = self.tasks.write().await;
        tasks.remove(&task_id);
    }

    pub async fn get(&self, task_id: Uuid) -> Option<Arc<TaskHandle>> {
        let tasks = self.tasks.read().await;
        tasks.get(&task_id).cloned()
    }

    pub async fn cancel(&self, task_id: Uuid) -> bool {
        if let Some(handle) = self.get(task_id).await {
            handle.cancel();
            true
        } else {
            false
        }
    }

    pub async fn list_ids(&self) -> Vec<Uuid> {
        let tasks = self.tasks.read().await;
        tasks.keys().copied().collect()
    }

    pub async fn clear(&self) {
        let mut tasks = self.tasks.write().await;
        tasks.clear();
    }

    pub async fn collect_snapshots(&self) -> Vec<crate::TaskScheduleSnapshot> {
        let tasks = self.tasks.read().await;
        let mut snapshots = Vec::new();
        for handle in tasks.values() {
            let status = handle.status.read().await;
            snapshots.push(status.clone());
        }
        snapshots
    }

    pub async fn get_task_status(&self, task_id: Uuid) -> Option<crate::TaskScheduleSnapshot> {
        let tasks = self.tasks.read().await;
        if let Some(handle) = tasks.get(&task_id) {
            let status = handle.status.read().await;
            Some(status.clone())
        } else {
            None
        }
    }
}

/// A handle to an active `schedule_task` loop.
pub struct TaskHandle {
    pub task_id: Uuid,
    /// Monotonic generation ID to distinguish between different runs of the
    /// same task ID (e.g. after pause/resume).
    pub generation: u64,
    pub cancel_token: ::adm_network::CancellationToken,
    /// The JoinHandle of the `schedule_task` spawn.
    pub join_handle: tokio::sync::Mutex<Option<JoinHandle<anyhow::Result<()>>>>,
    /// Live snapshot of the task's schedule state (pending chunks, active assignments).
    pub status: Arc<RwLock<crate::TaskScheduleSnapshot>>,
}

impl TaskHandle {
    pub fn new(
        task_id: Uuid,
        generation: u64,
        cancel_token: ::adm_network::CancellationToken,
    ) -> Self {
        Self {
            task_id,
            generation,
            cancel_token,
            join_handle: tokio::sync::Mutex::new(None),
            status: Arc::new(RwLock::new(crate::TaskScheduleSnapshot {
                task_id,
                pending_chunks: Vec::new(),
                active_assignments: Vec::new(),
            })),
        }
    }

    pub fn cancel(&self) {
        tracing::info!(task_id = %self.task_id, generation = self.generation, "cancelling task via ActiveTaskRegistry");
        self.cancel_token.cancel();
    }

    pub async fn set_handle(&self, handle: JoinHandle<anyhow::Result<()>>) {
        let mut lock = self.join_handle.lock().await;
        *lock = Some(handle);
    }

    pub async fn update_status(&self, snapshot: crate::TaskScheduleSnapshot) {
        let mut lock = self.status.write().await;
        *lock = snapshot;
    }
}

/// Transfer state graph representing current download state
#[derive(Debug, Clone)]
pub struct TransferGraph {
    pub task_id: Uuid,
    pub chunks: HashMap<Uuid, ChunkLease>,
    pub active_workers: HashMap<Uuid, WorkerAssignment>,
    pub transfer_stats: TransferSnapshot,
    pub history: ChunkHistoryBuffer,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl TransferGraph {
    #[must_use]
    pub fn new(task_id: Uuid) -> Self {
        let now = SystemTime::now();
        Self {
            task_id,
            chunks: HashMap::new(),
            active_workers: HashMap::new(),
            transfer_stats: TransferSnapshot::new(task_id),
            history: ChunkHistoryBuffer::new(256),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn add_chunk(&mut self, chunk: ChunkLease) {
        self.chunks.insert(chunk.chunk_id, chunk);
        self.history.push(
            "chunk.added",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        self.updated_at = SystemTime::now();
    }

    pub fn assign_worker(&mut self, chunk_id: Uuid, assignment: WorkerAssignment) {
        self.history.push(
            "chunk.assigned",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        let worker_id = assignment.worker_id;
        self.active_workers.insert(worker_id, assignment);
        if let Some(lease) = self.chunks.get_mut(&chunk_id) {
            lease.assigned_worker = Some(worker_id);
            lease.assigned_at = Some(SystemTime::now());
        }
        self.updated_at = SystemTime::now();
    }

    pub fn mark_chunk_complete(&mut self, chunk_id: Uuid) -> Result<(), TransferError> {
        if let Some(lease) = self.chunks.get_mut(&chunk_id) {
            lease.state = ChunkState::Completed;
            lease.completed_at = Some(SystemTime::now());
            self.transfer_stats.completed_chunks += 1;
            self.updated_at = SystemTime::now();
            Ok(())
        } else {
            Err(TransferError::ChunkNotFound(chunk_id))
        }
    }

    pub fn update_chunk_progress(
        &mut self,
        chunk: &crate::DownloadChunk,
    ) -> Result<(), TransferError> {
        if let Some(lease) = self.chunks.get_mut(&chunk.id) {
            lease.downloaded_bytes = chunk.downloaded_bytes;
            lease.state = chunk.state.clone();
            lease.retry_count = chunk.retry_attempts;
            self.history.push(
                format!("chunk.{}", chunk.state.as_str()),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            );
            self.transfer_stats.downloaded_bytes =
                self.chunks.values().map(|c| c.downloaded_bytes).sum();
            self.transfer_stats.total_chunks = self.chunks.len() as u32;
            self.transfer_stats.completed_chunks = self
                .chunks
                .values()
                .filter(|c| c.state == ChunkState::Completed)
                .count() as u32;
            self.transfer_stats.total_bytes = self.chunks.values().map(|c| c.length).sum();
            let elapsed_secs = SystemTime::now()
                .duration_since(self.created_at)
                .unwrap_or_default()
                .as_secs_f64()
                .max(0.001);
            self.transfer_stats.avg_throughput_bps =
                self.transfer_stats.downloaded_bytes as f64 / elapsed_secs;
            self.transfer_stats.update_eta();
            self.updated_at = SystemTime::now();
            Ok(())
        } else {
            Err(TransferError::ChunkNotFound(chunk.id))
        }
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.chunks
            .values()
            .all(|c| c.state == ChunkState::Completed)
    }

    #[must_use]
    pub fn total_downloaded(&self) -> u64 {
        self.chunks.values().map(|c| c.downloaded_bytes).sum()
    }
}

/// Lease representing a chunk's lifecycle and assignment
#[derive(Debug, Clone)]
pub struct ChunkLease {
    pub chunk_id: Uuid,
    pub task_id: Uuid,
    pub offset: u64,
    pub length: u64,
    pub state: ChunkState,
    pub downloaded_bytes: u64,
    pub assigned_worker: Option<Uuid>,
    pub assigned_at: Option<SystemTime>,
    pub completed_at: Option<SystemTime>,
    pub retry_count: u32,
    pub checksum: Option<String>,
}

impl ChunkLease {
    #[must_use]
    pub const fn new(chunk_id: Uuid, task_id: Uuid, offset: u64, length: u64) -> Self {
        Self {
            chunk_id,
            task_id,
            offset,
            length,
            state: ChunkState::Pending,
            downloaded_bytes: 0,
            assigned_worker: None,
            assigned_at: None,
            completed_at: None,
            retry_count: 0,
            checksum: None,
        }
    }

    #[must_use]
    pub fn is_stalled(&self, timeout: Duration) -> bool {
        if self.state != ChunkState::Downloading {
            return false;
        }
        if let Some(assigned_at) = self.assigned_at {
            if let Ok(elapsed) = SystemTime::now().duration_since(assigned_at) {
                return elapsed > timeout;
            }
        }
        false
    }
}

/// Worker assignment tracking for a specific chunk
#[derive(Debug, Clone)]
pub struct WorkerAssignment {
    pub worker_id: Uuid,
    pub chunk_id: Uuid,
    pub assigned_at: SystemTime,
    pub throughput_bps: f64,
}

impl WorkerAssignment {
    #[must_use]
    pub fn new(worker_id: Uuid, chunk_id: Uuid) -> Self {
        Self {
            worker_id,
            chunk_id,
            assigned_at: SystemTime::now(),
            throughput_bps: 0.0,
        }
    }

    #[must_use]
    pub fn elapsed_secs(&self) -> f64 {
        SystemTime::now()
            .duration_since(self.assigned_at)
            .unwrap_or_default()
            .as_secs_f64()
    }
}

/// Snapshot of transfer progress and metrics
#[derive(Debug, Clone)]
pub struct TransferSnapshot {
    pub task_id: Uuid,
    pub total_chunks: u32,
    pub completed_chunks: u32,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub avg_throughput_bps: f64,
    pub estimated_time_remaining: Option<Duration>,
    pub started_at: Option<SystemTime>,
    pub updated_at: SystemTime,
}

impl TransferSnapshot {
    #[must_use]
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            total_chunks: 0,
            completed_chunks: 0,
            total_bytes: 0,
            downloaded_bytes: 0,
            avg_throughput_bps: 0.0,
            estimated_time_remaining: None,
            started_at: None,
            updated_at: SystemTime::now(),
        }
    }

    #[must_use]
    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub fn update_eta(&mut self) {
        if self.avg_throughput_bps > 0.0 {
            let remaining_bytes = self.total_bytes.saturating_sub(self.downloaded_bytes);
            let remaining_secs = remaining_bytes as f64 / self.avg_throughput_bps;
            self.estimated_time_remaining = Some(Duration::from_secs_f64(remaining_secs));
        }
    }
}

/// Sliding throughput sampler for chunk and task metrics.
#[derive(Debug, Clone)]
pub struct ThroughputSampler {
    pub window: Duration,
    pub samples: VecDeque<(Instant, u64)>,
    pub total_bytes: u64,
}

impl ThroughputSampler {
    pub const fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
            total_bytes: 0,
        }
    }

    pub fn record(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        self.total_bytes += bytes;
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while let Some(&(instant, bytes)) = self.samples.front() {
            if now.duration_since(instant) > self.window {
                self.samples.pop_front();
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
            } else {
                break;
            }
        }
    }

    pub fn current_rate_bps(&mut self) -> f64 {
        let now = Instant::now();
        self.prune(now);
        let elapsed = self
            .samples
            .front()
            .map(|(inst, _)| now.duration_since(*inst))
            .unwrap_or_default();
        let seconds = elapsed.as_secs_f64().max(0.001);
        self.total_bytes as f64 / seconds
    }

    pub fn average_rate_bps(&mut self) -> f64 {
        let now = Instant::now();
        self.prune(now);
        let total_duration = self
            .samples
            .front()
            .map(|(inst, _)| now.duration_since(*inst))
            .unwrap_or_default()
            .as_secs_f64()
            .max(0.001);
        self.total_bytes as f64 / total_duration
    }

    pub fn compact(&mut self, cfg: &crate::CompactionConfig) {
        let now = Instant::now();
        // Drain expired samples and subtract their bytes from total_bytes.
        while let Some(&(inst, bytes)) = self.samples.front() {
            if now.duration_since(inst) > cfg.throughput_sample_max_age {
                self.samples.pop_front();
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
            } else {
                break;
            }
        }
        while self.samples.len() > cfg.max_throughput_samples {
            if let Some((_, bytes)) = self.samples.pop_front() {
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferStateAggregator {
    pub task_id: Uuid,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub completed_chunks: u32,
    pub pending_chunks: u32,
    pub active_workers: usize,
    pub global_speed_bps: f64,
    pub eta: Option<Duration>,
}

impl TransferStateAggregator {
    pub const fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            total_bytes: 0,
            downloaded_bytes: 0,
            completed_chunks: 0,
            pending_chunks: 0,
            active_workers: 0,
            global_speed_bps: 0.0,
            eta: None,
        }
    }

    pub fn update(
        &mut self,
        chunks: &[crate::DownloadChunk],
        active_workers: usize,
        throughput_bps: f64,
    ) {
        self.total_bytes = chunks.iter().map(|chunk| chunk.length).sum();
        self.downloaded_bytes = chunks.iter().map(|chunk| chunk.downloaded_bytes).sum();
        self.completed_chunks = chunks
            .iter()
            .filter(|chunk| chunk.state == ChunkState::Completed)
            .count() as u32;
        self.pending_chunks = chunks
            .iter()
            .filter(|chunk| !chunk.state.is_terminal())
            .count() as u32;
        self.active_workers = active_workers;
        self.global_speed_bps = throughput_bps;
        self.eta = if throughput_bps > 0.0 {
            let remaining = self.total_bytes.saturating_sub(self.downloaded_bytes) as f64;
            Some(Duration::from_secs_f64(remaining / throughput_bps))
        } else {
            None
        };
    }

    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

/// A compact range tracker for buffered and committed write segments.
#[derive(Debug, Clone)]
pub struct RangeMap {
    pub ranges: Vec<(u64, u64)>,
}

impl RangeMap {
    pub const fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    pub fn add(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        self.ranges.push((start, end));
        self.merge();
    }

    pub fn remove(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let mut updated = Vec::new();
        for &(existing_start, existing_end) in &self.ranges {
            if existing_end <= start || existing_start >= end {
                updated.push((existing_start, existing_end));
                continue;
            }
            if existing_start < start {
                updated.push((existing_start, start));
            }
            if existing_end > end {
                updated.push((end, existing_end));
            }
        }
        self.ranges = updated;
        self.merge();
    }

    pub fn merge(&mut self) {
        if self.ranges.is_empty() {
            return;
        }
        self.ranges.sort_by_key(|(s, _)| *s);
        let mut merged = Vec::new();
        let mut current = self.ranges[0];
        for &(start, end) in self.ranges.iter().skip(1) {
            if start <= current.1 {
                current.1 = current.1.max(end);
            } else {
                merged.push(current);
                current = (start, end);
            }
        }
        merged.push(current);
        self.ranges = merged;
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
    }

    pub fn overlaps(&self, start: u64, end: u64) -> bool {
        self.ranges
            .iter()
            .any(|&(existing_start, existing_end)| start < existing_end && end > existing_start)
    }

    pub fn covers(&self, start: u64, end: u64) -> bool {
        self.ranges
            .iter()
            .any(|&(existing_start, existing_end)| existing_start <= start && end <= existing_end)
    }

    pub fn coverage(&self) -> u64 {
        self.ranges.iter().map(|(s, e)| e.saturating_sub(*s)).sum()
    }

    pub fn is_complete(&self, total: u64) -> bool {
        self.ranges.len() == 1 && self.ranges[0].0 == 0 && self.ranges[0].1 == total
    }

    pub fn gaps(&self, total: u64) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut cursor = 0;
        for &(start, end) in &self.ranges {
            if cursor < start {
                gaps.push((cursor, start));
            }
            cursor = cursor.max(end);
        }
        if cursor < total {
            gaps.push((cursor, total));
        }
        gaps
    }

    pub fn first_gap_after(&self, offset: u64, total: u64) -> Option<(u64, u64)> {
        let gaps = self.gaps(total);
        gaps.into_iter().find(|&(start, _)| start >= offset)
    }
}

/// Scheduler metrics for monitoring and decision-making
#[derive(Debug, Clone)]
pub struct SchedulerMetrics {
    pub active_workers: u32,
    pub total_workers: u32,
    pub pending_chunks: u32,
    pub retry_count: u32,
    pub stalled_chunks: u32,
    pub current_throughput_bps: f64,
    pub efficiency: f64, // 0.0-1.0, completed/(total_workers*time)
}

impl SchedulerMetrics {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active_workers: 0,
            total_workers: 0,
            pending_chunks: 0,
            retry_count: 0,
            stalled_chunks: 0,
            current_throughput_bps: 0.0,
            efficiency: 0.0,
        }
    }

    #[must_use]
    pub fn should_trigger_reassignment(&self) -> bool {
        // Reassign if stalled chunks exist or efficiency drops
        self.stalled_chunks > 0 || self.efficiency < 0.5
    }
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors for transfer operations
#[derive(Debug, Error)]
pub enum TransferError {
    #[error("chunk not found: {0}")]
    ChunkNotFound(Uuid),
    #[error("worker not found: {0}")]
    WorkerNotFound(Uuid),
    #[error("invalid state transition")]
    InvalidStateTransition,
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("invalid reservation: {0}")]
    InvalidReservation(String),
    #[error("invalid write range")]
    InvalidWriteRange,
    #[error("write overlap detected")]
    OverlapError,
    #[error("partial write coverage")]
    PartialWrite,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("io: {0}")]
    IoError(String),
    #[error("other: {0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteState {
    Reserved,
    Partial,
    Committed,
    Cancelled,
}

impl WriteState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Partial => "partial",
            Self::Committed => "committed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "reserved" => Self::Reserved,
            "partial" => Self::Partial,
            "committed" => Self::Committed,
            "cancelled" => Self::Cancelled,
            other => {
                tracing::warn!(
                    value = other,
                    "WriteState::from_str: unrecognised value in persisted data; \
                     treating as Cancelled to prevent phantom reservations",
                );
                // Default to Cancelled rather than Reserved so that an
                // unrecognised (potentially corrupted) row is not treated as
                // an active reservation that blocks new writes.
                Self::Cancelled
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriteLease {
    pub id: Uuid,
    pub task_id: Uuid,
    pub chunk_id: Uuid,
    pub offset: u64,
    pub length: u64,
    pub state: WriteState,
    pub reserved_at: SystemTime,
    pub committed_at: Option<SystemTime>,
    /// Expected SHA-256 hex digest for the committed byte range.
    /// `None` means no verification is performed (server didn't supply one).
    pub checksum: Option<String>,
}

impl WriteLease {
    pub fn new(task_id: Uuid, chunk_id: Uuid, offset: u64, length: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_id,
            chunk_id,
            offset,
            length,
            state: WriteState::Reserved,
            reserved_at: SystemTime::now(),
            committed_at: None,
            checksum: None,
        }
    }

    pub const fn end(&self) -> u64 {
        self.offset + self.length
    }

    pub const fn overlaps(&self, start: u64, end: u64) -> bool {
        let mine_start = self.offset;
        let mine_end = self.end();
        start < mine_end && end > mine_start
    }
}

// ── Sharded pending-write map ─────────────────────────────────────────────
//
// The old design stored all in-flight chunk data in a single
// `RwLock<HashMap<u64, Vec<u8>>>`.  With 16 parallel workers every
// `queue_write` and `commit_reservation` call serialised on that one lock,
// causing severe write-path contention.
//
// The fix: 16 independent shards, keyed by `(offset >> 20) % SHARDS` (i.e.
// one shard per contiguous 1 MiB region).  Workers downloading different
// file regions now run entirely in parallel through separate shards.

const PENDING_SHARDS: usize = 16;

struct ShardedPendingWrites {
    shards: Vec<RwLock<HashMap<u64, Vec<u8>>>>,
}

impl ShardedPendingWrites {
    fn new() -> Self {
        Self {
            shards: (0..PENDING_SHARDS)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    #[inline]
    const fn shard_idx(offset: u64) -> usize {
        // Shard by 1 MiB region so adjacent offsets within a chunk land in
        // the same shard and contiguous flushes acquire only one lock.
        ((offset >> 20) as usize) % PENDING_SHARDS
    }

    fn shard(&self, offset: u64) -> &RwLock<HashMap<u64, Vec<u8>>> {
        &self.shards[Self::shard_idx(offset)]
    }

    async fn insert(&self, offset: u64, data: Vec<u8>) {
        self.shard(offset).write().await.insert(offset, data);
    }

    /// Check overlap across *all* shards (called only during `reserve_write`,
    /// not in the hot write path).
    async fn has_overlap(&self, start: u64, end: u64) -> bool {
        for shard in &self.shards {
            let s = shard.read().await;
            for (&off, data) in s.iter() {
                let e = off + data.len() as u64;
                if start < e && end > off {
                    return true;
                }
            }
        }
        false
    }

    /// Drain all entries whose offset falls within `[lease_start, lease_end)`.
    /// Returns entries sorted by offset for sequential disk writes.
    async fn drain_range(&self, lease_start: u64, lease_end: u64) -> Vec<(u64, Vec<u8>)> {
        let mut result = Vec::new();
        for shard in &self.shards {
            let mut s = shard.write().await;
            let keys: Vec<u64> = s
                .keys()
                .filter(|&&off| {
                    let e = off + s[&off].len() as u64;
                    off >= lease_start && e <= lease_end
                })
                .copied()
                .collect();
            for k in keys {
                if let Some(v) = s.remove(&k) {
                    result.push((k, v));
                }
            }
        }
        result.sort_unstable_by_key(|(off, _)| *off);
        result
    }

    /// Collect all entries for coverage calculation (read-only).
    async fn all_segments(&self) -> Vec<(u64, u64)> {
        let mut segs = Vec::new();
        for shard in &self.shards {
            let s = shard.read().await;
            for (&off, data) in s.iter() {
                segs.push((off, off + data.len() as u64));
            }
        }
        segs.sort_unstable_by_key(|(s, _)| *s);
        segs
    }

    async fn is_empty(&self) -> bool {
        for shard in &self.shards {
            if !shard.read().await.is_empty() {
                return false;
            }
        }
        true
    }

    /// Drain everything (for `flush_writes`).
    async fn drain_all(&self) -> Vec<(u64, Vec<u8>)> {
        let mut result = Vec::new();
        for shard in &self.shards {
            let mut s = shard.write().await;
            result.extend(s.drain());
        }
        result.sort_unstable_by_key(|(off, _)| *off);
        result
    }
}

/// File write coordinator for safe, ordered writes
pub struct FileWriteCoordinator {
    task_id: Uuid,
    /// Absolute path where the final file is written.
    save_path: PathBuf,
    total_bytes: Option<u64>,
    storage: Arc<Storage>,
    event_bus: EventBus,
    reservations: Arc<RwLock<HashMap<Uuid, WriteLease>>>,
    /// Sharded pending-write buffer — reduces contention from O(workers) to
    /// O(workers / `PENDING_SHARDS`) for workers accessing different regions.
    pending_writes: Arc<ShardedPendingWrites>,
    dirty_ranges: Arc<RwLock<RangeMap>>,
    completed_ranges: Arc<RwLock<RangeMap>>,
}

impl FileWriteCoordinator {
    const RESERVATION_LEASE_EXPIRY: Duration = Duration::from_mins(1);

    /// Create a new coordinator.  `save_path` must be the **absolute** path the
    /// file will be written to — the daemon resolves it from
    /// `DownloadTask::resolved_save_path()` before calling this.
    #[must_use]
    pub fn new(
        task_id: Uuid,
        save_path: PathBuf,
        total_bytes: Option<u64>,
        storage: Arc<Storage>,
        event_bus: EventBus,
    ) -> Self {
        Self {
            task_id,
            save_path,
            total_bytes,
            storage,
            event_bus,
            reservations: Arc::new(RwLock::new(HashMap::new())),
            pending_writes: Arc::new(ShardedPendingWrites::new()),
            dirty_ranges: Arc::new(RwLock::new(RangeMap::new())),
            completed_ranges: Arc::new(RwLock::new(RangeMap::new())),
        }
    }

    /// Restore from persisted state (crash-resume path).
    pub async fn load(
        task_id: Uuid,
        save_path: PathBuf,
        total_bytes: Option<u64>,
        storage: Arc<Storage>,
        event_bus: EventBus,
    ) -> Result<Self, TransferError> {
        tracing::debug!(task = %task_id, "FileWriteCoordinator::load ENTRY");
        let coordinator = Self::new(task_id, save_path, total_bytes, storage, event_bus);
        tracing::debug!(task = %task_id, "FileWriteCoordinator::load new() done, calling restore()");
        coordinator.restore().await?;
        tracing::debug!(task = %task_id, "FileWriteCoordinator::load restore() done, returning Ok");
        Ok(coordinator)
    }

    /// Pre-allocate the file on disk to avoid fragmentation and ensure the
    /// target directory exists.  Must be called once when a download starts.
    pub async fn preallocate(&self) -> Result<(), TransferError> {
        // Ensure parent directory exists
        if let Some(parent) = self.save_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                TransferError::IoError(format!("cannot create directory {}: {e}", parent.display()))
            })?;
        }

        if let Some(total) = self.total_bytes {
            let path = self.save_path.clone();
            tokio::task::spawn_blocking(move || -> Result<(), TransferError> {
                use std::fs::OpenOptions;
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)
                    .map_err(|e| TransferError::IoError(format!("preallocate open: {e}")))?;

                // Extend the file to the target length so the OS can allocate
                // contiguous disk space (reduces seeks during parallel writes).
                file.set_len(total)
                    .map_err(|e| TransferError::IoError(format!("set_len: {e}")))?;

                Ok(())
            })
            .await
            .map_err(|e| TransferError::IoError(format!("spawn_blocking: {e}")))??;
        }

        Ok(())
    }

    async fn restore(&self) -> Result<(), TransferError> {
        let reservations = self
            .storage
            .load_write_reservations_for_task(self.task_id)
            .await
            .map_err(|e| TransferError::StorageError(e.to_string()))?;
        let mut locked_reservations = self.reservations.write().await;
        locked_reservations.clear();
        for persisted in reservations {
            let mut lease = WriteLease::new(
                persisted.task_id,
                persisted.chunk_id,
                persisted.offset,
                persisted.length,
            );
            lease.id = persisted.id;
            lease.state = WriteState::from_str(&persisted.state);
            lease.reserved_at =
                UNIX_EPOCH + std::time::Duration::from_secs(persisted.reserved_at as u64);
            lease.committed_at = persisted
                .committed_at
                .map(|ts| UNIX_EPOCH + std::time::Duration::from_secs(ts as u64));
            lease.checksum = None; // checksums are verified at commit time, not on restore
            locked_reservations.insert(lease.id, lease);
        }

        let ranges = self
            .storage
            .load_write_ranges_for_task(self.task_id)
            .await
            .map_err(|e| TransferError::StorageError(e.to_string()))?;
        let mut locked_completed = self.completed_ranges.write().await;
        let mut locked_dirty = self.dirty_ranges.write().await;
        locked_completed.clear();
        locked_dirty.clear();
        for persisted in ranges {
            if persisted.state == "completed" {
                locked_completed.add(persisted.start, persisted.end);
            } else {
                locked_dirty.add(persisted.start, persisted.end);
            }
        }

        for &(start, end) in &locked_completed.ranges {
            locked_dirty.remove(start, end);
        }

        drop(locked_completed);
        drop(locked_dirty);

        // Immediately release any stale reservations that were left by the
        // previous session so the new session can re-reserve and complete
        // writes. Partial reservations with persisted dirty ranges are kept
        // intact, because they represent recoverable in-progress work.
        tracing::debug!(task = %self.task_id, loaded_reservations = locked_reservations.len(), "loaded persisted reservations");
        let mut to_cancel: Vec<Uuid> = Vec::new();
        {
            let dirty = self.dirty_ranges.read().await;
            for (id, lease) in locked_reservations.iter() {
                if lease.state == WriteState::Reserved
                    || (lease.state == WriteState::Partial
                        && !dirty.overlaps(lease.offset, lease.end()))
                {
                    to_cancel.push(*id);
                }
            }
        }

        for lease_id in to_cancel {
            if let Some(lease) = locked_reservations.get_mut(&lease_id) {
                tracing::debug!(task = %self.task_id, lease = %lease_id, state = %lease.state.as_str(), "cancelling persisted lease during restore");
                // Remove dirty ranges that belonged to this lease so new
                // reservations can be taken for the same offsets.
                let lease_offset = lease.offset;
                let lease_end = lease.end();
                {
                    let mut dirty = self.dirty_ranges.write().await;
                    dirty.remove(lease_offset, lease_end);
                    let _ = self
                        .storage
                        .delete_write_ranges_in_range(
                            self.task_id,
                            "dirty",
                            lease_offset,
                            lease_end,
                        )
                        .await;
                }

                lease.state = WriteState::Cancelled;
                // Persist the cancelled state back to storage
                let _ = self.persist_lease(lease).await;

                self.event_bus.publish(
                    "write.reservation.expired",
                    json!({
                        "task_id": self.task_id.to_string(),
                        "lease_id": lease_id.to_string(),
                        "offset": lease_offset,
                        "length": lease.length,
                    }),
                );
                tracing::debug!(task = %self.task_id, lease = %lease_id, "persisted cancelled lease");
            }
        }

        drop(locked_reservations);
        // Also run the usual expiry pass to catch any genuinely old leases.
        let _ = self.expire_stale_reservations().await?;
        let dirty_count = self.dirty_ranges.read().await.ranges.len();
        let completed_count = self.completed_ranges.read().await.ranges.len();
        tracing::debug!(task = %self.task_id, dirty_ranges = dirty_count, completed_ranges = completed_count, "restore done");
        Ok(())
    }

    async fn expire_stale_reservations(&self) -> Result<usize, TransferError> {
        let now = SystemTime::now();
        let expired: Vec<Uuid> = {
            let reservations = self.reservations.read().await;
            reservations
                .values()
                .filter(|lease| {
                    matches!(lease.state, WriteState::Reserved | WriteState::Partial)
                        && now.duration_since(lease.reserved_at).unwrap_or_default()
                            > Self::RESERVATION_LEASE_EXPIRY
                })
                .map(|lease| lease.id)
                .collect()
        };

        let mut cleaned = 0;
        for lease_id in expired {
            self.release_stale_reservation(lease_id).await?;
            cleaned += 1;
        }

        Ok(cleaned)
    }

    async fn release_stale_reservation(&self, lease_id: Uuid) -> Result<(), TransferError> {
        let mut reservations = self.reservations.write().await;
        let lease = reservations
            .get_mut(&lease_id)
            .ok_or(TransferError::InvalidReservation("lease not found".into()))?;
        if lease.state == WriteState::Committed {
            return Ok(());
        }

        let lease_offset = lease.offset;
        let lease_end = lease.end();

        {
            let mut dirty = self.dirty_ranges.write().await;
            dirty.remove(lease_offset, lease_end);
            let _ = self
                .storage
                .delete_write_ranges_in_range(self.task_id, "dirty", lease_offset, lease_end)
                .await;
        }

        lease.state = WriteState::Cancelled;
        self.persist_lease(lease).await?;

        self.event_bus.publish(
            "write.reservation.expired",
            json!({
                "task_id": self.task_id.to_string(),
                "lease_id": lease_id.to_string(),
                "offset": lease_offset,
                "length": lease.length,
            }),
        );

        Ok(())
    }

    pub async fn reserve_write(
        &self,
        chunk_id: Uuid,
        offset: u64,
        length: u64,
    ) -> Result<WriteLease, TransferError> {
        if let Some(total) = self.total_bytes {
            if offset + length > total {
                return Err(TransferError::InvalidReservation(
                    "reservation exceeds file bounds".into(),
                ));
            }
        }

        let end = offset + length;

        // Acquire the write lock upfront so that the overlap check and the subsequent
        // insert are a single atomic operation, eliminating the TOCTOU window that
        // existed when a read lock was dropped before re-acquiring as a write lock.
        let mut reservations = self.reservations.write().await;
        if let Some(existing) = reservations.values().find(|lease| {
            lease.chunk_id == chunk_id
                && lease.offset == offset
                && lease.length == length
                && lease.state != WriteState::Cancelled
                && lease.state != WriteState::Committed
        }) {
            return Ok(existing.clone());
        }

        for lease in reservations.values() {
            if lease.overlaps(offset, end) && lease.state != WriteState::Cancelled {
                return Err(TransferError::OverlapError);
            }
        }

        let completed = self.completed_ranges.read().await;
        if completed.overlaps(offset, end) {
            return Err(TransferError::OverlapError);
        }
        drop(completed);

        let dirty = self.dirty_ranges.read().await;
        if dirty.overlaps(offset, end) {
            return Err(TransferError::OverlapError);
        }
        drop(dirty);

        let mut lease = WriteLease::new(self.task_id, chunk_id, offset, length);
        let now = SystemTime::now();
        lease.reserved_at = now;

        self.persist_lease(&lease).await?;
        reservations.insert(lease.id, lease.clone());
        self.event_bus.publish(
            "write.reservation.created",
            json!({
                "task_id": self.task_id.to_string(),
                "chunk_id": chunk_id.to_string(),
                "lease_id": lease.id.to_string(),
                "offset": offset,
                "length": length,
            }),
        );
        Ok(lease)
    }

    pub async fn queue_write(
        &self,
        lease_id: Uuid,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), TransferError> {
        let mut reservations = self.reservations.write().await;
        let lease = reservations
            .get_mut(&lease_id)
            .ok_or(TransferError::InvalidReservation("lease not found".into()))?;
        if lease.state == WriteState::Cancelled || lease.state == WriteState::Committed {
            return Err(TransferError::InvalidReservation(
                "lease is no longer active".into(),
            ));
        }

        let end = offset + data.len() as u64;
        if offset < lease.offset || end > lease.end() {
            return Err(TransferError::InvalidWriteRange);
        }

        // Overlap check: only scan relevant shards (fast path for non-overlapping writes).
        if self.pending_writes.has_overlap(offset, end).await {
            return Err(TransferError::OverlapError);
        }

        // Insert into the shard for this offset — no global lock held.
        self.pending_writes.insert(offset, data).await;

        // Update lease state in the map while the write lock is still held,
        // then release the lock before the storage I/O so other workers are
        // not blocked for the duration of the persist call.
        lease.state = WriteState::Partial;
        let lease_snapshot = lease.clone();
        drop(reservations);

        self.persist_lease(&lease_snapshot).await?;

        let mut dirty = self.dirty_ranges.write().await;
        dirty.add(offset, end);
        self.persist_dirty_range(offset, end).await?;
        self.event_bus.publish(
            "write.buffered",
            json!({
                "task_id": self.task_id.to_string(),
                "lease_id": lease_id.to_string(),
                "offset": offset,
                "length": end - offset,
            }),
        );
        Ok(())
    }

    pub async fn commit_reservation(&self, lease_id: Uuid) -> Result<(), TransferError> {
        let mut reservations = self.reservations.write().await;
        let lease = reservations
            .get_mut(&lease_id)
            .ok_or(TransferError::InvalidReservation("lease not found".into()))?;
        if lease.state == WriteState::Cancelled {
            return Err(TransferError::InvalidReservation(
                "lease is cancelled".into(),
            ));
        }

        let covered_end = self.contiguous_coverage_end(lease.offset).await;
        if covered_end == lease.offset {
            return Err(TransferError::PartialWrite);
        }

        let lease_offset = lease.offset;
        let lease_end = lease.end();
        let lease_length = lease.length;

        // Release the write lock before performing disk I/O so other workers
        // can continue to call reserve_write / queue_write / commit_reservation
        // concurrently.  All the data we need (lease_offset, lease_end,
        // lease_length) has already been extracted into local variables.
        drop(reservations);

        // ── Flush pending writes for this lease to disk ───────────────────────
        // Drain only the writes that fall within this lease's range from the
        // sharded map; other workers' writes in different shards are unaffected.
        {
            let to_flush = self
                .pending_writes
                .drain_range(lease_offset, lease_end)
                .await;

            if !to_flush.is_empty() {
                let save_path = self.save_path.clone();
                tokio::task::spawn_blocking(move || -> Result<(), TransferError> {
                    use std::fs::OpenOptions;
                    use std::io::{BufWriter, Seek, SeekFrom, Write};

                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&save_path)
                        .map_err(|e| {
                            TransferError::IoError(format!(
                                "commit open {}: {e}",
                                save_path.display()
                            ))
                        })?;

                    // Use a 1 MiB BufWriter for sequential write coalescing.
                    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
                    for (offset, data) in to_flush {
                        writer
                            .seek(SeekFrom::Start(offset))
                            .map_err(|e| TransferError::IoError(format!("seek {offset}: {e}")))?;
                        writer
                            .write_all(&data)
                            .map_err(|e| TransferError::IoError(format!("write {offset}: {e}")))?;
                    }
                    writer
                        .flush()
                        .map_err(|e| TransferError::IoError(format!("flush: {e}")))?;
                    Ok(())
                })
                .await
                .map_err(|e| TransferError::IoError(format!("spawn_blocking: {e}")))??;
            }
        }

        if self.total_bytes.is_some() {
            if covered_end < lease_end {
                return Err(TransferError::PartialWrite);
            }
            let mut completed = self.completed_ranges.write().await;
            completed.add(lease_offset, lease_end);
            self.persist_completed_range(lease_offset, lease_end)
                .await?;
            let mut dirty = self.dirty_ranges.write().await;
            dirty.remove(lease_offset, lease_end);
            let _ = self
                .storage
                .delete_write_ranges_in_range(self.task_id, "dirty", lease_offset, lease_end)
                .await;
        } else {
            let mut completed = self.completed_ranges.write().await;
            completed.add(lease_offset, covered_end);
            self.persist_completed_range(lease_offset, covered_end)
                .await?;
            let mut dirty = self.dirty_ranges.write().await;
            dirty.remove(lease_offset, covered_end);
            let _ = self
                .storage
                .delete_write_ranges_in_range(self.task_id, "dirty", lease_offset, covered_end)
                .await;
        }

        // Re-acquire the write lock for the final state-change commit now
        // that disk I/O is done.  The lock window is very short: just the
        // in-memory mutation + a single storage upsert.
        {
            let mut reservations = self.reservations.write().await;
            let lease = reservations
                .get_mut(&lease_id)
                .ok_or(TransferError::InvalidReservation("lease not found".into()))?;
            lease.state = WriteState::Committed;
            lease.committed_at = Some(SystemTime::now());
            self.persist_lease(lease).await?;
        }

        self.event_bus.publish(
            "write.committed",
            json!({
                "task_id": self.task_id.to_string(),
                "lease_id": lease_id.to_string(),
                "offset": lease_offset,
                "length": lease_length,
            }),
        );
        // Fire-and-forget snapshot persistence to avoid blocking worker threads
        let storage = self.storage.clone();
        let task_id = self.task_id;
        let reservations = self.reservations.read().await;
        let dirty = self.dirty_ranges.read().await;
        let completed = self.completed_ranges.read().await;

        let snapshot = json!({
            "task_id": task_id.to_string(),
            "reservations": reservations.values().map(|lease| {
                json!({
                    "id": lease.id.to_string(),
                    "chunk_id": lease.chunk_id.to_string(),
                    "offset": lease.offset,
                    "length": lease.length,
                    "state": lease.state.as_str(),
                    "reserved_at": lease.reserved_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
                    "committed_at": lease.committed_at.map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()),
                })
            }).collect::<Vec<_>>(),
            "dirty_ranges": dirty.ranges.clone(),
            "completed_ranges": completed.ranges.clone(),
        });
        drop(reservations);
        drop(dirty);
        drop(completed);

        tokio::spawn(async move {
            let _ = storage
                .save_snapshot(&format!("coordinator_snapshot_{task_id}"), &snapshot, true)
                .await;
        });

        Ok(())
    }

    pub async fn release_reservation(&self, lease_id: Uuid) -> Result<(), TransferError> {
        let mut reservations = self.reservations.write().await;
        let lease = reservations
            .get_mut(&lease_id)
            .ok_or(TransferError::InvalidReservation("lease not found".into()))?;
        let lease_offset = lease.offset;
        let lease_end = lease.end();

        if lease.state != WriteState::Committed {
            let mut dirty = self.dirty_ranges.write().await;
            dirty.remove(lease_offset, lease_end);
            let _ = self
                .storage
                .delete_write_ranges_in_range(self.task_id, "dirty", lease_offset, lease_end)
                .await;
        }

        lease.state = WriteState::Cancelled;
        self.persist_lease(lease).await?;

        self.event_bus.publish(
            "write.reservation.released",
            json!({
                "task_id": self.task_id.to_string(),
                "lease_id": lease_id.to_string(),
                "offset": lease_offset,
                "length": lease.length,
            }),
        );
        Ok(())
    }

    /// Verify the chunk data on disk matches the expected SHA-256 checksum stored
    /// in the `WriteLease`.  When no expected checksum is set the check passes
    /// (checksums are optional and only populated when the server provides an
    /// `ETag` or `Content-MD5`/`Digest` header at the chunk level).
    ///
    /// Reads the committed bytes from disk synchronously inside `spawn_blocking`
    /// to avoid blocking the async runtime.
    pub async fn validate_checksum(&self, lease_id: Uuid) -> Result<bool, TransferError> {
        use sha2::{Digest, Sha256};

        let (expected_checksum, offset, length, save_path) = {
            let reservations = self.reservations.read().await;
            let lease = reservations
                .get(&lease_id)
                .ok_or_else(|| TransferError::InvalidReservation("lease not found".into()))?;
            (
                lease.checksum.clone(),
                lease.offset,
                lease.length,
                self.save_path.clone(),
            )
        };

        // No checksum to verify — accept the write unconditionally.
        let Some(expected) = expected_checksum else {
            return Ok(true);
        };

        let actual = tokio::task::spawn_blocking(move || -> Result<String, TransferError> {
            use std::fs::File;
            use std::io::{Read, Seek, SeekFrom};

            let mut file = File::open(&save_path)
                .map_err(|e| TransferError::IoError(format!("checksum open: {e}")))?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| TransferError::IoError(format!("checksum seek: {e}")))?;

            let mut hasher = Sha256::new();
            let mut remaining = length;
            let mut buf = vec![0u8; 64 * 1024]; // 64 KiB read buffer
            while remaining > 0 {
                let to_read = (remaining as usize).min(buf.len());
                let n = file
                    .read(&mut buf[..to_read])
                    .map_err(|e| TransferError::IoError(format!("checksum read: {e}")))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                remaining -= n as u64;
            }

            Ok(hex::encode(hasher.finalize()))
        })
        .await
        .map_err(|e| TransferError::IoError(format!("spawn_blocking: {e}")))??;

        if actual == expected {
            Ok(true)
        } else {
            tracing::warn!(
                task_id = %self.task_id,
                lease_id = %lease_id,
                expected = %expected,
                actual = %actual,
                "chunk checksum mismatch",
            );
            Ok(false)
        }
    }

    pub async fn get_coverage_percent(&self) -> f64 {
        let ranges = self.completed_ranges.read().await;
        let total_written = ranges.coverage();
        match self.total_bytes {
            Some(total) if total > 0 => (total_written as f64 / total as f64) * 100.0,
            Some(_) => 0.0,
            None => total_written as f64,
        }
    }

    pub async fn is_complete(&self) -> bool {
        if let Some(total) = self.total_bytes {
            self.completed_ranges.read().await.is_complete(total)
        } else {
            // Streaming mode: total size is not known upfront.
            // Treat as complete when there are no pending buffered writes and
            // no dirty (flushed-but-not-yet-confirmed) ranges outstanding.
            self.pending_writes.is_empty().await && self.dirty_ranges.read().await.ranges.is_empty()
        }
    }

    pub async fn get_pending_ranges(&self) -> Vec<(u64, u64)> {
        if let Some(total) = self.total_bytes {
            self.completed_ranges.read().await.gaps(total)
        } else {
            Vec::new()
        }
    }

    pub async fn get_dirty_ranges(&self) -> Vec<(u64, u64)> {
        self.dirty_ranges.read().await.ranges.clone()
    }

    pub async fn get_completed_ranges(&self) -> Vec<(u64, u64)> {
        self.completed_ranges.read().await.ranges.clone()
    }

    pub async fn is_range_completed(&self, start: u64, end: u64) -> bool {
        self.completed_ranges.read().await.covers(start, end)
    }

    async fn contiguous_coverage_end(&self, offset: u64) -> u64 {
        let mut segments = self.pending_writes.all_segments().await;
        segments.sort_by_key(|(s, _)| *s);

        let mut current = offset;
        for (start, finish) in segments {
            if start > current {
                break;
            }
            if finish <= current {
                continue;
            }
            current = finish;
        }
        current
    }

    async fn persist_lease(&self, lease: &WriteLease) -> Result<(), TransferError> {
        let reservation = PersistedWriteReservation {
            id: lease.id,
            task_id: lease.task_id,
            chunk_id: lease.chunk_id,
            offset: lease.offset,
            length: lease.length,
            state: lease.state.as_str().to_string(),
            reserved_at: lease
                .reserved_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            committed_at: lease
                .committed_at
                .map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64),
            created_at: lease
                .reserved_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            updated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };
        self.storage
            .save_write_reservation(reservation)
            .await
            .map_err(|e| TransferError::StorageError(e.to_string()))
    }

    async fn persist_dirty_range(&self, start: u64, end: u64) -> Result<(), TransferError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let range = PersistedWriteRange {
            id: 0,
            task_id: self.task_id,
            start,
            end,
            state: "dirty".to_string(),
            created_at: now,
            updated_at: now,
        };
        self.storage
            .save_write_range(range)
            .await
            .map_err(|e| TransferError::StorageError(e.to_string()))
    }

    async fn persist_completed_range(&self, start: u64, end: u64) -> Result<(), TransferError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let range = PersistedWriteRange {
            id: 0,
            task_id: self.task_id,
            start,
            end,
            state: "completed".to_string(),
            created_at: now,
            updated_at: now,
        };
        self.storage
            .save_write_range(range)
            .await
            .map_err(|e| TransferError::StorageError(e.to_string()))
    }

    /// Flush all pending in-memory writes to the file at `save_path`.
    ///
    /// Writes are performed in **offset order** so sequential I/O patterns are
    /// preferred even on HDDs.  Each chunk is written at its exact byte offset
    /// via `seek` so out-of-order network delivery is handled transparently.
    ///
    /// Returns the total number of bytes flushed.
    pub async fn flush_writes(&self) -> Result<u64, TransferError> {
        use std::io::SeekFrom;

        if self.pending_writes.is_empty().await {
            return Ok(0);
        }

        // Drain all shards and sort globally by offset for sequential I/O.
        let ordered = self.pending_writes.drain_all().await;

        let save_path = self.save_path.clone();
        let total_flushed = tokio::task::spawn_blocking(move || -> Result<u64, TransferError> {
            use std::fs::OpenOptions;
            use std::io::{BufWriter, Seek, Write};

            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&save_path)
                .map_err(|e| {
                    TransferError::IoError(format!("flush open {}: {e}", save_path.display()))
                })?;

            // 1 MiB write buffer for sequential coalescing.
            let mut writer = BufWriter::with_capacity(1024 * 1024, file);
            let mut flushed: u64 = 0;
            for (offset, data) in ordered {
                writer
                    .seek(SeekFrom::Start(offset))
                    .map_err(|e| TransferError::IoError(format!("seek to {offset}: {e}")))?;
                writer
                    .write_all(&data)
                    .map_err(|e| TransferError::IoError(format!("write at {offset}: {e}")))?;
                flushed += data.len() as u64;
            }

            writer
                .flush()
                .map_err(|e| TransferError::IoError(format!("flush: {e}")))?;

            Ok(flushed)
        })
        .await
        .map_err(|e| TransferError::IoError(format!("spawn_blocking: {e}")))??;

        Ok(total_flushed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn transfer_graph_tracks_progress() {
        let task_id = Uuid::new_v4();
        let mut graph = TransferGraph::new(task_id);

        let chunk_id = Uuid::new_v4();
        let lease = ChunkLease::new(chunk_id, task_id, 0, 1024);
        graph.add_chunk(lease);

        assert_eq!(graph.chunks.len(), 1);
        assert!(!graph.is_complete());
    }

    #[test]
    fn chunk_lease_detects_stalled() {
        let mut lease = ChunkLease::new(Uuid::new_v4(), Uuid::new_v4(), 0, 1024);
        lease.state = ChunkState::Downloading;
        lease.assigned_at = Some(SystemTime::now() - Duration::from_mins(1));

        assert!(lease.is_stalled(Duration::from_secs(30)));
        assert!(!lease.is_stalled(Duration::from_mins(2)));
    }

    #[test]
    fn transfer_snapshot_calculates_progress() {
        let mut snapshot = TransferSnapshot::new(Uuid::new_v4());
        snapshot.total_bytes = 1000;
        snapshot.downloaded_bytes = 500;

        assert_eq!(snapshot.progress_percent(), 50.0);
    }

    #[tokio::test]
    async fn file_write_coordinator_tracks_ranges() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            Storage::open(dir.path().join("coordinator_ranges.db"))
                .unwrap()
        );
        let coordinator = FileWriteCoordinator::new(
            Uuid::new_v4(),
            dir.path().join("test_file.bin"),
            Some(1024),
            storage.clone(),
            EventBus::new(32),
        );

        let lease = coordinator
            .reserve_write(Uuid::new_v4(), 0, 256)
            .await
            .unwrap();
        coordinator
            .queue_write(lease.id, 0, vec![0u8; 256])
            .await
            .unwrap();
        coordinator.commit_reservation(lease.id).await.unwrap();

        assert_eq!(coordinator.get_coverage_percent().await, 25.0);

        let pending = coordinator.get_pending_ranges().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], (256, 1024));
    }

    #[tokio::test]
    async fn file_write_coordinator_detects_completion() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            Storage::open(dir.path().join("coordinator_ranges.db"))
                .unwrap()
        );
        let coordinator = FileWriteCoordinator::new(
            Uuid::new_v4(),
            dir.path().join("test_file.bin"),
            Some(100),
            storage.clone(),
            EventBus::new(32),
        );

        let lease = coordinator
            .reserve_write(Uuid::new_v4(), 0, 100)
            .await
            .unwrap();
        coordinator
            .queue_write(lease.id, 0, vec![0u8; 100])
            .await
            .unwrap();
        coordinator.commit_reservation(lease.id).await.unwrap();

        assert!(coordinator.is_complete().await);

        let coordinator2 = FileWriteCoordinator::new(
            Uuid::new_v4(),
            dir.path().join("test_file.bin"),
            Some(100),
            storage.clone(),
            EventBus::new(32),
        );
        let lease2 = coordinator2
            .reserve_write(Uuid::new_v4(), 0, 50)
            .await
            .unwrap();
        coordinator2
            .queue_write(lease2.id, 0, vec![0u8; 50])
            .await
            .unwrap();
        assert!(!coordinator2.is_complete().await);
    }

    #[tokio::test]
    async fn file_write_coordinator_rejects_overlapping_reservations() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            Storage::open(dir.path().join("coordinator_ranges.db"))
                .unwrap()
        );
        let coordinator = FileWriteCoordinator::new(
            Uuid::new_v4(),
            dir.path().join("test_file.bin"),
            Some(1024),
            storage.clone(),
            EventBus::new(32),
        );

        coordinator
            .reserve_write(Uuid::new_v4(), 0, 512)
            .await
            .unwrap();
        let result = coordinator.reserve_write(Uuid::new_v4(), 256, 256).await;

        assert!(matches!(result, Err(TransferError::OverlapError)));
    }

    #[tokio::test]
    async fn file_write_coordinator_restores_partial_dirty_state() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            Storage::open(dir.path().join("coordinator_ranges.db"))
                .unwrap()
        );
        let task_id = Uuid::new_v4();
        let coordinator = FileWriteCoordinator::new(
            task_id,
            dir.path().join("test_file.bin"),
            Some(512),
            storage.clone(),
            EventBus::new(32),
        );

        let lease = coordinator
            .reserve_write(Uuid::new_v4(), 0, 256)
            .await
            .unwrap();
        coordinator
            .queue_write(lease.id, 0, vec![0u8; 128])
            .await
            .unwrap();

        let restored = FileWriteCoordinator::load(
            task_id,
            dir.path().join("test_file.bin"),
            Some(512),
            storage.clone(),
            EventBus::new(32),
        )
        .await
        .unwrap();
        assert_eq!(restored.get_dirty_ranges().await, vec![(0, 128)]);
        assert!(!restored.is_complete().await);
    }

    #[tokio::test]
    async fn file_write_coordinator_expires_stale_reservations() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            Storage::open(dir.path().join("coordinator_ranges.db"))
                .unwrap()
        );
        let task_id = Uuid::new_v4();
        let coordinator = FileWriteCoordinator::new(
            task_id,
            dir.path().join("test_file.bin"),
            Some(1024),
            storage.clone(),
            EventBus::new(32),
        );

        let lease = coordinator
            .reserve_write(Uuid::new_v4(), 0, 256)
            .await
            .unwrap();
        coordinator
            .queue_write(lease.id, 0, vec![0u8; 128])
            .await
            .unwrap();

        {
            let mut reservations = coordinator.reservations.write().await;
            if let Some(stale) = reservations.get_mut(&lease.id) {
                stale.reserved_at = SystemTime::now()
                    - Duration::from_secs(
                        FileWriteCoordinator::RESERVATION_LEASE_EXPIRY.as_secs() + 10,
                    );
            }
        }

        let cleaned = coordinator.expire_stale_reservations().await.unwrap();
        assert_eq!(cleaned, 1);
        assert!(coordinator.get_dirty_ranges().await.is_empty());
        let reservations = coordinator.reservations.read().await;
        let stale = reservations.get(&lease.id).expect("lease present");
        assert!(matches!(stale.state, WriteState::Cancelled));
    }
}
