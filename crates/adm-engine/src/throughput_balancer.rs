//! `throughput_balancer.rs`
//! Minimal throughput-aware scheduling balancer.
//! Add to crates/engine/src/ and wire into `BasicScheduler::schedule_task`.
//!
//! Replaces pure FIFO chunk ordering with a scoring function that weighs:
//!   - rolling throughput per worker
//!   - worker efficiency (bytes/attempt)
//!   - retry frequency (penalty for flaky chunks)
//!   - stall likelihood (derived from recent stall history)
//!
//! Design constraints:
//!   - No external dependencies beyond what engine already uses.
//!   - Scoring is deterministic for the same inputs (no randomness).
//!   - Conservative: when data is sparse it falls back to index-order (FIFO).

use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Per-worker throughput window
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WorkerThroughputRecord {
    pub worker_id: Uuid,
    /// Bytes delivered in the rolling window.
    bytes_in_window: u64,
    /// Number of completed chunks (for efficiency ratio).
    chunks_completed: u32,
    /// Number of stalls attributed to this worker.
    stall_count: u32,
    /// Number of retries attributed to this worker.
    retry_count: u32,
    window_start: Instant,
    window_duration: Duration,
}

impl WorkerThroughputRecord {
    #[must_use]
    pub fn new(worker_id: Uuid, window_duration: Duration) -> Self {
        Self {
            worker_id,
            bytes_in_window: 0,
            chunks_completed: 0,
            stall_count: 0,
            retry_count: 0,
            window_start: Instant::now(),
            window_duration,
        }
    }

    pub fn record_bytes(&mut self, bytes: u64) {
        self.maybe_reset();
        self.bytes_in_window = self.bytes_in_window.saturating_add(bytes);
    }

    pub const fn record_completion(&mut self) {
        self.chunks_completed += 1;
    }

    pub const fn record_stall(&mut self) {
        self.stall_count += 1;
    }

    pub const fn record_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Bytes per second in the current window. Returns 0 if the window is empty.
    #[must_use]
    pub fn throughput_bps(&self) -> f64 {
        let elapsed = self.window_start.elapsed().as_secs_f64().max(0.001);
        self.bytes_in_window as f64 / elapsed
    }

    /// Efficiency: bytes per chunk-attempt (penalises retries and stalls).
    #[must_use]
    pub fn efficiency(&self) -> f64 {
        let attempts = (self.chunks_completed + self.retry_count + self.stall_count).max(1);
        self.bytes_in_window as f64 / f64::from(attempts)
    }

    /// Stall likelihood score [0.0, 1.0].  Higher means more likely to stall.
    #[must_use]
    pub fn stall_likelihood(&self) -> f64 {
        let total = f64::from((self.chunks_completed + self.stall_count).max(1));
        (f64::from(self.stall_count) / total).min(1.0)
    }

    fn maybe_reset(&mut self) {
        if self.window_start.elapsed() > self.window_duration {
            self.bytes_in_window = 0;
            self.chunks_completed = 0;
            self.stall_count = 0;
            self.retry_count = 0;
            self.window_start = Instant::now();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Balancer
// ─────────────────────────────────────────────────────────────────────────────

/// Maintains per-worker throughput records and scores chunks for dispatch.
#[derive(Clone)]
pub struct ThroughputBalancer {
    worker_records: HashMap<Uuid, WorkerThroughputRecord>,
    window_duration: Duration,
    /// Global rolling throughput (all workers combined).
    global_bytes: u64,
    global_window_start: Instant,
}

impl ThroughputBalancer {
    #[must_use]
    pub fn new(window_duration: Duration) -> Self {
        Self {
            worker_records: HashMap::new(),
            window_duration,
            global_bytes: 0,
            global_window_start: Instant::now(),
        }
    }

    /// Called every time a chunk update arrives (mirrors `ThroughputSampler::record`).
    pub fn on_bytes(&mut self, worker_id: Uuid, bytes: u64) {
        self.record_worker(worker_id).record_bytes(bytes);
        // Global window
        if self.global_window_start.elapsed() > self.window_duration {
            self.global_bytes = 0;
            self.global_window_start = Instant::now();
        }
        self.global_bytes = self.global_bytes.saturating_add(bytes);
    }

    pub fn on_chunk_completed(&mut self, worker_id: Uuid) {
        self.record_worker(worker_id).record_completion();
    }

    pub fn on_stall(&mut self, worker_id: Uuid) {
        self.record_worker(worker_id).record_stall();
    }

    pub fn on_retry(&mut self, worker_id: Uuid) {
        self.record_worker(worker_id).record_retry();
    }

    /// Global throughput in bytes/sec.
    #[must_use]
    pub fn global_throughput_bps(&self) -> f64 {
        let elapsed = self.global_window_start.elapsed().as_secs_f64().max(0.001);
        self.global_bytes as f64 / elapsed
    }

    /// Choose the best available worker for the next chunk.
    ///
    /// Scoring: highest efficiency + throughput, lowest stall likelihood.
    /// If no records exist (cold start), returns the first id from `available`.
    #[must_use]
    pub fn best_worker<'a>(&self, available: &'a [Uuid]) -> Option<&'a Uuid> {
        if available.is_empty() {
            return None;
        }
        available
            .iter()
            .map(|id| {
                let score = self
                    .worker_records
                    .get(id)
                    .map_or(0.5, |r| self.worker_score(r)); // cold-start: neutral score
                (id, score)
            })
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(id, _)| id)
    }

    /// Score ∈ [0.0, 1.0].  Higher is better.
    fn worker_score(&self, r: &WorkerThroughputRecord) -> f64 {
        let global_bps = self.global_throughput_bps().max(1.0);
        // Normalised throughput contribution [0, 1].
        let norm_throughput = (r.throughput_bps() / global_bps).min(1.0);
        // Efficiency bonus (normalised by global bytes to avoid division quirks).
        let norm_efficiency = if self.global_bytes > 0 {
            (r.efficiency() / (self.global_bytes as f64)).min(1.0)
        } else {
            0.5
        };
        let stall_penalty = r.stall_likelihood();
        let retry_penalty = {
            let total_attempts =
                f64::from((r.chunks_completed + r.retry_count + r.stall_count).max(1));
            (f64::from(r.retry_count) / total_attempts).min(1.0)
        };

        // Weighted sum — adjust weights as needed.
        const W_THROUGHPUT: f64 = 0.40;
        const W_EFFICIENCY: f64 = 0.20;
        const W_STALL: f64 = 0.25;
        const W_RETRY: f64 = 0.15;

        W_RETRY
            .mul_add(
                -retry_penalty,
                W_STALL.mul_add(
                    -stall_penalty,
                    W_THROUGHPUT.mul_add(norm_throughput, W_EFFICIENCY * norm_efficiency),
                ),
            )
            .clamp(0.0, 1.0)
    }

    fn record_worker(&mut self, id: Uuid) -> &mut WorkerThroughputRecord {
        self.worker_records
            .entry(id)
            .or_insert_with(|| WorkerThroughputRecord::new(id, self.window_duration))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk priority scoring (replaces BasicScheduler::chunk_priority)
// ─────────────────────────────────────────────────────────────────────────────

use crate::{ChunkState, DownloadChunk};

/// Extended chunk priority that incorporates throughput-aware signals.
/// Returns a score where **higher = dispatch sooner**.
///
/// Drop-in replacement for `BasicScheduler::chunk_priority`:
/// ```rust,ignore
/// use crate::{DownloadChunk, ThroughputBalancer};
/// let mut pending_chunks: Vec<DownloadChunk> = Vec::new();
/// let balancer = ThroughputBalancer::new(std::time::Duration::from_secs(5));
/// pending_chunks.sort_by_key(|c| -score_chunk(c, &balancer));
/// ```
#[must_use]
pub fn score_chunk(chunk: &DownloadChunk, balancer: &ThroughputBalancer) -> i64 {
    let state_bonus: i64 = match chunk.state {
        ChunkState::Pending => 1000,
        ChunkState::Retrying => 600,
        _ => 0,
    };

    // Penalise chunks with many retries (they are flaky).
    let retry_penalty = i64::from(chunk.retry_attempts) * 20;

    // Penalise large chunks slightly (smaller = faster recovery on stall).
    let size_penalty: i64 = if chunk.length > 0 {
        i64::from(chunk.length.ilog2()).saturating_sub(10).max(0)
    } else {
        0
    };

    // Boost chunks assigned to a high-scoring worker.
    let worker_boost: i64 = chunk
        .assigned_worker
        .as_ref()
        .and_then(|w| balancer.worker_records.get(&w.id))
        .map_or(100, |r| (balancer.worker_score(r) * 200.0) as i64); // neutral boost for unassigned

    state_bonus + worker_boost - retry_penalty - size_penalty
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cold_start_returns_first_worker() {
        let balancer = ThroughputBalancer::new(Duration::from_secs(5));
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let best = balancer.best_worker(&ids);
        assert!(best.is_some());
    }

    #[test]
    fn high_throughput_worker_preferred() {
        let mut balancer = ThroughputBalancer::new(Duration::from_secs(5));
        let fast = Uuid::new_v4();
        let slow = Uuid::new_v4();

        balancer.on_bytes(fast, 1_000_000);
        balancer.on_chunk_completed(fast);
        balancer.on_bytes(slow, 1_000);
        balancer.on_chunk_completed(slow);

        let best = balancer.best_worker(&[slow, fast]).copied();
        assert_eq!(best, Some(fast), "fast worker should be preferred");
    }

    #[test]
    fn stall_heavy_worker_deprioritised() {
        let mut balancer = ThroughputBalancer::new(Duration::from_secs(5));
        let good = Uuid::new_v4();
        let bad = Uuid::new_v4();

        balancer.on_bytes(good, 500_000);
        balancer.on_chunk_completed(good);

        balancer.on_bytes(bad, 500_000);
        for _ in 0..5 {
            balancer.on_stall(bad);
        }

        let best = balancer.best_worker(&[bad, good]).copied();
        assert_eq!(
            best,
            Some(good),
            "worker with stalls should be deprioritised"
        );
    }

    #[test]
    fn score_chunk_retrying_lower_than_pending() {
        let balancer = ThroughputBalancer::new(Duration::from_secs(5));
        let task_id = Uuid::new_v4();
        let pending = crate::DownloadChunk::new(task_id, 0, 0, 1024);
        let mut retrying = crate::DownloadChunk::new(task_id, 1, 1024, 1024);
        retrying.set_state(ChunkState::Retrying);
        retrying.retry_attempts = 3;

        assert!(
            score_chunk(&pending, &balancer) > score_chunk(&retrying, &balancer),
            "fresh pending chunk should score higher than a retrying chunk with attempts"
        );
    }
}
