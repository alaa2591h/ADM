use crate::{ChunkState, ChunkUpdate, EventBus, WorkerHandle};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Taxonomy of stall reasons emitted by the detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StallReason {
    NoProgressTimeout,
    ThroughputBelowThreshold,
    DeadConnection,
    ExcessiveRetries,
    HeartbeatLost,
}

/// Recommendation action type emitted by the detector (scheduler decides).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdaptiveRecommendation {
    CancelAndRetry {
        chunk_id: Uuid,
    },
    RecycleWorker {
        worker_id: Uuid,
    },
    SplitChunk {
        chunk_id: Uuid,
        ranges: Vec<(u64, u64)>,
    },
    Reassign {
        chunk_id: Uuid,
        to_worker: Option<Uuid>,
    },
    NoAction,
}

/// Event published by the detector for consumption by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StallEvent {
    pub task_id: Uuid,
    pub chunk_id: Uuid,
    pub worker_id: Option<Uuid>,
    pub reason: StallReason,
    pub action: String,  // Added for scheduler consumption
    pub generation: u64, // Added for generation-aware adaptive control
    pub short_rate_bps: f64,
    pub long_rate_bps: f64,
    pub last_progress_secs: Option<u128>,
    pub timestamp_secs: u64,
    pub ranges: Option<Vec<(u64, u64)>>, // Optional ranges for SplitChunk
}

/// Minimal runtime metrics hook (in-memory collector or noop).
pub trait RuntimeMetrics: Send + Sync + 'static {
    fn record_stall(&self, reason: &StallReason);
    fn record_recommendation(&self, rec: &AdaptiveRecommendation);
}

#[derive(Default)]
pub struct NoopMetrics;
impl RuntimeMetrics for NoopMetrics {
    fn record_stall(&self, _reason: &StallReason) {}
    fn record_recommendation(&self, _rec: &AdaptiveRecommendation) {}
}

/// Adaptive policy abstraction (configurable thresholds).
pub trait AdaptivePolicy: Send + Sync + 'static {
    fn inactivity_timeout(&self) -> Duration;
    fn short_window(&self) -> Duration;
    fn long_window(&self) -> Duration;
    fn short_rate_floor_bps(&self) -> f64;
    fn long_rate_floor_bps(&self) -> f64;
    fn max_retries(&self) -> u32;
    fn heartbeat_timeout(&self) -> Duration;
    fn monitor_interval(&self) -> Duration;
}

/// Default conservative policy; tests can construct a fast policy.
pub struct DefaultAdaptivePolicy {
    pub inactivity_timeout: Duration,
    pub short_window: Duration,
    pub long_window: Duration,
    pub short_rate_floor_bps: f64,
    pub long_rate_floor_bps: f64,
    pub max_retries: u32,
    pub heartbeat_timeout: Duration,
    pub monitor_interval: Duration,
}

impl DefaultAdaptivePolicy {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inactivity_timeout: Duration::from_secs(10),
            short_window: Duration::from_secs(2),
            long_window: Duration::from_secs(10),
            short_rate_floor_bps: 64.0,
            long_rate_floor_bps: 16.0,
            max_retries: 3,
            heartbeat_timeout: Duration::from_secs(15),
            monitor_interval: Duration::from_millis(500),
        }
    }

    /// Fast test policy with short timers.
    #[must_use]
    pub const fn fast_test() -> Self {
        Self {
            inactivity_timeout: Duration::from_millis(250),
            short_window: Duration::from_millis(200),
            long_window: Duration::from_millis(600),
            short_rate_floor_bps: 1.0,
            long_rate_floor_bps: 0.5,
            max_retries: 2,
            heartbeat_timeout: Duration::from_millis(400),
            monitor_interval: Duration::from_millis(50),
        }
    }
}

impl Default for DefaultAdaptivePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptivePolicy for DefaultAdaptivePolicy {
    fn inactivity_timeout(&self) -> Duration {
        self.inactivity_timeout
    }
    fn short_window(&self) -> Duration {
        self.short_window
    }
    fn long_window(&self) -> Duration {
        self.long_window
    }
    fn short_rate_floor_bps(&self) -> f64 {
        self.short_rate_floor_bps
    }
    fn long_rate_floor_bps(&self) -> f64 {
        self.long_rate_floor_bps
    }
    fn max_retries(&self) -> u32 {
        self.max_retries
    }
    fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout
    }
    fn monitor_interval(&self) -> Duration {
        self.monitor_interval
    }
}

/// Simple rolling sampler for short/long windows.
#[derive(Debug, Clone)]
struct RollingSampler {
    short_window: Duration,
    long_window: Duration,
    samples: VecDeque<(Instant, u64)>,
    total_bytes: u64,
}

impl RollingSampler {
    const fn new(short_window: Duration, long_window: Duration) -> Self {
        Self {
            short_window,
            long_window,
            samples: VecDeque::new(),
            total_bytes: 0,
        }
    }

    fn record(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while let Some(&(ts, bytes)) = self.samples.front() {
            if now.duration_since(ts) > self.long_window {
                self.samples.pop_front();
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
            } else {
                break;
            }
        }
    }

    fn short_rate_bps(&mut self) -> f64 {
        let now = Instant::now();
        self.prune(now);
        let short_from = now.checked_sub(self.short_window).unwrap_or(now);
        let mut short_bytes = 0u64;
        for &(ts, bytes) in &self.samples {
            if ts >= short_from {
                short_bytes = short_bytes.saturating_add(bytes);
            }
        }
        let secs = self.short_window.as_secs_f64().max(0.001);
        u64_to_f64(short_bytes) / secs
    }

    fn long_rate_bps(&mut self) -> f64 {
        let now = Instant::now();
        self.prune(now);
        let long_from = now.checked_sub(self.long_window).unwrap_or(now);
        let mut long_bytes = 0u64;
        for &(ts, bytes) in &self.samples {
            if ts >= long_from {
                long_bytes = long_bytes.saturating_add(bytes);
            }
        }
        let secs = self.long_window.as_secs_f64().max(0.001);
        u64_to_f64(long_bytes) / secs
    }
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

struct StallState {
    chunk_samples: HashMap<Uuid, RollingSampler>,
    chunk_last_bytes: HashMap<Uuid, u64>,
    chunk_last_progress: HashMap<Uuid, Instant>,
    worker_heartbeats: HashMap<Uuid, Instant>,
    retry_history: HashMap<Uuid, u32>,
    /// Maps `chunk_id` to `task_id` so stall events carry the correct `task_id`.
    chunk_task_ids: HashMap<Uuid, Uuid>,
    /// Maps `chunk_id` to its current task `generation`.
    chunk_generations: HashMap<Uuid, u64>,
}

type ChunkTaskInfo = Vec<(Uuid, Uuid, u64)>;
type WorkerHeartbeats = Vec<(Uuid, Instant)>;
type MonitorSnapshot = (ChunkTaskInfo, WorkerHeartbeats, Duration);

impl StallState {
    fn new() -> Self {
        Self {
            chunk_samples: HashMap::new(),
            chunk_last_bytes: HashMap::new(),
            chunk_last_progress: HashMap::new(),
            worker_heartbeats: HashMap::new(),
            retry_history: HashMap::new(),
            chunk_task_ids: HashMap::new(),
            chunk_generations: HashMap::new(),
        }
    }
}

/// The `StallDetector` subsystem observes transfer health.
///
/// It maintains rolling throughput windows and emits stall
/// events/recommendations via the `EventBus`. The internal state uses
/// `parking_lot::Mutex` to avoid an async-runtime yield on every hot-path
/// scheduler update.
pub struct StallDetectorSubsystem {
    state: Arc<Mutex<StallState>>,
    event_bus: EventBus,
    policy: Arc<dyn AdaptivePolicy>,
    metrics: Arc<dyn RuntimeMetrics>,
}

impl StallDetectorSubsystem {
    pub fn new(
        event_bus: EventBus,
        policy: Arc<dyn AdaptivePolicy>,
        metrics: Arc<dyn RuntimeMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(StallState::new())),
            event_bus,
            policy,
            metrics,
        })
    }

    /// Observe a chunk update from the scheduler.
    pub fn observe_chunk_update(&self, update: &ChunkUpdate) {
        let id = update.chunk.id;
        let mut s = self.state.lock();
        let prev = s.chunk_last_bytes.get(&id).copied().unwrap_or(0u64);
        let delta = update.chunk.downloaded_bytes.saturating_sub(prev);
        // Insert the task association first, before borrowing chunk_samples,
        // to avoid overlapping mutable borrows of `s`.
        s.chunk_task_ids.insert(id, update.chunk.task_id);
        s.chunk_generations.insert(id, update.generation);
        let sampler = s.chunk_samples.entry(id).or_insert_with(|| {
            RollingSampler::new(self.policy.short_window(), self.policy.long_window())
        });
        if delta > 0 {
            sampler.record(delta);
            s.chunk_last_bytes.insert(id, update.chunk.downloaded_bytes);
            s.chunk_last_progress.insert(id, Instant::now());
        } else {
            s.worker_heartbeats.insert(update.worker.id, Instant::now());
        }
        if update.chunk.retry_attempts > 0 {
            s.retry_history.insert(id, update.chunk.retry_attempts);
        }

        if update.chunk.state == ChunkState::Completed
            || update.chunk.state == ChunkState::Failed
            || update.chunk.state == ChunkState::Cancelled
        {
            s.chunk_samples.remove(&id);
            s.chunk_last_bytes.remove(&id);
            s.chunk_last_progress.remove(&id);
            s.retry_history.remove(&id);
            s.chunk_task_ids.remove(&id);
            s.chunk_generations.remove(&id);
        }
    }

    /// Observe an explicit worker heartbeat.
    pub fn observe_worker_heartbeat(&self, worker: &WorkerHandle) {
        self.state
            .lock()
            .worker_heartbeats
            .insert(worker.id, Instant::now());
    }

    /// Start the detector monitor loop.
    ///
    /// Returns a `JoinHandle` that may be awaited or aborted.
    pub fn start(
        self: Arc<Self>,
        shutdown: adm_storage::ShutdownToken,
    ) -> tokio::task::JoinHandle<()> {
        tracing::debug!(interval_ms = ?self.policy.monitor_interval().as_millis(), "stall.detector: starting monitor loop");
        tokio::spawn(async move {
            let interval = self.policy.monitor_interval();
            loop {
                tokio::time::sleep(interval).await;
                if shutdown.is_cancelled() {
                    tracing::debug!("stall.detector: shutdown signalled, exiting");
                    break;
                }
                self.run_once();
            }
        })
    }

    fn monitor_snapshot(&self) -> MonitorSnapshot {
        let state = self.state.lock();
        let info = state
            .chunk_samples
            .keys()
            .map(|&chunk_id| {
                (
                    chunk_id,
                    state
                        .chunk_task_ids
                        .get(&chunk_id)
                        .copied()
                        .unwrap_or(Uuid::nil()),
                    state.chunk_generations.get(&chunk_id).copied().unwrap_or(0),
                )
            })
            .collect();
        let workers = state
            .worker_heartbeats
            .iter()
            .map(|(&id, &heartbeat)| (id, heartbeat))
            .collect();
        let max_idle = self.policy.long_window() * 2;
        drop(state);
        (info, workers, max_idle)
    }

    fn evaluate_chunk(
        &self,
        chunk_id: Uuid,
    ) -> Option<(f64, f64, Option<u128>, Option<StallReason>)> {
        let mut state = self.state.lock();
        let (short_rate, long_rate) = {
            let sampler = state.chunk_samples.get_mut(&chunk_id)?;
            (sampler.short_rate_bps(), sampler.long_rate_bps())
        };
        let last_progress = state.chunk_last_progress.get(&chunk_id).copied();
        let last_progress_secs = last_progress.map(|ts| ts.elapsed().as_millis());
        drop(state);

        let mut detected = last_progress
            .filter(|ts| ts.elapsed() <= self.policy.inactivity_timeout())
            .map_or(Some(StallReason::NoProgressTimeout), |_| None);
        if detected.is_none()
            && short_rate < self.policy.short_rate_floor_bps()
            && long_rate < self.policy.long_rate_floor_bps()
        {
            detected = Some(StallReason::ThroughputBelowThreshold);
        }
        Some((short_rate, long_rate, last_progress_secs, detected))
    }

    fn collect_chunk_events(
        &self,
        chunk_info: Vec<(Uuid, Uuid, u64)>,
        now_secs: u64,
    ) -> Vec<(StallEvent, AdaptiveRecommendation)> {
        let mut events = Vec::new();
        for (chunk_id, task_id, generation) in chunk_info {
            let Some((short_rate, long_rate, last_progress_secs, Some(reason))) =
                self.evaluate_chunk(chunk_id)
            else {
                continue;
            };

            // Choose recommendation based on reason
            let (action, ranges) = match reason {
                StallReason::ThroughputBelowThreshold | StallReason::NoProgressTimeout => {
                    ("split_chunk".to_string(), Some(vec![]))
                }
                _ => ("cancel_and_retry".to_string(), None),
            };

            tracing::debug!(
                chunk_id = ?chunk_id,
                reason = ?reason,
                action = %action,
                generation = generation,
                short_bps = short_rate,
                long_bps = long_rate,
                "stall.detector: detected stall and recommending action",
            );

            let recommendation = match reason {
                StallReason::ThroughputBelowThreshold | StallReason::NoProgressTimeout => {
                    AdaptiveRecommendation::SplitChunk {
                        chunk_id,
                        ranges: vec![], // Scheduler will compute midpoint
                    }
                }
                _ => AdaptiveRecommendation::CancelAndRetry { chunk_id },
            };

            events.push((
                StallEvent {
                    task_id,
                    chunk_id,
                    worker_id: None,
                    reason: reason.clone(),
                    action,
                    generation,
                    short_rate_bps: short_rate,
                    long_rate_bps: long_rate,
                    last_progress_secs,
                    timestamp_secs: now_secs,
                    ranges,
                },
                recommendation,
            ));
            self.metrics.record_stall(&reason);
        }
        events
    }

    fn collect_worker_events(
        &self,
        worker_states: &[(Uuid, Instant)],
        now_secs: u64,
    ) -> Vec<(StallEvent, AdaptiveRecommendation)> {
        let worker_timeout = self.policy.heartbeat_timeout();
        worker_states
            .iter()
            .filter(|(_, heartbeat)| heartbeat.elapsed() > worker_timeout)
            .map(|(worker_id, _)| {
                self.metrics.record_stall(&StallReason::HeartbeatLost);
                (
                    StallEvent {
                        task_id: Uuid::nil(),
                        chunk_id: Uuid::nil(),
                        worker_id: Some(*worker_id),
                        reason: StallReason::HeartbeatLost,
                        action: "recycle_worker".to_string(),
                        generation: 0,
                        short_rate_bps: 0.0,
                        long_rate_bps: 0.0,
                        last_progress_secs: None,
                        timestamp_secs: now_secs,
                        ranges: None,
                    },
                    AdaptiveRecommendation::RecycleWorker {
                        worker_id: *worker_id,
                    },
                )
            })
            .collect()
    }

    fn prune_stale_entries(&self, max_idle: Duration) {
        let now = Instant::now();
        let mut state = self.state.lock();
        state
            .worker_heartbeats
            .retain(|_, hb| hb.elapsed() <= max_idle);
        state
            .chunk_last_progress
            .retain(|_, progress_ts| now.duration_since(*progress_ts) <= max_idle);
    }

    fn emit_events(&self, events: Vec<(StallEvent, AdaptiveRecommendation)>) {
        for (event, recommendation) in events {
            let payload = serde_json::to_value(&event).unwrap_or_else(|_| json!({}));
            tracing::debug!(chunk_id = ?event.chunk_id, reason = ?event.reason, "stall.detector: emitting stall.recommendation");
            self.event_bus.publish("stall.recommendation", payload);
            self.metrics.record_recommendation(&recommendation);
        }
    }

    fn run_once(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let (chunk_info, worker_states, max_idle) = self.monitor_snapshot();
        let mut events = self.collect_chunk_events(chunk_info, now_secs);
        events.extend(self.collect_worker_events(&worker_states, now_secs));
        self.prune_stale_entries(max_idle);
        self.emit_events(events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventBus, WorkerHandle};

    #[tokio::test]
    async fn detects_no_progress_and_emits_event() {
        let bus = EventBus::new(16);
        let policy = Arc::new(DefaultAdaptivePolicy::fast_test());
        let metrics = Arc::new(NoopMetrics);
        let detector = StallDetectorSubsystem::new(bus.clone(), policy, metrics);
        let _jh = detector
            .clone()
            .start(adm_storage::ShutdownToken::default());

        // subscribe to event bus
        let mut rx = bus.subscribe();

        // create a chunk update but don't send subsequent progress
        let chunk = crate::DownloadChunk::new(Uuid::new_v4(), 0, 0, 1024);
        let worker = WorkerHandle::default();
        let update = ChunkUpdate {
            chunk: chunk.clone(),
            event: "chunk.downloading".to_string(),
            worker: worker.clone(),
            generation: 0,
            discovered_total_bytes: None,
        };
        detector.observe_chunk_update(&update);

        // Wait for recommendation event
        let mut seen = false;
        for _ in 0..20 {
            if let Ok(evt) = rx.recv().await {
                if evt.topic == "stall.recommendation" {
                    seen = true;
                    break;
                }
            }
        }
        assert!(seen, "expected a stall.recommendation event");
    }
}
