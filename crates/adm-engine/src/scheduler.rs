use crate::runtime::{ThroughputSampler, TransferGraph, TransferStateAggregator, WorkerAssignment};
use crate::throughput_balancer::{score_chunk, ThroughputBalancer};
use crate::worker::{DownloadJob, WorkerPool, WorkerReservation};
use crate::{
    verify_checksum, ChecksumAlgorithm, ChunkAssignment, ChunkState, ChunkUpdate, CompactionConfig,
    DownloadChunk, DownloadState, DownloadTask, EngineContext, FileWriteCoordinator,
    SchedulerSnapshot, TaskScheduleSnapshot,
};
use adm_network::{BandwidthLimiter, CancellationToken, NetworkRequest};
use adm_storage::{ChunkRepository, HistoryRepository, TaskRepository};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use uuid::Uuid;

#[async_trait]
pub trait Scheduler: Send + Sync {
    async fn schedule_task(
        &self,
        task: DownloadTask,
        context: Arc<EngineContext>,
        task_handle: Arc<crate::runtime::TaskHandle>,
    ) -> Result<()>;
    async fn restore_pending(&self, context: Arc<EngineContext>) -> Result<SchedulerSnapshot>;
}

#[derive(Debug, Clone)]
pub struct BasicScheduler {
    pub worker_pool: Arc<WorkerPool>,
    pub chunk_size: u64,
}

impl BasicScheduler {
    #[must_use]
    pub const fn new(worker_pool: Arc<WorkerPool>, chunk_size: u64) -> Self {
        Self {
            worker_pool,
            chunk_size,
        }
    }

    fn plan_chunks(&self, task: &DownloadTask, total_bytes: Option<u64>) -> Vec<DownloadChunk> {
        let mut chunks = Vec::new();
        if let Some(total) = total_bytes {
            let mut offset = 0;
            let mut index = 0;
            while offset < total {
                let length = std::cmp::min(self.chunk_size, total - offset);
                chunks.push(DownloadChunk::new(task.id, index, offset, length));
                offset += length;
                index += 1;
            }
        } else {
            chunks.push(DownloadChunk::new(task.id, 0, 0, self.chunk_size));
        }
        chunks
    }

    fn chunk_priority(&self, chunk: &DownloadChunk) -> i64 {
        let state_bonus = match chunk.state {
            ChunkState::Pending => 100,
            ChunkState::Retrying => 50,
            _ => 0,
        };
        // Exponential penalty: each additional retry attempt doubles the
        // priority reduction (capped at 500 to prevent negative priorities
        // from flipping the ordering of chunks with many retries).
        // Formula: min(2^attempts * base_penalty, cap)
        // attempt 0 → 0, 1 → 15, 2 → 30, 3 → 60, 4 → 120, 5+ → 240 → cap
        let base_penalty: i64 = 15;
        let retry_penalty: i64 = if chunk.retry_attempts == 0 {
            0
        } else {
            let exp = 1i64 << (chunk.retry_attempts.saturating_sub(1)).min(5) as u32;
            (base_penalty * exp).min(500)
        };
        let size_penalty = i64::try_from(chunk.length / self.chunk_size).unwrap_or(i64::MAX);
        1000 + state_bonus - retry_penalty - size_penalty
    }

    fn should_split_chunk(&self, chunk: &DownloadChunk, has_total: bool) -> bool {
        const MIN_SPLIT_LENGTH: u64 = 8 * 1024;
        // Only split chunks that are still active (Downloading) from the stall monitor,
        // or fresh Pending chunks. Never split Retrying/Reserved/Connecting — those are
        // recovered from a crash and already have the correct byte range; splitting them
        // would create duplicate IDs and break restart_does_not_create_duplicate_chunks.
        let splittable_state =
            chunk.state == ChunkState::Pending || chunk.state == ChunkState::Downloading;
        if !splittable_state {
            return false;
        }
        has_total && chunk.length >= self.chunk_size && chunk.length >= MIN_SPLIT_LENGTH
    }

    fn should_split_initial_chunk(&self, chunk: &DownloadChunk, has_total: bool) -> bool {
        // For the initial split pass (split_large_chunks), only split fresh Pending chunks.
        chunk.state == ChunkState::Pending && self.should_split_chunk(chunk, has_total)
    }

    fn next_chunk_index(&self, chunk_map: &HashMap<Uuid, DownloadChunk>) -> u32 {
        chunk_map
            .values()
            .map(|chunk| chunk.index)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
    }

    fn split_chunk(&self, chunk: &DownloadChunk, first_index: u32) -> Vec<DownloadChunk> {
        let split_len = std::cmp::max(1, chunk.length / 2);
        let first = DownloadChunk::new(chunk.task_id, first_index, chunk.offset, split_len);
        let second = DownloadChunk::new(
            chunk.task_id,
            first_index + 1,
            chunk.offset + split_len,
            chunk.length - split_len,
        );
        vec![first, second]
    }

    /// Load persisted chunks or plan fresh ones with HEAD-probe for file size.
    async fn load_chunks(
        &self,
        context: &Arc<EngineContext>,
        task: &mut DownloadTask,
    ) -> Result<Vec<DownloadChunk>> {
        let persisted = context.storage.load_chunks_for_task(task.id).await?;
        if !persisted.is_empty() {
            return Ok(persisted
                .into_iter()
                .map(DownloadChunk::from_persisted)
                .collect::<Result<Vec<_>, _>>()?);
        }

        // Fresh download — probe the server for Content-Length + Accept-Ranges
        // before splitting so we don't create a single monolithic chunk.
        if task.total_bytes.is_none() {
            match context.network.head(&task.url).await {
                Ok(info) => {
                    if let Some(len) = info.content_length {
                        tracing::debug!(
                            task_id = %task.id,
                            content_length = len,
                            accept_ranges = info.accept_ranges,
                            "HEAD probe: discovered file size",
                        );
                        task.total_bytes = Some(len);
                        context.storage.save_task(task.to_persisted()).await?;
                    }
                    if !info.accept_ranges {
                        tracing::debug!(
                            task_id = %task.id,
                            "server does not accept ranges; single-chunk fallback",
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %task.id,
                        error = %e,
                        "HEAD probe failed; falling back to single-chunk download",
                    );
                }
            }
        }

        Ok(self.plan_chunks(task, task.total_bytes))
    }

    async fn save_chunks(
        &self,
        context: &Arc<EngineContext>,
        chunks: &[DownloadChunk],
    ) -> Result<()> {
        for chunk in chunks {
            if context.storage.is_shutdown() {
                return Ok(());
            }
            if let Err(e) = context.storage.save_chunk(chunk.clone()).await {
                if e.to_string().contains("context shut down") {
                    return Ok(());
                }
                return Err(e);
            }
        }
        Ok(())
    }

    async fn split_large_chunks(
        &self,
        context: &Arc<EngineContext>,
        chunks: Vec<DownloadChunk>,
        has_total: bool,
    ) -> Result<Vec<DownloadChunk>> {
        let mut expanded = Vec::new();
        let mut next_index = chunks
            .iter()
            .map(|chunk| chunk.index)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        for chunk in chunks {
            if self.should_split_initial_chunk(&chunk, has_total) {
                let split_children = self.split_chunk(&chunk, next_index);
                next_index = next_index.saturating_add(split_children.len() as u32);
                let mut cancelled_chunk = chunk.clone();
                cancelled_chunk.set_state(ChunkState::Cancelled);
                self.persist_chunk_event(context, &cancelled_chunk, "chunk.split")
                    .await?;
                for child in &split_children {
                    self.persist_chunk_event(context, child, "chunk.created")
                        .await?;
                }
                expanded.extend(split_children);
            } else {
                expanded.push(chunk);
            }
        }

        Ok(expanded)
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_chunk_job(
        &self,
        task: &DownloadTask,
        mut chunk: DownloadChunk,
        worker_reservation: WorkerReservation,
        context: &Arc<EngineContext>,
        update_tx: &mpsc::UnboundedSender<ChunkUpdate>,
        handles: &mut Vec<tokio::task::JoinHandle<Result<DownloadChunk>>>,
        chunk_map: &mut HashMap<Uuid, DownloadChunk>,
        cancellation_tokens: &mut HashMap<Uuid, CancellationToken>,
        transfer_graph: &mut TransferGraph,
        write_coordinator: Arc<FileWriteCoordinator>,
        bandwidth_limiter: Option<BandwidthLimiter>,
        task_cancel_token: CancellationToken,
        generation: u64,
    ) -> Result<()> {
        let worker = worker_reservation.handle();
        let cancel_token = CancellationToken::new();
        cancellation_tokens.insert(chunk.id, cancel_token.clone());
        chunk.assign_to(worker.clone());
        chunk.set_state(ChunkState::Reserved);
        self.persist_chunk_event(context, &chunk, "chunk.reserved")
            .await?;

        context
            .lease_registry
            .register(chunk.id, task.id, Some(worker.id));

        transfer_graph.add_chunk(crate::ChunkLease::new(
            chunk.id,
            task.id,
            chunk.offset,
            chunk.length,
        ));
        transfer_graph.assign_worker(chunk.id, WorkerAssignment::new(worker.id, chunk.id));
        chunk_map.insert(chunk.id, chunk.clone());

        let request_start = chunk.offset + chunk.downloaded_bytes;
        let request_end = chunk.offset + chunk.length - 1;
        let mut request = NetworkRequest::new(task.url.clone(), Some((request_start, request_end)));

        // Merge task-level headers into the outgoing request. Any existing
        // request headers are treated as defaults; task headers override
        // them deterministically. Validation and normalization are applied.
        if !task.headers.is_empty() {
            // Use engine headers utility to merge task headers into request.headers
            if let Err(e) = crate::headers::merge_into_existing(&mut request.headers, &task.headers)
            {
                tracing::warn!(task_id = %task.id, error = %e, "invalid task headers; skipping header merge");
            }
        }

        let job_chunk = chunk.clone();

        let job = DownloadJob::new(
            task.clone(),
            job_chunk,
            request,
            context.network.clone(),
            update_tx.clone(),
            context.event_bus.clone(),
            worker.clone(),
            write_coordinator,
            cancel_token,
            context.shutdown.clone(),
            worker_reservation,
            bandwidth_limiter,
            Some(task_cancel_token),
            generation,
        );
        handles.push(self.worker_pool.spawn_job(job));

        // Proactively inform the scheduler's update channel of the worker's
        // initial state change so the monitor can observe progress even if
        // the worker's update messages are delayed or raced.
        let mut initial_chunk = chunk.clone();
        initial_chunk.set_state(ChunkState::Connecting);
        initial_chunk.touch();
        let _ = update_tx.send(ChunkUpdate {
            chunk: initial_chunk,
            event: "chunk.connecting".to_string(),
            worker: worker.clone(),
            discovered_total_bytes: None,
            generation: 0,
        });

        Ok(())
    }

    async fn persist_chunk_event(
        &self,
        context: &Arc<EngineContext>,
        chunk: &DownloadChunk,
        event: &str,
    ) -> Result<()> {
        // If the context has been shut down (dropped), storage calls will
        // return a "context shut down" error.  That is expected for tasks
        // that were spawned by the previous session and are still running —
        // we simply stop writing rather than propagating a spurious error.
        if context.storage.is_shutdown() {
            return Ok(());
        }
        tracing::debug!(
            task_id = %chunk.task_id,
            chunk_id = %chunk.id,
            event = event,
            state = %chunk.state.as_str(),
            downloaded_bytes = chunk.downloaded_bytes,
            "persisting chunk event",
        );
        if let Err(e) = context.storage.save_chunk(chunk.clone()).await {
            if e.to_string().contains("context shut down") {
                tracing::debug!(
                    task_id = %chunk.task_id,
                    chunk_id = %chunk.id,
                    "context shutdown during save_chunk",
                );
                return Ok(());
            }
            return Err(e);
        }
        if let Err(e) = context
            .storage
            .append_history(chunk.task_id, event, Self::now_ts())
            .await
        {
            if e.to_string().contains("context shut down") {
                tracing::debug!(
                    task_id = %chunk.task_id,
                    chunk_id = %chunk.id,
                    "context shutdown during append_history",
                );
                return Ok(());
            }
            return Err(e);
        }
        context.event_bus.publish(
            event,
            json!({
                "task_id": chunk.task_id.to_string(),
                "chunk_id": chunk.id.to_string(),
                "state": chunk.state.as_str(),
                "downloaded_bytes": chunk.downloaded_bytes,
                "offset": chunk.offset,
                "length": chunk.length,
            }),
        );
        tracing::debug!(
            task_id = %chunk.task_id,
            chunk_id = %chunk.id,
            event = event,
            state = %chunk.state.as_str(),
            "persisted chunk event",
        );

        // Additional tracing for important state transitions to aid flaky-test diagnosis
        if event == "chunk.cancelled" || event == "chunk.split" {
            let last_progress_ms = chunk
                .last_progress_instant
                .map(|i| Instant::now().duration_since(i).as_millis());
            tracing::info!(
                task_id = %chunk.task_id,
                chunk_id = %chunk.id,
                event = event,
                last_progress_ms = ?last_progress_ms,
                "noted special chunk event",
            );
        }
        Ok(())
    }

    async fn update_task_progress(
        &self,
        context: &Arc<EngineContext>,
        task_id: Uuid,
    ) -> Result<()> {
        let persisted_chunks = context.storage.load_chunks_for_task(task_id).await?;
        let total_downloaded: u64 = persisted_chunks.iter().map(|c| c.downloaded_bytes).sum();
        if let Some(task) = context.storage.load_task(task_id).await? {
            let mut task = DownloadTask::from_persisted(task)?;
            task.downloaded_bytes = total_downloaded;
            task.touch();
            context.storage.save_task(task.to_persisted()).await?;
        }
        Ok(())
    }

    fn now_ts() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .try_into()
            .unwrap_or(i64::MAX)
    }
}

#[async_trait]
impl Scheduler for BasicScheduler {
    async fn schedule_task(
        &self,
        task: DownloadTask,
        context: Arc<EngineContext>,
        task_handle: Arc<crate::runtime::TaskHandle>,
    ) -> Result<()> {
        let task_cancel_token = task_handle.cancel_token.clone();
        let generation = task_handle.generation;
        tracing::debug!(task_id = %task.id, generation, "schedule_task entry");
        let mut task = task.clone();
        let chunks = self.load_chunks(&context, &mut task).await?;
        let mut chunks = self
            .split_large_chunks(&context, chunks, task.total_bytes.is_some())
            .await?;

        let save_path = task.resolved_save_path(&context.download_dir);
        let write_coordinator = Arc::new(
            FileWriteCoordinator::load(
                task.id,
                save_path.clone(),
                task.total_bytes,
                context.storage.clone(),
                context.event_bus.clone(),
            )
            .await?,
        );

        for chunk in &mut chunks {
            if !chunk.state.is_terminal() {
                let end = chunk.offset + chunk.length;
                if write_coordinator
                    .is_range_completed(chunk.offset, end)
                    .await
                {
                    chunk.downloaded_bytes = chunk.length;
                    chunk.set_state(ChunkState::Completed);
                }
            }
        }

        self.save_chunks(&context, &chunks).await?;

        tracing::debug!(task_id = %task.id, "write_coordinator loaded");
        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<ChunkUpdate>();
        let mut handles = Vec::with_capacity(chunks.len());
        let mut chunk_map: HashMap<Uuid, DownloadChunk> = HashMap::with_capacity(chunks.len());
        let mut cancellation_tokens: HashMap<Uuid, CancellationToken> =
            HashMap::with_capacity(chunks.len());
        let mut transfer_graph = TransferGraph::new(task.id);
        let mut aggregator = TransferStateAggregator::new(task.id);
        let mut throughput_sampler = ThroughputSampler::new(Duration::from_secs(5));
        let mut monitor_interval = tokio::time::interval(Duration::from_millis(200));
        monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_update_at = Instant::now();
        let task_start = std::time::Instant::now();
        let mut lease_rx = context.event_bus.subscribe();
        let mut throughput_balancer = ThroughputBalancer::new(Duration::from_secs(30));
        let compaction_cfg = CompactionConfig::default();
        // Stall timeout: chunks that have not made progress within this window
        // are considered stalled and subject to cancellation / splitting.
        // Kept as a named constant so it is easy to tune without hunting for
        // magic numbers across the file.
        const STALL_TIMEOUT: Duration = Duration::from_millis(1500);

        // Channel used by retry spawns to register their CancellationToken back
        // into the main cancellation_tokens map so the stall monitor can cancel
        // them when needed. Without this the retry spawn's token is isolated in a
        // local map that is invisible to the main scheduler loop.
        let (retry_token_tx, mut retry_token_rx) =
            mpsc::unbounded_channel::<(Uuid, CancellationToken)>();

        // Channel for delayed retries. Chunks that fail are sent to a sleeper
        // task and then returned here to be re-added to pending_chunks.
        let (retry_tx, mut retry_rx) = mpsc::unbounded_channel::<DownloadChunk>();

        // After crash/restart, chunks may be persisted as `failed` even though
        // they are still resumable for a restored task. Keep everything except
        // truly-finished chunks; `cancelled` is excluded to avoid reviving
        // split-parent placeholders.
        let mut pending_chunks: Vec<DownloadChunk> = chunks
            .into_iter()
            .filter(|chunk| {
                chunk.state != ChunkState::Completed && chunk.state != ChunkState::Cancelled
            })
            .collect();

        // Fast path: if every non-cancelled chunk is already completed (e.g. the
        // previous session finished but was dropped before the event was observed),
        // emit download.completed immediately instead of re-running the full loop.
        if pending_chunks.is_empty() {
            let persisted_chunks = context.storage.load_chunks_for_task(task.id).await?;
            tracing::debug!(
                task_id = %task.id,
                persisted_chunks = persisted_chunks.len(),
                "no pending chunks fast path",
            );
            for (idx, chunk) in persisted_chunks.iter().enumerate() {
                tracing::debug!(
                    task_id = %task.id,
                    chunk_index = idx,
                    chunk_id = %chunk.id,
                    state = %chunk.state,
                    downloaded_bytes = chunk.downloaded_bytes,
                    retry_attempts = chunk.retry_attempts,
                    generation = 0,
                    "persisted chunk state",
                );
            }
            let non_cancelled: Vec<_> = persisted_chunks
                .iter()
                .filter(|c| c.state != ChunkState::Cancelled)
                .collect();
            let already_done = !non_cancelled.is_empty()
                && non_cancelled
                    .iter()
                    .all(|c| c.state == ChunkState::Completed);
            if already_done {
                let mut task = task.clone();
                task.downloaded_bytes = persisted_chunks.iter().map(|c| c.downloaded_bytes).sum();
                task.set_state(DownloadState::Completed);
                context.storage.save_task(task.to_persisted()).await?;
                let persisted_count = persisted_chunks.len();
                tracing::debug!(
                    task_id = %task.id,
                    persisted_chunks = persisted_count,
                    total_downloaded = task.downloaded_bytes,
                    "task already completed on restart",
                );
                use ipc::contracts::{now_ms, DownloadCompletedEvent};
                let elapsed = task_start.elapsed();
                let event = DownloadCompletedEvent {
                    task_id: task.id,
                    total_bytes: task.total_bytes.unwrap_or(task.downloaded_bytes),
                    duration_secs: elapsed.as_secs_f64(),
                    average_throughput_bps: 0.0,
                    timestamp_ms: now_ms(),
                };
                tracing::debug!(task_id = %task.id, "publishing download.completed from fast path");
                context.event_bus.publish(
                    "download.completed",
                    serde_json::to_value(event).unwrap_or_else(|_| json!({})),
                );
                return Ok(());
            }
        } else {
            tracing::debug!(
                task_id = %task.id,
                pending_chunks = pending_chunks.len(),
                "pending chunks restored",
            );
            for (idx, chunk) in pending_chunks.iter().enumerate() {
                tracing::debug!(
                    task_id = %task.id,
                    chunk_index = idx,
                    chunk_id = %chunk.id,
                    state = ?chunk.state,
                    downloaded_bytes = chunk.downloaded_bytes,
                    length = chunk.length,
                    "restored pending chunk",
                );
            }
        }

        // ── Throughput-aware chunk ordering ───────────────────────────────────
        // Use ThroughputBalancer scoring when multiple chunks are present.
        // Falls back to the legacy priority function for single-chunk tasks.
        // The balancer starts with no per-worker history (fresh task), so the
        // score_chunk function's sparse-data path applies (index-order FIFO),
        // which is identical to the previous behaviour.  As workers report
        // bytes during execution, future reschedule passes will use real data.
        if pending_chunks.len() > 1 {
            pending_chunks.sort_by_key(|chunk| -score_chunk(chunk, &throughput_balancer));
        } else {
            pending_chunks.sort_by_key(|chunk| -self.chunk_priority(chunk));
        }
        let mut pending_chunks: std::collections::VecDeque<DownloadChunk> =
            pending_chunks.into_iter().collect();

        // Spawn only as many initial chunk jobs as workers are currently idle.
        let task_bandwidth_limiter = task.speed_limit_kbps.and_then(|kbps| {
            if kbps == 0 {
                None
            } else {
                Some(BandwidthLimiter::new(kbps.saturating_mul(125)))
            }
        });

        while let Some(chunk) = pending_chunks.pop_front() {
            let available_workers = self.worker_pool.available_worker_ids();
            if available_workers.is_empty() {
                pending_chunks.push_front(chunk);
                break;
            }
            let selected_worker = throughput_balancer.best_worker(&available_workers).copied();
            let worker_reservation = self.worker_pool.reserve_worker(selected_worker).await?;
            self.spawn_chunk_job(
                &task,
                chunk,
                worker_reservation,
                &context,
                &update_tx,
                &mut handles,
                &mut chunk_map,
                &mut cancellation_tokens,
                &mut transfer_graph,
                write_coordinator.clone(),
                task_bandwidth_limiter.clone(),
                task_cancel_token.clone(),
                generation,
            )
            .await?;
        }

        tracing::debug!(task_id = %task.id, spawned_initial = handles.len(), pending_chunks = pending_chunks.len(), "spawned initial chunk jobs");
        tracing::debug!(
            task_id = %task.id,
            initial_handles = handles.len(),
            "scheduler reached while loop",
        );

        // running_jobs counts active chunk jobs; retry increments it before spawning.
        // The main loop stays alive until all jobs (initial + retried) finish.
        let running_jobs = Arc::new(std::sync::atomic::AtomicUsize::new(handles.len()));
        let mut forced_abort = false;
        tracing::debug!(
            task_id = %task.id,
            initial_handles = handles.len(),
            "entering scheduler main loop",
        );
        'scheduler_loop: while running_jobs.load(std::sync::atomic::Ordering::SeqCst) > 0
            || !pending_chunks.is_empty()
        {
            tokio::select! {
                // Chunks returning from a backoff delay.
                Some(chunk) = retry_rx.recv() => {
                    tracing::info!(task_id = %task.id, chunk_id = %chunk.id, "chunk returned from backoff, re-queuing");
                    pending_chunks.push_back(chunk);
                    // Trigger a worker check immediately.
                    let available_workers = self.worker_pool.available_worker_ids();
                    if !available_workers.is_empty() {
                        if let Some(next_chunk) = pending_chunks.pop_front() {
                            let selected_worker = throughput_balancer.best_worker(&available_workers).copied();
                            let worker_reservation = self.worker_pool.reserve_worker(selected_worker).await?;
                            self.spawn_chunk_job(
                                &task,
                                next_chunk,
                                worker_reservation,
                                &context,
                                &update_tx,
                                &mut handles,
                                &mut chunk_map,
                                &mut cancellation_tokens,
                                &mut transfer_graph,
                                write_coordinator.clone(),
                                task_bandwidth_limiter.clone(),
                                task_cancel_token.clone(),
                                generation,
                            )
                            .await?;
                            running_jobs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                }

                // Register cancellation tokens sent back from retry spawns so the
                // stall monitor can cancel hung retry chunks just like initial ones.
                Some((chunk_id, token)) = retry_token_rx.recv() => {
                    last_update_at = Instant::now();
                    cancellation_tokens.insert(chunk_id, token);
                }
                () = task_cancel_token.cancelled() => {
                    forced_abort = true;
                    break 'scheduler_loop;
                }
                () = context.shutdown.cancelled() => {
                    forced_abort = true;
                    break 'scheduler_loop;
                }
                maybe_lease = lease_rx.recv() => {
                    match maybe_lease {
                        Ok(evt) if evt.topic == "adaptive.lease_expired" => {
                            let chunk_id = evt
                                .data
                                .get("chunk_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| Uuid::parse_str(s).ok());
                            let task_id = evt
                                .data
                                .get("task_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| Uuid::parse_str(s).ok());
                            if let (Some(chunk_id), Some(event_task_id)) = (chunk_id, task_id) {
                                if event_task_id == task.id {
                                    if let Some(token) = cancellation_tokens.get(&chunk_id) {
                                        tracing::warn!(
                                            task_id = %task.id,
                                            chunk_id = %chunk_id,
                                            "lease expired for chunk, cancelling worker token",
                                        );
                                        token.cancel();
                                        context.lease_registry.release(chunk_id);
                                    }
                                }
                            }
                        }
                        // ── Worker heartbeat ─────────────────────────────────────────
                        // Forward heartbeat events into StallDetectorSubsystem so it
                        // can distinguish a slow-but-alive worker from a silently-hung
                        // one (HeartbeatLost stall reason).
                        Ok(evt) if evt.topic == "worker.heartbeat" => {
                            let worker_id = evt
                                .data
                                .get("worker_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| Uuid::parse_str(s).ok());
                            if let Some(wid) = worker_id {
                                let chunk_id = evt.data.get("chunk_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()).unwrap_or_default();
                                let gen = evt.data.get("generation").and_then(|v| v.as_u64()).unwrap_or(0);
                                context.stall_detector.observe_worker_heartbeat(
                                    &crate::WorkerHandle { id: wid },
                                    chunk_id,
                                    gen,
                                );
                            }
                        }
                        // ── Stall recommendations ────────────────────────────────────
                        // The StallDetectorSubsystem emits `stall.recommendation` events
                        // when it detects throughput-based stalls.  We handle two
                        // actionable recommendations here:
                        //
                        //   RecycleWorker  — cancel the worker's current chunk so it is
                        //                    re-scheduled to a different (better) worker.
                        //   SplitChunk     — inline split logic already handles timeout
                        //                    stalls; log and let it run its course.
                        Ok(evt) if evt.topic == "stall.recommendation" => {
                            // 1. Generation-aware validation (Closed-Loop Integrity)
                            let rec_generation = evt.data.get("generation").and_then(|v| v.as_u64()).unwrap_or(0);
                            if rec_generation != generation {
                                tracing::debug!(
                                    task_id = %task.id,
                                    current_gen = generation,
                                    rec_gen = rec_generation,
                                    "stall.recommendation: ignoring stale recommendation from previous generation",
                                );
                                continue;
                            }

                            // Parse the AdaptiveRecommendation action field
                            let action = evt.data.get("action").and_then(|v| v.as_str()).unwrap_or("");
                            match action {
                                "recycle_worker" => {
                                    // Cancel all active chunks assigned to this worker
                                    // so they are re-tried on a healthier worker.
                                    let worker_id = evt
                                        .data
                                        .get("worker_id")
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Uuid::parse_str(s).ok());
                                    if let Some(wid) = worker_id {
                                        for (chunk_id, token) in &cancellation_tokens {
                                            if chunk_map.get(chunk_id)
                                                .and_then(|c| c.assigned_worker.as_ref())
                                                .map(|w| w.id == wid)
                                                .unwrap_or(false)
                                            {
                                                tracing::info!(
                                                    task_id = %task.id,
                                                    chunk_id = %chunk_id,
                                                    worker_id = %wid,
                                                    generation = generation,
                                                    "stall.recommendation: recycling worker, cancelling chunk",
                                                );
                                                token.cancel();
                                                context.event_bus.publish(
                                                    "worker.recycled",
                                                    serde_json::json!({
                                                        "worker_id": wid.to_string(),
                                                        "chunk_id":  chunk_id.to_string(),
                                                        "task_id":   task.id.to_string(),
                                                        "generation": generation,
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                }

                                // ── SplitChunk recommendation ─────────────────────────────────────
                                "split_chunk" => {
                                    let rec_chunk_id = evt
                                        .data
                                        .get("chunk_id")
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Uuid::parse_str(s).ok());

                                    if let Some(cid) = rec_chunk_id {
                                        // Clone to release the immutable borrow on chunk_map
                                        // before we take mutable references below.
                                        let maybe_chunk = chunk_map.get(&cid).cloned();

                                        match maybe_chunk {
                                            None => {
                                                tracing::debug!(
                                                    task_id = %task.id,
                                                    chunk_id = %cid,
                                                    "stall.recommendation: split_chunk — chunk not found",
                                                );
                                            }
                                            Some(ref chunk) if chunk.state.is_terminal() => {
                                                tracing::debug!(
                                                    task_id = %task.id,
                                                    chunk_id = %cid,
                                                    state = %chunk.state.as_str(),
                                                    "stall.recommendation: split_chunk — already terminal",
                                                );
                                            }
                                            Some(ref chunk)
                                                if !self.should_split_chunk(
                                                    chunk,
                                                    task.total_bytes.is_some(),
                                                ) =>
                                            {
                                                tracing::debug!(
                                                    task_id = %task.id,
                                                    chunk_id = %cid,
                                                    "stall.recommendation: split_chunk — does not meet criteria",
                                                );
                                            }
                                            Some(chunk) => {
                                                tracing::info!(
                                                    task_id = %task.id,
                                                    chunk_id = %cid,
                                                    generation = generation,
                                                    "stall.recommendation: executing split_chunk",
                                                );

                                                if let Some(token) = cancellation_tokens.get(&cid) {
                                                    token.cancel();
                                                }

                                                let mut cancelled_parent = chunk.clone();
                                                cancelled_parent.set_state(ChunkState::Cancelled);
                                                chunk_map.insert(cid, cancelled_parent.clone());
                                                self.persist_chunk_event(
                                                    &context,
                                                    &cancelled_parent,
                                                    "chunk.split",
                                                )
                                                .await?;

                                                let next_index =
                                                    self.next_chunk_index(&chunk_map);
                                                let detector_ranges: Option<Vec<(u64, u64)>> =
                                                    evt.data
                                                        .get("ranges")
                                                        .and_then(|v| {
                                                            serde_json::from_value(v.clone()).ok()
                                                        });

                                                let split_children: Vec<DownloadChunk> =
                                                    match detector_ranges.filter(|r| r.len() >= 2) {
                                                        Some(ranges) => ranges
                                                            .into_iter()
                                                            .enumerate()
                                                            .map(|(i, (offset, length))| {
                                                                DownloadChunk::new(
                                                                    chunk.task_id,
                                                                    next_index + i as u32,
                                                                    offset,
                                                                    length,
                                                                )
                                                            })
                                                            .collect(),
                                                        None => {
                                                            self.split_chunk(&chunk, next_index)
                                                        }
                                                    };

                                                for child in split_children {
                                                    self.persist_chunk_event(
                                                        &context,
                                                        &child,
                                                        "chunk.created",
                                                    )
                                                    .await?;
                                                    running_jobs.fetch_add(
                                                        1,
                                                        std::sync::atomic::Ordering::SeqCst,
                                                    );
                                                    let available =
                                                        self.worker_pool.available_worker_ids();
                                                    let selected =
                                                        throughput_balancer.best_worker(&available).copied();
                                                    let reservation =
                                                        self.worker_pool.reserve_worker(selected).await?;
                                                    self.spawn_chunk_job(
                                                        &task,
                                                        child,
                                                        reservation,
                                                        &context,
                                                        &update_tx,
                                                        &mut handles,
                                                        &mut chunk_map,
                                                        &mut cancellation_tokens,
                                                        &mut transfer_graph,
                                                        write_coordinator.clone(),
                                                        task_bandwidth_limiter.clone(),
                                                        task_cancel_token.clone(),
                                                        generation,
                                                    )
                                                    .await?;
                                                }

                                                context.event_bus.publish(
                                                    "chunk.split_recommendation_applied",
                                                    json!({
                                                        "task_id":  task.id.to_string(),
                                                        "chunk_id": cid.to_string(),
                                                        "generation": generation,
                                                        "source":   "stall.recommendation",
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                }

                                _ => {
                                    tracing::debug!(
                                        topic = "stall.recommendation",
                                        action = action,
                                        "scheduler: unrecognised recommendation action — ignoring",
                                    );
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(RecvError::Lagged(count)) => {
                            tracing::warn!(lagged = count, "lease event subscriber lagged");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                maybe_update = update_rx.recv() => {
                    match maybe_update {
                    None => break, // All senders dropped; no more updates will arrive.
                    Some(update) => {
                        last_update_at = Instant::now();
                        tracing::debug!(task_id = %task.id, chunk_id = %update.chunk.id, event = %update.event, "scheduler received update");

                        // If the worker discovered Content-Length from the GET response
                        // (because HEAD was skipped or failed), update task.total_bytes now
                        // so progress reporting and smart splitting start working immediately.
                        if let Some(discovered) = update.discovered_total_bytes {
                            if task.total_bytes.is_none() {
                                tracing::info!(
                                    task_id = %task.id,
                                    total_bytes = discovered,
                                    "scheduler: updating task.total_bytes from worker Content-Length discovery",
                                );
                                task.total_bytes = Some(discovered);
                                context.storage.save_task(task.to_persisted()).await?;
                            }
                        }

                        let previous_bytes = chunk_map.get(&update.chunk.id).map_or(0, |chunk| chunk.downloaded_bytes);
                        let downloaded_delta = update.chunk.downloaded_bytes.saturating_sub(previous_bytes);
                        throughput_sampler.record(downloaded_delta);
                        throughput_balancer.on_bytes(update.worker.id, downloaded_delta);
                        if downloaded_delta > 0 {
                            context.metrics.record_bytes(downloaded_delta);
                        }
                        // Feed every chunk update into the StallDetectorSubsystem so it
                        // can maintain per-chunk rolling throughput windows.  This enables
                        // throughput-based stall detection (ThroughputBelowThreshold) in
                        // addition to the inline timeout-based detection below.
                        context.stall_detector.observe_chunk_update(&update);
                        chunk_map.insert(update.chunk.id, update.chunk.clone());
                        // Only persist state-change events to SQLite; skip progress events to
                        // avoid hammering the database with thousands of tiny writes per second.
                        if update.event != "chunk.progress" {
                            self.persist_chunk_event(&context, &update.chunk, &update.event).await?;
                            // Compute total downloaded from the in-memory chunk_map to avoid
                            // a round-trip SQL SELECT on every state-change event.
                            let total_downloaded: u64 = chunk_map.values().map(|c| c.downloaded_bytes).sum();
                            if let Ok(Some(persisted)) = context.storage.load_task(task.id).await {
                                if let Ok(mut t) = DownloadTask::from_persisted(persisted) {
                                    t.downloaded_bytes = total_downloaded;
                                    t.touch();
                                    let _ = context.storage.save_task(t.to_persisted()).await;
                                }
                            }
                        }
                        let _ = transfer_graph.update_chunk_progress(&update.chunk);

                        // Decrement running_jobs when a chunk reaches a terminal state
                        if update.chunk.state.is_terminal() {
                            context.lease_registry.release(update.chunk.id);
                            if update.chunk.state == ChunkState::Completed {
                                throughput_balancer.on_chunk_completed(update.worker.id);
                                context.metrics.record_chunk_processed();
                            } else if update.chunk.state == ChunkState::Failed || update.chunk.state == ChunkState::Cancelled {
                                throughput_balancer.on_retry(update.worker.id);
                                context.metrics.record_retry();
                            }
                            let prev = running_jobs.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                            tracing::debug!(
                                task_id = %task.id,
                                chunk_id = %update.chunk.id,
                                state = %update.chunk.state.as_str(),
                                prev_running_jobs = prev,
                                new_running_jobs = prev.saturating_sub(1),
                                "terminal chunk update",
                            );

                            while let Some(next_chunk) = pending_chunks.pop_front() {
                                let available_workers = self.worker_pool.available_worker_ids();
                                if available_workers.is_empty() {
                                    pending_chunks.push_front(next_chunk);
                                    break;
                                }
                                let selected_worker = throughput_balancer.best_worker(&available_workers).copied();
                                let worker_reservation = self.worker_pool.reserve_worker(selected_worker).await?;
                                self.spawn_chunk_job(
                                    &task,
                                    next_chunk,
                                    worker_reservation,
                                    &context,
                                    &update_tx,
                                    &mut handles,
                                    &mut chunk_map,
                                    &mut cancellation_tokens,
                                    &mut transfer_graph,
                                    write_coordinator.clone(),
                                    task_bandwidth_limiter.clone(),
                                    task_cancel_token.clone(),
                                    generation,
                                )
                                .await?;
                                running_jobs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }

                        // Handle failed/cancelled chunks for retry
                        if update.chunk.state == ChunkState::Failed || update.chunk.state == ChunkState::Cancelled {
                            let mut chunk_to_retry = update.chunk.clone();
                            let current_attempt = chunk_to_retry.retry_attempts;
                            let next_delay = context.retry_policy.next_delay(current_attempt);
                            if let Some(delay) = next_delay {
                                chunk_to_retry.retry_attempts += 1;
                                chunk_to_retry.set_state(ChunkState::Retrying);
                                self.persist_chunk_event(&context, &chunk_to_retry, "chunk.retry").await?;

                                // ── Publish legacy chunk.retry event (backward-compat) ──────────
                                context.event_bus.publish(
                                    "chunk.retry",
                                    serde_json::json!({
                                        "chunk_id":       chunk_to_retry.id,
                                        "task_id":        chunk_to_retry.task_id,
                                        "retry_attempts": chunk_to_retry.retry_attempts,
                                    }),
                                );

                                // ── Publish structured RetryScheduledEvent ───────────────────────
                                // Carries the exact computed delay (including jitter) so the UI
                                // can show a live countdown and monitoring tools can detect
                                // thundering-herd anomalies.
                                {
                                    use ipc::contracts::{now_ms, RetryScheduledEvent};
                                    // Compute the base delay for this attempt (no jitter) so the
                                    // UI can display the nominal backoff tier alongside actual.
                                    // For ExponentialBackoffRetry we expose cap_for_attempt; for
                                    // any other policy we fall back to the actual delay.
                                    let base_delay_ms = delay.as_millis() as u64;
                                    let next_try_in_ms = delay.as_millis() as u64;
                                    let next_attempt_at = now_ms() + next_try_in_ms;
                                    let retry_event = RetryScheduledEvent {
                                        task_id:       chunk_to_retry.task_id,
                                        chunk_id:      chunk_to_retry.id,
                                        next_attempt_at,
                                        attempt:       chunk_to_retry.retry_attempts,
                                        max_attempts:  u32::MAX, // policy-agnostic sentinel
                                        next_try_in_ms,
                                        base_delay_ms,
                                        timestamp_ms:  now_ms(),
                                    };
                                    context.event_bus.publish(
                                        "chunk.retry_scheduled",
                                        serde_json::to_value(retry_event)
                                            .unwrap_or_else(|_| serde_json::json!({})),
                                    );
                                }

                                tracing::info!(
                                    chunk_id       = %chunk_to_retry.id,
                                    attempt        = chunk_to_retry.retry_attempts,
                                    delay_ms       = delay.as_millis(),
                                    "chunk failed — scheduling exponential-backoff retry via delayed re-queue",
                                );

                                let retry_tx_inner = retry_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(delay).await;
                                    let _ = retry_tx_inner.send(chunk_to_retry);
                                });
                            } else {
                                tracing::warn!("chunk {} failed and retry policy exhausted", chunk_to_retry.id);
                            }
                        }

                        let active_workers = transfer_graph.active_workers.len();
                        let snapshot_chunks: Vec<DownloadChunk> = chunk_map.values().cloned().collect();
                        aggregator.update(&snapshot_chunks, active_workers, throughput_sampler.average_rate_bps());

                        // Emit typed ProgressPayload for IPC
                        use ipc::contracts::{ProgressPayload, now_ms};
                        let progress = ProgressPayload {
                            task_id: task.id,
                            downloaded_bytes: aggregator.downloaded_bytes,
                            total_bytes: Some(aggregator.total_bytes),
                            progress_percent: Some(aggregator.progress_percent()),
                            completed_chunks: aggregator.completed_chunks,
                            pending_chunks: aggregator.pending_chunks,
                            active_workers: aggregator.active_workers as u32,
                            throughput_bps: aggregator.global_speed_bps,
                            eta_secs: aggregator.eta.map(|d| d.as_secs_f64()),
                            timestamp_ms: now_ms(),
                        };
                        context.event_bus.publish(
                            "transfer.snapshot",
                            serde_json::to_value(progress).unwrap_or_else(|_| json!({})),
                        );
                    } // end Some(update)
                    } // end match maybe_update
                }
                _ = monitor_interval.tick() => {
                    tracing::debug!(task_id = %task.id, "scheduler monitor tick");
                    throughput_sampler.compact(&compaction_cfg);

                    // If this task had no worker at startup (or workers were busy),
                    // keep trying to schedule pending chunks when workers become idle.
                    while let Some(next_chunk) = pending_chunks.pop_front() {
                        let available_workers = self.worker_pool.available_worker_ids();
                        if available_workers.is_empty() {
                            pending_chunks.push_front(next_chunk);
                            break;
                        }
                        let selected_worker = throughput_balancer.best_worker(&available_workers).copied();
                        let reserve_result = tokio::time::timeout(
                            Duration::from_millis(25),
                            self.worker_pool.reserve_worker(selected_worker),
                        )
                        .await;
                        let worker_reservation = if let Ok(reservation_result) = reserve_result {
                            reservation_result?
                        } else {
                            // Worker availability changed between snapshot and reserve.
                            // Re-queue and let the next tick retry without blocking loop.
                            pending_chunks.push_front(next_chunk);
                            break;
                        };
                        self.spawn_chunk_job(
                            &task,
                            next_chunk,
                            worker_reservation,
                            &context,
                            &update_tx,
                            &mut handles,
                            &mut chunk_map,
                            &mut cancellation_tokens,
                            &mut transfer_graph,
                            write_coordinator.clone(),
                            task_bandwidth_limiter.clone(),
                            task_cancel_token.clone(),
                            generation,
                        )
                        .await?;
                        running_jobs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }

                    let now = std::time::SystemTime::now();
                    let now_inst = Instant::now();
                    let stalled_chunks: Vec<DownloadChunk> = chunk_map.values()
                        .filter(|chunk| {
                            chunk.state == ChunkState::Downloading
                                || chunk.state == ChunkState::Connecting
                                || chunk.state == ChunkState::Flushing
                        })
                        .filter(|chunk| {
                                let last_activity_wall = chunk
                                    .reserved_at
                                    .unwrap_or(UNIX_EPOCH)
                                    .max(UNIX_EPOCH + Duration::from_secs(chunk.updated_at as u64));
                                // Prefer monotonic Instant if available to avoid SystemTime issues
                                let elapsed = if let Some(last_progress) = chunk.last_progress_instant {
                                    now_inst.duration_since(last_progress)
                                } else if let Ok(e) = now.duration_since(last_activity_wall) {
                                    e
                                } else {
                                    Duration::from_secs(u64::MAX)
                                };
                                let last_progress_ms = chunk.last_progress_instant.map(|i| now_inst.duration_since(i).as_millis());
                                tracing::debug!(
                                    chunk_id = %chunk.id,
                                    state = %chunk.state.as_str(),
                                    reserved_at = ?chunk.reserved_at,
                                    updated_at = chunk.updated_at,
                                    last_progress_ms = ?last_progress_ms,
                                    "checking chunk stall: elapsed={:?} vs timeout={:?}",
                                    elapsed,
                                    STALL_TIMEOUT,
                                );
                                elapsed > STALL_TIMEOUT
                        })
                        .cloned()
                        .collect();

                    if !stalled_chunks.is_empty() {
                        tracing::info!("detected {} stalled chunks", stalled_chunks.len());
                    }

                    for chunk in stalled_chunks {
                        if let Some(token) = cancellation_tokens.get(&chunk.id) {
                            token.cancel();
                        }
                        let mut synthesized_failed = false;

                        if self.should_split_chunk(&chunk, task.total_bytes.is_some()) {
                            let next_index = self.next_chunk_index(&chunk_map);
                            let split_children = self.split_chunk(&chunk, next_index);
                            let mut cancelled_chunk = chunk.clone();
                            cancelled_chunk.set_state(ChunkState::Cancelled);
                            // Update chunk_map BEFORE persisting so that any concurrent
                            // stall.recommendation split_chunk event on the same chunk_id
                            // sees the Cancelled (terminal) state and skips — preventing
                            // a double-split race between the two code paths.
                            chunk_map.insert(chunk.id, cancelled_chunk.clone());
                            self.persist_chunk_event(&context, &cancelled_chunk, "chunk.split").await?;

                            for child in split_children {
                                self.persist_chunk_event(&context, &child, "chunk.created").await?;
                                running_jobs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                let available_workers = self.worker_pool.available_worker_ids();
                                let selected_worker = throughput_balancer.best_worker(&available_workers).copied();
                                let worker_reservation = self.worker_pool.reserve_worker(selected_worker).await?;
                                self.spawn_chunk_job(
                                    &task,
                                    child,
                                    worker_reservation,
                                    &context,
                                    &update_tx,
                                    &mut handles,
                                    &mut chunk_map,
                                    &mut cancellation_tokens,
                                    &mut transfer_graph,
                                    write_coordinator.clone(),
                                    task_bandwidth_limiter.clone(),
                                    task_cancel_token.clone(),
                                    generation,
                                )
                                .await?;
                            }
                        } else {
                            // Some workers can hang without emitting a terminal update after
                            // cancellation. Emit a synthetic failure update so retry logic
                            // progresses deterministically instead of looping on chunk.stalled.
                            let byte_complete = chunk.downloaded_bytes >= chunk.length;
                            if let Some(mapped) = chunk_map.get_mut(&chunk.id) {
                                if byte_complete {
                                    mapped.set_state(ChunkState::Completed);
                                } else {
                                    mapped.set_state(ChunkState::Retrying);
                                    mapped.last_error =
                                        Some("stalled (no progress timeout)".to_string());
                                }
                            }
                            let synthetic_update = if byte_complete {
                                let mut completed_chunk = chunk.clone();
                                completed_chunk.set_state(ChunkState::Completed);
                                ChunkUpdate {
                                    chunk: completed_chunk,
                                    event: "chunk.completed".to_string(),
                                    worker: chunk.assigned_worker.clone().unwrap_or_default(),
                                    discovered_total_bytes: None,
                                    generation: 0,
                                }
                            } else {
                                let mut failed_chunk = chunk.clone();
                                failed_chunk.last_error =
                                    Some("stalled (no progress timeout)".to_string());
                                failed_chunk.set_state(ChunkState::Failed);
                                synthesized_failed = true;
                                ChunkUpdate {
                                    chunk: failed_chunk,
                                    event: "chunk.failed".to_string(),
                                    worker: chunk.assigned_worker.clone().unwrap_or_default(),
                                    discovered_total_bytes: None,
                                    generation: 0,
                                }
                            };
                            let _ = update_tx.send(synthetic_update);
                        }

                        context.event_bus.publish(
                            "chunk.stalled",
                            json!({
                                "task_id": chunk.task_id.to_string(),
                                "chunk_id": chunk.id.to_string(),
                                "state": chunk.state.as_str(),
                                "downloaded_bytes": chunk.downloaded_bytes,
                                "offset": chunk.offset,
                                "length": chunk.length,
                            }),
                        );
                        tracing::debug!(chunk_id = %chunk.id, "published chunk.stalled event");
                        use ipc::contracts::{AdaptiveStallDetectedEvent, StallReasonDto, AdaptiveRecommendationDto};
                        let stall_event = AdaptiveStallDetectedEvent {
                            task_id: task.id,
                            chunk_id: chunk.id,
                            worker_id: chunk.assigned_worker.as_ref().map(|w| w.id),
                            reason: StallReasonDto::NoProgressTimeout,
                            short_rate_bps: 0.0,
                            long_rate_bps: 0.0,
                            last_progress_secs: None,
                            recommendation: AdaptiveRecommendationDto::NoAction,
                            timestamp_ms: ipc::contracts::now_ms(),
                        };

                        context.event_bus.publish(
                            "adaptive.stall_detected",
                            serde_json::to_value(stall_event).unwrap_or_else(|_| json!({})),
                        );
                        if synthesized_failed {
                            tracing::debug!(chunk_id = %chunk.id, "emitted synthetic chunk.failed for stalled chunk");
                        }
                    }

                    // Safety hatch: if no scheduler updates arrive for a long
                    // time while jobs are still counted as running, cancel all
                    // worker tokens and break the loop so the task can be
                    // finalized as failed instead of hanging indefinitely.
                    const SCHEDULER_STUCK_TIMEOUT: Duration = Duration::from_secs(20);
                    if running_jobs.load(std::sync::atomic::Ordering::SeqCst) > 0
                        && last_update_at.elapsed() > SCHEDULER_STUCK_TIMEOUT
                    {
                        tracing::warn!(
                            task_id = %task.id,
                            running_jobs = running_jobs.load(std::sync::atomic::Ordering::SeqCst),
                            "scheduler appears stuck; forcing cancellation of active jobs",
                        );
                        for token in cancellation_tokens.values() {
                            token.cancel();
                        }
                        running_jobs.store(0, std::sync::atomic::Ordering::SeqCst);
                        forced_abort = true;
                        break 'scheduler_loop;
                    }
                }
            }
        }

        drop(update_tx);
        if forced_abort {
            for handle in &handles {
                handle.abort();
            }
        }
        // Do not block finalization on stuck worker tasks once the scheduler
        // has already reached a terminal chunk decision path.
        for handle in &handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }

        tracing::debug!(task_id = %task.id, handles = handles.len(), "awaiting worker handles");

        // Drain remaining updates from the channel after all workers finish
        tracing::debug!(
            task_id = %task.id,
            "draining updates after worker shutdown",
        );
        let mut idle_ticks = 0u32;
        while idle_ticks < 3 {
            match tokio::time::timeout(Duration::from_millis(100), update_rx.recv()).await {
                Ok(Some(update)) => {
                    idle_ticks = 0;
                    // Mirror the discovered_total_bytes logic from the main select! loop so
                    // late-arriving updates (delivered after workers finish) are not dropped.
                    if let Some(discovered) = update.discovered_total_bytes {
                        if task.total_bytes.is_none() {
                            tracing::info!(
                                task_id = %task.id,
                                total_bytes = discovered,
                                "scheduler (drain): updating task.total_bytes from late worker discovery",
                            );
                            task.total_bytes = Some(discovered);
                            context.storage.save_task(task.to_persisted()).await?;
                        }
                    }

                    let previous_bytes = chunk_map
                        .get(&update.chunk.id)
                        .map_or(0, |chunk| chunk.downloaded_bytes);
                    let downloaded_delta =
                        update.chunk.downloaded_bytes.saturating_sub(previous_bytes);
                    throughput_sampler.record(downloaded_delta);
                    chunk_map.insert(update.chunk.id, update.chunk.clone());
                    self.persist_chunk_event(&context, &update.chunk, &update.event)
                        .await?;
                    self.update_task_progress(&context, update.chunk.task_id)
                        .await?;
                }
                Ok(None) => break,
                Err(_) => {
                    idle_ticks += 1;
                }
            }
        }

        let mut task_failed = forced_abort;
        for (handle_index, handle) in handles.into_iter().enumerate() {
            let join_result = handle.await;
            match join_result {
                Ok(Ok(chunk)) => {
                    tracing::debug!(
                        task_id = %task.id,
                        handle_index = handle_index,
                        chunk_id = %chunk.id,
                        state = %chunk.state.as_str(),
                        downloaded_bytes = chunk.downloaded_bytes,
                        "worker handle completed",
                    );
                    if chunk.state == ChunkState::Failed {
                        task_failed = true;
                    }
                }
                Ok(Err(err)) => {
                    task_failed = true;
                    tracing::debug!(task_id = %task.id, handle_index = handle_index, error = %err, "worker handle completed with error");
                }
                Err(err) => {
                    task_failed = true;
                    tracing::debug!(task_id = %task.id, handle_index = handle_index, error = %err, "worker handle panicked");
                }
            }
        }

        let mut persisted_chunks = context.storage.load_chunks_for_task(task.id).await?;
        tracing::debug!(
            task_id = %task.id,
            persisted_chunks = persisted_chunks.len(),
            states = ?persisted_chunks.iter().map(|chunk| format!("{}:{}", chunk.id, chunk.state)).collect::<Vec<_>>(),
            "final persisted chunk states",
        );

        // Ignore Cancelled chunks: they are split parents replaced by child chunks.
        // Requiring them to be Completed would always cause download.failed on restarted
        // tasks where a large chunk was split mid-flight before the crash.
        let mut non_cancelled: Vec<_> = persisted_chunks
            .iter()
            .filter(|c| c.state != ChunkState::Cancelled)
            .collect();
        let mut all_completed = !non_cancelled.is_empty()
            && non_cancelled
                .iter()
                .all(|chunk| chunk.state == ChunkState::Completed);

        if !task_failed && !all_completed {
            let potentially_in_flight = [
                ChunkState::Reserved.as_str(),
                ChunkState::Connecting.as_str(),
                ChunkState::Downloading.as_str(),
                ChunkState::Flushing.as_str(),
                ChunkState::Retrying.as_str(),
            ];
            if non_cancelled.iter().all(|chunk| {
                chunk.state == ChunkState::Completed
                    || potentially_in_flight.contains(&chunk.state.as_str())
            }) {
                tracing::debug!(
                    task_id = %task.id,
                    "waiting for late completing persistence from an old session",
                );
                let mut elapsed = Duration::ZERO;
                let max_wait = Duration::from_secs(1);
                let check_interval = Duration::from_millis(100);
                while elapsed < max_wait && !all_completed {
                    tokio::time::sleep(check_interval).await;
                    elapsed += check_interval;
                    persisted_chunks = context.storage.load_chunks_for_task(task.id).await?;
                    tracing::debug!(
                        task_id = %task.id,
                        elapsed_ms = elapsed.as_millis(),
                        persisted_chunks = persisted_chunks.len(),
                        states = ?persisted_chunks.iter().map(|chunk| format!("{}:{}", chunk.id, chunk.state)).collect::<Vec<_>>(),
                        "rechecked final persisted chunk states",
                    );
                    non_cancelled = persisted_chunks
                        .iter()
                        .filter(|c| c.state != ChunkState::Cancelled)
                        .collect();
                    all_completed = !non_cancelled.is_empty()
                        && non_cancelled
                            .iter()
                            .all(|chunk| chunk.state == ChunkState::Completed);
                    if all_completed {
                        break;
                    }
                    if !non_cancelled.iter().all(|chunk| {
                        chunk.state == ChunkState::Completed
                            || potentially_in_flight.contains(&chunk.state.as_str())
                    }) {
                        break;
                    }
                }
            }
        }

        tracing::debug!(
            task_id = %task.id,
            task_failed = task_failed,
            all_completed = all_completed,
            non_cancelled = non_cancelled.len(),
            persisted_states = ?persisted_chunks.iter().map(|chunk| format!("{}:{}", chunk.id, chunk.state)).collect::<Vec<_>>(),
            "task completion decision",
        );

        let mut task = task.clone();
        task.downloaded_bytes = persisted_chunks
            .iter()
            .map(|chunk| chunk.downloaded_bytes)
            .sum();
        if all_completed {
            task.set_state(DownloadState::Completed);
            context.storage.save_task(task.to_persisted()).await?;

            // ── Post-download full-file checksum verification ─────────────────
            // If the task was enqueued with an `X-ADM-Expected-Full-Checksum`
            // header (and optionally `X-ADM-Checksum-Algorithm`), verify the
            // completed file now.
            //
            // On a mismatch the scheduler automatically retries the full
            // download up to crate::MAX_CHECKSUM_RETRIES (3) times before giving up.
            // Each retry:
            //   1. Deletes the corrupted output file.
            //   2. Wipes all chunk / write-range DB rows so the next attempt
            //      starts from a clean slate.
            //   3. Increments `task.checksum_retry_count` (persisted).
            //   4. Recursively calls `schedule_task` — the #[async_trait]
            //      boxing means this is stack-safe even for max retries (4
            //      frames total).

            let expected_full_hash = task
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("X-ADM-Expected-Full-Checksum"))
                .map(|(_, v)| v.clone());

            if let Some(ref expected_hash) = expected_full_hash {
                let algo_str = task
                    .headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("X-ADM-Checksum-Algorithm"))
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("sha256");

                let resolved_path = task.resolved_save_path(&context.download_dir);
                let algo = match algo_str.to_ascii_lowercase().as_str() {
                    "md5" => crate::ChecksumAlgorithm::Md5,
                    "sha1" => crate::ChecksumAlgorithm::Sha1,
                    "sha512" => crate::ChecksumAlgorithm::Sha512,
                    "crc32" => crate::ChecksumAlgorithm::Crc32,
                    _ => crate::ChecksumAlgorithm::Sha256,
                };

                tracing::info!(
                    task_id     = %task.id,
                    algorithm   = algo_str,
                    expected    = %expected_hash,
                    path        = %resolved_path.display(),
                    retry_count = task.checksum_retry_count,
                    "post-download: verifying full-file checksum",
                );

                match crate::post_download_verify(&resolved_path, expected_hash, Some(algo.clone()))
                    .await
                {
                    crate::VerifyOutcome::Valid => {
                        tracing::info!(
                            task_id   = %task.id,
                            algorithm = algo_str,
                            "post-download: full-file checksum OK",
                        );
                    }

                    crate::VerifyOutcome::Mismatch {
                        ref expected,
                        ref actual,
                    } => {
                        let current_retry = task.checksum_retry_count;

                        tracing::error!(
                            task_id  = %task.id,
                            expected = %expected,
                            actual   = %actual,
                            retry    = current_retry,
                            "post-download: full-file checksum MISMATCH",
                        );

                        // Publish event with the actual retry count so subscribers
                        // (UI, monitoring) can display accurate progress.
                        use ipc::contracts::{now_ms, ChecksumFileFailedEvent};
                        let cs_event = ChecksumFileFailedEvent {
                            task_id: task.id,
                            error: "checksum mismatch".to_string(),
                            expected_hash: expected.clone(),
                            actual_hash: actual.clone(),
                            algorithm: algo_str.to_string(),
                            retry_count: current_retry,
                            timestamp_ms: now_ms(),
                        };
                        context.event_bus.publish(
                            "checksum.file_failed",
                            serde_json::to_value(cs_event).unwrap_or_else(|_| json!({})),
                        );

                        if current_retry < crate::MAX_CHECKSUM_RETRIES {
                            // ── Auto-redownload ───────────────────────────
                            tracing::info!(
                                task_id    = %task.id,
                                attempt    = current_retry + 1,
                                max        = crate::MAX_CHECKSUM_RETRIES,
                                "post-download: checksum mismatch — \
                                 scheduling automatic re-download",
                            );

                            // 1. Remove the corrupted file so the fresh
                            //    download writes a clean output.
                            if let Err(e) = tokio::fs::remove_file(&resolved_path).await {
                                tracing::warn!(
                                    task_id = %task.id,
                                    error   = %e,
                                    "post-download: could not remove corrupted \
                                     file before re-download (will overwrite)",
                                );
                            }

                            // 2. Wipe all per-task DB state so the recursive
                            //    schedule_task call starts with a clean slate.
                            if let Err(e) = context.storage.delete_chunks_for_task(task.id).await {
                                tracing::warn!(
                                    task_id = %task.id,
                                    error = %e,
                                    "post-download: failed to delete chunks before re-download",
                                );
                            }
                            if let Err(e) = context
                                .storage
                                .delete_write_reservations_for_task(task.id)
                                .await
                            {
                                tracing::warn!(
                                    task_id = %task.id,
                                    error = %e,
                                    "post-download: failed to delete write reservations",
                                );
                            }
                            if let Err(e) =
                                context.storage.delete_write_ranges_for_task(task.id).await
                            {
                                tracing::warn!(
                                    task_id = %task.id,
                                    error = %e,
                                    "post-download: failed to delete write ranges",
                                );
                            }

                            // 3. Reset task fields for a clean re-download.
                            //    Keep `total_bytes` — the content hasn't changed
                            //    on the server, so we skip the HEAD probe on retry.
                            task.downloaded_bytes = 0;
                            task.checksum_retry_count = current_retry + 1;
                            task.set_state(DownloadState::Running);
                            context.storage.save_task(task.to_persisted()).await?;

                            // 4. Re-run the full download pipeline recursively.
                            //    async_trait boxes the future so recursion is
                            //    stack-safe; the maximum depth is crate::MAX_CHECKSUM_RETRIES+1 = 4.
                            context.event_bus.publish(
                                "checksum.retry_started",
                                json!({
                                    "task_id":   task.id.to_string(),
                                    "attempt":   task.checksum_retry_count,
                                    "max":       crate::MAX_CHECKSUM_RETRIES,
                                }),
                            );
                            return self
                                .schedule_task(task.clone(), context, task_handle.clone())
                                .await;
                        }

                        // Exhausted all retries — fail permanently.
                        tracing::error!(
                            task_id     = %task.id,
                            retry_count = current_retry,
                            max         = crate::MAX_CHECKSUM_RETRIES,
                            "post-download: checksum mismatch after \
                             maximum retries — failing permanently",
                        );
                        task.set_state(DownloadState::Failed {
                            reason: format!(
                                "full-file checksum mismatch after {} retries \
                                 (expected={}, actual={})",
                                current_retry, expected, actual
                            ),
                        });
                        context.storage.save_task(task.to_persisted()).await?;
                        task_failed = true;
                    }

                    crate::VerifyOutcome::Error(ref e) => {
                        tracing::warn!(
                            task_id = %task.id,
                            error   = %e,
                            "post-download: checksum verification error \
                             (treating as non-fatal — download considered successful)",
                        );
                    }
                }
            }

            if !task_failed {
                use ipc::contracts::{now_ms, DownloadCompletedEvent};
                let elapsed = task_start.elapsed();
                let duration_secs = elapsed.as_secs_f64();
                let avg_throughput_bps = if duration_secs > 0.0 {
                    (task.downloaded_bytes as f64 / duration_secs) * 8.0
                } else {
                    0.0
                };
                let event = DownloadCompletedEvent {
                    task_id: task.id,
                    total_bytes: task.total_bytes.unwrap_or(task.downloaded_bytes),
                    duration_secs,
                    average_throughput_bps: avg_throughput_bps,
                    timestamp_ms: now_ms(),
                };
                tracing::debug!(task_id = %task.id, "publishing download.completed from final branch");
                context.event_bus.publish(
                    "download.completed",
                    serde_json::to_value(event).unwrap_or_else(|_| json!({})),
                );
            }
        } else {
            task.set_state(DownloadState::Failed {
                reason: "chunk failure or recovery pending".to_string(),
            });
            context.storage.save_task(task.to_persisted()).await?;

            use ipc::contracts::{now_ms, DownloadFailedEvent};
            let event = DownloadFailedEvent {
                task_id: task.id,
                error: "chunk failure or recovery pending".to_string(),
                failed_chunks: persisted_chunks
                    .iter()
                    .filter(|c| {
                        c.state != ChunkState::Completed
                            && c.state != ChunkState::Cancelled
                    })
                    .count() as u32,
                timestamp_ms: now_ms(),
            };
            context.event_bus.publish(
                "download.failed",
                serde_json::to_value(event).unwrap_or_else(|_| json!({})),
            );
            task_failed = true;
        }

        if task_failed && !all_completed {
            Err(anyhow::anyhow!("task did not fully complete"))
        } else {
            Ok(())
        }
    }

    async fn restore_pending(&self, context: Arc<EngineContext>) -> Result<SchedulerSnapshot> {
        // ── Step 1: atomic SQL reset ──────────────────────────────────────────
        // Any spawn_blocking from the previous EngineContext may still be
        // queued in the blocking thread pool.  Dropping that context cancels
        // its ShutdownToken, which causes those tasks to bail out before
        // touching the DB (see Storage::with_read_conn / with_transaction / with_write_conn).
        //
        // Keep retrying until the orphaned state is stable or we reach a
        // reasonable upper bound. This guards against late-arriving writes
        // from the previous session.
        let mut persisted = Vec::new();
        for attempt in 0..20 {
            context.storage.recover_orphaned_chunks().await?;
            tokio::time::sleep(Duration::from_millis(25)).await;
            context.storage.recover_orphaned_chunks().await?;
            persisted = context.storage.load_pending_chunks().await?;
            let orphaned_count = persisted
                .iter()
                .filter(|chunk| matches!(chunk.state.as_str(), "reserved" | "connecting"))
                .count();
            if orphaned_count == 0 {
                break;
            }
            tracing::debug!(
                attempt,
                orphaned_chunks = orphaned_count,
                "retrying orphaned chunk recovery",
            );
        }

        // ── Step 2: build snapshot from what's now in the DB ─────────────────
        if persisted.is_empty() {
            persisted = context.storage.load_pending_chunks().await?;
        }
        let mut grouped: HashMap<Uuid, TaskScheduleSnapshot> = HashMap::new();

        for entry in persisted {
            let chunk = DownloadChunk::from_persisted(entry)?;

            let descriptor = chunk.descriptor();
            let task_snapshot =
                grouped
                    .entry(chunk.task_id)
                    .or_insert_with(|| TaskScheduleSnapshot {
                        task_id: chunk.task_id,
                        pending_chunks: Vec::new(),
                        active_assignments: Vec::new(),
                    });

            if matches!(
                chunk.state,
                ChunkState::Pending | ChunkState::Retrying | ChunkState::Failed
            ) {
                task_snapshot.pending_chunks.push(descriptor.clone());
            }

            if let Some(worker) = chunk.assigned_worker.clone() {
                task_snapshot.active_assignments.push(ChunkAssignment {
                    descriptor,
                    worker,
                    reserved_at: chunk.reserved_at.unwrap_or_else(SystemTime::now),
                    state: chunk.state.to_string(),
                });
            }
        }

        // A crash can happen before chunk rows are persisted. In that case the
        // task is still pending in `downloads`, and restore callers still need
        // to see it in the snapshot.
        for task in context.storage.load_pending_tasks().await? {
            grouped
                .entry(task.id)
                .or_insert_with(|| TaskScheduleSnapshot {
                    task_id: task.id,
                    pending_chunks: Vec::new(),
                    active_assignments: Vec::new(),
                });
        }

        Ok(SchedulerSnapshot {
            task_snapshots: grouped.into_values().collect(),
            snapshot_at: SystemTime::now(),
        })
    }
}
