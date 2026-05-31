pub mod adaptive;
pub mod additional_extractors;
pub mod checksums;
pub mod clipboard_monitor;
pub mod credentials;
pub mod decompression;
pub mod errors;
pub mod events;
pub mod ffmpeg_mux;
pub mod headers;
pub mod https_policy;
pub mod lease_and_compaction;
pub mod mp4_mux;
pub mod native_mux;
pub mod network;
pub mod notifications;
pub mod plugin_registry;
pub mod runtime;
pub mod scheduler;
pub mod throughput_balancer;
pub mod torrent_tracker;
pub mod url_importer;
pub mod webhooks;
pub mod webui_api;
pub mod worker;

#[cfg(test)]
mod headers_runtime_tests;

pub use adm_network::{Downloader, HeadInfo, MockNetworkClient, NetworkClient, NetworkError, NetworkRequest, ResponseStream};
pub use adm_storage::{ChunkRepository, HistoryRepository, ShutdownToken, Storage, TaskRepository};
pub use adm_types::{
    ChunkDescriptor, ChunkState, ChunkUpdate, DownloadChunk, DownloadSnapshot, DownloadState,
    DownloadTask, SystemStats, WorkerHandle,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;
pub use events::{Event, EventBus};
pub use scheduler::BasicScheduler;
pub use worker::{DownloadJob, WorkerPool};

pub struct Engine {
    storage: Arc<Storage>,
    downloader: Arc<Downloader>,
}

/// Type alias for API compatibility.
pub type DownloadEngine = Engine;

impl Engine {
    #[must_use]
    pub const fn new(storage: Arc<Storage>, downloader: Arc<Downloader>) -> Self {
        Self { storage, downloader }
    }

    pub async fn add_task(&self, url: String) -> Result<Uuid> {
        let mut task = DownloadTask::new(url);
        let config = self.storage.load_config().await?;
        task.save_path = Some(config.download_dir);
        let id = task.id;
        self.storage.save_task(task).await?;
        Ok(id)
    }

    pub async fn create_download(&self, url: &str, filename: Option<&str>, priority: u8) -> Result<Uuid> {
        let mut task = DownloadTask::new(url);
        let config = self.storage.load_config().await?;
        task.save_path = Some(config.download_dir);
        task.filename = filename.map(str::to_string);
        task.priority = priority;
        let id = task.id;
        self.storage.save_task(task).await?;
        Ok(id)
    }

    pub async fn start_task(&self, id: Uuid) -> Result<()> {
        tracing::info!(task_id = %id, "Starting task");
        Ok(())
    }

    pub async fn get_download(&self, id: Uuid) -> Result<DownloadSnapshot> {
        let task = self.storage.load_task(id).await?
            .ok_or_else(|| anyhow::anyhow!("task not found: {id}"))?;
        let task = DownloadTask::from_persisted(task)?;
        Ok(DownloadSnapshot {
            task_id: task.id,
            url: task.url.clone(),
            filename: adm_types::derive_filename_from_url(&task.url),
            state: task.state.clone(),
            progress: task.total_bytes.filter(|&t| t > 0)
                .map(|t| task.downloaded_bytes as f64 / t as f64 * 100.0)
                .unwrap_or(0.0),
            downloaded_bytes: task.downloaded_bytes,
            total_bytes: task.total_bytes,
            created_at: task.created_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        })
    }

    pub async fn list_downloads(&self, _limit: usize, _offset: usize) -> Result<Vec<DownloadSnapshot>> {
        Ok(vec![])
    }

    pub async fn pause_download(&self, id: Uuid) -> Result<()> {
        tracing::info!(task_id = %id, "Pausing task");
        Ok(())
    }

    pub async fn resume_download(&self, id: Uuid) -> Result<()> {
        tracing::info!(task_id = %id, "Resuming task");
        Ok(())
    }

    pub async fn cancel_download(&self, id: Uuid) -> Result<()> {
        tracing::info!(task_id = %id, "Cancelling task");
        Ok(())
    }

    pub async fn retry_download(&self, id: Uuid) -> Result<()> {
        tracing::info!(task_id = %id, "Retrying task");
        Ok(())
    }

    pub async fn system_stats(&self) -> Result<SystemStats> {
        Ok(SystemStats { active_downloads: 0, total_throughput_bps: 0.0, uptime_secs: 0 })
    }
}

// Additional re-exports for the engine
pub use adm_observability::{WorkerPoolSnapshot, WorkerStateSnapshot};
pub use checksums::{post_download_verify, verify_checksum, ChecksumAlgorithm, VerifyOutcome};
pub use errors::EngineError;
pub use lease_and_compaction::{LeaseRegistry, DEFAULT_RESERVATION_LEASE};
pub use runtime::{ChunkLease, FileWriteCoordinator, TaskHandle};

pub const MAX_CHECKSUM_RETRIES: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAssignment {
    pub descriptor: ChunkDescriptor,
    pub worker: WorkerHandle,
    pub reserved_at: std::time::SystemTime,
    pub state: String,
}

pub struct CompactionConfig {
    pub threshold_bytes: u64,
    pub throughput_sample_max_age: std::time::Duration,
    pub max_throughput_samples: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 1024 * 1024,
            throughput_sample_max_age: std::time::Duration::from_secs(60),
            max_throughput_samples: 100,
        }
    }
}

pub struct EngineContext {
    pub storage: Arc<Storage>,
    pub event_bus: EventBus,
    pub scheduler: Arc<dyn scheduler::Scheduler>,
    pub network: Arc<dyn NetworkClient>,
    pub shutdown: ShutdownToken,
    pub metrics: SchedulerMetrics,
    pub stall_detector: StallDetector,
    pub lease_registry: Arc<LeaseRegistry>,
    pub retry_policy: Arc<dyn RetryPolicy>,
    pub download_dir: PathBuf,
}

impl EngineContext {
    pub fn new(
        event_bus: EventBus,
        storage: Arc<Storage>,
        _storage_clone: Arc<Storage>,
        scheduler: Arc<dyn scheduler::Scheduler>,
        network: Arc<dyn NetworkClient>,
        retry_policy: Arc<dyn RetryPolicy>,
        lease_registry: Arc<LeaseRegistry>,
        download_dir: PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            event_bus,
            scheduler,
            network,
            shutdown: ShutdownToken::default(),
            metrics: SchedulerMetrics,
            stall_detector: StallDetector,
            lease_registry,
            retry_policy,
            download_dir,
        })
    }
}

pub struct SchedulerMetrics;
impl SchedulerMetrics {
    pub fn record_bytes(&self, _delta: u64) {}
    pub fn record_chunk_processed(&self) {}
    pub fn record_retry(&self) {}
}

pub struct StallDetector;
impl StallDetector {
    pub fn observe_worker_heartbeat(
        &self,
        _worker: &WorkerHandle,
        _chunk_id: Uuid,
        _generation: u64,
    ) {
    }
    pub fn observe_chunk_update(&self, _update: &ChunkUpdate) {}
}

pub trait RetryPolicy: Send + Sync {
    fn next_delay(&self, attempt: u32) -> Option<std::time::Duration>;
}

#[derive(Debug, Clone)]
pub struct FixedRetry {
    pub max: u32,
    pub delay: std::time::Duration,
}

impl FixedRetry {
    #[must_use]
    pub fn new(max: u32, delay: std::time::Duration) -> Self {
        Self { max, delay }
    }
}

impl RetryPolicy for FixedRetry {
    fn next_delay(&self, attempt: u32) -> Option<std::time::Duration> {
        if attempt >= self.max {
            None
        } else {
            Some(self.delay)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub task_snapshots: Vec<TaskScheduleSnapshot>,
    pub snapshot_at: std::time::SystemTime,
}

pub struct QueueManager {
    context: Arc<EngineContext>,
    pending_tasks: Mutex<VecDeque<DownloadTask>>,
    task_handles: Mutex<HashMap<Uuid, Arc<TaskHandle>>>,
}

impl QueueManager {
    pub fn new(context: Arc<EngineContext>) -> Self {
        Self {
            context,
            pending_tasks: Mutex::new(VecDeque::new()),
            task_handles: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_task(&self, mut task: DownloadTask) -> Result<Uuid> {
        task.set_state(DownloadState::Queued);
        let id = task.id;
        self.context.storage.save_task(task.to_persisted()).await?;
        self.pending_tasks.lock().await.push_back(task);
        Ok(id)
    }

    pub async fn start_next(&self) -> Result<()> {
        let task = self.pending_tasks.lock().await.pop_front();
        let task = task.ok_or_else(|| anyhow::anyhow!("no task available"))?;
        let handle = Arc::new(TaskHandle::new(task.id, 0, ::adm_network::CancellationToken::new()));
        self.task_handles.lock().await.insert(task.id, handle.clone());
        let scheduler = self.context.scheduler.clone();
        let ctx = self.context.clone();
        tokio::spawn(async move {
            let _ = scheduler.schedule_task(task, ctx, handle).await;
        });
        Ok(())
    }

    pub async fn restore_pending_tasks(&self) -> Result<()> {
        let pending = self.context.storage.load_pending_tasks().await?;
        let mut queue = self.pending_tasks.lock().await;
        for task in pending {
            queue.push_back(task);
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScheduleSnapshot {
    pub task_id: Uuid,
    pub pending_chunks: Vec<ChunkDescriptor>,
    pub active_assignments: Vec<ChunkAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    pub id: Uuid,
    pub state: String,
    pub active_task_id: Option<Uuid>,
    pub active_chunk_id: Option<Uuid>,
    pub generation: u64,
    pub uptime_secs: u64,
    pub throughput_bps: f64,
}
