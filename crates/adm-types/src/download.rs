use crate::utils;
use serde::{Deserialize, Serialize};
use std::time::{Instant, SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadState {
    Created,
    Queued,
    Running,
    Paused,
    Completed,
    Failed { reason: String },
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChunkState {
    Pending,
    Reserved,
    Connecting,
    Downloading,
    Flushing,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

impl ChunkState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Reserved => "reserved",
            Self::Connecting => "connecting",
            Self::Downloading => "downloading",
            Self::Flushing => "flushing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retrying => "retrying",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub fn from_str_lossy(value: &str) -> Self {
        match value {
            "pending" => Self::Pending,
            "reserved" => Self::Reserved,
            "connecting" => Self::Connecting,
            "downloading" => Self::Downloading,
            "flushing" => Self::Flushing,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "retrying" => Self::Retrying,
            "cancelled" => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Pending
                | Self::Reserved
                | Self::Connecting
                | Self::Downloading
                | Self::Flushing
                | Self::Retrying
                | Self::Failed
                | Self::Cancelled
        )
    }
}

impl std::fmt::Display for ChunkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub task_id: Uuid,
    pub index: u32,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct WorkerHandle {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadChunk {
    pub id: Uuid,
    pub task_id: Uuid,
    pub index: u32,
    pub offset: u64,
    pub length: u64,
    pub state: ChunkState,
    pub downloaded_bytes: u64,
    pub retry_attempts: u32,
    pub last_error: Option<String>,
    pub checksum: Option<String>,
    pub speed_bytes_per_sec: f64,
    pub assigned_worker: Option<WorkerHandle>,
    pub reserved_at: Option<SystemTime>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip)]
    pub last_progress_instant: Option<Instant>,
}

impl DownloadChunk {
    #[must_use]
    pub fn new(task_id: Uuid, index: u32, offset: u64, length: u64) -> Self {
        let now = utils::unix_secs() as i64;
        Self {
            id: Uuid::new_v4(),
            task_id,
            index,
            offset,
            length,
            state: ChunkState::Pending,
            downloaded_bytes: 0,
            retry_attempts: 0,
            last_error: None,
            checksum: None,
            speed_bytes_per_sec: 0.0,
            assigned_worker: None,
            reserved_at: None,
            created_at: now,
            updated_at: now,
            last_progress_instant: Some(Instant::now()),
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = utils::unix_secs() as i64;
        self.last_progress_instant = Some(Instant::now());
    }

    pub fn set_state(&mut self, state: ChunkState) {
        self.state = state;
        self.touch();
    }

    #[must_use]
    pub fn to_persisted(&self) -> Self {
        self.clone()
    }

    pub fn from_persisted(chunk: Self) -> Result<Self, anyhow::Error> {
        Ok(chunk)
    }

    pub fn assign_to(&mut self, worker: WorkerHandle) {
        self.assigned_worker = Some(worker);
        self.reserved_at = Some(SystemTime::now());
        self.set_state(ChunkState::Reserved);
    }

    #[must_use]
    pub const fn descriptor(&self) -> ChunkDescriptor {
        ChunkDescriptor {
            task_id: self.task_id,
            index: self.index,
            offset: self.offset,
            length: self.length,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkUpdate {
    pub chunk: DownloadChunk,
    pub event: String,
    pub worker: WorkerHandle,
    pub generation: u64,
    /// Populated by the worker when the server returns a `Content-Length` on the
    /// first GET response and `task.total_bytes` was unknown at chunk-creation
    /// time (i.e. HEAD failed or was skipped). The scheduler propagates this
    /// value to `task.total_bytes` so progress reporting and smart splitting
    /// work correctly even without a prior HEAD probe.
    pub discovered_total_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: Uuid,
    pub url: String,
    pub state: DownloadState,
    pub created_at: SystemTime,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub retry_attempts: u32,
    pub last_error: Option<String>,
    pub updated_at: SystemTime,
    pub filename: Option<String>,
    pub save_path: Option<std::path::PathBuf>,
    pub headers: Vec<(String, String)>,
    pub priority: u8,
    pub speed_limit_kbps: Option<u64>,
    pub av_group_id: Option<String>,
    pub av_role: Option<String>,
    pub av_order: Option<i64>,
    pub checksum_retry_count: u32,
}

impl DownloadTask {
    pub fn new(url: impl Into<String>) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4(),
            url: url.into(),
            state: DownloadState::Created,
            created_at: now,
            downloaded_bytes: 0,
            total_bytes: None,
            retry_attempts: 0,
            last_error: None,
            updated_at: now,
            filename: None,
            save_path: None,
            headers: Vec::new(),
            priority: 127,
            speed_limit_kbps: None,
            av_group_id: None,
            av_role: None,
            av_order: None,
            checksum_retry_count: 0,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = SystemTime::now();
    }

    pub fn set_state(&mut self, state: DownloadState) {
        self.state = state;
        self.touch();
    }

    #[must_use]
    pub fn to_persisted(&self) -> Self {
        self.clone()
    }

    pub fn from_persisted(task: Self) -> Result<Self, anyhow::Error> {
        Ok(task)
    }

    #[must_use]
    pub fn resolved_save_path(&self, default_dir: &std::path::Path) -> std::path::PathBuf {
        let dir = self.save_path.as_deref().unwrap_or(default_dir);
        let name = self.filename.as_deref().map_or_else(
            || utils::derive_filename_from_url(&self.url),
            std::string::ToString::to_string,
        );
        dir.join(name)
    }
}
