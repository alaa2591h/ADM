use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadState {
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
    Retrying,
    Cancelled,
    Failed,
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
            Self::Retrying => "retrying",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: Uuid,
    pub url: String,
    pub state: DownloadState,
    pub filename: String,
    pub save_path: std::path::PathBuf,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub created_at: SystemTime,
    pub headers: HashMap<String, String>,
    pub speed_limit_kbps: Option<u64>,
    pub checksum_retry_count: u32,
}

impl DownloadTask {
    pub fn set_state(&mut self, state: DownloadState) {
        self.state = state;
    }

    pub fn resolved_save_path(&self, _base_dir: &std::path::Path) -> std::path::PathBuf {
        self.save_path.clone()
    }

    pub fn to_persisted(&self) -> Self {
        self.clone()
    }

    pub fn from_persisted(task: Self) -> Result<Self, anyhow::Error> {
        Ok(task)
    }

    pub fn touch(&mut self) {
        // Update updated_at if it existed, for now do nothing
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadChunk {
    pub id: Uuid,
    pub task_id: Uuid,
    pub index: u32,
    pub offset: u64,
    pub length: u64,
    pub downloaded_bytes: u64,
    pub completed: bool,
    pub last_error: Option<String>,
    pub state: ChunkState,
    pub reserved_at: Option<u64>,
    pub updated_at: u64,
    #[serde(skip)]
    pub last_progress_instant: Option<std::time::Instant>,
    pub assigned_worker: Option<Uuid>,
    pub speed_bytes_per_sec: f64,
    pub retry_attempts: u32,
}

impl DownloadChunk {
    pub fn new(task_id: Uuid, index: u32, offset: u64, length: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_id,
            index,
            offset,
            length,
            downloaded_bytes: 0,
            completed: false,
            last_error: None,
            state: ChunkState::Pending,
            reserved_at: None,
            updated_at: 0,
            last_progress_instant: None,
            assigned_worker: None,
            speed_bytes_per_sec: 0.0,
            retry_attempts: 0,
        }
    }

    pub fn set_state(&mut self, state: ChunkState) {
        self.state = state;
    }

    pub fn touch(&mut self) {
        self.updated_at = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn from_persisted(chunk: Self) -> Result<Self, anyhow::Error> {
        Ok(chunk)
    }

    pub fn descriptor(&self) -> ChunkDescriptor {
        ChunkDescriptor {
            id: self.id,
            index: self.index,
            offset: self.offset,
            length: self.length,
        }
    }

    pub fn assign_to(&mut self, worker_id: Uuid) {
        self.assigned_worker = Some(worker_id);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    pub id: Uuid,
    pub index: u32,
    pub offset: u64,
    pub length: u64,
}
