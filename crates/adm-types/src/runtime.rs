use crate::download::DownloadState;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSnapshot {
    pub task_id: Uuid,
    pub url: String,
    pub filename: String,
    pub state: DownloadState,
    pub progress: f64,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub active_downloads: u32,
    pub total_throughput_bps: f64,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAssignment {
    pub descriptor: crate::download::ChunkDescriptor,
    pub worker: crate::download::WorkerHandle,
    pub reserved_at: std::time::SystemTime,
    pub state: crate::download::ChunkState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScheduleSnapshot {
    pub task_id: Uuid,
    pub pending_chunks: Vec<crate::download::ChunkDescriptor>,
    pub active_assignments: Vec<ChunkAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub task_snapshots: Vec<TaskScheduleSnapshot>,
    pub snapshot_at: std::time::SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    pub id: Uuid,
    pub active_task_id: Option<Uuid>,
    pub active_chunk_id: Option<Uuid>,
    pub generation: u64,
    pub uptime_secs: u64,
    pub throughput_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRuntimeSnapshot {
    pub pending_count: usize,
    pub active_count: usize,
    pub task_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub timestamp_ms: u64,
    pub scheduler: SchedulerSnapshot,
    pub queue: QueueRuntimeSnapshot,
    pub workers: Vec<WorkerSnapshot>,
    // Removed direct metrics dependency for now to keep it minimal
}
