use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSnapshot {
    pub scheduler: SchedulerDiagnostics,
    pub worker_pools: HashMap<String, WorkerPoolSnapshot>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerDiagnostics {
    pub worker_count: usize,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerPoolSnapshot {
    pub name: String,
    pub queue_depth: usize,
    pub active_workers: usize,
    pub max_workers: usize,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub workers: Vec<WorkerStateSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStateSnapshot {
    pub id: String,
    pub active_task_id: Option<String>,
    pub active_chunk_id: Option<String>,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueSnapshot {
    pub name: String,
    pub size: usize,
    pub capacity: usize,
}
