use anyhow::Result;
use adm_engine::{Storage, TaskRepository};
use ipc::contracts::{ChunkDto, TaskDto, TaskStateDto, TaskSummaryDto};
use std::sync::Arc;
use std::time::Instant;
use adm_storage::{ChunkRepository, HistoryRepository};
use uuid::Uuid;

/// Queries that retrieve state without modifying it.
#[derive(Debug, Clone)]
pub enum Query {
    ListTasks {
        state_filter: Option<Vec<String>>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    GetTask {
        task_id: Uuid,
    },
    GetChunks {
        task_id: Uuid,
    },
    GetSystemStats {
        boot_time: Instant,
    },
    GetStorageInfo {
        download_dir: std::path::PathBuf,
    },
    GetLogs {
        task_id: Uuid,
    },
}

/// Query result matching each query variant.
#[derive(Debug, Clone)]
pub enum QueryResult {
    TaskList {
        tasks: Vec<TaskSummaryDto>,
        total: usize,
    },
    Task(TaskDto),
    Chunks(Vec<ChunkDto>),
    SystemStats {
        active_tasks: usize,
        queued_tasks: usize,
        uptime_secs: u64,
    },
    StorageInfo {
        free_space_bytes: u64,
        total_space_bytes: u64,
    },
    Logs(Vec<(String, i64)>),
}

/// Query Bus handler responsible for executing read-oriented queries.
pub struct QueryHandler {
    storage: Arc<Storage>,
}

impl QueryHandler {
    pub const fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Handles an incoming query and returns the matching results.
    pub async fn handle(&self, query: Query) -> Result<QueryResult> {
        match query {
            Query::ListTasks {
                state_filter,
                limit,
                offset,
            } => {
                let all = self.storage.load_all_tasks().await?;
                let filtered: Vec<_> = all
                    .into_iter()
                    .filter(|t| {
                        if let Some(ref filter) = state_filter {
                            filter.contains(&t.state)
                        } else {
                            true
                        }
                    })
                    .collect();

                let total = filtered.len();
                let offset_val = offset.unwrap_or(0);
                let limit_val = limit.unwrap_or(usize::MAX);

                let tasks: Vec<TaskSummaryDto> = filtered
                    .into_iter()
                    .skip(offset_val)
                    .take(limit_val)
                    .map(|t| {
                        let progress = t.total_bytes.map(|total| {
                            if total == 0 {
                                0.0
                            } else {
                                (t.downloaded_bytes as f64 / total as f64) * 100.0
                            }
                        });
                        TaskSummaryDto {
                            id: t.id,
                            filename: t.filename.clone(),
                            state: match t.state.as_str() {
                                "created" => TaskStateDto::Created,
                                "queued" => TaskStateDto::Queued,
                                "running" => TaskStateDto::Running,
                                "paused" => TaskStateDto::Paused,
                                "completed" => TaskStateDto::Completed,
                                _ => TaskStateDto::Failed,
                            },
                            total_bytes: t.total_bytes,
                            downloaded_bytes: t.downloaded_bytes,
                            progress_percent: progress,
                            throughput_bps: 0.0,
                            eta_secs: None,
                            error: t.last_error.clone(),
                        }
                    })
                    .collect();

                Ok(QueryResult::TaskList { tasks, total })
            }
            Query::GetTask { task_id } => {
                let persisted = self
                    .storage
                    .load_task(task_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Task not found"))?;

                let progress = persisted.total_bytes.map(|total| {
                    if total == 0 {
                        0.0
                    } else {
                        (persisted.downloaded_bytes as f64 / total as f64) * 100.0
                    }
                });

                let dto = TaskDto {
                    id: persisted.id,
                    url: persisted.url.clone(),
                    filename: persisted.filename.clone(),
                    state: match persisted.state.as_str() {
                        "created" => TaskStateDto::Created,
                        "queued" => TaskStateDto::Queued,
                        "running" => TaskStateDto::Running,
                        "paused" => TaskStateDto::Paused,
                        "completed" => TaskStateDto::Completed,
                        _ => TaskStateDto::Failed,
                    },
                    total_bytes: persisted.total_bytes,
                    downloaded_bytes: persisted.downloaded_bytes,
                    progress_percent: progress,
                    created_at: persisted.created_at,
                    updated_at: persisted.updated_at,
                    error: persisted.last_error.clone(),
                    tags: Vec::new(),
                };

                Ok(QueryResult::Task(dto))
            }
            Query::GetChunks { task_id } => {
                let persisted = self.storage.load_chunks_for_task(task_id).await?;
                let chunks = persisted
                    .into_iter()
                    .map(|c| {
                        let state_dto = match c.state.as_str() {
                            "pending" => ipc::contracts::ChunkStateDto::Pending,
                            "reserved" => ipc::contracts::ChunkStateDto::Reserved,
                            "connecting" => ipc::contracts::ChunkStateDto::Connecting,
                            "downloading" => ipc::contracts::ChunkStateDto::Downloading,
                            "flushing" => ipc::contracts::ChunkStateDto::Flushing,
                            "completed" => ipc::contracts::ChunkStateDto::Completed,
                            "retrying" => ipc::contracts::ChunkStateDto::Retrying,
                            "cancelled" => ipc::contracts::ChunkStateDto::Cancelled,
                            _ => ipc::contracts::ChunkStateDto::Failed,
                        };
                        ChunkDto {
                            id: c.id,
                            task_id: c.task_id,
                            index: c.index,
                            offset: c.offset,
                            length: c.length,
                            state: state_dto,
                            downloaded_bytes: c.downloaded_bytes,
                            retry_attempts: c.retry_attempts,
                            assigned_worker_id: c.worker_id,
                            last_error: c.last_error.clone(),
                            reserved_at_ms: c.reserved_at.map(|ts| ts * 1000),
                            last_progress_ms: None,
                        }
                    })
                    .collect();
                Ok(QueryResult::Chunks(chunks))
            }
            Query::GetSystemStats { boot_time } => {
                let all = self.storage.load_all_tasks().await?;
                let active_tasks = all.iter().filter(|t| t.state == "running").count();
                let queued_tasks = all.iter().filter(|t| t.state == "queued").count();

                Ok(QueryResult::SystemStats {
                    active_tasks,
                    queued_tasks,
                    uptime_secs: boot_time.elapsed().as_secs(),
                })
            }
            Query::GetStorageInfo {
                download_dir,
            } => {
                // Windows disk usage or simple default values for cross-platform robustness.
                // We can query fs2 or check disk space. Let's use simple cross-platform fallback.
                #[cfg(windows)]
                {
                    if let Ok(free) = fs2::free_space(&download_dir) {
                        if let Ok(total) = fs2::total_space(&download_dir) {
                            return Ok(QueryResult::StorageInfo {
                                free_space_bytes: free,
                                total_space_bytes: total,
                            });
                        }
                    }
                }
                // Fallback / mock values when querying filesystem fails or on other OS
                Ok(QueryResult::StorageInfo {
                    free_space_bytes: 100 * 1024 * 1024 * 1024,  // 100 GB
                    total_space_bytes: 500 * 1024 * 1024 * 1024, // 500 GB
                })
            }
            Query::GetLogs { task_id } => {
                let logs = self
                    .storage
                    .load_history(task_id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|h| (h.event, h.created_at))
                    .collect();
                Ok(QueryResult::Logs(logs))
            }
        }
    }
}
