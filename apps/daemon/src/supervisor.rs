use anyhow::Result;
use adm_engine::{Storage, TaskRepository};
use std::sync::Arc;
use adm_storage::ChunkRepository;
use uuid::Uuid;

/// Resilient Task Supervisor that oversees the stability of the download execution pipeline.
pub struct TaskSupervisor {
    storage: Arc<Storage>,
}

impl TaskSupervisor {
    pub const fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Performs crash recovery and integrity checks on startup.
    /// Resets any tasks left in a volatile "running" or "preparing" state to "queued"
    /// so the `QueueManager` can pick them up and resume download segments.
    pub async fn recover_crashed_tasks(&self) -> Result<Vec<Uuid>> {
        let all_tasks = self.storage.load_all_tasks().await?;
        let mut recovered_ids = Vec::new();

        for entry in all_tasks {
            // Volatile non-terminal states that imply a crash if the daemon just booted
            if entry.state == "running" || entry.state == "preparing" {
                tracing::warn!(
                    task_id = %entry.id,
                    former_state = ?entry.state,
                    "Task was interrupted mid-execution. Recovering task back to queued state."
                );

                let mut task = adm_engine::DownloadTask::from_persisted(entry.clone())?;
                task.set_state(adm_engine::DownloadState::Queued);
                task.touch();

                // Reset any reserved or connecting chunks back to pending so they are re-downloaded
                let chunks = self.storage.load_chunks_for_task(task.id).await?;
                for chunk in chunks {
                    if chunk.state == "reserved"
                        || chunk.state == "connecting"
                        || chunk.state == "downloading"
                        || chunk.state == "flushing"
                    {
                        let mut domain_chunk = adm_engine::DownloadChunk::from_persisted(chunk)?;
                        domain_chunk.set_state(adm_engine::ChunkState::Pending);
                        domain_chunk.touch();
                        self.storage
                            .save_chunk_with_retry(domain_chunk.to_persisted())
                            .await?;
                    }
                }

                // Persist the task reset
                self.storage
                    .save_task_with_retry(task.to_persisted())
                    .await?;
                recovered_ids.push(task.id);
            }
        }

        Ok(recovered_ids)
    }

    /// Isolated worker error handling. Determines if a task failure is recoverable or terminal.
    pub async fn handle_worker_failure(
        &self,
        task_id: Uuid,
        error_message: String,
    ) -> Result<bool> {
        let persisted = self.storage.load_task(task_id).await?;
        if let Some(entry) = persisted {
            let mut task = adm_engine::DownloadTask::from_persisted(entry)?;

            // Check if retry budget is exhausted
            let max_retries = 5; // Default/configurable
            if task.retry_attempts < max_retries {
                task.retry_attempts += 1;
                task.last_error = Some(error_message.clone());
                task.set_state(adm_engine::DownloadState::Queued);
                self.storage
                    .save_task_with_retry(task.to_persisted())
                    .await?;
                tracing::info!(task_id = %task_id, attempt = task.retry_attempts, "Supervisor scheduled task for retry.");
                Ok(true) // Recoverable, retry scheduled
            } else {
                task.set_state(adm_engine::DownloadState::Failed {
                    reason: error_message.clone(),
                });
                task.last_error = Some(error_message);
                self.storage
                    .save_task_with_retry(task.to_persisted())
                    .await?;
                tracing::error!(task_id = %task_id, "Supervisor marked task as failed: retries exhausted.");
                Ok(false) // Terminal failure
            }
        } else {
            Ok(false)
        }
    }
}
