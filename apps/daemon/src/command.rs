//! Command adapter — dispatches daemon commands through the [`DownloadEngine`].

use std::sync::Arc;
use adm_engine::DownloadEngine;
use uuid::Uuid;

/// Commands that mutate the system state (daemon / telegram API surface).
#[derive(Debug, Clone)]
pub enum Command {
  CreateDownload {
    url: String,
    filename: Option<String>,
  },
  PauseDownload {
    task_id: Uuid,
  },
  ResumeDownload {
    task_id: Uuid,
  },
  RetryDownload {
    task_id: Uuid,
  },
  CancelDownload {
    task_id: Uuid,
  },
}

/// Forwards to `adm-engine` directly.
pub struct CommandHandler {
  engine: Arc<DownloadEngine>,
}

impl CommandHandler {
  #[must_use]
  pub const fn new(engine: Arc<DownloadEngine>) -> Self {
    Self { engine }
  }

  /// Handles an incoming command.
  pub async fn handle(&self, cmd: Command) -> anyhow::Result<Option<Uuid>> {
    match cmd {
      Command::CreateDownload { url, filename } => {
        let task_id = self
          .engine
          .create_download(&url, filename.as_deref(), 128)
          .await?;
        Ok(Some(task_id))
      }
      Command::PauseDownload { task_id } => {
        self.engine.pause_download(task_id).await?;
        Ok(None)
      }
      Command::ResumeDownload { task_id } => {
        self.engine.resume_download(task_id).await?;
        Ok(None)
      }
      Command::RetryDownload { task_id } => {
        self.engine.retry_download(task_id).await?;
        Ok(None)
      }
      Command::CancelDownload { task_id } => {
        self.engine.cancel_download(task_id).await?;
        Ok(None)
      }
    }
  }
}
