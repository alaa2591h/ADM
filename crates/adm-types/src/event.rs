use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    DownloadCreated {
        id: Uuid,
        url: String,
    },
    DownloadPaused {
        id: Uuid,
    },
    DownloadResumed {
        id: Uuid,
    },
    DownloadCancelled {
        id: Uuid,
    },
    DownloadCompleted {
        id: Uuid,
    },
    DownloadFailed {
        id: Uuid,
        error: String,
    },
    ChunkProgress {
        id: Uuid,
        downloaded: u64,
        total: Option<u64>,
    },
}
