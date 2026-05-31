// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — contracts/events.rs                                           ║
// ║  Event contracts — backend-agnostic event definitions                    ║
// ║  These events are emitted by the runtime and consumed by the UI          ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::contracts::dto::DownloadDTO;

/// RuntimeEvent — events emitted by the backend runtime
/// The UI layer subscribes to these and updates accordingly
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    // ── Download lifecycle ──────────────────────────────────────────────────
    /// A download was added to the queue
    DownloadAdded(DownloadDTO),
    
    /// Download progress updated
    DownloadProgress {
        download_id: String,
        downloaded_bytes: u64,
        total_bytes: u64,
        speed_bps: f64,
    },
    
    /// Download status changed
    DownloadStatusChanged {
        download_id: String,
        new_status: crate::contracts::dto::DownloadStatus,
    },
    
    /// Download completed successfully
    DownloadCompleted {
        download_id: String,
        final_bytes: u64,
    },
    
    /// Download failed
    DownloadFailed {
        download_id: String,
        error: String,
    },
    
    /// Download paused
    DownloadPaused(String),
    
    /// Download resumed
    DownloadResumed(String),
    
    /// Download removed
    DownloadRemoved(String),
    
    // ── Chunk updates ────────────────────────────────────────────────────────
    /// A chunk completed
    ChunkCompleted {
        download_id: String,
        chunk_id: u32,
    },
    
    /// Chunk failed
    ChunkFailed {
        download_id: String,
        chunk_id: u32,
        error: String,
    },
    
    // ── System events ────────────────────────────────────────────────────────
    /// Runtime initialized and ready
    RuntimeInitialized,
    
    /// Runtime shutting down
    RuntimeShuttingDown,
    
    /// Statistics updated
    StatisticsUpdated {
        total_downloads: usize,
        completed: usize,
        failed: usize,
        running: usize,
    },
    
    /// Notification to display to user
    NotificationRaised {
        message: String,
        duration_ms: u32,
    },
    
    /// Error occurred in runtime
    RuntimeError(String),
}

impl RuntimeEvent {
    pub fn name(&self) -> &str {
        match self {
            RuntimeEvent::DownloadAdded(_) => "DownloadAdded",
            RuntimeEvent::DownloadProgress { .. } => "DownloadProgress",
            RuntimeEvent::DownloadStatusChanged { .. } => "DownloadStatusChanged",
            RuntimeEvent::DownloadCompleted { .. } => "DownloadCompleted",
            RuntimeEvent::DownloadFailed { .. } => "DownloadFailed",
            RuntimeEvent::DownloadPaused(_) => "DownloadPaused",
            RuntimeEvent::DownloadResumed(_) => "DownloadResumed",
            RuntimeEvent::DownloadRemoved(_) => "DownloadRemoved",
            RuntimeEvent::ChunkCompleted { .. } => "ChunkCompleted",
            RuntimeEvent::ChunkFailed { .. } => "ChunkFailed",
            RuntimeEvent::RuntimeInitialized => "RuntimeInitialized",
            RuntimeEvent::RuntimeShuttingDown => "RuntimeShuttingDown",
            RuntimeEvent::StatisticsUpdated { .. } => "StatisticsUpdated",
            RuntimeEvent::NotificationRaised { .. } => "NotificationRaised",
            RuntimeEvent::RuntimeError(_) => "RuntimeError",
        }
    }
}
