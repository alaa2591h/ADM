// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — contracts/dto.rs                                              ║
// ║  Data Transfer Objects — shared between UI and Runtime                   ║
// ║  These represent the canonical data model for backend integration        ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::collections::HashMap;

/// DownloadStatus — canonical enum for backend and UI
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadStatus {
    Running,
    Completed,
    Paused,
    Queued,
    Failed,
    Scheduled,
    Deleted,
}

impl DownloadStatus {
    pub fn as_str(&self) -> &str {
        match self {
            DownloadStatus::Running => "Running",
            DownloadStatus::Completed => "Completed",
            DownloadStatus::Paused => "Paused",
            DownloadStatus::Queued => "Queued",
            DownloadStatus::Failed => "Failed",
            DownloadStatus::Scheduled => "Scheduled",
            DownloadStatus::Deleted => "Deleted",
        }
    }
}

/// ChunkDTO — represents a single download chunk
#[derive(Debug, Clone)]
pub struct ChunkDTO {
    pub id: u32,
    pub start_byte: u64,
    pub end_byte: u64,
    pub downloaded_bytes: u64,
    pub status: DownloadStatus,
}

impl ChunkDTO {
    pub fn new(id: u32, start_byte: u64, end_byte: u64) -> Self {
        ChunkDTO {
            id,
            start_byte,
            end_byte,
            downloaded_bytes: 0,
            status: DownloadStatus::Queued,
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.end_byte.saturating_sub(self.start_byte) + 1
    }

    pub fn progress_percent(&self) -> f64 {
        let total = self.total_bytes();
        if total == 0 {
            0.0
        } else {
            (self.downloaded_bytes as f64 / total as f64) * 100.0
        }
    }
}

/// StatisticsDTO — overall download statistics
#[derive(Debug, Clone, Default)]
pub struct StatisticsDTO {
    pub total_downloads: usize,
    pub completed: usize,
    pub failed: usize,
    pub running: usize,
    pub paused: usize,
    pub queued: usize,
    pub total_bytes_downloaded: u64,
    pub total_bytes_size: u64,
}

impl StatisticsDTO {
    pub fn new() -> Self {
        StatisticsDTO {
            total_downloads: 0,
            completed: 0,
            failed: 0,
            running: 0,
            paused: 0,
            queued: 0,
            total_bytes_downloaded: 0,
            total_bytes_size: 0,
        }
    }

    pub fn overall_progress(&self) -> f64 {
        if self.total_bytes_size == 0 {
            0.0
        } else {
            (self.total_bytes_downloaded as f64 / self.total_bytes_size as f64) * 100.0
        }
    }
}

/// DownloadDTO — complete download item data
/// This is the canonical DTO for transferring download state between layers
#[derive(Debug, Clone)]
pub struct DownloadDTO {
    pub id: String,
    pub filename: String,
    pub url: String,
    pub icon: String,
    pub status: DownloadStatus,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: f64,
    pub error: String,
    pub num_chunks: u32,
    pub chunks: HashMap<u32, ChunkDTO>,
    
    // Optional metadata
    pub created_at: u64,           // timestamp
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub scheduled_time: Option<String>,
}

impl DownloadDTO {
    pub fn new(id: String, filename: String, url: String) -> Self {
        DownloadDTO {
            id,
            filename,
            url,
            icon: String::new(),
            status: DownloadStatus::Queued,
            total_bytes: 0,
            downloaded_bytes: 0,
            speed_bps: 0.0,
            error: String::new(),
            num_chunks: 1,
            chunks: HashMap::new(),
            created_at: 0,
            started_at: None,
            completed_at: None,
            scheduled_time: None,
        }
    }

    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub fn estimated_time_remaining(&self) -> u64 {
        if self.speed_bps <= 0.0 {
            0
        } else {
            let remaining = self.total_bytes.saturating_sub(self.downloaded_bytes);
            (remaining as f64 / self.speed_bps) as u64
        }
    }
}

/// RuntimeConfig — configuration passed to runtime at initialization
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub max_concurrent_downloads: usize,
    pub max_connections_per_download: usize,
    pub chunk_size: u64,
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
    pub user_agent: String,
}

impl RuntimeConfig {
    pub fn default() -> Self {
        RuntimeConfig {
            max_concurrent_downloads: 3,
            max_connections_per_download: 4,
            chunk_size: 1024 * 1024,  // 1 MB
            timeout_seconds: 30,
            retry_attempts: 3,
            user_agent: "APEX-DM/0.2.0".to_string(),
        }
    }
}
