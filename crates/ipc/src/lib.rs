pub mod contracts {
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ChecksumChunkFailedEvent {
        pub task_id: Uuid,
        pub chunk_id: Uuid,
        pub error: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DownloadCompletedEvent {
        pub task_id: Uuid,
        pub total_bytes: u64,
        pub duration_secs: f64,
        pub average_throughput_bps: f64,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ChecksumFileFailedEvent {
        pub task_id: Uuid,
        pub error: String,
        pub expected_hash: String,
        pub actual_hash: String,
        pub algorithm: String,
        pub retry_count: u32,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DownloadFailedEvent {
        pub task_id: Uuid,
        pub error: String,
        pub failed_chunks: u32,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RetryScheduledEvent {
        pub task_id: Uuid,
        pub chunk_id: Uuid,
        pub next_attempt_at: u64,
        pub attempt: u32,
        pub max_attempts: u32,
        pub next_try_in_ms: u64,
        pub base_delay_ms: u64,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ProgressPayload {
        pub task_id: Uuid,
        pub downloaded_bytes: u64,
        pub total_bytes: Option<u64>,
        pub progress_percent: Option<f64>,
        pub completed_chunks: u32,
        pub pending_chunks: u32,
        pub active_workers: u32,
        pub throughput_bps: f64,
        pub eta_secs: Option<f64>,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AdaptiveStallDetectedEvent {
        pub task_id: Uuid,
        pub chunk_id: Uuid,
        pub reason: StallReasonDto,
        pub worker_id: Option<Uuid>,
        pub short_rate_bps: f64,
        pub long_rate_bps: f64,
        pub last_progress_secs: Option<u64>,
        pub recommendation: AdaptiveRecommendationDto,
        pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum StallReasonDto {
        NoProgressTimeout,
        HeartbeatLost,
        LowThroughput,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum AdaptiveRecommendationDto {
        NoAction,
        RetryChunk,
        SplitChunk,
    }
}
