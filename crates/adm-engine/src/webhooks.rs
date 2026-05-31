//! Webhook integration for remote notifications and event delivery.
//!
//! Sends download events to external HTTP endpoints for:
//! - Real-time monitoring dashboards
//! - Third-party integrations (Slack, Discord, custom APIs)
//! - Event streaming and analytics platforms
//! - Custom automation workflows

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub url: String,
    pub auth_token: Option<String>,
    pub retry_attempts: u32,
    pub timeout_secs: u64,
    pub events: Vec<WebhookEventType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookEventType {
    DownloadStarted,
    DownloadProgress,
    DownloadCompleted,
    DownloadFailed,
    DownloadPaused,
    DownloadResumed,
    TaskQueued,
    TaskRemoved,
    MuxingStarted,
    MuxingCompleted,
    MuxingFailed,
    ArchiveExtracted,
    ChecksumVerified,
}

impl WebhookEventType {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DownloadStarted => "download.started",
            Self::DownloadProgress => "download.progress",
            Self::DownloadCompleted => "download.completed",
            Self::DownloadFailed => "download.failed",
            Self::DownloadPaused => "download.paused",
            Self::DownloadResumed => "download.resumed",
            Self::TaskQueued => "task.queued",
            Self::TaskRemoved => "task.removed",
            Self::MuxingStarted => "muxing.started",
            Self::MuxingCompleted => "muxing.completed",
            Self::MuxingFailed => "muxing.failed",
            Self::ArchiveExtracted => "archive.extracted",
            Self::ChecksumVerified => "checksum.verified",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPayload {
    pub event_type: String,
    pub timestamp: i64,
    pub task_id: String,
    pub data: Value,
}

impl WebhookPayload {
    #[must_use]
    pub fn new(event_type: WebhookEventType, task_id: String, data: Value) -> Self {
        Self {
            event_type: event_type.as_str().to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            task_id,
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WebhookResponse {
    pub status_code: u16,
    pub response_time_ms: u128,
    pub success: bool,
    pub error: Option<String>,
}

/// Send webhook payload with automatic retries.
pub async fn send_webhook(
    config: &WebhookConfig,
    payload: WebhookPayload,
) -> Result<WebhookResponse> {
    if !config.enabled {
        return Ok(WebhookResponse {
            status_code: 0,
            response_time_ms: 0,
            success: true,
            error: None,
        });
    }

    let client = reqwest::Client::new();
    let mut attempt = 0;
    let mut last_error = None;

    while attempt < config.retry_attempts {
        let start = std::time::Instant::now();

        let mut req = client
            .post(&config.url)
            .json(&payload)
            .timeout(Duration::from_secs(config.timeout_secs));

        if let Some(token) = &config.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        req = req.header("Content-Type", "application/json");
        req = req.header("User-Agent", "ADM-WebhookClient/1.0");
        req = req.header("X-ADM-Event", &payload.event_type);
        req = req.header("X-ADM-Task-ID", &payload.task_id);

        match req.send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                let duration = start.elapsed().as_millis();

                if (200..300).contains(&status) {
                    tracing::info!(
                        webhook_url = %config.url,
                        event_type = %payload.event_type,
                        status = status,
                        duration_ms = duration,
                        "Webhook delivery successful"
                    );
                    return Ok(WebhookResponse {
                        status_code: status,
                        response_time_ms: duration,
                        success: true,
                        error: None,
                    });
                } else {
                    last_error = Some(format!("HTTP {status}"));
                    attempt += 1;
                    if attempt < config.retry_attempts {
                        tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
                    }
                }
            }
            Err(e) => {
                last_error = Some(e.to_string());
                attempt += 1;
                if attempt < config.retry_attempts {
                    tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
                }
            }
        }
    }

    let error = last_error.unwrap_or_else(|| "Unknown error".to_string());
    tracing::warn!(
        webhook_url = %config.url,
        event_type = %payload.event_type,
        error = %error,
        attempts = config.retry_attempts,
        "Webhook delivery failed after retries"
    );

    Ok(WebhookResponse {
        status_code: 0,
        response_time_ms: 0,
        success: false,
        error: Some(error),
    })
}

/// Builder for webhook payloads with convenience methods.
pub struct WebhookPayloadBuilder {
    event_type: WebhookEventType,
    task_id: String,
    data: Value,
}

impl WebhookPayloadBuilder {
    #[must_use]
    pub fn new(event_type: WebhookEventType, task_id: String) -> Self {
        Self {
            event_type,
            task_id,
            data: json!({}),
        }
    }

    #[must_use]
    pub fn with_progress(mut self, downloaded: u64, total: u64, percent: f64) -> Self {
        self.data = json!({
            "downloaded_bytes": downloaded,
            "total_bytes": total,
            "progress_percent": percent
        });
        self
    }

    #[must_use]
    pub fn with_error(mut self, error: &str, error_code: Option<String>) -> Self {
        let mut error_obj = json!({
            "message": error
        });
        if let Some(code) = error_code {
            error_obj["code"] = json!(code);
        }
        self.data = error_obj;
        self
    }

    #[must_use]
    pub fn with_completion(
        mut self,
        file_path: String,
        size_bytes: u64,
        duration_secs: f64,
    ) -> Self {
        self.data = json!({
            "file_path": file_path,
            "size_bytes": size_bytes,
            "duration_secs": duration_secs
        });
        self
    }

    #[must_use]
    pub fn with_custom_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }

    #[must_use]
    pub fn build(self) -> WebhookPayload {
        WebhookPayload::new(self.event_type, self.task_id, self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_event_strings() {
        assert_eq!(
            WebhookEventType::DownloadStarted.as_str(),
            "download.started"
        );
        assert_eq!(
            WebhookEventType::DownloadCompleted.as_str(),
            "download.completed"
        );
        assert_eq!(WebhookEventType::MuxingFailed.as_str(), "muxing.failed");
    }

    #[test]
    fn test_webhook_payload_builder() {
        let payload =
            WebhookPayloadBuilder::new(WebhookEventType::DownloadCompleted, "task-123".to_string())
                .with_completion("file.mp4".to_string(), 1024000, 10.5)
                .build();

        assert_eq!(payload.event_type, "download.completed");
        assert_eq!(payload.task_id, "task-123");
        assert_eq!(payload.data["file_path"], "file.mp4");
        assert_eq!(payload.data["size_bytes"], 1024000);
    }

    #[test]
    fn test_webhook_payload_progress() {
        let payload =
            WebhookPayloadBuilder::new(WebhookEventType::DownloadProgress, "task-456".to_string())
                .with_progress(500000, 1000000, 50.0)
                .build();

        assert_eq!(payload.data["downloaded_bytes"], 500000);
        assert_eq!(payload.data["total_bytes"], 1000000);
        assert_eq!(payload.data["progress_percent"], 50.0);
    }

    #[test]
    fn test_webhook_config_disabled() {
        let config = WebhookConfig {
            enabled: false,
            url: "https://example.com/webhook".to_string(),
            auth_token: None,
            retry_attempts: 3,
            timeout_secs: 10,
            events: vec![WebhookEventType::DownloadCompleted],
        };

        assert!(!config.enabled);
    }
}
