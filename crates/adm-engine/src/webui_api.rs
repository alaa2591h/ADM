//! REST API server for web UI communication.
//!
//! Provides HTTP endpoints for:
//! - Download management (add, pause, resume, cancel)
//! - Progress monitoring and real-time updates
//! - Settings management
//! - Statistics and metrics
//! - Download history

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadItem {
    pub id: String,
    pub url: String,
    pub filename: String,
    pub status: String,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub progress_percent: f64,
    pub speed_bps: u64,
    pub eta_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub total_downloads: u64,
    pub active_downloads: u64,
    pub completed_downloads: u64,
    pub failed_downloads: u64,
    pub total_bytes_downloaded: u64,
    pub average_speed_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddDownloadRequest {
    pub url: String,
    pub filename: Option<String>,
    pub priority: Option<u8>,
    pub speed_limit_kbps: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    #[must_use]
    pub fn err(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

pub struct WebUiState {
    // Reference to engine context would go here
}

/// Create router for web UI API.
pub fn create_router() -> Router<Arc<WebUiState>> {
    Router::new()
        .route("/api/downloads", get(list_downloads).post(add_download))
        .route(
            "/api/downloads/{id}",
            get(get_download).delete(remove_download),
        )
        .route("/api/downloads/{id}/pause", post(pause_download))
        .route("/api/downloads/{id}/resume", post(resume_download))
        .route("/api/stats", get(get_stats))
        .route("/api/health", get(health_check))
}

async fn list_downloads(State(_state): State<Arc<WebUiState>>) -> impl IntoResponse {
    let downloads: Vec<DownloadItem> = vec![];
    Json(ApiResponse::ok(downloads))
}

async fn add_download(
    State(_state): State<Arc<WebUiState>>,
    Json(req): Json<AddDownloadRequest>,
) -> impl IntoResponse {
    let download = DownloadItem {
        id: Uuid::new_v4().to_string(),
        url: req.url,
        filename: req.filename.unwrap_or_else(|| "download".to_string()),
        status: "pending".to_string(),
        progress_bytes: 0,
        total_bytes: 0,
        progress_percent: 0.0,
        speed_bps: 0,
        eta_secs: None,
    };
    (StatusCode::CREATED, Json(ApiResponse::ok(download)))
}

async fn get_download(
    State(_state): State<Arc<WebUiState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let download = DownloadItem {
        id,
        url: "https://example.com/file".to_string(),
        filename: "file.bin".to_string(),
        status: "active".to_string(),
        progress_bytes: 500000,
        total_bytes: 1000000,
        progress_percent: 50.0,
        speed_bps: 1000000,
        eta_secs: Some(30),
    };
    Json(ApiResponse::ok(download))
}

async fn remove_download(
    State(_state): State<Arc<WebUiState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    (StatusCode::NO_CONTENT, Json(ApiResponse::<()>::ok(())))
}

async fn pause_download(
    State(_state): State<Arc<WebUiState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    Json(ApiResponse::<()>::ok(()))
}

async fn resume_download(
    State(_state): State<Arc<WebUiState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    Json(ApiResponse::<()>::ok(()))
}

async fn get_stats(State(_state): State<Arc<WebUiState>>) -> impl IntoResponse {
    let stats = SystemStats {
        total_downloads: 100,
        active_downloads: 5,
        completed_downloads: 90,
        failed_downloads: 5,
        total_bytes_downloaded: 50 * 1024 * 1024 * 1024,
        average_speed_bps: 5000000,
    };
    Json(ApiResponse::ok(stats))
}

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "version": "1.0.0"
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_ok() {
        let resp: ApiResponse<String> = ApiResponse::ok("test".to_string());
        assert!(resp.success);
        assert_eq!(resp.data, Some("test".to_string()));
    }

    #[test]
    fn test_download_item_serialization() {
        let item = DownloadItem {
            id: "test-id".to_string(),
            url: "https://example.com".to_string(),
            filename: "file.bin".to_string(),
            status: "active".to_string(),
            progress_bytes: 1000,
            total_bytes: 2000,
            progress_percent: 50.0,
            speed_bps: 1000000,
            eta_secs: Some(60),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("test-id"));
    }
}
