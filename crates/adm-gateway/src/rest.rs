//! REST API (Axum).

use axum::{
  extract::{Path, State},
  http::StatusCode,
  response::IntoResponse,
  routing::{get, post},
  Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

use adm_engine::{DownloadEngine, DownloadSnapshot, EventBus};

/// Simple token-bucket rate limiter (100 req/s) implemented with atomics.
/// Avoids the `governor` dependency.
static WRITE_LIMITER: std::sync::LazyLock<RateLimiterState> =
  std::sync::LazyLock::new(|| RateLimiterState::new(100));

struct RateLimiterState {
  count: std::sync::atomic::AtomicU64,
  window_start: std::sync::Mutex<std::time::Instant>,
  limit: u64,
}

impl RateLimiterState {
  fn new(limit: u64) -> Self {
    Self {
      count: std::sync::atomic::AtomicU64::new(0),
      window_start: std::sync::Mutex::new(std::time::Instant::now()),
      limit,
    }
  }

  fn check(&self) -> Result<(), ()> {
    let mut start = self.window_start.lock().unwrap();
    if start.elapsed().as_secs() >= 1 {
      *start = std::time::Instant::now();
      self.count.store(0, std::sync::atomic::Ordering::SeqCst);
    }
    let prev = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if prev >= self.limit { Err(()) } else { Ok(()) }
  }
}

/// Check rate limit before processing write operations
/// Returns 429 Too Many Requests if limit exceeded
pub fn check_write_rate_limit() -> Option<(StatusCode, Json<ApiResponse<()>>)> {
  if WRITE_LIMITER.check().is_err() {
    tracing::warn!("⚠️  Rate limit exceeded for write operation");
    return Some((
      StatusCode::TOO_MANY_REQUESTS,
      Json(ApiResponse::err(
        "❌ Too many requests - rate limited to 100 req/sec".to_string(),
      )),
    ));
  }
  None
}

/// Gateway listen configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
  pub bind_addr: SocketAddr,
  pub enable_sse: bool,
}

impl Default for GatewayConfig {
  fn default() -> Self {
    Self {
      bind_addr: "127.0.0.1:8080".parse().expect("valid default bind"),
      enable_sse: true,
    }
  }
}

#[derive(Serialize, Deserialize)]
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

#[derive(Clone)]
pub struct ApiState {
  pub engine: Arc<DownloadEngine>,
  pub event_bus: EventBus,
  pub enable_sse: bool,
  pub auth_token: Option<String>,
}

pub fn create_router(state: ApiState) -> Router {
  let mut router = Router::new()
    .route(
      "/api/v1/downloads",
      post(create_download).get(list_downloads),
    )
    .route(
      "/api/v1/downloads/{id}",
      get(get_download).delete(delete_download),
    )
    // Browser extension compatibility routes
    .route("/v1/add", post(extension_add_tasks))
    .route("/v1/ping", get(health_check))
    // Support both /downloads and /tasks for backward compatibility
    .route("/api/v1/downloads/{id}/pause", post(pause_download))
    .route("/api/v1/downloads/{id}/resume", post(resume_download))
    .route("/api/v1/downloads/{id}/cancel", post(cancel_download))
    .route("/api/v1/downloads/{id}/retry", post(retry_download))
    .route("/api/v1/tasks/{id}/pause", post(pause_download))
    .route("/api/v1/tasks/{id}/resume", post(resume_download))
    .route("/api/v1/tasks/{id}/cancel", post(cancel_download))
    .route("/api/v1/tasks/{id}/retry", post(retry_download))
    .route("/api/v1/system/stats", get(get_system_stats))
    .route("/api/v1/system/health", get(health_check))
    .route("/ws", get(crate::ws::websocket_handler));

  if state.enable_sse {
    router = router.route("/api/v1/events/stream", get(crate::sse::events_stream));
  }

  router.with_state(state)
}

#[derive(Serialize, Deserialize)]
pub struct CreateDownloadRequest {
  pub url: String,
  pub filename: Option<String>,
  pub priority: Option<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateDownloadResponse {
  pub task_id: Uuid,
}

async fn create_download(
  State(state): State<ApiState>,
  Json(req): Json<CreateDownloadRequest>,
) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state
    .engine
    .create_download(&req.url, req.filename.as_deref(), req.priority.unwrap_or(128))
    .await
  {
    Ok(task_id) => (
      StatusCode::CREATED,
      Json(ApiResponse::ok(CreateDownloadResponse { task_id })),
    )
      .into_response(),
    Err(e) => (
      StatusCode::BAD_REQUEST,
      Json(ApiResponse::<CreateDownloadResponse>::err(e.to_string())),
    )
      .into_response(),
  }
}

#[derive(Serialize, Deserialize)]
pub struct DownloadInfo {
  pub task_id: Uuid,
  pub url: String,
  pub filename: String,
  pub progress: f64,
}

fn snapshot_to_info(snapshot: DownloadSnapshot) -> DownloadInfo {
  DownloadInfo {
    task_id: snapshot.task_id,
    url: snapshot.url,
    filename: snapshot.filename,
    progress: snapshot.progress,
  }
}

#[derive(Serialize, Deserialize)]
pub struct ExtensionAddRequest {
  pub items: Vec<ExtensionAddItem>,
}

#[derive(Serialize, Deserialize)]
pub struct ExtensionAddItem {
  pub url: String,
  #[serde(rename = "fileName")]
  pub filename: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ExtensionAddResponse {
  pub ok: bool,
  pub task_ids: Vec<Uuid>,
}

async fn extension_add_tasks(
  State(state): State<ApiState>,
  Json(req): Json<ExtensionAddRequest>,
) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }

  let mut task_ids = Vec::new();
  for item in req.items {
    match state
      .engine
      .create_download(&item.url, item.filename.as_deref(), 128)
      .await
    {
      Ok(id) => task_ids.push(id),
      Err(e) => {
        return (
          StatusCode::INTERNAL_SERVER_ERROR,
          Json(ApiResponse::<()>::err(format!(
            "Failed to add task {}: {}",
            item.url, e
          ))),
        )
          .into_response();
      }
    }
  }

  Json(ExtensionAddResponse {
    ok: true,
    task_ids,
  })
  .into_response()
}

async fn get_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  match state.engine.get_download(id).await {
    Ok(snapshot) => (StatusCode::OK, Json(ApiResponse::ok(snapshot_to_info(snapshot)))),
    Err(e) => (
      StatusCode::NOT_FOUND,
      Json(ApiResponse::<DownloadInfo>::err(e.to_string())),
    ),
  }
}

async fn list_downloads(State(state): State<ApiState>) -> impl IntoResponse {
  match state.engine.list_downloads(100, 0).await {
    Ok(response) => {
      let downloads: Vec<DownloadInfo> = response.into_iter().map(snapshot_to_info).collect();
      (StatusCode::OK, Json(ApiResponse::ok(downloads)))
    }
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<Vec<DownloadInfo>>::err(e.to_string())),
    ),
  }
}

async fn delete_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state.engine.cancel_download(id).await {
    Ok(()) => (StatusCode::NO_CONTENT, Json(ApiResponse::ok(()))).into_response(),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<()>::err(e.to_string())),
    )
      .into_response(),
  }
}

async fn pause_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state.engine.pause_download(id).await {
    Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))).into_response(),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<()>::err(e.to_string())),
    )
      .into_response(),
  }
}

async fn resume_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state.engine.resume_download(id).await {
    Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))).into_response(),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<()>::err(e.to_string())),
    )
      .into_response(),
  }
}

async fn cancel_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state.engine.cancel_download(id).await {
    Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))).into_response(),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<()>::err(e.to_string())),
    )
      .into_response(),
  }
}

async fn retry_download(State(state): State<ApiState>, Path(id): Path<Uuid>) -> impl IntoResponse {
  if let Some(res) = check_write_rate_limit() {
    return res.into_response();
  }
  match state.engine.retry_download(id).await {
    Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))).into_response(),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<()>::err(e.to_string())),
    )
      .into_response(),
  }
}

#[derive(Serialize)]
pub struct SystemStats {
  pub active_downloads: u32,
  pub total_throughput_bps: f64,
}

async fn get_system_stats(State(state): State<ApiState>) -> impl IntoResponse {
  match state.engine.system_stats().await {
    Ok(s) => {
      let stats = SystemStats {
        active_downloads: s.active_downloads,
        total_throughput_bps: s.total_throughput_bps,
      };
      (StatusCode::OK, Json(ApiResponse::ok(stats)))
    }
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(ApiResponse::<SystemStats>::err(e.to_string())),
    ),
  }
}

#[derive(Serialize)]
pub struct HealthResponse {
  pub status: String,
}

async fn health_check(State(_state): State<ApiState>) -> impl IntoResponse {
  (
    StatusCode::OK,
    Json(ApiResponse::ok(HealthResponse {
      status: "healthy".to_string(),
    })),
  )
}
