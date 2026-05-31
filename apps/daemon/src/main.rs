#![allow(
    clippy::assigning_clones,
    clippy::too_many_lines,
)]

//! APEX Download Manager (ADM) daemon — composition root.
//!
//! Wires every crate together and owns the process lifetime.
//! Nothing in here contains business logic — that all lives in `engine`.
//!
//! ## Responsibility map
//! - Construct singletons (`EventBus`, Storage, Network, Scheduler, `QueueManager`)
//! - Register all JSON-RPC method handlers against the Dispatcher
//! - Spawn transport tasks (WS gateway, Unified API Gateway, native messaging host)
//! - Hold every Arc alive until Ctrl-C / SIGTERM
//!
//! ## Transport layers
//! - WebSocket gateway (port 9001) — primary IPC for the Qt/QML UI
//! - Unified API Gateway (port 57423) — REST & WebSocket (Axum)
//! - Native messaging host — stdin/stdout JSON-RPC proxy for the extension
//!   to wake/check the app; download dispatch goes through Unified API
//!
//! ## Extractor gating (Phase-1 isolation)
//! The `extractor` crate (HLS/DASH) is compiled in but gated behind a URL
//! content-type heuristic.  Plain HTTP URLs (e.g. .zip, .exe, .pdf) skip
//! extractor probing entirely and are enqueued as direct downloads.
//! Media manifest URLs (.m3u8, .mpd) are allowed through the extractor.
//! This prevents the extractor from blocking the critical path for every
//! ordinary download while preserving extensibility for Phase 2.

use anyhow::Result;
use api_gateway::{create_router as create_api_router, ApiState as GatewayApiState};
use utils::validation::{FilePathValidator, UrlValidator};
use ipc::contracts::{
    methods, AddTaskBatchParams, AddTaskParams, AddTaskResult, ChunkDto, ChunkStateDto,
    EngineStatusResult, GetChunksParams, GetChunksResult, ListTasksParams, ListTasksResult,
    TaskDto, TaskIdParams, TaskStateDto, TaskSummaryDto,
};
use settings_core::SettingsManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;
use url::Url;

#[cfg(test)]
mod api_rate_limit_tests;
mod command;
#[cfg(test)]
mod encryption_tests;
#[cfg(test)]
mod final_integration_tests;
mod https_server;
#[cfg(test)]
mod performance_tests;
mod query;
#[cfg(test)]
mod security_scanning_tests;
mod state;
mod supervisor;
mod telegram;
#[cfg(test)]
mod tls_certificate_tests;

// ── DTO mapping helpers ────────────────────────────────────────────────────────
// These live in main.rs (not in a crate) because they cross the
// storage ↔ ipc boundary — a mapping concern of the composition root only.

fn task_state_str_to_dto(s: &str) -> TaskStateDto {
    match s {
        "created" => TaskStateDto::Created,
        "queued" => TaskStateDto::Queued,
        "running" => TaskStateDto::Running,
        "paused" => TaskStateDto::Paused,
        "completed" => TaskStateDto::Completed,
        _ => TaskStateDto::Failed,
    }
}

fn persisted_to_summary(t: &adm_storage::PersistedTask) -> TaskSummaryDto {
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
        state: task_state_str_to_dto(&t.state),
        total_bytes: t.total_bytes,
        downloaded_bytes: t.downloaded_bytes,
        progress_percent: progress,
        throughput_bps: 0.0, // live throughput comes from transfer.snapshot events
        eta_secs: None,
        error: t.last_error.clone(),
    }
}

fn persisted_to_dto(t: &adm_storage::PersistedTask) -> TaskDto {
    let progress = t.total_bytes.map(|total| {
        if total == 0 {
            0.0
        } else {
            (t.downloaded_bytes as f64 / total as f64) * 100.0
        }
    });
    TaskDto {
        id: t.id,
        url: t.url.clone(),
        filename: t.filename.clone(),
        state: task_state_str_to_dto(&t.state),
        total_bytes: t.total_bytes,
        downloaded_bytes: t.downloaded_bytes,
        progress_percent: progress,
        created_at: t.created_at,
        updated_at: t.updated_at,
        error: t.last_error.clone(),
        tags: vec![],
    }
}

fn persisted_chunk_to_dto(c: &adm_storage::PersistedChunk) -> ChunkDto {
    let state_dto = match c.state.as_str() {
        "pending" => ChunkStateDto::Pending,
        "reserved" => ChunkStateDto::Reserved,
        "connecting" => ChunkStateDto::Connecting,
        "downloading" => ChunkStateDto::Downloading,
        "flushing" => ChunkStateDto::Flushing,
        "completed" => ChunkStateDto::Completed,
        "retrying" => ChunkStateDto::Retrying,
        "cancelled" => ChunkStateDto::Cancelled,
        _ => ChunkStateDto::Failed,
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
        reserved_at_ms: c.reserved_at.map(|ts| ts * 1000), // secs → ms
        last_progress_ms: None, // populated by live transfer.snapshot events
    }
}

// ── Composition root ──────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    adm_observability::init_tracing();
    adm_observability::init_metrics();

    info!("APEX Download Manager (ADM) daemon starting — composition root");

    let boot_time = Instant::now();

    // ── Telemetry ────────────────────────────────────────────────────────────
    let telemetry_manager = Arc::new(adm_observability::TelemetryManager::new());

    // ── Engine initialization ────────────────────────────────────────────────
    let settings_manager = Arc::new(SettingsManager::load_from_path("settings.toml")?);
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "adm.db".to_string());
    let download_dir = std::env::var("DOWNLOAD_PATH").ok().map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let s = settings_manager.get(|s| s.downloads.default_download_directory.clone());
            if s.is_empty() {
                std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
            } else {
                std::path::PathBuf::from(s)
            }
        });

    let engine = adm_engine::DownloadEngine::bootstrap(db_path, download_dir).await?;
    let event_bus = engine.event_bus();
    let dispatcher = jsonrpc::DispatcherHandle::new();
    dispatcher.attach_event_bus(event_bus.clone());

    let queue_manager = engine.queue_manager.clone();
    let storage = engine.context.storage.clone();
    let download_dir = engine.context.download_dir.clone();
    let lease_registry = engine.context.lease_registry.clone();
    let accepting_tasks = Arc::new(AtomicBool::new(true));

    let command_handler = Arc::new(command::CommandHandler::new(engine.clone()));
    let query_handler = Arc::new(query::QueryHandler::new(storage.clone()));

    // ── Crash recovery ────────────────────────────────────────────────────────
    // Reset any tasks that were left in volatile states (running/preparing) by
    // a previous daemon crash so QueueManager can pick them up and resume.
    {
        let task_supervisor = supervisor::TaskSupervisor::new(storage.clone());
        match task_supervisor.recover_crashed_tasks().await {
            Ok(recovered) if !recovered.is_empty() => {
                tracing::info!("{} crashed task(s) recovered and re-queued", recovered.len());
                for _ in &recovered {
                    let _ = queue_manager.start_next().await;
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("Crash recovery encountered an error: {e}"),
        }
    }

    info!("Engine runtime ready");

    // ── Extractor registry (Phase-1 gated) ───────────────────────────────────
    let extractor_network = engine.context.network.clone();
    let extractor_registry = Arc::new(extractor::ExtractorRegistry::with_defaults());
    info!(
        "ExtractorRegistry ready ({} extractors registered)",
        extractor_registry.extractor_count(),
    );

    // ── IPC method handlers ───────────────────────────────────────────────────

    register_echo(&dispatcher).await;
    register_task_add(
        &dispatcher,
        queue_manager.clone(),
        extractor_registry.clone(),
        extractor_network.clone(),
        accepting_tasks.clone(),
    )
    .await;
    register_task_add_batch(
        &dispatcher,
        queue_manager.clone(),
        extractor_registry.clone(),
        extractor_network,
        accepting_tasks.clone(),
    )
    .await;
    register_task_list(&dispatcher, storage.clone()).await;
    register_task_get(&dispatcher, storage.clone()).await;
    register_task_pause(&dispatcher, queue_manager.clone()).await;
    register_task_resume(&dispatcher, queue_manager.clone()).await;
    register_task_cancel(&dispatcher, queue_manager.clone(), storage.clone()).await;
    register_task_chunks(&dispatcher, storage.clone()).await;
    register_engine_status(&dispatcher, storage.clone(), engine.context.scheduler.worker_pool(), boot_time).await;
    register_settings(&dispatcher, settings_manager.clone()).await;
    register_runtime_snapshot(&dispatcher, telemetry_manager.clone()).await;

    info!("All IPC method handlers registered ({} methods)", 15);

    // ── Transport tasks ───────────────────────────────────────────────────────

    let ws_bind = std::env::var("WS_BIND").unwrap_or_else(|_| "127.0.0.1:9001".to_string());

    // 1. WebSocket gateway — primary IPC channel for Qt/QML UI.
    let ws_dispatcher = dispatcher.clone();
    let ws_bind_clone = ws_bind.clone();
    tokio::spawn(async move {
        if let Err(e) = ws_gateway::run(ws_dispatcher, &ws_bind_clone).await {
            tracing::error!("WS gateway fatal: {:?}", e);
        }
    });

    // 2. Native messaging host — stdin/stdout proxy for extension wake/status
    //    checks via chrome.runtime.sendNativeMessage.
    let nm_dispatcher = dispatcher.clone();
    tokio::spawn(async move {
        if let Err(e) = native_host::run(nm_dispatcher).await {
            tracing::error!("native host fatal: {:?}", e);
        }
    });

    // 3. Lease reaper — background task that expires stale write-reservations
    //    left over from crashes.  Runs every 10 seconds.
    {
        let lease_event_bus = event_bus.clone();
        let storage_shutdown = storage.shutdown.clone();
        tokio::spawn(async move {
            adm_engine::run_lease_reaper(
                lease_registry.clone(),
                lease_event_bus,
                Duration::from_secs(10),
                storage_shutdown,
            )
            .await;
        });
    }

    // 5. REST & WebSocket API Gateway (Unified)
    let api_bind = std::env::var("API_BIND").unwrap_or_else(|_| "127.0.0.1:57423".to_string());
    let api_state = GatewayApiState {
        engine: engine.clone(),
        event_bus: event_bus.clone(),
        enable_sse: true,
        auth_token: std::env::var("API_TOKEN").ok(),
    };
    let api_addr: std::net::SocketAddr = api_bind.parse()?;
    let api_router = create_api_router(api_state);

    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(api_addr).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::error!("Failed to bind API server to {api_addr}: {e}");
                return;
            }
        };
        tracing::info!("🔐 Unified API gateway listening on http://{api_addr}");
        if let Err(e) = axum::serve(
            listener,
            api_router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        {
            tracing::error!("API server error: {e}");
        }
    });

    // 6. Telegram Bot Integration
    let tg_settings = settings_manager.get(|s| s.telegram.clone());
    let tg_token_env = std::env::var("TELEGRAM_BOT_TOKEN").ok();
    let mut tg_settings = tg_settings;
    if let Some(token) = tg_token_env {
        tg_settings.bot_token = token;
        tg_settings.enabled = true;
    }
    let tg_cmd = command_handler.clone();
    let tg_q = query_handler.clone();
    let tg_bus = event_bus.clone();
    let tg_dl_dir = download_dir.clone();
    tokio::spawn(async move {
        if let Err(e) =
            telegram::start_telegram_integration(tg_settings, tg_cmd, tg_q, tg_bus, tg_dl_dir).await
        {
            tracing::error!("Failed to start Telegram Bot: {e}");
        }
    });

    info!("APEX Download Manager (ADM) daemon initialized — awaiting shutdown signal");

    let shutdown_timeout = Duration::from_secs(10);
    let shutdown_start = Instant::now();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received (Ctrl-C)");
        }
    }

    info!("Initiating graceful shutdown...");

    info!("Step 1/3: Stopping new task acceptance");
    accepting_tasks.store(false, Ordering::SeqCst);

    info!("Step 2/3: Signaling active tasks to finish");
    let shutdown_result = tokio::select! {
        result = queue_manager.shutdown() => {
            info!("Queue manager shutdown completed");
            result
        }
        () = tokio::time::sleep(shutdown_timeout) => {
            tracing::warn!(
                "Shutdown timeout ({:?}) exceeded after {:?}",
                shutdown_timeout,
                shutdown_start.elapsed()
            );
            Err(anyhow::anyhow!("Shutdown timeout exceeded"))
        }
    };

    if let Err(e) = shutdown_result {
        tracing::error!("Error during engine shutdown: {:?}", e);
    }

    info!("Step 3/3: Final cleanup");
    tokio::time::sleep(Duration::from_millis(500)).await;

    drop(queue_manager);
    drop(storage);
    info!(
        "Shutdown complete (total time: {:?})",
        shutdown_start.elapsed()
    );
    Ok(())
}

// ── Method registrations ──────────────────────────────────────────────────────
// Each registration is a free function to keep main() readable and to give
// each handler its own precise capture list.

async fn register_echo(dispatcher: &jsonrpc::DispatcherHandle) {
    dispatcher
        .register_method(
            "echo",
            Arc::new(jsonrpc::FuncMethod(Arc::new(|_ctx, params| {
                Box::pin(async move { Ok(params) })
            }))),
        )
        .await;
}

async fn register_task_add(
    dispatcher: &jsonrpc::DispatcherHandle,
    qm: Arc<adm_engine::QueueManager>,
    registry: Arc<extractor::ExtractorRegistry>,
    extractor_client: Arc<dyn adm_engine::NetworkClient>,
    accepting_tasks: Arc<AtomicBool>,
) {
    dispatcher
        .register_method(
            methods::TASK_ADD,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let qm = qm.clone();
                let registry = registry.clone();
                let client = extractor_client.clone();
                let accepting_tasks = accepting_tasks.clone();

                Box::pin(async move {
                    let p: AddTaskParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;

                    if !accepting_tasks.load(Ordering::SeqCst) {
                        return Err(jsonrpc::RpcError::Internal(
                            "APEX daemon is shutting down and not accepting new tasks".into(),
                        ));
                    }

                    let url = p.url.trim().to_string();
                    let validated_url = UrlValidator::validate(&url).map_err(|e| {
                        tracing::warn!(url = %url, "Invalid URL rejected: {}", e);
                        jsonrpc::RpcError::InvalidParams(format!("Invalid URL: {e}"))
                    })?;

                    if let Some(ref filename) = p.filename {
                        FilePathValidator::validate(filename).map_err(|e| {
                            jsonrpc::RpcError::InvalidParams(format!("Invalid filename: {e}"))
                        })?;
                    }

                    let url = validated_url.as_str().to_string();

                    // ── Extractor probe (Phase-1 gated) ──────────────────
                    // The extractor crate (HLS, DASH) is only invoked when
                    // the URL path ends with a known manifest extension.
                    // All other URLs bypass extractor probing entirely and
                    // are scheduled as plain HTTP downloads immediately.
                    //
                    // This guard keeps the critical path latency low for
                    // ordinary downloads (.zip, .exe, .pdf, …) and prevents
                    // the extractor from issuing network requests speculatively.
                    //
                    // Phase 2: once the core is stable, `url_needs_extraction`
                    // can be extended to cover content-type sniffing via a
                    // HEAD probe before allowing extractors to run.
                    let task_ids: Vec<uuid::Uuid> = if url_needs_extraction(&url) {
                        match registry.extract(&url, client).await {
                            // No extractor claimed the URL → plain download.
                            Err(extractor::ExtractorError::Unsupported(_)) => {
                                tracing::debug!(url, "no extractor matched — plain download");
                                vec![enqueue_plain_task(&qm, &url, &p).await?]
                            }

                            // Valid manifest but empty → fall back.
                            Err(extractor::ExtractorError::NoStreams) => {
                                tracing::warn!(
                                    url,
                                    "extractor found no streams — falling back to plain download"
                                );
                                vec![enqueue_plain_task(&qm, &url, &p).await?]
                            }

                            // Successful extraction → segment tasks.
                            Ok(graph) => {
                                tracing::info!(
                                    url,
                                    format = graph.format,
                                    variants = graph.variants.len(),
                                    "extractor produced StreamGraph — enqueuing segment tasks",
                                );
                                enqueue_stream_graph(&qm, graph, p.filename.as_deref(), &p).await?
                            }

                            // Hard extractor error — surface to caller.
                            Err(e) => {
                                return Err(jsonrpc::RpcError::Internal(format!(
                                    "extractor error: {e}"
                                )));
                            }
                        }
                    } else {
                        // Fast path: plain direct download, no extractor overhead.
                        tracing::debug!(url, "fast-path plain download (no manifest extension)");
                        vec![enqueue_plain_task(&qm, &url, &p).await?]
                    };

                    // Best-effort: kick the engine if it was idle.
                    let _ = qm.start_next().await;

                    serde_json::to_value(AddTaskResult { task_ids })
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn register_task_add_batch(
    dispatcher: &jsonrpc::DispatcherHandle,
    qm: Arc<adm_engine::QueueManager>,
    registry: Arc<extractor::ExtractorRegistry>,
    extractor_client: Arc<dyn adm_engine::NetworkClient>,
    accepting_tasks: Arc<AtomicBool>,
) {
    dispatcher
        .register_method(
            methods::TASK_ADD_BATCH,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let qm = qm.clone();
                let registry = registry.clone();
                let client = extractor_client.clone();
                let accepting_tasks = accepting_tasks.clone();

                Box::pin(async move {
                    let p: AddTaskBatchParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;

                    if !accepting_tasks.load(Ordering::SeqCst) {
                        return Err(jsonrpc::RpcError::Internal(
                            "APEX daemon is shutting down and not accepting new tasks".into(),
                        ));
                    }

                    let mut task_ids = Vec::new();
                    for task_params in p.tasks {
                        let url = task_params.url.trim().to_string();
                        let validated_url = UrlValidator::validate(&url).map_err(|e| {
                            tracing::warn!(url = %url, "Invalid URL rejected: {}", e);
                            jsonrpc::RpcError::InvalidParams(format!("Invalid URL: {e}"))
                        })?;

                        if let Some(ref filename) = task_params.filename {
                            FilePathValidator::validate(filename).map_err(|e| {
                                jsonrpc::RpcError::InvalidParams(format!("Invalid filename: {e}"))
                            })?;
                        }

                        let url = validated_url.as_str().to_string();
                        if url_needs_extraction(&url) {
                            match registry.extract(&url, client.clone()).await {
                                Err(extractor::ExtractorError::Unsupported(_)) => {
                                    tracing::debug!(url, "no extractor matched — plain download");
                                    task_ids.push(enqueue_plain_task(&qm, &url, &task_params).await?);
                                }
                                Err(extractor::ExtractorError::NoStreams) => {
                                    tracing::warn!(
                                        url,
                                        "extractor found no streams — falling back to plain download"
                                    );
                                    task_ids.push(enqueue_plain_task(&qm, &url, &task_params).await?);
                                }
                                Ok(graph) => {
                                    tracing::info!(
                                        url,
                                        format = graph.format,
                                        variants = graph.variants.len(),
                                        "extractor produced StreamGraph — enqueuing segment tasks",
                                    );
                                    task_ids.extend(
                                        enqueue_stream_graph(
                                            &qm,
                                            graph,
                                            task_params.filename.as_deref(),
                                            &task_params,
                                        )
                                        .await?,
                                    );
                                }
                                Err(e) => {
                                    return Err(jsonrpc::RpcError::Internal(format!(
                                        "extractor error: {e}"
                                    )));
                                }
                            }
                        } else {
                            tracing::debug!(url, "fast-path plain download (no manifest extension)");
                            task_ids.push(enqueue_plain_task(&qm, &url, &task_params).await?);
                        }
                    }

                    let _ = qm.start_next().await;
                    serde_json::to_value(AddTaskResult { task_ids })
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn enqueue_stream_graph(
    qm: &adm_engine::QueueManager,
    graph: extractor::StreamGraph,
    filename_override: Option<&str>,
    params: &AddTaskParams,
) -> Result<Vec<uuid::Uuid>, jsonrpc::RpcError> {
    let av_group_id = uuid::Uuid::new_v4().to_string();
    // Choose the most playable variant, not only the highest bitrate.
    let variant = match select_best_variant(&graph) {
        Some(v) => v,
        None => {
            return Err(jsonrpc::RpcError::Internal(
                "StreamGraph has no variants".into(),
            ));
        }
    };

    let title_base = filename_override
        .map(std::string::ToString::to_string)
        .or_else(|| graph.title.clone())
        .unwrap_or_else(|| "stream".to_string());

    let format = graph.format.clone();
    let variant_label = variant.label.clone();

    let mut ids = Vec::with_capacity(variant.segments.len().max(1));

    let has_embedded_audio = variant.codecs.iter().any(|c| {
        let lc = c.to_ascii_lowercase();
        lc.contains("mp4a")
            || lc.contains("aac")
            || lc.contains("opus")
            || lc.contains("vorbis")
            || lc.contains("ac-3")
    });

    if variant.segments.is_empty() {
        // The extractor returned a variant with no pre-resolved segments
        // (e.g. a live stream or a lazy DASH manifest).  Enqueue the
        // playlist URL itself so the engine handles it as a progressive
        // download and the user gets *something* rather than nothing.
        tracing::warn!(
            url = variant.playlist_url,
            label = variant_label,
            "StreamGraph variant has no segments — enqueuing playlist URL as plain task",
        );
        let mut task = adm_engine::DownloadTask::new(&variant.playlist_url);
        task.filename = Some(title_base.clone());
        apply_task_params(&mut task, params);
        let id = qm
            .add_task(task)
            .await
            .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
        ids.push(id);
    } else {
        tracing::debug!(
            segments = variant.segments.len(),
            label = variant_label,
            format,
            "enqueuing segment tasks",
        );

        for (i, seg) in variant.segments.iter().enumerate() {
            let seg_filename = format!("{title_base}.{format}.seg{i:05}");
            let mut task = adm_engine::DownloadTask::new(&seg.url);
            task.filename = Some(seg_filename);
            task.av_group_id = Some(av_group_id.clone());
            task.av_role = Some(
                if has_embedded_audio {
                    "video_av"
                } else {
                    "video"
                }
                .to_string(),
            );
            task.av_order = Some(i as i64);
            apply_task_params(&mut task, params);
            // Propagate HLS AES encryption metadata (if any) via task headers
            if let Some(ref key_url) = seg.encryption_key_url {
                task.headers
                    .push(("X-ADM-Encryption-Key-URL".to_string(), key_url.clone()));
                task.headers
                    .push(("X-ADM-Encryption-SEQ".to_string(), seg.sequence.to_string()));
            }
            if let Some(ref iv) = seg.encryption_iv {
                task.headers
                    .push(("X-ADM-Encryption-IV".to_string(), iv.clone()));
            }

            // Embed provenance metadata in tags via the task's extra fields.
            // The engine stores `tags` as a JSON array; using a well-known
            // prefix lets the future mux step identify sibling segments.
            // NOTE: adm_engine::DownloadTask has no `tags` field yet; we leave
            // a breadcrumb comment here as a migration guide.
            // task.tags = vec![
            //     format!("adm:stream_graph"),
            //     format!("adm:format={format}"),
            //     format!("adm:variant={variant_label}"),
            //     format!("adm:seq={i}"),
            // ];

            let id = qm
                .add_task(task)
                .await
                .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

            ids.push(id);
        }

        tracing::info!(
            count = ids.len(),
            label = variant_label,
            "enqueued segment tasks for stream variant",
        );
    }

    // Native fallback for separate A/V manifests:
    // if the chosen video variant appears video-only, enqueue a companion audio track.
    if !has_embedded_audio {
        let chosen_audio = variant
            .associated_tracks
            .iter()
            .chain(graph.standalone_tracks.iter())
            .filter(|t| t.kind == extractor::StreamKind::Audio)
            .max_by_key(|t| if t.default_track { 2 } else { 1 });

        if let Some(audio_track) = chosen_audio {
            let audio_name = format!("{title_base}.audio.m4a");
            let mut audio_task = adm_engine::DownloadTask::new(&audio_track.url);
            audio_task.filename = Some(audio_name);
            audio_task.av_group_id = Some(av_group_id.clone());
            audio_task.av_role = Some("audio".to_string());
            audio_task.av_order = Some(0);
            apply_task_params(&mut audio_task, params);
            let audio_id = qm
                .add_task(audio_task)
                .await
                .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
            ids.push(audio_id);
            tracing::info!(
                label = variant_label,
                track_url = audio_track.url,
                "video-only variant detected: enqueued companion audio track",
            );
        }
    }

    // Optional subtitle assist: enqueue a default subtitle track if provided.
    let subtitle_track = variant
        .associated_tracks
        .iter()
        .chain(graph.standalone_tracks.iter())
        .filter(|t| t.kind == extractor::StreamKind::Subtitle)
        .max_by_key(|t| if t.default_track { 2 } else { 1 });

    if let Some(sub) = subtitle_track {
        let sub_ext = sub
            .url
            .split('?')
            .next()
            .and_then(|u| u.rsplit('.').next())
            .unwrap_or("vtt");
        let sub_name = format!("{title_base}.subtitle.{sub_ext}");
        let mut sub_task = adm_engine::DownloadTask::new(&sub.url);
        sub_task.filename = Some(sub_name);
        sub_task.av_group_id = Some(av_group_id.clone());
        sub_task.av_role = Some("subtitle".to_string());
        sub_task.av_order = Some(0);
        apply_task_params(&mut sub_task, params);
        match qm
            .add_task(sub_task)
            .await
            .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
        {
            Ok(sub_id) => ids.push(sub_id),
            Err(err) => tracing::warn!(error = %err, "failed to enqueue subtitle track"),
        }
    }

    Ok(ids)
}

fn select_best_variant(graph: &extractor::StreamGraph) -> Option<extractor::StreamVariant> {
    // Prefer the variant that has:
    // 1) a video stream, 2) embedded audio codec (more playable), 3) segment list.
    // If there are ties, keep the default marker and then bandwidth/resolution.
    let mut candidates: Vec<_> = graph.video_variants().into_iter().collect();
    if candidates.is_empty() {
        candidates = graph.variants.iter().collect();
    }

    fn has_audio_codec(v: &extractor::StreamVariant) -> bool {
        v.codecs.iter().any(|c| {
            let lc = c.to_ascii_lowercase();
            lc.contains("mp4a")
                || lc.contains("aac")
                || lc.contains("opus")
                || lc.contains("vorbis")
                || lc.contains("ac-3")
        })
    }

    fn score(v: &extractor::StreamVariant) -> (u8, u8, u8, u64, u32) {
        let playable = u8::from(has_audio_codec(v));
        let has_segments = u8::from(!v.segments.is_empty());
        let preferred_default = u8::from(v.is_default);
        let height = v.resolution.map_or(0, |(_, h)| h);
        (
            playable,
            has_segments,
            preferred_default,
            v.bandwidth_bps,
            height,
        )
    }

    candidates.into_iter().max_by_key(|v| score(v)).cloned()
}

fn apply_task_params(task: &mut adm_engine::DownloadTask, params: &AddTaskParams) {
    if task.filename.is_none() {
        if let Some(ref filename) = params.filename {
            task.filename = Some(filename.clone());
        }
    }

    if let Some(auth) = &params.auth {
        auth.apply_to_headers(&mut task.headers);
    }

    if let Some(limit) = params.speed_limit_kbps {
        task.speed_limit_kbps = Some(u64::from(limit));
    }

    if let Some(priority) = params.priority {
        task.priority = priority;
    }
}

async fn enqueue_plain_task(
    qm: &adm_engine::QueueManager,
    url: &str,
    params: &AddTaskParams,
) -> Result<uuid::Uuid, jsonrpc::RpcError> {
    let mut task = adm_engine::DownloadTask::new(url);
    apply_task_params(&mut task, params);
    let task_id = qm
        .add_task(task)
        .await
        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
    Ok(task_id)
}

/// Returns `true` only for URLs whose path ends with a known media-manifest
/// extension (.m3u8 / .mpd).  All other URLs — including plain file downloads
/// take the fast path and skip extractor probing.
///
/// This keeps Phase-2 media logic isolated from the critical download path.
fn url_needs_extraction(url: &str) -> bool {
    // Only HTTP/HTTPS manifest URLs are currently supported by the
    // extractor pipeline. FTP/FTPS URLs are downloaded as plain tasks
    // because the extractor registry is built around web-based playlist
    // resolution and segment fetching.
    let parsed = match Url::parse(url) {
        Ok(parsed) => parsed,
        Err(_) => return false,
    };

    match parsed.scheme() {
        "http" | "https" => {
            let lower_path = parsed.path().to_lowercase();
            lower_path.ends_with(".m3u8") || lower_path.ends_with(".mpd")
        }
        _ => false,
    }
}

#[cfg(test)]
mod variant_selection_tests {
    use super::select_best_variant;
    use extractor::{StreamGraph, StreamKind, StreamVariant};

    fn mk_variant(
        label: &str,
        bandwidth_bps: u64,
        codecs: &[&str],
        is_default: bool,
        with_segments: bool,
    ) -> StreamVariant {
        StreamVariant {
            kind: StreamKind::Video,
            label: label.to_string(),
            bandwidth_bps,
            resolution: None,
            codecs: codecs
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            playlist_url: format!("https://cdn.example.com/{label}.m3u8"),
            segments: if with_segments {
                vec![extractor::SegmentInfo {
                    url: "https://cdn.example.com/seg0.ts".to_string(),
                    sequence: 0,
                    duration_secs: 2.0,
                    byte_range: None,
                    encryption_key_url: None,
                    encryption_iv: None,
                }]
            } else {
                vec![]
            },
            associated_tracks: vec![],
            is_default,
        }
    }

    #[test]
    fn prefers_playable_video_with_audio_codec() {
        let mut g = StreamGraph::new("https://x", "hls");
        g.variants.push(mk_variant(
            "video_only",
            8_000_000,
            &["avc1.640028"],
            true,
            true,
        ));
        g.variants.push(mk_variant(
            "av_with_audio",
            5_000_000,
            &["avc1.4d401f", "mp4a.40.2"],
            false,
            true,
        ));

        let best = select_best_variant(&g).expect("variant");
        assert_eq!(best.label, "av_with_audio");
    }
}

async fn register_task_list(dispatcher: &jsonrpc::DispatcherHandle, storage: Arc<adm_engine::Storage>) {
    dispatcher
        .register_method(
            methods::TASK_LIST,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let storage = storage.clone();
                Box::pin(async move {
                    let p: ListTasksParams =
                        serde_json::from_value(params).unwrap_or(ListTasksParams {
                            state_filter: None,
                            limit: None,
                            offset: None,
                        });

                    use adm_storage::TaskRepository;
                    let all = storage
                        .load_all_tasks()
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

                    let filtered: Vec<_> = all
                        .iter()
                        .filter(|t| {
                            if let Some(ref filter) = p.state_filter {
                                filter.contains(&task_state_str_to_dto(&t.state))
                            } else {
                                true
                            }
                        })
                        .collect();

                    let total = filtered.len();
                    let offset = p.offset.unwrap_or(0);
                    let tasks = filtered
                        .into_iter()
                        .skip(offset)
                        .take(p.limit.unwrap_or(usize::MAX))
                        .map(persisted_to_summary)
                        .collect();

                    serde_json::to_value(ListTasksResult { tasks, total })
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn register_task_get(dispatcher: &jsonrpc::DispatcherHandle, storage: Arc<adm_engine::Storage>) {
    dispatcher
        .register_method(
            methods::TASK_GET,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let storage = storage.clone();
                Box::pin(async move {
                    let p: TaskIdParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;

                    use adm_storage::TaskRepository;
                    let persisted = storage
                        .load_task(p.task_id)
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?
                        .ok_or_else(|| {
                            jsonrpc::RpcError::InvalidParams(format!(
                                "task {} not found",
                                p.task_id
                            ))
                        })?;

                    serde_json::to_value(persisted_to_dto(&persisted))
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn register_task_pause(
    dispatcher: &jsonrpc::DispatcherHandle,
    qm: Arc<adm_engine::QueueManager>,
) {
    dispatcher
        .register_method(
            methods::TASK_PAUSE,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let qm = qm.clone();
                Box::pin(async move {
                    let p: TaskIdParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;
                    qm.pause_task(p.task_id)
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    Ok(serde_json::json!({ "ok": true }))
                })
            }))),
        )
        .await;
}

async fn register_task_resume(
    dispatcher: &jsonrpc::DispatcherHandle,
    qm: Arc<adm_engine::QueueManager>,
) {
    dispatcher
        .register_method(
            methods::TASK_RESUME,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let qm = qm.clone();
                Box::pin(async move {
                    let p: TaskIdParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;
                    qm.resume_task(p.task_id)
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    // Kick the engine — the resumed task is now re-queued.
                    let _ = qm.start_next().await;
                    Ok(serde_json::json!({ "ok": true }))
                })
            }))),
        )
        .await;
}

async fn register_task_cancel(
    dispatcher: &jsonrpc::DispatcherHandle,
    qm: Arc<adm_engine::QueueManager>,
    storage: Arc<adm_engine::Storage>,
) {
    dispatcher
        .register_method(
            methods::TASK_CANCEL,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let qm = qm.clone();
                let storage = storage.clone();
                Box::pin(async move {
                    let p: TaskIdParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;

                    // Remove from in-memory queue (best-effort — may already be
                    // running, in which case pause_task returns TaskNotFound).
                    let _ = qm.pause_task(p.task_id).await;

                    // Hard-delete from persistent store.
                    use adm_storage::TaskRepository;
                    storage
                        .delete_task(p.task_id)
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

                    Ok(serde_json::json!({ "ok": true, "task_id": p.task_id }))
                })
            }))),
        )
        .await;
}

async fn register_task_chunks(
    dispatcher: &jsonrpc::DispatcherHandle,
    storage: Arc<adm_engine::Storage>,
) {
    dispatcher
        .register_method(
            methods::TASK_CHUNKS,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let storage = storage.clone();
                Box::pin(async move {
                    let p: GetChunksParams = serde_json::from_value(params)
                        .map_err(|e| jsonrpc::RpcError::InvalidParams(e.to_string()))?;

                    use adm_storage::ChunkRepository;
                    let persisted = storage
                        .load_chunks_for_task(p.task_id)
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

                    let chunks: Vec<ChunkDto> =
                        persisted.iter().map(persisted_chunk_to_dto).collect();

                    serde_json::to_value(GetChunksResult { chunks })
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn register_engine_status(
    dispatcher: &jsonrpc::DispatcherHandle,
    storage: Arc<adm_engine::Storage>,
    worker_pool: Arc<adm_engine::WorkerPool>,
    boot_time: Instant,
) {
    dispatcher
        .register_method(
            methods::ENGINE_STATUS,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, _params| {
                let storage = storage.clone();
                let worker_pool = worker_pool.clone();
                Box::pin(async move {
                    use adm_storage::TaskRepository;
                    let all = storage
                        .load_all_tasks()
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

                    let active_tasks = all.iter().filter(|t| t.state == "running").count();
                    let queued_tasks = all.iter().filter(|t| t.state == "queued").count();
                    let worker_count = worker_pool.snapshot().max_workers;

                    serde_json::to_value(EngineStatusResult {
                        active_tasks,
                        queued_tasks,
                        total_throughput_bps: 0.0, // aggregated by live transfer.snapshot events
                        worker_count,
                        uptime_secs: boot_time.elapsed().as_secs(),
                    })
                    .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

async fn register_settings(
    dispatcher: &jsonrpc::DispatcherHandle,
    settings_mgr: Arc<settings_core::SettingsManager>,
) {
    use ipc::contracts::methods as m;

    // settings.snapshot -> returns full AppSettings as JSON
    let snapshot_settings_mgr = Arc::clone(&settings_mgr);
    dispatcher
        .register_method(
            m::SETTINGS_SNAPSHOT,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, _params| {
                let mgr = Arc::clone(&snapshot_settings_mgr);
                Box::pin(async move {
                    let snap = mgr.snapshot();
                    let v = serde_json::to_value(&snap)
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    Ok(v)
                })
            }))),
        )
        .await;

    // settings.update -> accepts { key: string, value: any } and applies via update()
    let update_settings_mgr = Arc::clone(&settings_mgr);
    dispatcher
        .register_method(
            m::SETTINGS_UPDATE,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let mgr = Arc::clone(&update_settings_mgr);
                Box::pin(async move {
                    let key = params
                        .get("key")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| jsonrpc::RpcError::InvalidParams("missing key".into()))?;
                    let value = params
                        .get("value")
                        .cloned()
                        .ok_or_else(|| jsonrpc::RpcError::InvalidParams("missing value".into()))?;

                    mgr.update(|s| {
                        // apply simple scalar updates for common types; keep robust fallback
                        match value {
                            serde_json::Value::Bool(b) => match key {
                                "general.launch_on_startup" => s.general.launch_on_startup = b,
                                "general.minimize_to_tray" => s.general.minimize_to_tray = b,
                                _ => {}
                            },
                            serde_json::Value::String(ref st) => match key {
                                "appearance.theme_mode" => {
                                    if let Ok(mode) =
                                        serde_json::from_value::<settings_schema::ThemeMode>(
                                            serde_json::Value::String(st.clone()),
                                        )
                                    {
                                        s.appearance.theme_mode = mode;
                                    }
                                }
                                "appearance.accent_color" => s.appearance.accent_color = st.clone(),
                                "general.language_mode" => {
                                    if let Ok(mode) =
                                        serde_json::from_value::<settings_schema::LanguageMode>(
                                            serde_json::Value::String(st.clone()),
                                        )
                                    {
                                        s.language_mode = mode;
                                    }
                                }
                                "last_opened_page" => s.last_opened_page = st.clone(),
                                "last_search_query" => s.last_search_query = st.clone(),
                                _ => {}
                            },
                            serde_json::Value::Number(ref n) => {
                                if let Some(i) = n.as_i64() {
                                    if key == "downloads.concurrent_downloads_limit" {
                                        s.downloads.concurrent_downloads_limit = i as usize;
                                    }
                                }
                            }
                            _ => {}
                        }
                    })
                    .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;

                    // return updated snapshot
                    let snap = mgr.snapshot();
                    serde_json::to_value(&snap)
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;

    // settings.search -> { query } -> returns array of SearchEntry
    let search_settings_mgr = Arc::clone(&settings_mgr);
    dispatcher
        .register_method(
            m::SETTINGS_SEARCH,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, params| {
                let mgr = Arc::clone(&search_settings_mgr);
                Box::pin(async move {
                    let q = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
                    let results = mgr.search_entries(q);
                    let v = serde_json::to_value(
                        results
                            .into_iter()
                            .map(|r| {
                                serde_json::json!({
                                    "key": r.key,
                                    "label": r.label,
                                    "group": r.group,
                                    "description": r.description,
                                    "value": r.value
                                })
                            })
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    Ok(v)
                })
            }))),
        )
        .await;

    // settings.save -> persist immediately
    let save_settings_mgr = Arc::clone(&settings_mgr);
    dispatcher
        .register_method(
            m::SETTINGS_SAVE,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, _params| {
                let mgr = Arc::clone(&save_settings_mgr);
                Box::pin(async move {
                    mgr.save_now()
                        .await
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    Ok(serde_json::json!({ "ok": true }))
                })
            }))),
        )
        .await;

    // settings.reset -> reset to defaults
    let reset_settings_mgr = Arc::clone(&settings_mgr);
    dispatcher
        .register_method(
            m::SETTINGS_RESET,
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, _params| {
                let mgr = Arc::clone(&reset_settings_mgr);
                Box::pin(async move {
                    mgr.reset_defaults()
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))?;
                    Ok(serde_json::json!({ "ok": true }))
                })
            }))),
        )
        .await;
}

async fn register_runtime_snapshot(
    dispatcher: &jsonrpc::DispatcherHandle,
    telemetry_manager: Arc<adm_observability::TelemetryManager>,
) {
    dispatcher
        .register_method(
            "runtime.snapshot",
            Arc::new(jsonrpc::FuncMethod(Arc::new(move |_ctx, _params| {
                let tm = telemetry_manager.clone();
                Box::pin(async move {
                    let scheduler = adm_runtime::get_scheduler_diagnostics();
                    let snapshot = tm.collect_runtime_snapshot(scheduler);
                    serde_json::to_value(snapshot)
                        .map_err(|e| jsonrpc::RpcError::Internal(e.to_string()))
                })
            }))),
        )
        .await;
}

// ── WebSocket gateway ─────────────────────────────────────────────────────────
//
// Inlined here — the gateway is not independently deployable; it needs the
// Dispatcher by value and shares the process lifetime with the daemon.

mod ws_gateway {
    use anyhow::Result;
    use futures_util::{SinkExt, StreamExt};

    use jsonrpc::DispatcherHandle;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    pub async fn run(dispatcher: DispatcherHandle, bind_addr: &str) -> Result<()> {
        let listener = TcpListener::bind(bind_addr).await?;
        tracing::info!("WS gateway listening on {}", bind_addr);

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!("WS accept error: {:?}", e);
                    continue;
                }
            };
            tracing::debug!("WS connection from {}", peer);

            let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    tracing::warn!("WS handshake failed for {}: {:?}", peer, e);
                    continue;
                }
            };

            let dispatcher = dispatcher.clone();
            tokio::spawn(async move {
                handle_connection(ws_stream, dispatcher, peer).await;
            });
        }
    }

    async fn handle_connection(
        ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        dispatcher: DispatcherHandle,
        peer: std::net::SocketAddr,
    ) {
        let (mut sink, mut src) = ws_stream.split();

        // Subscribe to all topics — the UI filters by topic on its side.
        let (sub_id, mut events) = dispatcher.subscribe("*").await;

        loop {
            tokio::select! {
                biased;

                // ── Inbound request ──────────────────────────────────────────
                msg = src.next() => {
                    match msg {
                        Some(Ok(Message::Text(txt))) => {
                            match serde_json::from_str::<ipc::RequestMessage>(&txt) {
                                Ok(req) => {
                                    let id     = req.id;
                                    let method = req.method.clone();
                                    let res    = dispatcher.call_method(&method, req.params).await;
                                    let (result_opt, error_opt) = match res {
                                        Ok(v)  => (Some(v), None),
                                        Err(e) => (None,    Some(e.message())),
                                    };
                                    let resp = ipc::ResponseMessage {
                                        id,
                                        msg_type: ipc::MessageType::Response,
                                        result: result_opt,
                                        error: error_opt,
                                    };
                                    if let Ok(s) = serde_json::to_string(&resp) {
                                        if sink.send(Message::Text(s.into())).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("WS bad JSON from {}: {}", peer, e);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(d))) => { let _ = sink.send(Message::Pong(d)).await; }
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => {}
                    }
                }

                // ── Outbound event ───────────────────────────────────────────
                evt = events.recv() => {
                    match evt {
                        Some(e) => {
                            if let Ok(s) = serde_json::to_string(&e) {
                                if sink.send(Message::Text(s.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        dispatcher.unsubscribe(sub_id).await;
        tracing::debug!("WS connection closed: {}", peer);
    }
}

// ── Native Messaging host ─────────────────────────────────────────────────────
//
// Chrome/Firefox Native Messaging protocol:
//   stdin  → [4-byte LE u32 length][utf-8 JSON]
//   stdout ← [4-byte LE u32 length][utf-8 JSON]
//
// Blocking stdio is moved to spawn_blocking to avoid stalling the executor.

mod native_host {
    use anyhow::Result;

    use jsonrpc::DispatcherHandle;
    use std::io::{self, Read, Write};

    fn read_frame() -> io::Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match io::stdin().lock().read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 {
            return Ok(Some(vec![]));
        }
        let mut buf = vec![0u8; len];
        io::stdin().lock().read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    fn write_frame(payload: &[u8]) -> io::Result<()> {
        let len = payload.len() as u32;
        let mut out = io::stdout().lock();
        out.write_all(&len.to_le_bytes())?;
        out.write_all(payload)?;
        out.flush()
    }

    pub async fn run(dispatcher: DispatcherHandle) -> Result<()> {
        loop {
            let frame = tokio::task::spawn_blocking(read_frame).await??;
            match frame {
                None => {
                    tracing::info!("native messaging port closed");
                    break;
                }
                Some(bytes) if bytes.is_empty() => continue,
                Some(bytes) => {
                    let txt = String::from_utf8_lossy(&bytes);
                    match serde_json::from_str::<ipc::RequestMessage>(&txt) {
                        Ok(req) => {
                            let id = req.id;
                            let res = dispatcher.call_method(&req.method, req.params).await;
                            let (result_opt, error_opt) = match res {
                                Ok(v) => (Some(v), None),
                                Err(e) => (None, Some(e.message())),
                            };
                            let resp = ipc::ResponseMessage {
                                id,
                                msg_type: ipc::MessageType::Response,
                                result: result_opt,
                                error: error_opt,
                            };
                            if let Ok(payload) = serde_json::to_vec(&resp) {
                                tokio::task::spawn_blocking(move || write_frame(&payload))
                                    .await??;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("native host malformed frame: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

