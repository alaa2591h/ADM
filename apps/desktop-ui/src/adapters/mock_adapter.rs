// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — adapters/mock_adapter.rs                                      ║
// ║  Mock runtime adapter — wraps the existing fake runtime to implement     ║
// ║  the RuntimeAdapter trait. This allows testing the UI layer with a       ║
// ║  predictable backend before integrating the real Tokio engine.          ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::contracts::{
    RuntimeAdapter, RuntimeCommand, RuntimeConfig, RuntimeError, RuntimeEvent, RuntimeResult,
    DownloadDTO, DownloadStatus, StatisticsDTO,
};
use crate::models::download_item::{DownloadEntry, DownloadStatus as InternalStatus};
use crate::runtime::fake_events::AppEvent as InternalEvent;
use crate::state::app_state::AppState;

/// MockAdapter wraps the internal AppState and provides the RuntimeAdapter interface
/// This is a bridge between the old internal model and the new contract-based interface
pub struct MockAdapter {
    state: Arc<Mutex<AppState>>,
    initialized: bool,
    /// Events converted from internal representation to contract events
    pending_events: VecDeque<RuntimeEvent>,
}

impl MockAdapter {
    /// Create a new mock adapter
    pub fn new(state: Arc<Mutex<AppState>>) -> Self {
        MockAdapter {
            state,
            initialized: false,
            pending_events: VecDeque::new(),
        }
    }

    /// Convert internal DownloadEntry to DTO
    fn entry_to_dto(entry: &DownloadEntry) -> DownloadDTO {
        let mut dto = DownloadDTO::new(
            entry.id.clone(),
            entry.filename.clone(),
            entry.url.clone(),
        );
        
        dto.icon = entry.icon.clone();
        dto.status = Self::internal_status_to_contract(entry.status.clone());
        dto.total_bytes = entry.total_bytes;
        dto.downloaded_bytes = entry.downloaded_bytes;
        dto.speed_bps = entry.speed_bps;
        dto.error = entry.error.clone();
        dto.num_chunks = entry.num_chunks;
        
        dto
    }

    /// Convert internal status to contract status
    fn internal_status_to_contract(status: InternalStatus) -> DownloadStatus {
        match status {
            InternalStatus::Running => DownloadStatus::Running,
            InternalStatus::Completed => DownloadStatus::Completed,
            InternalStatus::Paused => DownloadStatus::Paused,
            InternalStatus::Queued => DownloadStatus::Queued,
            InternalStatus::Failed => DownloadStatus::Failed,
            InternalStatus::Scheduled => DownloadStatus::Scheduled,
            InternalStatus::Deleted   => DownloadStatus::Deleted,
        }
    }

    /// Convert contract command to internal event
    fn command_to_event(command: RuntimeCommand) -> Option<InternalEvent> {
        match command {
            RuntimeCommand::AddDownload { url, filename, connections } => {
                Some(InternalEvent::AddDownload {
                    url,
                    name: filename,
                    conns: connections.to_string(),
                })
            }
            RuntimeCommand::AddScheduledDownload { url, filename, scheduled_time, .. } => {
                Some(InternalEvent::AddScheduled {
                    url,
                    name: filename,
                    sched_time: scheduled_time,
                })
            }
            RuntimeCommand::PauseDownload(id) => {
                Some(InternalEvent::PauseDownload(id))
            }
            RuntimeCommand::ResumeDownload(id) => {
                Some(InternalEvent::ResumeDownload(id))
            }
            RuntimeCommand::DeleteDownload(id) => {
                Some(InternalEvent::DeleteDownload(id))
            }
            RuntimeCommand::RetryDownload(id) => {
                Some(InternalEvent::RetryDownload(id))
            }
            RuntimeCommand::OpenFolder(id) => {
                Some(InternalEvent::OpenFolder(id))
            }
            RuntimeCommand::MoveToQueue(id) => {
                Some(InternalEvent::MoveToQueue(id))
            }
            RuntimeCommand::MoveSelectedToQueue => {
                Some(InternalEvent::MoveSelectedToQueue)
            }
            RuntimeCommand::SelectRow { id, shift, control } => {
                Some(InternalEvent::SelectRow { id, shift, control })
            }
            RuntimeCommand::SelectPrev { shift, control } => {
                Some(InternalEvent::SelectPrev { shift, control })
            }
            RuntimeCommand::SelectNext { shift, control } => {
                Some(InternalEvent::SelectNext { shift, control })
            }
            RuntimeCommand::SelectPageUp { shift, control } => {
                Some(InternalEvent::SelectPageUp { shift, control })
            }
            RuntimeCommand::SelectPageDown { shift, control } => {
                Some(InternalEvent::SelectPageDown { shift, control })
            }
            RuntimeCommand::ToggleSelected => {
                Some(InternalEvent::ToggleSelected)
            }
            RuntimeCommand::ShowChunkDetails(id) => {
                Some(InternalEvent::ShowChunkDetails(id))
            }
            RuntimeCommand::SetFilter(value) => {
                Some(InternalEvent::SetFilter(value))
            }
            RuntimeCommand::SearchChanged(value) => {
                Some(InternalEvent::SearchChanged(value))
            }
            RuntimeCommand::PauseAll => {
                Some(InternalEvent::PauseAll)
            }
            RuntimeCommand::ResumeAll => {
                Some(InternalEvent::ResumeAll)
            }
            RuntimeCommand::DeleteAll => {
                Some(InternalEvent::DeleteAll)
            }
            RuntimeCommand::DeleteSelected => {
                Some(InternalEvent::DeleteSelected)
            }
            RuntimeCommand::ToggleRTL => {
                Some(InternalEvent::ToggleRTL)
            }
            RuntimeCommand::UpdateConfig { .. } => {
                None
            }
            RuntimeCommand::SetStoragePath(_) => {
                None
            }
            RuntimeCommand::SaveSettings {
                path, max_conns, workers,
                launch_on_startup, start_minimized, auto_update, send_telemetry,
                ask_download_path, create_subfolder, open_folder_on_done, on_complete_action,
                max_simultaneous, retry_failed, retry_count, retry_delay,
                skip_duplicates, duplicate_action, verify_integrity, auto_extract,
                use_proxy, proxy_type, proxy_auth, bind_interface, timeout_secs,
                force_ipv4, server_rate_limit, remember_creds, user_agent,
                global_dl_limit, global_ul_limit, speed_profile, slow_speed_kbps,
                scheduler_enabled, sched_speed_mode,
                capture_chrome, capture_firefox, capture_edge, capture_brave, capture_opera,
                capture_all, capture_confirm, capture_video, min_capture_mb,
                notif_done, notif_fail, notif_pause, notif_disk, notif_new,
                sound_done, sound_type, do_not_disturb,
                theme_mode, font_size, taskbar_progress, speed_graph,
                compact_rows, show_chunks, animations,
                app_lock, biometric, lock_timeout_mins, av_scan, block_malware,
                warn_exe, https_only, verify_ssl, history_retention, clear_on_exit,
                chunk_size_kb, buffer_mb, disk_write_strategy, rest_api,
                daemon_host, daemon_port, log_level, max_log_mb,
                language, date_fmt, num_fmt,
            } => {
                Some(InternalEvent::SaveSettings {
                    path, max_conns, workers,
                    launch_on_startup, start_minimized, auto_update, send_telemetry,
                    ask_download_path, create_subfolder, open_folder_on_done, on_complete_action,
                    max_simultaneous, retry_failed, retry_count, retry_delay,
                    skip_duplicates, duplicate_action, verify_integrity, auto_extract,
                    use_proxy, proxy_type, proxy_auth, bind_interface, timeout_secs,
                    force_ipv4, server_rate_limit, remember_creds, user_agent,
                    global_dl_limit, global_ul_limit, speed_profile, slow_speed_kbps,
                    scheduler_enabled, sched_speed_mode,
                    capture_chrome, capture_firefox, capture_edge, capture_brave, capture_opera,
                    capture_all, capture_confirm, capture_video, min_capture_mb,
                    notif_done, notif_fail, notif_pause, notif_disk, notif_new,
                    sound_done, sound_type, do_not_disturb,
                    theme_mode, font_size, taskbar_progress, speed_graph,
                    compact_rows, show_chunks, animations,
                    app_lock, biometric, lock_timeout_mins, av_scan, block_malware,
                    warn_exe, https_only, verify_ssl, history_retention, clear_on_exit,
                    chunk_size_kb, buffer_mb, disk_write_strategy, rest_api,
                    daemon_host, daemon_port, log_level, max_log_mb,
                    language, date_fmt, num_fmt,
                })
            }
            RuntimeCommand::RequestStateSync => {
                None
            }
            RuntimeCommand::RequestStatistics => {
                None
            }
            // ── New settings actions ──────────────────────────────────────
            RuntimeCommand::DismissNotification => {
                Some(InternalEvent::DismissNotification)
            }
            RuntimeCommand::ResetSettings => {
                // In the mock we treat this as a SaveSettings with default values.
                // The real adapter would reload the config file from disk.
                Some(InternalEvent::SaveSettings {
                    path:                String::new(),
                    max_conns:           "8".to_string(),
                    workers:             "0".to_string(),
                    launch_on_startup:   true,
                    start_minimized:     false,
                    auto_update:         true,
                    send_telemetry:      false,
                    ask_download_path:   true,
                    create_subfolder:    true,
                    open_folder_on_done: false,
                    on_complete_action:  1,
                    max_simultaneous:    5,
                    retry_failed:        true,
                    retry_count:         3,
                    retry_delay:         5,
                    skip_duplicates:     true,
                    duplicate_action:    0,
                    verify_integrity:    true,
                    auto_extract:        false,
                    use_proxy:           false,
                    proxy_type:          2,
                    proxy_auth:          false,
                    bind_interface:      0,
                    timeout_secs:        30,
                    force_ipv4:          false,
                    server_rate_limit:   true,
                    remember_creds:      true,
                    user_agent:          0,
                    global_dl_limit:     false,
                    global_ul_limit:     false,
                    speed_profile:       0,
                    slow_speed_kbps:     256,
                    scheduler_enabled:   true,
                    sched_speed_mode:    0,
                    capture_chrome:      true,
                    capture_firefox:     true,
                    capture_edge:        false,
                    capture_brave:       false,
                    capture_opera:       false,
                    capture_all:         true,
                    capture_confirm:     true,
                    capture_video:       true,
                    min_capture_mb:      1,
                    notif_done:          true,
                    notif_fail:          true,
                    notif_pause:         false,
                    notif_disk:          true,
                    notif_new:           true,
                    sound_done:          true,
                    sound_type:          0,
                    do_not_disturb:      false,
                    theme_mode:          0,
                    font_size:           1,
                    taskbar_progress:    true,
                    speed_graph:         true,
                    compact_rows:        false,
                    show_chunks:         true,
                    animations:          true,
                    app_lock:            false,
                    biometric:           false,
                    lock_timeout_mins:   15,
                    av_scan:             true,
                    block_malware:       true,
                    warn_exe:            true,
                    https_only:          false,
                    verify_ssl:          true,
                    history_retention:   1,
                    clear_on_exit:       false,
                    chunk_size_kb:       512,
                    buffer_mb:           16,
                    disk_write_strategy: 0,
                    rest_api:            false,
                    daemon_host:         "127.0.0.1".to_string(),
                    daemon_port:         "7878".to_string(),
                    log_level:           1,
                    max_log_mb:          50,
                    language:            0,
                    date_fmt:            1,
                    num_fmt:             0,
                })
            }
            // BrowseFolder and BrowseLanguagePack are OS-level native pickers;
            // in mock mode they are no-ops (the real adapter opens the OS dialog).
            RuntimeCommand::BrowseFolder => None,
            RuntimeCommand::BrowseLanguagePack => None,
            RuntimeCommand::OpenLogsFolder => Some(InternalEvent::OpenLogsFolder),
            RuntimeCommand::OpenUrl(url) => Some(InternalEvent::OpenUrl(url)),
        }
    }

    /// Generate events based on state changes — called once per tick.
    /// Statistics are only emitted every 10 ticks (~1 s) to reduce noise.
    fn generate_events(&mut self) {
        let tick = {
            let state = self.state.lock().unwrap();
            state.tick_count
        };
        if tick % 10 == 0 {
            let stats = self.compute_statistics();
            self.pending_events.push_back(RuntimeEvent::StatisticsUpdated {
                total_downloads: stats.total_downloads,
                completed:       stats.completed,
                failed:          stats.failed,
                running:         stats.running,
            });
        }
    }

    /// Compute statistics from current state
    fn compute_statistics(&self) -> StatisticsDTO {
        let state = self.state.lock().unwrap();
        let mut stats = StatisticsDTO::new();
        
        for entry in &state.downloads {
            stats.total_downloads += 1;
            stats.total_bytes_size += entry.total_bytes;
            stats.total_bytes_downloaded += entry.downloaded_bytes;
            
            match entry.status {
                InternalStatus::Completed => stats.completed += 1,
                InternalStatus::Failed    => stats.failed    += 1,
                InternalStatus::Running   => stats.running   += 1,
                InternalStatus::Paused    => stats.paused    += 1,
                InternalStatus::Queued    => stats.queued    += 1,
                InternalStatus::Scheduled => { /* not counted in active stats */ }
                InternalStatus::Deleted   => { /* excluded entirely */ }
            }
        }
        
        stats
    }
}

impl RuntimeAdapter for MockAdapter {
    fn initialize(&mut self, config: RuntimeConfig) -> RuntimeResult<()> {
        self.initialized = true;
        let mut state = self.state.lock().unwrap();
        state.max_concurrent = config.max_concurrent_downloads;
        self.pending_events.push_back(RuntimeEvent::RuntimeInitialized);
        Ok(())
    }

    fn tick(&mut self) -> RuntimeResult<bool> {
        if !self.initialized {
            return Err(RuntimeError::NotInitialized);
        }

        // Advance the internal state by one tick (100ms)
        const DT: f64 = 0.1;
        self.state.lock().unwrap().tick(DT);

        // Generate events based on state changes
        self.generate_events();

        Ok(true)
    }

    fn shutdown(&mut self) -> RuntimeResult<()> {
        self.initialized = false;
        self.pending_events.push_back(RuntimeEvent::RuntimeShuttingDown);
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn execute_command(&mut self, command: RuntimeCommand) -> RuntimeResult<()> {
        if let Some(event) = Self::command_to_event(command) {
            self.state.lock().unwrap().push_event(event);
            Ok(())
        } else {
            Ok(())
        }
    }

    fn get_downloads(&self) -> RuntimeResult<Vec<DownloadDTO>> {
        let state = self.state.lock().unwrap();
        Ok(state.downloads
            .iter()
            .map(Self::entry_to_dto)
            .collect())
    }

    fn get_download(&self, id: &str) -> RuntimeResult<DownloadDTO> {
        let state = self.state.lock().unwrap();
        state.downloads
            .iter()
            .find(|d| d.id == id)
            .map(Self::entry_to_dto)
            .ok_or_else(|| RuntimeError::DownloadNotFound(id.to_string()))
    }

    fn get_statistics(&self) -> RuntimeResult<StatisticsDTO> {
        Ok(self.compute_statistics())
    }

    fn drain_events(&mut self) -> Vec<RuntimeEvent> {
        self.pending_events.drain(..).collect()
    }

    fn has_pending_events(&self) -> bool {
        !self.pending_events.is_empty()
    }
}

/// Factory for creating mock adapters.
/// NOTE: The factory always creates a *fresh* AppState for standalone use
/// (e.g. integration tests). For the main application the `FakeRuntime`
/// constructs `MockAdapter` directly with the shared `Arc<Mutex<AppState>>`
/// so that all layers share the same state.
pub struct MockAdapterFactory {
    initial_state: Arc<Mutex<AppState>>,
}

impl MockAdapterFactory {
    pub fn new() -> Box<dyn crate::contracts::RuntimeAdapterFactory> {
        Box::new(MockAdapterFactory {
            initial_state: Arc::new(Mutex::new(AppState::new())),
        })
    }

    /// Create a factory that shares an existing state (for the main runtime).
    pub fn with_state(state: Arc<Mutex<AppState>>) -> Box<dyn crate::contracts::RuntimeAdapterFactory> {
        Box::new(MockAdapterFactory { initial_state: state })
    }
}

impl crate::contracts::RuntimeAdapterFactory for MockAdapterFactory {
    fn create(&self) -> Box<dyn RuntimeAdapter> {
        Box::new(MockAdapter::new(self.initial_state.clone()))
    }
}
