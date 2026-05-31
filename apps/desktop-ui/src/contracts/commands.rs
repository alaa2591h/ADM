// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — contracts/commands.rs                                         ║
// ║  Command contracts — operations that can be sent to the runtime          ║
// ║  UI layer dispatches these; backend processes them                       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// RuntimeCommand — operations that can be sent to the runtime
/// These are dispatched from the UI layer and processed by the backend
#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    // ── Download management ─────────────────────────────────────────────────
    /// Add a new download
    AddDownload {
        url: String,
        filename: String,
        connections: u32,
    },
    
    /// Add a scheduled download
    AddScheduledDownload {
        url: String,
        filename: String,
        scheduled_time: String,
        connections: u32,
    },
    
    /// Pause a download
    PauseDownload(String),
    
    /// Resume a download
    ResumeDownload(String),
    
    /// Delete a download
    DeleteDownload(String),
    
    /// Retry a failed download
    RetryDownload(String),

    /// Open the folder containing the downloaded file
    OpenFolder(String),
    
    /// Move download from scheduled to queue
    MoveToQueue(String),
    
    /// Move all selected downloads to the queue
    MoveSelectedToQueue,
    
    /// Select a download row in the UI
    SelectRow { id: String, shift: bool, control: bool },
    /// Select previous download
    SelectPrev { shift: bool, control: bool },
    /// Select next download
    SelectNext { shift: bool, control: bool },
    /// Page up selection
    SelectPageUp { shift: bool, control: bool },
    /// Page down selection
    SelectPageDown { shift: bool, control: bool },
    /// Toggle pause/resume of the currently selected download
    ToggleSelected,

    /// Request chunk details for a specific download
    ShowChunkDetails(String),

    /// Change the active filter in the UI
    SetFilter(String),

    /// Change the search query in the UI
    SearchChanged(String),

    /// Pause all running downloads
    PauseAll,

    /// Resume all paused downloads
    ResumeAll,
    
    /// Delete all downloads
    DeleteAll,
    
    /// Delete selected download
    DeleteSelected,

    // ── UI Settings ──────────────────────────────────────────────────────────
    /// Toggle between LTR (English) and RTL (Arabic) layout direction
    ToggleRTL,

    // ── Configuration ────────────────────────────────────────────────────────
    /// Update runtime configuration
    UpdateConfig {
        max_concurrent: usize,
        max_connections_per_download: u32,
    },

    /// Set download storage path
    SetStoragePath(String),

    /// Save all settings at once — full settings payload.
    /// Every tab in SettingsDialog contributes fields here so the real adapter
    /// can persist the complete configuration in a single atomic write.
    SaveSettings {
        // ── General ──────────────────────────────────────────────────────
        path:                String,   // Default download folder
        launch_on_startup:   bool,
        start_minimized:     bool,
        auto_update:         bool,
        send_telemetry:      bool,
        ask_download_path:   bool,
        create_subfolder:    bool,
        open_folder_on_done: bool,
        on_complete_action:  i32,      // 0=nothing,1=notify,2=sound,3=shutdown,4=sleep

        // ── Downloads ────────────────────────────────────────────────────
        max_simultaneous:    i32,
        max_conns:           String,   // connections per file (kept as String for compat)
        retry_failed:        bool,
        retry_count:         i32,
        retry_delay:         i32,
        skip_duplicates:     bool,
        duplicate_action:    i32,      // 0=rename,1=overwrite,2=ask,3=skip
        verify_integrity:    bool,
        auto_extract:        bool,

        // ── Connection ───────────────────────────────────────────────────
        use_proxy:           bool,
        proxy_type:          i32,      // 0=HTTP,1=HTTPS,2=SOCKS5,3=SOCKS4
        proxy_auth:          bool,
        bind_interface:      i32,      // 0=auto,1=ethernet,2=wifi
        timeout_secs:        i32,
        force_ipv4:          bool,
        server_rate_limit:   bool,
        remember_creds:      bool,
        user_agent:          i32,      // 0=ADM,1=Chrome,2=Firefox,3=Custom

        // ── Speed ────────────────────────────────────────────────────────
        global_dl_limit:     bool,
        global_ul_limit:     bool,
        speed_profile:       i32,      // 0=full,1=limited,2=scheduled
        slow_speed_kbps:     i32,

        // ── Scheduler ────────────────────────────────────────────────────
        scheduler_enabled:   bool,
        sched_speed_mode:    i32,      // 0=full,1=slow,2=pause

        // ── Browser ──────────────────────────────────────────────────────
        capture_chrome:      bool,
        capture_firefox:     bool,
        capture_edge:        bool,
        capture_brave:       bool,
        capture_opera:       bool,
        capture_all:         bool,
        capture_confirm:     bool,
        capture_video:       bool,
        min_capture_mb:      i32,

        // ── Notifications ────────────────────────────────────────────────
        notif_done:          bool,
        notif_fail:          bool,
        notif_pause:         bool,
        notif_disk:          bool,
        notif_new:           bool,
        sound_done:          bool,
        sound_type:          i32,      // 0=chime,1=bell,2=pop,3=custom
        do_not_disturb:      bool,

        // ── Appearance ───────────────────────────────────────────────────
        theme_mode:          i32,      // 0=dark,1=light,2=auto
        font_size:           i32,      // 0=small,1=medium,2=large
        taskbar_progress:    bool,
        speed_graph:         bool,
        compact_rows:        bool,
        show_chunks:         bool,
        animations:          bool,

        // ── Security ─────────────────────────────────────────────────────
        app_lock:            bool,
        biometric:           bool,
        lock_timeout_mins:   i32,
        av_scan:             bool,
        block_malware:       bool,
        warn_exe:            bool,
        https_only:          bool,
        verify_ssl:          bool,
        history_retention:   i32,      // 0=forever,1=90d,2=30d,3=7d,4=never
        clear_on_exit:       bool,

        // ── Advanced ─────────────────────────────────────────────────────
        workers:             String,   // thread-pool size (0=auto)
        chunk_size_kb:       i32,
        buffer_mb:           i32,
        disk_write_strategy: i32,      // 0=sequential,1=preallocate,2=sparse
        rest_api:            bool,
        daemon_host:         String,   // e.g. "127.0.0.1"
        daemon_port:         String,   // e.g. "7878"
        log_level:           i32,      // 0=off,1=error,2=warn,3=info,4=debug,5=verbose
        max_log_mb:          i32,

        // ── Language ─────────────────────────────────────────────────────
        language:            i32,      // 0=auto,1=en,2=ar,3=fr,4=de,5=es,6=zh,7=ja,8=ru
        date_fmt:            i32,      // 0=DD/MM,1=MM/DD,2=ISO
        num_fmt:             i32,      // 0=western,1=arabic,2=european
    },
    
    // ── Housekeeping ────────────────────────────────────────────────────────
    /// Request full state sync
    RequestStateSync,
    
    /// Request statistics update
    RequestStatistics,

    // ── Settings actions ─────────────────────────────────────────────────
    /// Open a native folder picker and return the selected path
    BrowseFolder,

    /// Reset all settings to their defaults
    ResetSettings,

    /// Open a native file picker for a language pack (.adml)
    BrowseLanguagePack,

    /// Open the logs folder in the system file manager
    OpenLogsFolder,

    /// Dismiss the current toast notification immediately
    DismissNotification,

    /// Open a URL or path in the system default app (browser / file manager)
    OpenUrl(String),
}

impl RuntimeCommand {
    pub fn name(&self) -> &str {
        match self {
            RuntimeCommand::AddDownload { .. } => "AddDownload",
            RuntimeCommand::AddScheduledDownload { .. } => "AddScheduledDownload",
            RuntimeCommand::PauseDownload(_) => "PauseDownload",
            RuntimeCommand::ResumeDownload(_) => "ResumeDownload",
            RuntimeCommand::DeleteDownload(_) => "DeleteDownload",
            RuntimeCommand::RetryDownload(_) => "RetryDownload",
            RuntimeCommand::OpenFolder(_) => "OpenFolder",
            RuntimeCommand::MoveToQueue(_) => "MoveToQueue",
            RuntimeCommand::MoveSelectedToQueue => "MoveSelectedToQueue",
            RuntimeCommand::SelectRow { .. } => "SelectRow",
            RuntimeCommand::SelectPrev { .. } => "SelectPrev",
            RuntimeCommand::SelectNext { .. } => "SelectNext",
            RuntimeCommand::SelectPageUp { .. } => "SelectPageUp",
            RuntimeCommand::SelectPageDown { .. } => "SelectPageDown",
            RuntimeCommand::ToggleSelected => "ToggleSelected",
            RuntimeCommand::ShowChunkDetails(_) => "ShowChunkDetails",
            RuntimeCommand::SetFilter(_) => "SetFilter",
            RuntimeCommand::SearchChanged(_) => "SearchChanged",
            RuntimeCommand::PauseAll => "PauseAll",
            RuntimeCommand::ResumeAll => "ResumeAll",
            RuntimeCommand::DeleteAll => "DeleteAll",
            RuntimeCommand::DeleteSelected => "DeleteSelected",
            RuntimeCommand::ToggleRTL => "ToggleRTL",
            RuntimeCommand::UpdateConfig { .. } => "UpdateConfig",
            RuntimeCommand::SetStoragePath(_) => "SetStoragePath",
            RuntimeCommand::SaveSettings { .. } => "SaveSettings",
            RuntimeCommand::RequestStateSync => "RequestStateSync",
            RuntimeCommand::RequestStatistics => "RequestStatistics",
            RuntimeCommand::BrowseFolder => "BrowseFolder",
            RuntimeCommand::ResetSettings => "ResetSettings",
            RuntimeCommand::BrowseLanguagePack => "BrowseLanguagePack",
            RuntimeCommand::OpenLogsFolder => "OpenLogsFolder",
            RuntimeCommand::DismissNotification => "DismissNotification",
            RuntimeCommand::OpenUrl(_) => "OpenUrl",
        }
    }
}
