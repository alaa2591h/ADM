// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — state/app_state.rs                                            ║
// ║  Global application state. Single source of truth for the entire app.   ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::collections::VecDeque;

use crate::models::download_item::{
    DownloadEntry, DownloadStatus, SimState,
    icon_for, category_for, filename_from_url,
    initial_downloads, xorshift,
};
use crate::models::statistics::AppStatistics;
use crate::runtime::fake_events::AppEvent;
use crate::state::download_state::{advance_tick, retry_download, start_queued};
use crate::state::filters::{FilterCounts, FilterMode, passes_filter};

/// A pending toast notification.
#[derive(Debug, Clone)]
pub struct Notification {
    pub text: String,
    /// Ticks remaining before auto-dismiss (each tick = 100 ms → 30 ticks = 3 s).
    pub ttl: u32,
}

pub struct AppState {
    pub downloads: Vec<DownloadEntry>,
    pub filter:    FilterMode,
    pub search:    String,
    pub selected_id: String,
    pub selected_ids: Vec<String>,
    pub selected_anchor: Option<String>,

    /// ID counter for new downloads.
    next_id: u32,

    /// Tick counter (100 ms per tick).
    pub tick_count: u64,

    /// Current toast notification (at most one at a time).
    pub notification: Option<Notification>,

    /// Whether data is currently loading (shows skeleton placeholders).
    pub is_loading: bool,

    /// Whether layout is right-to-left (RTL for Arabic, Hebrew, etc.).
    pub is_rtl: bool,

    /// Maximum concurrent running downloads.
    pub max_concurrent: usize,

    /// Small RNG for misc use.
    rng: u64,

    /// Queued events to drain on the next tick.
    pending_events: VecDeque<AppEvent>,

    /// The ID of the download whose chunk details are shown in ChunkPanel.
    pub chunk_detail_id: String,
}

impl AppState {
    pub fn new() -> Self {
        AppState {
            downloads:       initial_downloads(),
            filter:          FilterMode::All,
            search:          String::new(),
            selected_id:     String::new(),
            selected_ids:    Vec::new(),
            selected_anchor: None,
            next_id:         11,
            tick_count:      0,
            notification:    None,
            is_loading:      false,
            is_rtl:          false,
            max_concurrent:  3,
            rng:             0xcafe_babe_dead_beef,
            pending_events:      VecDeque::new(),
            chunk_detail_id:     String::new(),
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Public API
    // ─────────────────────────────────────────────────────────────────────

    /// Queue an event for processing on the next simulation tick.
    pub fn push_event(&mut self, event: AppEvent) {
        self.pending_events.push_back(event);
    }

    /// Advance the simulation by `dt` seconds (called every timer tick).
    pub fn tick(&mut self, dt: f64) {
        // Drain and process queued events first.
        let events: Vec<AppEvent> = self.pending_events.drain(..).collect();
        for ev in events {
            self.handle_event(ev);
        }

        self.tick_count += 1;

        // Tick notification TTL.
        if let Some(n) = &mut self.notification {
            if n.ttl == 0 {
                self.notification = None;
            } else {
                n.ttl -= 1;
            }
        }

        // Advance all Running downloads.
        let mut new_notifications: Vec<String> = Vec::new();
        for dl in &mut self.downloads {
            if let Some(notif) = advance_tick(dl, dt) {
                new_notifications.push(notif);
            }
        }

        // Show the most recent notification.
        if let Some(text) = new_notifications.into_iter().last() {
            self.show_notification(text);
        }

        // Auto-purge entries that have been soft-deleted for > 30 ticks (~3 s).
        // This prevents the downloads Vec from growing unboundedly in long sessions.
        if self.tick_count % 30 == 0 {
            self.purge_deleted();
        }

        // Auto-start Queued downloads when slots are available.
        self.schedule_queued();

        // Trigger Scheduled downloads after their countdown expires.
        self.trigger_scheduled();
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Event handling
    // ─────────────────────────────────────────────────────────────────────

    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::AddDownload { url, name, conns } =>
                self.add_download(url, name, conns),
            AppEvent::AddScheduled { url, name, sched_time } =>
                self.add_scheduled(url, name, sched_time),
            AppEvent::PauseDownload(id) =>
                self.set_status(&id, DownloadStatus::Paused),
            AppEvent::ResumeDownload(id) =>
                self.resume_download(&id),
            AppEvent::DeleteDownload(id) =>
                self.delete_download(&id),
            AppEvent::RetryDownload(id) =>
                self.retry(&id),
            AppEvent::OpenFolder(id) =>
                self.open_folder(&id),
            AppEvent::PauseAll =>
                self.pause_all(),
            AppEvent::ResumeAll =>
                self.resume_all(),
            AppEvent::DeleteAll =>
                self.delete_all(),
            AppEvent::DeleteSelected =>
                self.delete_selected(),
            AppEvent::MoveToQueue(id) =>
                self.move_to_queue(&id),
            AppEvent::MoveSelectedToQueue => {
                let ids = self.selected_ids.clone();
                for id in ids {
                    self.move_to_queue(&id);
                }
            }
            AppEvent::SelectRow { id, shift, control } => {
                self.select_row(&id, shift, control);
            }
            AppEvent::ShowChunkDetails(id) => {
                self.chunk_detail_id = id;
            }
            AppEvent::ToggleSelected => {
                let id = self.selected_id.clone();
                if id.is_empty() { return; }
                if let Some(dl) = self.downloads.iter().find(|d| d.id == id) {
                    match dl.status {
                        DownloadStatus::Running => self.set_status(&id, DownloadStatus::Paused),
                        DownloadStatus::Paused => self.resume_download(&id),
                        DownloadStatus::Failed => self.retry(&id),
                        _ => {}
                    }
                }
            }
            AppEvent::SelectPrev { shift, control } => {
                self.select_move(shift, control, -1);
            }
            AppEvent::SelectNext { shift, control } => {
                self.select_move(shift, control, 1);
            }
            AppEvent::SelectPageUp { shift, control } => {
                self.select_move(shift, control, -10);
            }
            AppEvent::SelectPageDown { shift, control } => {
                self.select_move(shift, control, 10);
            }
            AppEvent::SetFilter(f) =>
                self.filter = FilterMode::from_str(&f),
            AppEvent::SearchChanged(q) =>
                self.search = q,
            AppEvent::ToggleRTL => {
                self.is_rtl = !self.is_rtl;
            }
            AppEvent::SaveSettings {
                path,
                max_conns,
                workers,
                max_simultaneous,
                theme_mode,
                animations,
                compact_rows,
                scheduler_enabled,
                use_proxy,
                https_only,
                verify_ssl,
                av_scan,
                rest_api,
                log_level,
                daemon_host,
                daemon_port,
                language,
                ..
            } => {
                // Apply numeric settings that affect simulation behaviour
                if let Ok(n) = max_conns.parse::<usize>() {
                    self.max_concurrent = n.clamp(1, 32);
                }
                if let Ok(w) = workers.parse::<usize>() {
                    let clamped = w.clamp(1, 16);
                    self.max_concurrent = self.max_concurrent.max(clamped);
                }
                if max_simultaneous > 0 {
                    self.max_concurrent = (max_simultaneous as usize).clamp(1, 32);
                }

                // Build a compact human-readable summary for the notification
                let mut flags: Vec<&str> = Vec::new();
                if use_proxy          { flags.push("proxy"); }
                if https_only         { flags.push("HTTPS-only"); }
                if !verify_ssl        { flags.push("SSL-skip"); }
                if av_scan            { flags.push("AV-scan"); }
                if rest_api           { flags.push("REST-API"); }
                if !animations        { flags.push("no-anim"); }
                if compact_rows       { flags.push("compact"); }
                if !scheduler_enabled { flags.push("sched-off"); }

                let theme_str = match theme_mode {
                    1 => "light",
                    2 => "auto",
                    _ => "dark",
                };
                let log_str = match log_level {
                    0 => "off", 1 => "error", 2 => "warn",
                    3 => "info", 4 => "debug", _ => "verbose",
                };
                let lang_str = match language {
                    1 => "en", 2 => "ar", 3 => "fr", 4 => "de",
                    5 => "es", 6 => "zh", 7 => "ja", 8 => "ru", _ => "auto",
                };

                let daemon_info = if !daemon_host.is_empty() && !daemon_port.is_empty() {
                    format!("  •  daemon={}:{}", daemon_host, daemon_port)
                } else {
                    String::new()
                };

                let summary = if !path.is_empty() {
                    format!(
                        "✅  Settings saved  •  {}  •  theme={} log={} lang={}{}  {}",
                        &path[..path.len().min(30)],
                        theme_str, log_str, lang_str,
                        daemon_info,
                        flags.join(" "),
                    )
                } else {
                    format!(
                        "✅  Settings saved  •  theme={} log={} lang={}{}  {}",
                        theme_str, log_str, lang_str,
                        daemon_info,
                        flags.join(" "),
                    )
                };
                self.show_notification(summary);
            }
            AppEvent::SchedulerTriggered =>
                self.trigger_scheduled(),
            AppEvent::ProgressUpdated => {} // no-op: progress is advanced by advance_tick
            AppEvent::DismissNotification => {
                self.notification = None;
            }
            AppEvent::OpenUrl(url) => {
                // In production the real adapter calls open::that(&url).
                // In mock mode we just show a notification so the action is visible.
                self.show_notification(format!("🌐  Opening: {}", &url[..url.len().min(50)]));
            }
            AppEvent::OpenLogsFolder => {
                self.show_notification("📂  Opening logs folder…".to_string());
            }
        }
    }

    fn select_move(&mut self, shift: bool, _control: bool, amount: isize) {
        let visible = self.visible_downloads();
        if visible.is_empty() {
            self.selected_id.clear();
            self.selected_ids.clear();
            self.selected_anchor = None;
            return;
        }
        let ids: Vec<String> = visible.iter().map(|d| d.id.clone()).collect();
        let len = ids.len() as isize;
        let current_pos = ids.iter().position(|id| id == &self.selected_id).map(|p| p as isize);
        let pos = match current_pos {
            Some(p) => p,
            None => if amount >= 0 { 0 } else { len - 1 },
        };
        let mut new = pos + amount;
        if new < 0 { new = 0; }
        if new >= len { new = len - 1; }
        let target_id = ids[new as usize].clone();
        if shift {
            self.select_row(&target_id, true, false);
        } else {
            self.selected_id = target_id.clone();
            self.selected_ids = vec![target_id.clone()];
            self.selected_anchor = Some(target_id);
        }
    }

    fn select_row(&mut self, id: &str, shift: bool, control: bool) {
        let visible = self.visible_downloads();
        let ids: Vec<String> = visible.iter().map(|d| d.id.clone()).collect();
        if ids.is_empty() {
            self.selected_id.clear();
            self.selected_ids.clear();
            self.selected_anchor = None;
            return;
        }
        if !ids.contains(&id.to_string()) {
            return;
        }

        if shift {
            let anchor = self.selected_anchor
                .clone()
                .filter(|anchor| !anchor.is_empty())
                .unwrap_or_else(|| self.selected_id.clone());
            let anchor = if anchor.is_empty() { id.to_string() } else { anchor };
            let start = ids.iter().position(|item| item == &anchor).unwrap_or(0);
            let end = ids.iter().position(|item| item == id).unwrap_or(0);
            let range = if start <= end { start..=end } else { end..=start };
            let mut selection: Vec<String> = ids[range].to_vec();
            if control {
                for existing in &self.selected_ids {
                    if !selection.contains(existing) && ids.contains(existing) {
                        selection.push(existing.clone());
                    }
                }
            }
            self.selected_ids = selection;
            self.selected_id = id.to_string();
        } else if control {
            self.selected_id = id.to_string();
            if self.selected_ids.contains(&id.to_string()) {
                self.selected_ids.retain(|sid| sid != id);
            } else {
                self.selected_ids.push(id.to_string());
            }
        } else {
            self.selected_id = id.to_string();
            self.selected_ids = vec![id.to_string()];
        }

        if self.selected_anchor.is_none() || (!control && !shift) {
            self.selected_anchor = Some(self.selected_id.clone());
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  View helpers
    // ─────────────────────────────────────────────────────────────────────

    /// Returns downloads that pass the current filter+search, preserving
    /// original insertion order. The Slint model is rebuilt from this.
    pub fn visible_downloads(&self) -> Vec<&DownloadEntry> {
        self.downloads.iter()
            .filter(|dl| passes_filter(dl, &self.filter, &self.search))
            .collect()
    }

    pub fn filter_counts(&self) -> FilterCounts {
        FilterCounts::compute(&self.downloads)
    }

    pub fn statistics(&self) -> AppStatistics {
        AppStatistics::compute(&self.downloads)
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Internal mutations
    // ─────────────────────────────────────────────────────────────────────

    fn add_download(&mut self, url: String, name: String, _conns: String) {
        let filename = if name.is_empty() { filename_from_url(&url) } else { name };
        let id = format!("dl-{:03}", self.next_id);
        self.next_id += 1;

        // Randomise total size: 1 MB – 8 GB.
        let r = xorshift(&mut self.rng);
        let total_bytes = (1_048_576.0 * (1.0 + r * 7999.0)) as u64;

        let entry = DownloadEntry {
            id: id.clone(),
            filename: filename.clone(),
            url,
            icon: icon_for(&filename).to_string(),
            status: DownloadStatus::Queued,
            total_bytes,
            downloaded_bytes: 0,
            speed_bps: 0.0,
            num_chunks: 8 + ((xorshift(&mut self.rng) * 24.0) as u32).next_power_of_two().clamp(8, 32),
            error: String::new(),
            sched_time: String::new(),
            category: category_for(&filename).to_string(),
            sim: SimState::new(self.rng, 4_194_304.0),
        };

        self.downloads.push(entry);
        self.show_notification(format!("➕  Added: {}", &filename[..filename.len().min(40)]));
    }

    fn add_scheduled(&mut self, url: String, name: String, sched_time: String) {
        let filename = if name.is_empty() { filename_from_url(&url) } else { name };
        let id = format!("dl-{:03}", self.next_id);
        self.next_id += 1;

        let r = xorshift(&mut self.rng);
        let total_bytes = (1_048_576.0 * (10.0 + r * 5000.0)) as u64;

        let entry = DownloadEntry {
            id,
            filename: filename.clone(),
            url,
            icon: icon_for(&filename).to_string(),
            status: DownloadStatus::Scheduled,
            total_bytes,
            downloaded_bytes: 0,
            speed_bps: 0.0,
            num_chunks: 16,
            error: String::new(),
            sched_time,
            category: category_for(&filename).to_string(),
            sim: SimState::new(self.rng, 8_388_608.0),
        };

        self.downloads.push(entry);
        self.show_notification(format!("🕐  Scheduled: {}", &filename[..filename.len().min(40)]));
    }

    fn set_status(&mut self, id: &str, status: DownloadStatus) {
        if let Some(dl) = self.downloads.iter_mut().find(|d| d.id == id) {
            dl.status = status;
            dl.speed_bps = 0.0;
            dl.sim.smooth_speed = 0.0;
        }
    }

    fn resume_download(&mut self, id: &str) {
        if let Some(dl) = self.downloads.iter_mut().find(|d| d.id == id) {
            match dl.status {
                DownloadStatus::Paused => {
                    dl.status = DownloadStatus::Running;
                    let r = xorshift(&mut dl.sim.rng);
                    dl.sim.target_speed = 1_048_576.0 + r * 8_388_608.0;
                    dl.sim.smooth_speed = dl.sim.target_speed * 0.1;
                    dl.speed_bps = dl.sim.smooth_speed;
                }
                DownloadStatus::Failed => retry_download(dl),
                _ => {}
            }
        }
    }

    fn retry(&mut self, id: &str) {
        let name = if let Some(dl) = self.downloads.iter_mut().find(|d| d.id == id) {
            retry_download(dl);
            Some(dl.filename[..dl.filename.len().min(40)].to_string())
        } else {
            None
        };

        if let Some(name) = name {
            self.show_notification(format!("🔄  Retrying: {}", name));
        }
    }

    fn open_folder(&mut self, id: &str) {
        if let Some(dl) = self.downloads.iter().find(|d| d.id == id) {
            // In production this would call open::that(path). Here we emit a notification.
            let name = dl.filename[..dl.filename.len().min(40)].to_string();
            self.show_notification(format!("📂  Opening folder for: {}", name));
        }
    }

    fn delete_download(&mut self, id: &str) {
        // Soft-delete: mark as Deleted so the "Deleted" filter can show it.
        // The entry stays in `downloads`; a future purge action can hard-remove it.
        if let Some(dl) = self.downloads.iter_mut().find(|d| d.id == id) {
            dl.status    = DownloadStatus::Deleted;
            dl.speed_bps = 0.0;
            dl.sim.smooth_speed = 0.0;
        }
        // Deselect the row
        if self.selected_id == id { self.selected_id.clear(); }
        self.selected_ids.retain(|sid| sid != id);
    }

    /// Permanently removes all soft-deleted entries from the list.
    pub fn purge_deleted(&mut self) {
        self.downloads.retain(|dl| dl.status != DownloadStatus::Deleted);
    }

    fn pause_all(&mut self) {
        for dl in &mut self.downloads {
            if dl.status == DownloadStatus::Running {
                dl.status = DownloadStatus::Paused;
                dl.speed_bps = 0.0;
                dl.sim.smooth_speed = 0.0;
            }
        }
        self.show_notification("⏸  All active downloads paused".to_string());
    }

    fn resume_all(&mut self) {
        let mut count = 0;
        for dl in &mut self.downloads {
            if dl.status == DownloadStatus::Paused {
                dl.status = DownloadStatus::Running;
                let r = xorshift(&mut dl.sim.rng);
                dl.sim.target_speed = 1_048_576.0 + r * 8_388_608.0;
                dl.sim.smooth_speed = dl.sim.target_speed * 0.15;
                dl.speed_bps = dl.sim.smooth_speed;
                count += 1;
            }
        }
        if count > 0 {
            self.show_notification(format!("▶  Resumed {} download(s)", count));
        }
    }

    fn delete_all(&mut self) {
        // Hard-delete: "Delete All" is an explicit bulk purge — no recovery expected.
        // Mark everything Deleted first so counts update, then purge immediately.
        let count = self.downloads.len();
        self.downloads.clear();
        self.selected_id.clear();
        self.selected_ids.clear();
        self.show_notification(format!("🗑  Deleted all {} downloads", count));
    }

    fn delete_selected(&mut self) {
        if !self.selected_ids.is_empty() {
            let ids = self.selected_ids.clone();
            for id in ids {
                self.delete_download(&id);
            }
            self.selected_ids.clear();
            self.selected_id.clear();
        }
    }

    fn move_to_queue(&mut self, id: &str) {
        if let Some(dl) = self.downloads.iter_mut().find(|d| d.id == id) {
            if dl.status != DownloadStatus::Completed {
                dl.status = DownloadStatus::Queued;
                dl.speed_bps = 0.0;
                dl.sim.smooth_speed = 0.0;
            }
        }
    }

    /// Auto-start queued downloads when running slots are available.
    fn schedule_queued(&mut self) {
        let running = self.downloads.iter()
            .filter(|d| d.status == DownloadStatus::Running)
            .count();
        let available = self.max_concurrent.saturating_sub(running);
        if available == 0 { return; }

        let mut started = 0;
        for dl in &mut self.downloads {
            if started >= available { break; }
            if dl.status == DownloadStatus::Queued {
                start_queued(dl);
                started += 1;
            }
        }
    }

    /// Trigger scheduled downloads whose countdown has expired.
    fn trigger_scheduled(&mut self) {
        // Each scheduled download has a `scheduled_countdown` field.
        // We decrement it each tick and convert to Queued when it hits 0.
        let mut triggered_names: Vec<String> = Vec::new();
        for dl in &mut self.downloads {
            if dl.status == DownloadStatus::Scheduled {
                dl.sim.scheduled_countdown -= 1;
                if dl.sim.scheduled_countdown <= 0 {
                    dl.status = DownloadStatus::Queued;
                    dl.sched_time.clear();
                    triggered_names.push(dl.filename[..dl.filename.len().min(36)].to_string());
                }
            }
        }
        for name in triggered_names {
            self.show_notification(format!("🕐  Scheduled download started: {}", name));
        }
    }

    fn show_notification(&mut self, text: String) {
        self.notification = Some(Notification { text, ttl: 35 }); // ≈ 3.5 s
    }
}
