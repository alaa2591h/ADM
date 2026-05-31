// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — state/filters.rs                                              ║
// ║  Filter modes and download list filtering logic.                         ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::models::download_item::{DownloadEntry, DownloadStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterMode {
    All,
    Running,
    Completed,
    Queued,
    Scheduled,
    Paused,
    Failed,
    Deleted,
}

impl FilterMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "running"   => FilterMode::Running,
            "completed" => FilterMode::Completed,
            "queued"    => FilterMode::Queued,
            "scheduled" => FilterMode::Scheduled,
            "paused"    => FilterMode::Paused,
            "failed"    => FilterMode::Failed,
            "deleted"   => FilterMode::Deleted,
            _           => FilterMode::All,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FilterCounts {
    pub all:       i32,
    pub running:   i32,
    pub completed: i32,
    pub queued:    i32,
    pub scheduled: i32,
    pub paused:    i32,
    pub failed:    i32,
    pub deleted:   i32,
}

impl FilterCounts {
    pub fn compute(downloads: &[DownloadEntry]) -> Self {
        let mut c = FilterCounts::default();
        for dl in downloads {
            match dl.status {
                DownloadStatus::Running   => { c.running   += 1; c.all += 1; }
                DownloadStatus::Completed => { c.completed += 1; c.all += 1; }
                DownloadStatus::Queued    => { c.queued    += 1; c.all += 1; }
                DownloadStatus::Scheduled => { c.scheduled += 1; c.all += 1; }
                DownloadStatus::Paused    => { c.paused    += 1; c.all += 1; }
                DownloadStatus::Failed    => { c.failed    += 1; c.all += 1; }
                // Deleted items are counted in their own bucket but excluded from "All"
                DownloadStatus::Deleted   => { c.deleted   += 1; }
            }
        }
        c
    }
}

/// Returns true if `entry` passes the current filter + search query.
pub fn passes_filter(entry: &DownloadEntry, filter: &FilterMode, search: &str) -> bool {
    let status_ok = match filter {
        FilterMode::All       => true,
        FilterMode::Running   => entry.status == DownloadStatus::Running,
        FilterMode::Completed => entry.status == DownloadStatus::Completed,
        FilterMode::Queued    => entry.status == DownloadStatus::Queued,
        FilterMode::Scheduled => entry.status == DownloadStatus::Scheduled,
        FilterMode::Paused    => entry.status == DownloadStatus::Paused,
        FilterMode::Failed    => entry.status == DownloadStatus::Failed,
        FilterMode::Deleted   => entry.status == DownloadStatus::Deleted,
    };
    if !status_ok { return false; }
    if search.is_empty() { return true; }
    let q = search.to_lowercase();
    entry.filename.to_lowercase().contains(&q)
        || entry.url.to_lowercase().contains(&q)
        || entry.category.to_lowercase().contains(&q)
}
