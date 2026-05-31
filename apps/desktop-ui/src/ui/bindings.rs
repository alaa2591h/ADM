// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — ui/bindings.rs                                                ║
// ║  Converts between Rust-native types and the Slint-generated structs.    ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use slint::SharedString;

pub use crate::models::download_item::{
    DownloadEntry, DownloadStatus,
    format_bytes, format_speed, format_eta, format_size_text,
};
use crate::models::statistics::AppStatistics;
use crate::state::filters::FilterCounts;

// Re-export Slint generated types for use throughout the crate.
pub use crate::{DlState, DownloadItem, FilterCounts as SlintFilterCounts, AppStats};


// ─────────────────────────────────────────────────────────────────────────────
//  DownloadEntry → Slint DownloadItem
// ─────────────────────────────────────────────────────────────────────────────

pub fn to_slint_item(dl: &DownloadEntry, selected: bool) -> DownloadItem {
    let pct       = dl.pct();
    let remaining = dl.remaining_bytes();

    DownloadItem {
        id:         SharedString::from(dl.id.as_str()),
        filename:   SharedString::from(dl.filename.as_str()),
        url:        SharedString::from(dl.url.as_str()),
        icon:       SharedString::from(dl.icon.as_str()),
        state:      to_slint_state(&dl.status),
        pct,
        pct_text:   SharedString::from(format!("{}%", pct as u32).as_str()),
        size_text:  SharedString::from(
            format_size_text(dl.downloaded_bytes, dl.total_bytes, &dl.status).as_str()
        ),
        dl_text:    SharedString::from(format_bytes(dl.downloaded_bytes).as_str()),
        speed_text: SharedString::from(
            if dl.status == DownloadStatus::Running {
                format_speed(dl.speed_bps)
            } else {
                "—".to_string()
            }.as_str()
        ),
        eta_text:   SharedString::from(
            if dl.status == DownloadStatus::Running {
                format_eta(remaining, dl.speed_bps)
            } else {
                "—".to_string()
            }.as_str()
        ),
        bps:        dl.speed_bps as f32,
        chunks:     dl.num_chunks as i32,
        error:      SharedString::from(dl.error.as_str()),
        sched_time: SharedString::from(dl.sched_time.as_str()),
        category:   SharedString::from(dl.category.as_str()),
        selected:   selected,
    }
}

pub fn to_slint_state(status: &DownloadStatus) -> DlState {
    match status {
        DownloadStatus::Running   => DlState::Running,
        DownloadStatus::Completed => DlState::Completed,
        DownloadStatus::Paused    => DlState::Paused,
        DownloadStatus::Queued    => DlState::Queued,
        DownloadStatus::Failed    => DlState::Failed,
        DownloadStatus::Scheduled => DlState::Scheduled,
        DownloadStatus::Deleted   => DlState::Deleted,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  ChunkInfo helpers for the Slint ChunkPanel
//
//  The ChunkPanel widget drives its grid entirely from three scalar properties
//  (`chunk-count`, `chunk-pct`, `chunk-state`) — it computes per-cell state
//  itself in Slint.  These helpers therefore do NOT push a full item model;
//  they compute the display values the UI already expects.
// ─────────────────────────────────────────────────────────────────────────────

use crate::models::chunk_info::{build_chunks, ChunkStatus};

/// Return the `(count, pct)` pair that should be pushed to `chunk-count` /
/// `chunk-pct` on the main window, derived from the runtime `ChunkInfo` slice.
///
/// If `chunks` is empty the raw `fallback_pct` is used so the panel still
/// shows progress even before the real backend supplies chunk data.
pub fn chunk_display_values(
    chunks: &[crate::models::chunk_info::ChunkInfo],
    fallback_pct: f32,
) -> (i32, f32) {
    if chunks.is_empty() {
        return (0, fallback_pct);
    }
    let count = chunks.len() as i32;
    // Derive overall pct from the chunk slice so the panel stays consistent
    // with what the engine actually reports rather than the polled DownloadEntry.
    let completed = chunks
        .iter()
        .filter(|c| c.status == ChunkStatus::Completed)
        .count();
    let active_progress: f32 = chunks
        .iter()
        .filter(|c| c.status == ChunkStatus::Downloading)
        .map(|c| c.progress)
        .sum();
    let pct =
        ((completed as f32 + active_progress) / chunks.len() as f32) * 100.0;
    (count, pct.clamp(0.0, 100.0))
}

/// Build a `ChunkInfo` slice from the scalar fields already on a
/// `DownloadEntry` and expose it as the authoritative chunk representation.
/// This keeps the mock adapter working without a live engine connection.
pub fn chunk_info_from_entry(
    entry: &DownloadEntry,
) -> Vec<crate::models::chunk_info::ChunkInfo> {
    build_chunks(entry.num_chunks as usize, entry.pct())
}

// ─────────────────────────────────────────────────────────────────────────────
//  FilterCounts → Slint FilterCounts
// ─────────────────────────────────────────────────────────────────────────────

pub fn to_slint_counts(c: &FilterCounts) -> SlintFilterCounts {
    SlintFilterCounts {
        all:       c.all,
        running:   c.running,
        completed: c.completed,
        queued:    c.queued,
        scheduled: c.scheduled,
        paused:    c.paused,
        failed:    c.failed,
        deleted:   c.deleted,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  AppStatistics → Slint AppStats
// ─────────────────────────────────────────────────────────────────────────────

pub fn to_slint_stats(s: &AppStatistics) -> AppStats {
    AppStats {
        total_downloaded: SharedString::from(s.total_downloaded.as_str()),
        active_speed:     SharedString::from(s.active_speed.as_str()),
        active_count:     s.active_count,
        completed_today:  s.completed_today,
    }
}
