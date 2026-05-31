// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — bridge/ui_bridge.rs                                           ║
// ║  Synchronises the Rust AppState into the Slint window properties.       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::rc::Rc;
use slint::{ModelRc, VecModel, SharedString};

use crate::MainWindow;
use crate::state::app_state::AppState;
use crate::ui::bindings::{to_slint_item, to_slint_counts, to_slint_stats, to_slint_state, format_speed, format_eta, chunk_info_from_entry, chunk_display_values};

/// Push the current AppState into every relevant Slint window property.
/// Called from the simulation timer on every tick.
pub fn sync_to_ui(state: &AppState, window: &MainWindow) {
    // ── Downloads model ───────────────────────────────────────────────────
    let visible = state.visible_downloads();
    let total_count = state.downloads.len() as i32;
    let items: Vec<_> = visible.iter().map(|dl| {
        let is_selected = (state.selected_id == dl.id) || state.selected_ids.iter().any(|s| s == &dl.id);
        to_slint_item(dl, is_selected)
    }).collect();

    let model = Rc::new(VecModel::from(items));
    window.set_downloads(ModelRc::from(model));
    window.set_total_downloads(total_count);
    window.set_has_search(!state.search.is_empty());
    window.set_is_loading(state.is_loading);
    window.set_is_rtl(state.is_rtl);

    // ── Chunk panel data ──────────────────────────────────────────────────
    if !state.chunk_detail_id.is_empty() {
        if let Some(dl) = state.downloads.iter().find(|d| d.id == state.chunk_detail_id) {
            // Build a ChunkInfo slice from the entry so the panel reflects
            // actual per-chunk state rather than a bare scalar progress value.
            let chunks = chunk_info_from_entry(dl);
            let (count, pct) = chunk_display_values(&chunks, dl.pct());

            window.set_chunk_title(SharedString::from(dl.filename.as_str()));
            window.set_chunk_count(count);
            window.set_chunk_pct(pct);
            window.set_chunk_state(to_slint_state(&dl.status));
            window.set_chunk_speed(SharedString::from(
                if dl.status == crate::models::download_item::DownloadStatus::Running {
                    format_speed(dl.speed_bps)
                } else { String::new() }.as_str()
            ));
            window.set_chunk_eta(SharedString::from(
                if dl.status == crate::models::download_item::DownloadStatus::Running {
                    format_eta(dl.remaining_bytes(), dl.speed_bps)
                } else { String::new() }.as_str()
            ));
        }
    }

    // ── Filter counts ─────────────────────────────────────────────────────
    let counts = state.filter_counts();
    window.set_counts(to_slint_counts(&counts));

    // ── Statistics ────────────────────────────────────────────────────────
    let stats = state.statistics();
    window.set_stats(to_slint_stats(&stats));

    // ── Active filter ─────────────────────────────────────────────────────
    let filter_str = match &state.filter {
        crate::state::filters::FilterMode::All       => "all",
        crate::state::filters::FilterMode::Running   => "running",
        crate::state::filters::FilterMode::Completed => "completed",
        crate::state::filters::FilterMode::Queued    => "queued",
        crate::state::filters::FilterMode::Scheduled => "scheduled",
        crate::state::filters::FilterMode::Paused    => "paused",
        crate::state::filters::FilterMode::Failed    => "failed",
        crate::state::filters::FilterMode::Deleted   => "deleted",
    };
    window.set_active_filter(SharedString::from(filter_str));

    // ── Selected row(s) ───────────────────────────────────────────────────
    window.set_selected_id(SharedString::from(state.selected_id.as_str()));

    // selected-ids as a comma-separated string: "dl-001,dl-003,dl-007"
    // Slint code can check membership with `selected-ids-csv.contains(id)`
    // without allocating a full ModelRc on every tick.
    let _sel_csv: String = state.selected_ids.join(","); // reserved for future CSV property
    let sel_ids: Vec<SharedString> = state.selected_ids
        .iter()
        .map(|s| SharedString::from(s.as_str()))
        .collect();
    let sel_model = Rc::new(VecModel::from(sel_ids));
    window.set_selected_ids(ModelRc::from(sel_model));

    // ── Toast notification ────────────────────────────────────────────────
    if let Some(notif) = &state.notification {
        window.set_notification_text(SharedString::from(notif.text.as_str()));
        window.set_show_notification(true);
    } else {
        window.set_show_notification(false);
        window.set_notification_text(SharedString::from(""));
    }
}
