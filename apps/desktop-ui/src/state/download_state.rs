// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — state/download_state.rs                                       ║
// ║  Per-download simulation state helpers.                                  ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::models::download_item::{DownloadEntry, DownloadStatus, xorshift};

/// Range constants for simulated download speeds.
pub const SPEED_MIN_BPS: f64 =   256.0 * 1024.0;  //  256 KB/s
pub const SPEED_MAX_BPS: f64 = 24.0 * 1_048_576.0; //   24 MB/s

/// Advance one simulation tick (dt seconds) for a single Running download.
/// Mutates `entry` in-place; returns an optional notification string if
/// the download just completed or failed.
pub fn advance_tick(entry: &mut DownloadEntry, dt: f64) -> Option<String> {
    if entry.status != DownloadStatus::Running {
        return None;
    }

    let rng = &mut entry.sim.rng;

    // ── Speed variation (random walk toward target) ───────────────────────
    entry.sim.speed_change_countdown -= 1;
    if entry.sim.speed_change_countdown <= 0 {
        let r = xorshift(rng);
        // New target: biased toward the middle of the speed range
        entry.sim.target_speed = SPEED_MIN_BPS + r * (SPEED_MAX_BPS - SPEED_MIN_BPS);
        entry.sim.speed_change_countdown = 8 + (xorshift(rng) * 25.0) as i32;
    }

    // Smooth speed with a fast / slow approach
    let alpha = 0.12; // approach rate per tick
    let jitter = (xorshift(rng) - 0.5) * 0.06 * entry.sim.target_speed;
    entry.sim.smooth_speed = (entry.sim.smooth_speed * (1.0 - alpha)
        + entry.sim.target_speed * alpha
        + jitter)
        .clamp(SPEED_MIN_BPS, SPEED_MAX_BPS);
    entry.speed_bps = entry.sim.smooth_speed;

    // ── Progress advance ─────────────────────────────────────────────────
    let bytes_this_tick = (entry.speed_bps * dt) as u64;
    entry.downloaded_bytes = (entry.downloaded_bytes + bytes_this_tick).min(entry.total_bytes);

    // ── Completion check ─────────────────────────────────────────────────
    if entry.downloaded_bytes >= entry.total_bytes {
        entry.status = DownloadStatus::Completed;
        entry.speed_bps = 0.0;
        entry.sim.smooth_speed = 0.0;
        entry.downloaded_bytes = entry.total_bytes;
        let name = short_name(&entry.filename);
        return Some(format!("✅  {} — completed!", name));
    }

    // ── Random transient failure (≈0.04 % per tick ≈ 0.4 %/s) ──────────
    if xorshift(rng) < 0.0004 {
        entry.status = DownloadStatus::Failed;
        entry.speed_bps = 0.0;
        entry.sim.smooth_speed = 0.0;
        let reasons = [
            "Connection reset by peer",
            "Network timeout",
            "Server error 503",
            "SSL handshake failed",
        ];
        let idx = (xorshift(rng) * reasons.len() as f64) as usize;
        entry.error = reasons[idx.min(reasons.len() - 1)].to_string();
        let name = short_name(&entry.filename);
        return Some(format!("❌  {} — {}", name, entry.error));
    }

    None
}

/// Auto-start a Queued download immediately.
pub fn start_queued(entry: &mut DownloadEntry) {
    if entry.status != DownloadStatus::Queued { return; }
    entry.status = DownloadStatus::Running;
    let rng = &mut entry.sim.rng;
    let r = xorshift(rng);
    entry.sim.target_speed = SPEED_MIN_BPS + r * (SPEED_MAX_BPS - SPEED_MIN_BPS);
    entry.sim.smooth_speed = entry.sim.target_speed * 0.3;
    entry.speed_bps = entry.sim.smooth_speed;
}

/// Retry a Failed download.
pub fn retry_download(entry: &mut DownloadEntry) {
    if entry.status != DownloadStatus::Failed { return; }
    entry.error.clear();
    entry.status = DownloadStatus::Running;
    let rng = &mut entry.sim.rng;
    xorshift(rng); // advance RNG a bit
    entry.sim.target_speed = SPEED_MIN_BPS + xorshift(rng) * (SPEED_MAX_BPS - SPEED_MIN_BPS);
    entry.sim.smooth_speed = entry.sim.target_speed * 0.2;
    entry.speed_bps = entry.sim.smooth_speed;
}

fn short_name(filename: &str) -> String {
    if filename.len() <= 32 {
        filename.to_string()
    } else {
        format!("{}…", &filename[..29])
    }
}
