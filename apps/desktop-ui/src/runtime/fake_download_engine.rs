// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — runtime/fake_download_engine.rs                               ║
// ║  Stateless helpers for per-download simulation.                          ║
// ║  The real mutation happens in state/download_state.rs via advance_tick.  ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// Speed profiles for different "network conditions".
#[derive(Debug, Clone, Copy)]
pub enum SpeedProfile {
    /// Fast fibre-like connection (5–24 MB/s).
    FastFibre,
    /// Moderate cable connection (1–8 MB/s).
    Cable,
    /// Slow / throttled connection (64 KB/s – 1 MB/s).
    Slow,
    /// Random, unpredictable (default for new downloads).
    Volatile,
}

impl SpeedProfile {
    /// Returns (min_bps, max_bps) for the profile.
    pub fn range(&self) -> (f64, f64) {
        match self {
            SpeedProfile::FastFibre => (5_242_880.0, 25_165_824.0),
            SpeedProfile::Cable     => (1_048_576.0,  8_388_608.0),
            SpeedProfile::Slow      => (   65_536.0,  1_048_576.0),
            SpeedProfile::Volatile  => (  262_144.0, 20_971_520.0),
        }
    }
}

/// Maps a download filename to a likely speed profile (cosmetic only).
pub fn profile_for(filename: &str) -> SpeedProfile {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "iso" | "tar" | "gz" | "xz" | "7z" | "zip" => SpeedProfile::FastFibre,
        "mkv" | "mp4" | "avi"                       => SpeedProfile::Cable,
        "pdf" | "mp3" | "flac"                      => SpeedProfile::Slow,
        _                                           => SpeedProfile::Volatile,
    }
}
