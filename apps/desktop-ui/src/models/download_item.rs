// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — models/download_item.rs                                      ║
// ║  Core download entry. Mirrors the Slint DownloadItem struct but holds    ║
// ║  rich simulation state for the fake runtime engine.                      ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// Maps to the Slint `DlState` enum — keep variants in sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadStatus {
    Running,
    Completed,
    Paused,
    Queued,
    Failed,
    Scheduled,
    Deleted,
}

/// Simulation parameters kept per download (not sent to UI directly).
#[derive(Debug, Clone)]
pub struct SimState {
    /// xorshift64 RNG state — seeded uniquely per download.
    pub rng: u64,
    /// Current speed target (bytes/sec). Actual speed trends toward this.
    pub target_speed: f64,
    /// Countdown ticks before we pick a new speed target.
    pub speed_change_countdown: i32,
    /// Smooth actual speed (exponentially weighted).
    pub smooth_speed: f64,
    /// Countdown to auto-start for Scheduled items.
    pub scheduled_countdown: i32,
}

impl SimState {
    pub fn new(seed: u64, initial_speed: f64) -> Self {
        let mut rng = seed ^ 0xdeadbeef_cafebabe;
        // Ensure non-zero RNG state.
        if rng == 0 { rng = 1; }
        SimState {
            rng,
            target_speed: initial_speed,
            speed_change_countdown: 15,
            smooth_speed: initial_speed,
            scheduled_countdown: 30 + (xorshift(&mut rng) * 90.0) as i32,
        }
    }
}

/// The full download entry held in Rust state.
#[derive(Debug, Clone)]
pub struct DownloadEntry {
    pub id: String,
    pub filename: String,
    pub url: String,
    pub icon: String,
    pub status: DownloadStatus,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: f64,
    pub num_chunks: u32,
    pub error: String,
    pub sched_time: String,
    pub category: String,
    pub sim: SimState,
}

impl DownloadEntry {
    pub fn pct(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.downloaded_bytes as f64 / self.total_bytes as f64 * 100.0).min(100.0) as f32
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.downloaded_bytes)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  Formatting helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn format_bytes(bytes: u64) -> String {
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    const KB: f64 = 1_024.0;
    let b = bytes as f64;
    if b >= GB       { format!("{:.2} GB", b / GB) }
    else if b >= MB  { format!("{:.1} MB", b / MB) }
    else if b >= KB  { format!("{:.0} KB", b / KB) }
    else             { format!("{} B", bytes) }
}

pub fn format_speed(bps: f64) -> String {
    if bps <= 0.0 { return "—".to_string(); }
    const MB: f64 = 1_048_576.0;
    const KB: f64 = 1_024.0;
    if bps >= MB      { format!("{:.1} MB/s", bps / MB) }
    else if bps >= KB { format!("{:.0} KB/s", bps / KB) }
    else              { format!("{:.0} B/s", bps) }
}

pub fn format_eta(remaining: u64, speed_bps: f64) -> String {
    if speed_bps <= 0.0 || remaining == 0 { return "—".to_string(); }
    let secs = (remaining as f64 / speed_bps) as u64;
    if secs < 60          { format!("{}s", secs) }
    else if secs < 3600   { format!("{}m {}s", secs / 60, secs % 60) }
    else                  { format!("{}h {}m", secs / 3600, (secs % 3600) / 60) }
}

pub fn format_size_text(downloaded: u64, total: u64, status: &DownloadStatus) -> String {
    match status {
        DownloadStatus::Completed              => format_bytes(total),
        DownloadStatus::Failed                 => format!("{} (failed)", format_bytes(total)),
        DownloadStatus::Deleted                => format!("{} (deleted)", format_bytes(total)),
        DownloadStatus::Queued
        | DownloadStatus::Scheduled            => format_bytes(total),
        _                                      => format!("{} / {}", format_bytes(downloaded), format_bytes(total)),
    }
}

/// Determine icon from filename extension.
pub fn icon_for(filename: &str) -> &'static str {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "iso" | "img"                                     => "💿",
        "pdf"                                             => "📄",
        "mp4" | "mkv" | "avi" | "mov" | "webm"           => "🎬",
        "mp3" | "flac" | "wav" | "ogg" | "aac"           => "🎵",
        "zip" | "7z" | "tar" | "gz" | "xz" | "bz2" | "rar" => "📦",
        "exe" | "deb" | "rpm" | "dmg" | "appimage"       => "⚙️",
        "jpg" | "jpeg" | "png" | "gif" | "webp"          => "🖼",
        "doc" | "docx" | "odt" | "txt" | "md"            => "📝",
        _                                                 => "🔧",
    }
}

/// Determine category from filename extension.
pub fn category_for(filename: &str) -> &'static str {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "mp4" | "mkv" | "avi" | "mov" | "webm"           => "video",
        "mp3" | "flac" | "wav" | "ogg" | "aac"           => "audio",
        "pdf" | "doc" | "docx" | "odt" | "txt" | "md"    => "document",
        "zip" | "7z" | "tar" | "gz" | "xz" | "bz2" | "rar" => "archive",
        _                                                 => "other",
    }
}

/// Extract filename from a URL.
pub fn filename_from_url(url: &str) -> String {
    url.rsplit('/').next()
        .filter(|s| !s.is_empty() && s.contains('.'))
        .unwrap_or("download")
        .to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
//  xorshift64 — no-alloc, no-dep PRNG
// ─────────────────────────────────────────────────────────────────────────────

/// Advance xorshift64 state and return a float in [0, 1).
pub fn xorshift(state: &mut u64) -> f64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    if x == 0 { x = 1; }
    *state = x;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

// ─────────────────────────────────────────────────────────────────────────────
//  Factory: initial 10 mock downloads (mirrors mock-state.slint)
// ─────────────────────────────────────────────────────────────────────────────

pub fn initial_downloads() -> Vec<DownloadEntry> {
    vec![
        mk("dl-001", "ubuntu-24.04.1-desktop-amd64.iso",
           "https://releases.ubuntu.com/24.04/ubuntu-24.04.1-desktop-amd64.iso",
           DownloadStatus::Running,  4_713_717_350, 2_941_359_947, 14_897_152.0, 16, 1),

        mk("dl-002", "Rust.Programming.Language.2nd.Edition.pdf",
           "https://example.com/rust-book.pdf",
           DownloadStatus::Running,  16_252_928,   5_038_407,   2_202_009.0,  8, 2),

        mk("dl-003", "Big.Buck.Bunny.4K.HDR.mkv",
           "https://example.com/bbb-4k.mkv",
           DownloadStatus::Paused,   7_516_192_768, 3_442_416_290, 0.0, 32, 3),

        mk("dl-004", "archlinux-2024.10.01-x86_64.iso",
           "https://archlinux.org/iso/latest/archlinux-x86_64.iso",
           DownloadStatus::Completed, 940_769_280, 940_769_280, 0.0, 16, 4),

        mk("dl-005", "node-v22.6.0-linux-x64.tar.gz",
           "https://nodejs.org/dist/v22.6.0/node-v22.6.0-linux-x64.tar.gz",
           DownloadStatus::Queued,   46_546_329,  0,           0.0, 8, 5),

        mk("dl-006", "Synthwave.Essentials.Vol.3.zip",
           "https://example.com/music.zip",
           DownloadStatus::Failed,   134_217_728, 24_575_999,  0.0, 8, 6),

        mk_sched("dl-007", "windows11-22H2-enterprise.iso",
           "https://example.com/win11.iso",
           5_768_609_792, 32, "2025-10-15 02:00", 7),

        mk("dl-008", "ffmpeg-git-full.7z",
           "https://example.com/ffmpeg.7z",
           DownloadStatus::Completed, 93_454_950, 93_454_950,  0.0, 8, 8),

        mk("dl-009", "Blender.3.6.LTS.tar.xz",
           "https://download.blender.org/release/Blender3.6/blender-3.6.tar.xz",
           DownloadStatus::Paused,   206_569_472, 160_091_341, 0.0, 16, 9),

        mk("dl-010", "The.Grand.Tour.S05E08.2160p.WEB-DL.mkv",
           "https://example.com/tgt.mkv",
           DownloadStatus::Running,  10_737_418_240, 954_629_283, 9_126_093.0, 32, 10),
    ]
}

fn mk(id: &str, file: &str, url: &str,
      status: DownloadStatus,
      total: u64, downloaded: u64,
      speed: f64, chunks: u32, seed_offset: u64) -> DownloadEntry {
    DownloadEntry {
        id: id.to_string(),
        filename: file.to_string(),
        url: url.to_string(),
        icon: icon_for(file).to_string(),
        status,
        total_bytes: total,
        downloaded_bytes: downloaded,
        speed_bps: speed,
        num_chunks: chunks,
        error: String::new(),
        sched_time: String::new(),
        category: category_for(file).to_string(),
        sim: SimState::new(0x1234_5678_9abc_def0 ^ seed_offset, speed.max(2_097_152.0)),
    }
}

fn mk_sched(id: &str, file: &str, url: &str,
            total: u64, chunks: u32, sched: &str, seed_offset: u64) -> DownloadEntry {
    DownloadEntry {
        id: id.to_string(),
        filename: file.to_string(),
        url: url.to_string(),
        icon: icon_for(file).to_string(),
        status: DownloadStatus::Scheduled,
        total_bytes: total,
        downloaded_bytes: 0,
        speed_bps: 0.0,
        num_chunks: chunks,
        error: String::new(),
        sched_time: sched.to_string(),
        category: category_for(file).to_string(),
        sim: SimState::new(0x1234_5678_9abc_def0 ^ seed_offset, 8_388_608.0),
    }
}
