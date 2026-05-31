// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — models/chunk_info.rs                                          ║
// ║  Chunk-level state for the chunk visualization panel.                    ║
// ║  Not yet wired to the mock runtime; reserved for the real backend.       ║
// ╚══════════════════════════════════════════════════════════════════════════╝
#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStatus {
    Pending,
    Downloading,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub index: usize,
    pub status: ChunkStatus,
    pub progress: f32,   // 0.0 – 1.0
}

/// Build a deterministic chunk state array from overall download progress.
/// The first `floor(pct/100 * n)` chunks are Completed; the next one is
/// Downloading (with fractional fill); the rest are Pending.
pub fn build_chunks(count: usize, pct: f32) -> Vec<ChunkInfo> {
    if count == 0 { return Vec::new(); }
    let done_f = pct / 100.0 * count as f32;
    let done = done_f.floor() as usize;
    let frac = done_f.fract();

    (0..count).map(|i| {
        let status = if i < done {
            ChunkStatus::Completed
        } else if i == done && frac > 0.0 {
            ChunkStatus::Downloading
        } else {
            ChunkStatus::Pending
        };
        let progress = if i < done { 1.0 }
                       else if i == done { frac }
                       else { 0.0 };
        ChunkInfo { index: i, status, progress }
    }).collect()
}
