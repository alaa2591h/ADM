// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — runtime/fake_scheduler.rs                                     ║
// ║  Fake scheduler: converts Scheduled → Queued after a countdown.         ║
// ║  The real countdown logic lives in AppState::trigger_scheduled.         ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// Returns a human-readable description of the scheduler's behaviour for
/// display in logs / about panels.
pub fn scheduler_description() -> &'static str {
    "FakeScheduler v1.0 — mock scheduler that auto-triggers \
     scheduled downloads after their countdown expires"
}

/// Given a sched_time string like "2025-10-15 02:00", derive a countdown
/// (in ticks) for the fake scheduler. Since all scheduled times are in the
/// past (mock data), we use a short countdown derived from the hash of the
/// string so different items fire at different times.
pub fn countdown_for_sched_time(sched_time: &str, rng_bias: u64) -> i32 {
    // Simple hash → 15..120 ticks (1.5 s – 12 s)
    let hash: u64 = sched_time.bytes().fold(rng_bias, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(b as u64)
    });
    (15 + (hash % 105)) as i32
}
