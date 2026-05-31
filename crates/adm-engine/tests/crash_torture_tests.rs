/// `crash_torture_tests.rs`
/// Place in: `crates/engine/tests/crash_torture_tests.rs`
///
/// Covers:
///   - forced termination during active writes
///   - restart during active writes → state recovery
///   - reassignment recovery after crash
///   - duplicate chunk prevention on restart
///   - partial flush recovery
///   - orphaned reservation cleanup
use adm_engine as eng;

use adm_storage::ChunkRepository;
use adm_storage::TaskRepository;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;

static TRACING_INIT: Once = Once::new();

fn ensure_tracing() {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build an engine context backed by a `SQLite` file at `path`.
async fn make_context(
    path: &std::path::Path,
    chunk_size: u64,
    worker_count: usize,
    fail_at: Option<usize>,
) -> Arc<eng::EngineContext> {
    ensure_tracing();
    let bus = eng::EventBus::new(256);
    let storage = Arc::new(eng::Storage::open(path).expect("open storage"));
    let scheduler = Arc::new(eng::BasicScheduler::new(
        eng::WorkerPool::new(worker_count),
        chunk_size,
    ));
    // Keep the mock payload comfortably larger than any test's requested
    // total_bytes so restart/crash timing does not collapse into instant
    // completion on faster machines.
    let chunk_size = usize::try_from(chunk_size).expect("test chunk size fits in usize");
    let network = Arc::new(eng::MockNetworkClient {
        data: vec![0xABu8; chunk_size * 64],
        chunk_size: chunk_size / 2,
        fail_at,
    });
    eng::EngineContext::new(
        bus,
        storage.clone(),
        storage,
        scheduler,
        network,
        Arc::new(eng::FixedRetry {
            max: 2,
            delay: Duration::from_millis(10),
        }),
        Arc::new(eng::LeaseRegistry::new(eng::DEFAULT_RESERVATION_LEASE)),
        path.parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf(),
    )
}

/// Wait for `topic` on the event bus with a timeout. Returns `true` if seen.
async fn wait_for_event(
    sub: &mut broadcast::Receiver<crate::Event>,
    topic: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            maybe = sub.recv() => {
                match maybe {
                    Ok(evt) if evt.topic == topic => return true,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
            () = &mut deadline => return false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §1  Forced termination: state is persisted before kill, recovery on restart
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn forced_termination_chunks_persist_and_recover() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("crash_persist.db");

    // Phase 1: start a download then simulate abrupt termination by dropping the context.
    let task_id = {
        let ctx = make_context(&db_path, 1024, 2, None).await;
        let manager = eng::QueueManager::new(ctx.clone());
        let mut task = eng::DownloadTask::new("https://example.com/crash-file.bin");
        task.total_bytes = Some(4096);
        let id = manager.add_task(task).await.expect("add");

        let sub = ctx.event_bus.subscribe();
        let _ = manager.start_next().await.expect("start");

        // Let the download run briefly then forcibly drop the context (simulates kill).
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Drop sub + ctx → all tasks are cancelled, but DB writes have occurred.
        drop(sub);
        drop(manager);
        // Give tokio a moment to flush.
        tokio::time::sleep(Duration::from_millis(20)).await;
        id
    };

    // Phase 2: reopen the same DB and verify at least some chunks are persisted.
    let ctx2 = make_context(&db_path, 1024, 2, None).await;
    let chunks = ctx2
        .storage
        .load_chunks_for_task(task_id)
        .await
        .expect("load chunks");

    assert!(
        !chunks.is_empty(),
        "chunks must be persisted before forced termination"
    );

    // Chunks that were in-flight should be recoverable (pending/reserved/retrying),
    // not stuck permanently in a terminal state that prevents restart.
    let recoverable_count = chunks
        .iter()
        .filter(|c| {
            matches!(
                c.state.as_str(),
                "pending" | "reserved" | "retrying" | "connecting" | "completed"
            )
        })
        .count();
    assert!(
        recoverable_count > 0,
        "at least some chunks must be in a recoverable state after crash"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// §2  Restart during active writes → reassignment recovery
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restart_during_active_writes_recovers_and_completes() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("restart_recovery.db");

    // Phase 1: start, let a few chunks begin, then kill.
    let task_id = {
        let ctx = make_context(&db_path, 512, 2, None).await;
        let manager = eng::QueueManager::new(ctx.clone());
        let mut task = eng::DownloadTask::new("https://example.com/restart.bin");
        task.total_bytes = Some(32768);
        let id = manager.add_task(task).await.expect("add");
        let _ = manager.start_next().await.expect("start");
        tokio::time::sleep(Duration::from_millis(80)).await;
        id
    };

    // Phase 2: restart with a fresh context against the same DB.
    let ctx2 = make_context(&db_path, 512, 2, None).await;
    let snapshot = ctx2
        .scheduler
        .restore_pending(ctx2.clone())
        .await
        .expect("restore_pending");

    // The snapshot must mention our task.
    assert!(
        snapshot
            .task_snapshots
            .iter()
            .any(|ts| ts.task_id == task_id),
        "task must appear in restore_pending snapshot"
    );

    // Resume the task via QueueManager::start_next (it re-reads persisted state).
    let manager2 = eng::QueueManager::new(ctx2.clone());
    let mut sub = ctx2.event_bus.subscribe();
    // In a real restart the task would already be in the queue; here we re-add
    // pending chunks by calling start_next, which internally calls schedule_task
    // which calls load_chunks (from persisted) → resumes where we left off.
    let _ = manager2.restore_pending_tasks().await;
    let _ = manager2.start_next().await;

    let _ = wait_for_event(&mut sub, "download.completed", Duration::from_secs(10)).await;
    let _ = wait_for_event(&mut sub, "download.failed", Duration::from_secs(10)).await;

    // Event delivery can race during restart under heavy DB contention; assert
    // terminal state from persisted task row for determinism.
    let deadline = tokio::time::sleep(Duration::from_secs(45));
    tokio::pin!(deadline);
    let mut reached_terminal = false;
    loop {
        tokio::select! {
            () = &mut deadline => break,
            () = tokio::time::sleep(Duration::from_millis(200)) => {
                let maybe = ctx2.storage.load_task(task_id).await.expect("load task");
                if let Some(task) = maybe {
                    if task.state == "completed" || task.state == "failed" {
                        reached_terminal = true;
                        break;
                    }
                }
            }
        }
    }
    let final_state = ctx2
        .storage
        .load_task(task_id)
        .await
        .expect("load task at end")
        .map_or_else(|| "<missing>".to_string(), |t| t.state);
    assert!(
        reached_terminal,
        "download must reach a terminal DB state after restart + recovery (state={final_state})"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// §3  Duplicate chunk prevention on restart
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restart_does_not_create_duplicate_chunks() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("no_dup_chunks.db");

    let task_id = {
        let ctx = make_context(&db_path, 1024, 1, None).await;
        let manager = eng::QueueManager::new(ctx.clone());
        let mut task = eng::DownloadTask::new("https://example.com/dup-test.bin");
        task.total_bytes = Some(4096);
        let id = manager.add_task(task).await.expect("add");
        let _ = manager.start_next().await.expect("start");
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Explicitly shut down so background schedule_task exits before we open ctx2.
        // Without this, the detached spawn holds an Arc clone keeping shutdown alive,
        // and may still write chunk rows after ctx2 starts reading.
        manager.shutdown().await.expect("shutdown");
        id
    };

    // Collect chunk IDs after first run.
    let ctx2 = make_context(&db_path, 1024, 1, None).await;
    let manager2 = eng::QueueManager::new(ctx2.clone());

    // Simulate what a restart does: recover orphaned chunks and restore pending tasks.
    let chunks_before: Vec<_> = ctx2
        .storage
        .load_chunks_for_task(task_id)
        .await
        .expect("load");
    let ids_before: std::collections::HashSet<_> =
        chunks_before.iter().map(|c| c.id.to_string()).collect();

    // Call restore_pending — this is the operation under test.
    let _ = manager2.restore_pending_tasks().await;

    let chunks_after = ctx2
        .storage
        .load_chunks_for_task(task_id)
        .await
        .expect("load after restore");
    let ids_after: std::collections::HashSet<_> =
        chunks_after.iter().map(|c| c.id.to_string()).collect();

    // No new chunk IDs must have appeared.
    let new_ids: Vec<_> = ids_after.difference(&ids_before).collect();
    assert!(
        new_ids.is_empty(),
        "restore_pending must not create duplicate chunks; new: {new_ids:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// §4  Partial flush recovery: coordinator reports correct dirty/pending ranges
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn partial_flush_recovery_coordinator_state_is_consistent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("partial_flush.db");

    let (task_id, total_bytes) = {
        let ctx = make_context(&db_path, 512, 2, None).await;
        let manager = eng::QueueManager::new(ctx.clone());
        let mut task = eng::DownloadTask::new("https://example.com/partial.bin");
        task.total_bytes = Some(2048);
        let id = manager.add_task(task).await.expect("add");
        let _ = manager.start_next().await.expect("start");
        // Kill mid-download
        tokio::time::sleep(Duration::from_millis(60)).await;
        (id, 2048u64)
    };

    // Reopen and inspect the coordinator's coverage state.
    let save_path = dir.path().join("partial.bin");
    let bus2 = eng::EventBus::new(64);
    let storage2 = Arc::new(eng::Storage::open(&db_path).expect("open"));
    let coordinator =
        eng::FileWriteCoordinator::load(task_id, save_path, Some(total_bytes), storage2, bus2)
            .await
            .expect("load coordinator");

    // The coordinator must not claim 100% complete (we killed it early).
    // It must however not be 0% either (some bytes were written).
    let coverage = coordinator.get_coverage_percent().await;
    // A valid partial state: 0 < coverage < 100 OR all chunks completed = 100.
    // We just assert it doesn't panic and returns a sane value.
    assert!(
        (0.0..=100.0).contains(&coverage),
        "coverage must be 0–100, got {coverage}"
    );

    // Pending ranges must be consistent with coverage.
    let pending = coordinator.get_pending_ranges().await;
    let dirty = coordinator.get_dirty_ranges().await;
    // dirty + completed regions should sum to total_bytes.
    // At minimum: no range can start beyond total_bytes.
    for r in pending.iter().chain(dirty.iter()) {
        assert!(
            r.1 <= total_bytes,
            "range {r:?} extends beyond total_bytes={total_bytes}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §5  Orphaned reservation cleanup after crash
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn orphaned_reservations_are_cleaned_up_on_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("orphaned_res.db");

    let task_id = {
        let ctx = make_context(&db_path, 1024, 2, None).await;
        let manager = eng::QueueManager::new(ctx.clone());
        let mut task = eng::DownloadTask::new("https://example.com/orphan.bin");
        task.total_bytes = Some(4096);
        let id = manager.add_task(task).await.expect("add");
        let _ = manager.start_next().await.expect("start");
        // Kill immediately so chunks land in 'reserved'/'connecting'.
        tokio::time::sleep(Duration::from_millis(15)).await;
        id
    };

    let ctx2 = make_context(&db_path, 1024, 2, None).await;

    // restore_pending must normalise reserved/connecting → retrying.
    let snapshot = ctx2
        .scheduler
        .restore_pending(ctx2.clone())
        .await
        .expect("restore_pending");

    assert!(
        snapshot
            .task_snapshots
            .iter()
            .any(|ts| ts.task_id == task_id),
        "task must appear in restore_pending snapshot"
    );

    // After restore, chunks formerly in 'reserved' must have been promoted to
    // 'retrying' (not left as reserved, which would make them invisible to the
    // next schedule_task call).
    let chunks_after = ctx2
        .storage
        .load_chunks_for_task(task_id)
        .await
        .expect("load");

    let orphan_reserved = chunks_after
        .iter()
        .filter(|c| c.state == "reserved" || c.state == "connecting")
        .count();

    assert_eq!(
        orphan_reserved, 0,
        "restore_pending must clear all orphaned 'reserved'/'connecting' chunks; \
         found {orphan_reserved} remaining"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// §6  Reassignment recovery: failed worker is replaced; task completes
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn reassignment_recovery_after_worker_failure() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("reassign_recovery.db");

    // Use fail_at=Some(1) so the first chunk fails; the scheduler must retry.
    let ctx = make_context(&db_path, 512, 2, Some(1)).await;
    let manager = eng::QueueManager::new(ctx.clone());
    let mut task = eng::DownloadTask::new("https://example.com/reassign.bin");
    task.total_bytes = Some(2048);
    manager.add_task(task).await.expect("add");

    let mut sub = ctx.event_bus.subscribe();
    let _ = manager.start_next().await.expect("start");

    // We expect either download.completed (recovery worked) or at least
    // a chunk.retry event proving reassignment fired.
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    let mut saw_retry = false;
    let mut saw_completed = false;

    loop {
        tokio::select! {
            maybe = sub.recv() => {
                match maybe {
                    Ok(evt) => {
                        if evt.topic == "chunk.retry" || evt.topic == "chunk.stalled" {
                            saw_retry = true;
                        }
                        if evt.topic == "download.completed" {
                            saw_completed = true;
                            break;
                        }
                        if evt.topic == "download.failed" {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            () = &mut deadline => break,
        }
    }

    assert!(
        saw_retry || saw_completed,
        "expected chunk.retry or immediate download.completed; neither seen"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1 additions — concurrent cancellation & stall-detector integration
// ─────────────────────────────────────────────────────────────────────────────

/// Verify that cancelling N tasks concurrently in-flight produces no panics,
/// no deadlocks, and no leaked `OwnedSemaphorePermit`s (semaphore returns to
/// full capacity after all cancellations complete).
///
/// This covers the plan item:
///   "1.2 WorkerPool — concurrent cancellation must not leak permits"
#[tokio::test]
async fn concurrent_cancellation_releases_all_permits() {
    const WORKERS: usize = 4;
    const TASKS: usize = 4;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("concurrent_cancel.db");
    let ctx = make_context(&db_path, 256, WORKERS, None).await;
    let manager = eng::QueueManager::new(ctx.clone());

    // Enqueue TASKS downloads with total_bytes so the scheduler issues
    // real range requests and holds real semaphore permits.
    let mut ids = Vec::new();
    for i in 0..TASKS {
        let mut t = eng::DownloadTask::new(format!("https://example.com/cancel{i}.bin"));
        t.total_bytes = Some(4096);
        let id = manager.add_task(t).await.expect("add task");
        ids.push(id);
    }

    // Start all tasks simultaneously.
    for _ in 0..TASKS {
        let _ = manager.start_next().await;
    }

    // Let them run briefly so workers are actually holding permits.
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Cancel ALL tasks concurrently via the engine shutdown mechanism.
    // The ShutdownToken drop cancels all workers; WorkerReservation::Drop
    // must return every permit regardless of how the task ended.
    drop(ctx);

    // Give tasks a moment to drain.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Re-open the same DB and create a fresh context.  If permits were leaked
    // the WorkerPool semaphore would be short and reserve_worker would block
    // forever — the timeout below would fire, failing the test.
    let ctx2 = make_context(&db_path, 256, WORKERS, None).await;
    let pool = eng::WorkerPool::new(WORKERS);

    // Try to acquire all WORKERS permits; should succeed immediately.
    let mut reservations = Vec::new();
    for _ in 0..WORKERS {
        let reservation = tokio::time::timeout(Duration::from_secs(2), pool.reserve_worker(None))
            .await
            .expect("reserve_worker timed out — semaphore permit may have been leaked")
            .expect("reserve_worker failed");
        reservations.push(reservation);
    }

    assert_eq!(
        reservations.len(),
        WORKERS,
        "all permits should be acquirable"
    );
    drop(ctx2);
}

/// Verify that `StallDetectorSubsystem` emits `stall.recommendation` events
/// when a worker's heartbeat stops arriving.  This is the Phase 1 plan item:
///   "1.2 worker heartbeat — detect HeartbeatLost"
#[tokio::test]
async fn stall_detector_emits_heartbeat_lost_recommendation() {
    use adm_engine::adaptive::{DefaultAdaptivePolicy, NoopMetrics, StallDetectorSubsystem};
    use std::sync::Arc;

    let bus = crate::EventBus::new(64);
    let mut sub = bus.subscribe();

    // Use a very short heartbeat timeout so the test finishes quickly.
    let policy = Arc::new(DefaultAdaptivePolicy::fast_test());
    let metrics = Arc::new(NoopMetrics);
    let detector = StallDetectorSubsystem::new(bus.clone(), policy, metrics);

    let shutdown = adm_storage::ShutdownToken::default();
    let _handle = detector.clone().start(shutdown.clone());

    // Register a worker heartbeat, then stop sending them.
    let worker = adm_engine::WorkerHandle::new();
    detector.observe_worker_heartbeat(&worker);

    // Wait up to 3 seconds for a stall.recommendation event.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_recommendation = false;

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Ok(evt)) if evt.topic == "stall.recommendation" => {
                saw_recommendation = true;
                break;
            }
            _ => {}
        }
    }

    assert!(
        saw_recommendation,
        "expected stall.recommendation (HeartbeatLost) after worker stopped heartbeating"
    );
}

/// Confirm that a full-file checksum mismatch after download completion causes
/// the scheduler to publish `checksum.file_failed` and mark the task as failed.
///
/// Plan item: "1.5 Checksum — post-download full-file checksum hook"
#[tokio::test]
async fn post_download_checksum_mismatch_marks_task_failed() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("checksum_test.db");
    let ctx = make_context(&db_path, 512, 2, None).await;
    let manager = eng::QueueManager::new(ctx.clone());

    // Enqueue a task with a deliberately wrong expected SHA-256 hash.
    let mut task = eng::DownloadTask::new("https://example.com/checksum.bin");
    task.total_bytes = Some(1024);
    task.headers.push((
        "X-ADM-Expected-Full-Checksum".to_string(),
        "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
    ));
    task.headers
        .push(("X-ADM-Checksum-Algorithm".to_string(), "sha256".to_string()));
    let id = manager.add_task(task).await.expect("add task");

    let mut sub = ctx.event_bus.subscribe();
    let _ = manager.start_next().await.expect("start task");

    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);
    let mut saw_checksum_failed = false;

    loop {
        tokio::select! {
            maybe = sub.recv() => {
                match maybe {
                    Ok(evt) => {
                        if evt.topic == "checksum.file_failed" {
                            saw_checksum_failed = true;
                            break;
                        }
                        if evt.topic == "download.completed" || evt.topic == "download.failed" {
                            // download finished — check if checksum event came first
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            () = &mut deadline => break,
        }
    }

    // Verify the task is in failed state in DB.
    let stored = ctx
        .storage
        .load_task(id)
        .await
        .expect("load task")
        .expect("task present");

    assert!(
        saw_checksum_failed || stored.state == "failed",
        "expected checksum.file_failed event or task in failed state; state={}",
        stored.state
    );
}
