//! Basic metrics collection for APEX Download Manager (ADM)

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Metrics {
    pub downloads_started: Arc<AtomicU64>,
    pub downloads_completed: Arc<AtomicU64>,
    pub downloads_failed: Arc<AtomicU64>,
    pub bytes_downloaded: Arc<AtomicU64>,
    pub chunks_processed: Arc<AtomicU64>,
    pub retries_executed: Arc<AtomicU64>,
    pub mux_started: Arc<AtomicU64>,
    pub mux_completed: Arc<AtomicU64>,
    pub mux_failed: Arc<AtomicU64>,
    pub mux_retries: Arc<AtomicU64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            downloads_started: Arc::new(AtomicU64::new(0)),
            downloads_completed: Arc::new(AtomicU64::new(0)),
            downloads_failed: Arc::new(AtomicU64::new(0)),
            bytes_downloaded: Arc::new(AtomicU64::new(0)),
            chunks_processed: Arc::new(AtomicU64::new(0)),
            retries_executed: Arc::new(AtomicU64::new(0)),
            mux_started: Arc::new(AtomicU64::new(0)),
            mux_completed: Arc::new(AtomicU64::new(0)),
            mux_failed: Arc::new(AtomicU64::new(0)),
            mux_retries: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_download_started(&self) {
        self.downloads_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_download_completed(&self) {
        self.downloads_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_download_failed(&self) {
        self.downloads_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_chunk_processed(&self) {
        self.chunks_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mux_started(&self) {
        self.mux_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mux_completed(&self) {
        self.mux_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mux_failed(&self) {
        self.mux_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mux_retry(&self) {
        self.mux_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            downloads_started: self.downloads_started.load(Ordering::Relaxed),
            downloads_completed: self.downloads_completed.load(Ordering::Relaxed),
            downloads_failed: self.downloads_failed.load(Ordering::Relaxed),
            bytes_downloaded: self.bytes_downloaded.load(Ordering::Relaxed),
            chunks_processed: self.chunks_processed.load(Ordering::Relaxed),
            retries_executed: self.retries_executed.load(Ordering::Relaxed),
            mux_started: self.mux_started.load(Ordering::Relaxed),
            mux_completed: self.mux_completed.load(Ordering::Relaxed),
            mux_failed: self.mux_failed.load(Ordering::Relaxed),
            mux_retries: self.mux_retries.load(Ordering::Relaxed),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of engine metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub downloads_started: u64,
    pub downloads_completed: u64,
    pub downloads_failed: u64,
    pub bytes_downloaded: u64,
    pub chunks_processed: u64,
    pub retries_executed: u64,
    pub mux_started: u64,
    pub mux_completed: u64,
    pub mux_failed: u64,
    pub mux_retries: u64,
}

impl MetricsSnapshot {
    pub fn success_rate(&self) -> f64 {
        if self.downloads_started == 0 {
            return 0.0;
        }
        (self.downloads_completed as f64 / self.downloads_started as f64) * 100.0
    }
}
