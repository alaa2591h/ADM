#![allow(
    clippy::cast_precision_loss,
    clippy::float_cmp,
    clippy::must_use_candidate
)]

pub mod metrics;
pub mod snapshots;
pub mod tracing;

pub use metrics::{Metrics, MetricsSnapshot};
pub use snapshots::*;
pub use tracing::init_tracing;

pub fn init_metrics() {
    if let Err(e) = metrics_exporter_prometheus::PrometheusBuilder::new().install() {
        ::tracing::warn!("Prometheus recorder already installed or failed: {}", e);
    }
}

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub trait SnapshotProvider: Send + Sync {
    fn get_snapshot(&self) -> WorkerPoolSnapshot;
}

#[derive(Default)]
pub struct TelemetryManager {
    providers: RwLock<HashMap<String, Arc<dyn SnapshotProvider>>>,
}

impl TelemetryManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_provider(&self, name: String, provider: Arc<dyn SnapshotProvider>) {
        self.providers.write().insert(name, provider);
    }

    pub fn collect_runtime_snapshot(&self, scheduler: SchedulerDiagnostics) -> RuntimeSnapshot {
        let mut worker_pools = HashMap::new();
        let providers = self.providers.read();

        for (name, provider) in providers.iter() {
            worker_pools.insert(name.clone(), provider.get_snapshot());
        }

        RuntimeSnapshot {
            scheduler,
            worker_pools,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_snapshot_success_rate() {
        let metrics = Metrics::new();
        metrics.record_download_started();
        metrics.record_download_completed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.downloads_started, 1);
        assert_eq!(snapshot.downloads_completed, 1);
        assert_eq!(snapshot.success_rate(), 100.0);
    }
}
