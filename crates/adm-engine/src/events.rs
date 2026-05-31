//! Minimal direct broadcast event bus for the engine.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const DEFAULT_BUFFER: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub sequence_id: u64,
    pub topic: String,
    pub data: Value,
    pub timestamp_ms: u64,
}

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Event>,
    sequence: Arc<AtomicU64>,
}

impl EventBus {
    #[must_use]
    pub fn new(buffer: usize) -> Self {
        let buffer = buffer.max(DEFAULT_BUFFER);
        let (sender, _) = broadcast::channel(buffer);
        Self {
            sender,
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub fn publish(&self, topic: impl Into<String>, data: Value) {
        let topic = topic.into();
        let sequence_id = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let _ = self.sender.send(Event {
            sequence_id,
            topic,
            data,
            timestamp_ms,
        });
    }
}
