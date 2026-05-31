//! Detailed torrent tracking: peers, blocks, seeders/leechers.
//!
//! This module tracks granular information about torrent downloads:
//! - Connected peers and their upload/download rates
//! - Block completion status and peer source attribution
//! - Seeder/leecher count and peer discovery metrics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Peer connection metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub addr: String,
    pub peer_id: String,
    pub is_seeder: bool,
    pub upload_rate_bps: u64,
    pub download_rate_bps: u64,
    pub connected_at: i64,
    pub blocks_provided: u32,
    pub blocks_rejected: u32,
}

impl PeerInfo {
    #[must_use]
    pub fn new(addr: String, peer_id: String, is_seeder: bool) -> Self {
        Self {
            addr,
            peer_id,
            is_seeder,
            upload_rate_bps: 0,
            download_rate_bps: 0,
            connected_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            blocks_provided: 0,
            blocks_rejected: 0,
        }
    }

    #[must_use]
    pub fn efficiency(&self) -> f64 {
        if self.blocks_provided == 0 {
            0.0
        } else {
            f64::from(self.blocks_provided)
                / (f64::from(self.blocks_provided) + f64::from(self.blocks_rejected))
        }
    }
}

/// Block completion and source information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub index: u32,
    pub offset: u64,
    pub length: u64,
    pub state: BlockState,
    pub source_peer: Option<String>,
    pub retrieved_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl BlockState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Overall torrent stats: seeders, leechers, discovery metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentStats {
    pub info_hash: String,
    pub total_seeders: u32,
    pub total_leechers: u32,
    pub connected_peers: u32,
    pub total_peers_discovered: u32,
    pub blocks_completed: u32,
    pub blocks_pending: u32,
    pub blocks_failed: u32,
    pub avg_peer_efficiency: f64,
    pub total_downloaded_bytes: u64,
    pub last_peer_discovery_at: i64,
}

/// Torrent download tracker.
pub struct TorrentTracker {
    pub info_hash: String,
    peers: Arc<Mutex<HashMap<String, PeerInfo>>>,
    blocks: Arc<Mutex<HashMap<u32, BlockInfo>>>,
    stats: Arc<Mutex<TorrentStats>>,
}

impl TorrentTracker {
    /// Create a new torrent tracker for a given info hash.
    #[must_use]
    pub fn new(info_hash: String) -> Self {
        let stats = TorrentStats {
            info_hash: info_hash.clone(),
            total_seeders: 0,
            total_leechers: 0,
            connected_peers: 0,
            total_peers_discovered: 0,
            blocks_completed: 0,
            blocks_pending: 0,
            blocks_failed: 0,
            avg_peer_efficiency: 0.0,
            total_downloaded_bytes: 0,
            last_peer_discovery_at: 0,
        };

        Self {
            info_hash,
            peers: Arc::new(Mutex::new(HashMap::new())),
            blocks: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(stats)),
        }
    }

    /// Register a newly discovered peer.
    pub async fn add_peer(&self, peer: PeerInfo) {
        let mut peers = self.peers.lock().await;
        peers.insert(peer.addr.clone(), peer.clone());

        let mut stats = self.stats.lock().await;
        stats.total_peers_discovered += 1;
        if peer.is_seeder {
            stats.total_seeders += 1;
        } else {
            stats.total_leechers += 1;
        }
        stats.connected_peers = peers.len() as u32;
        stats.last_peer_discovery_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
    }

    /// Remove a peer from tracking (e.g., disconnection).
    pub async fn remove_peer(&self, addr: &str) {
        let mut peers = self.peers.lock().await;
        if let Some(peer) = peers.remove(addr) {
            let mut stats = self.stats.lock().await;
            stats.connected_peers = peers.len() as u32;
            if peer.is_seeder {
                stats.total_seeders = stats.total_seeders.saturating_sub(1);
            } else {
                stats.total_leechers = stats.total_leechers.saturating_sub(1);
            }
        }
    }

    /// Update a peer's throughput metrics.
    pub async fn update_peer_rates(&self, addr: &str, upload_bps: u64, download_bps: u64) {
        let mut peers = self.peers.lock().await;
        if let Some(peer) = peers.get_mut(addr) {
            peer.upload_rate_bps = upload_bps;
            peer.download_rate_bps = download_bps;
        }
    }

    /// Record a block completion with its source peer.
    pub async fn complete_block(&self, block_index: u32, source_peer: String, block_size: u64) {
        let mut blocks = self.blocks.lock().await;
        if let Some(block) = blocks.get_mut(&block_index) {
            block.state = BlockState::Completed;
            block.source_peer = Some(source_peer.clone());
            block.retrieved_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            );
        }

        let mut peers = self.peers.lock().await;
        if let Some(peer) = peers.get_mut(&source_peer) {
            peer.blocks_provided += 1;
        }

        let mut stats = self.stats.lock().await;
        stats.blocks_completed += 1;
        stats.blocks_pending = stats.blocks_pending.saturating_sub(1);
        stats.total_downloaded_bytes += block_size;
        self.update_avg_efficiency(&mut stats, &peers);
    }

    /// Record a block failure (e.g., checksum mismatch).
    pub async fn fail_block(&self, block_index: u32, peer_addr: String) {
        let mut blocks = self.blocks.lock().await;
        if let Some(block) = blocks.get_mut(&block_index) {
            block.state = BlockState::Failed;
        }

        let mut peers = self.peers.lock().await;
        if let Some(peer) = peers.get_mut(&peer_addr) {
            peer.blocks_rejected += 1;
        }

        let mut stats = self.stats.lock().await;
        stats.blocks_failed += 1;
        self.update_avg_efficiency(&mut stats, &peers);
    }

    /// Initialize blocks for a torrent (call once when torrent metadata is known).
    pub async fn init_blocks(&self, total_size: u64, block_size: u64) {
        let mut blocks = self.blocks.lock().await;
        blocks.clear();
        let mut offset = 0u64;
        let mut index = 0u32;

        while offset < total_size {
            let len = std::cmp::min(block_size, total_size - offset);
            blocks.insert(
                index,
                BlockInfo {
                    index,
                    offset,
                    length: len,
                    state: BlockState::Pending,
                    source_peer: None,
                    retrieved_at: None,
                },
            );
            offset += len;
            index += 1;
        }

        let mut stats = self.stats.lock().await;
        stats.blocks_pending = index;
    }

    /// Get current stats snapshot.
    pub async fn snapshot(&self) -> TorrentStats {
        self.stats.lock().await.clone()
    }

    /// Get all connected peers.
    pub async fn connected_peers(&self) -> Vec<PeerInfo> {
        self.peers.lock().await.values().cloned().collect()
    }

    /// Get block completion map.
    pub async fn block_completion_map(&self) -> HashMap<u32, BlockState> {
        self.blocks
            .lock()
            .await
            .iter()
            .map(|(idx, block)| (*idx, block.state.clone()))
            .collect()
    }

    /// Compute average peer efficiency (`blocks_provided` / (`blocks_provided` + `blocks_rejected`)).
    fn update_avg_efficiency(&self, stats: &mut TorrentStats, peers: &HashMap<String, PeerInfo>) {
        if peers.is_empty() {
            stats.avg_peer_efficiency = 0.0;
        } else {
            let total_eff: f64 = peers.values().map(PeerInfo::efficiency).sum();
            stats.avg_peer_efficiency = total_eff / peers.len() as f64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracker_discovers_peers() {
        let tracker = TorrentTracker::new("abc123def456".to_string());
        let peer1 = PeerInfo::new("192.168.1.100:6881".to_string(), "peer-1".to_string(), true);
        let peer2 = PeerInfo::new(
            "192.168.1.101:6881".to_string(),
            "peer-2".to_string(),
            false,
        );

        tracker.add_peer(peer1).await;
        tracker.add_peer(peer2).await;

        let stats = tracker.snapshot().await;
        assert_eq!(stats.total_peers_discovered, 2);
        assert_eq!(stats.total_seeders, 1);
        assert_eq!(stats.total_leechers, 1);
    }

    #[tokio::test]
    async fn tracker_monitors_block_completion() {
        let tracker = TorrentTracker::new("xyz789".to_string());
        tracker.init_blocks(1024 * 1024, 16 * 1024).await;

        let peer = PeerInfo::new("127.0.0.1:6881".to_string(), "test-peer".to_string(), true);
        tracker.add_peer(peer).await;

        tracker
            .complete_block(0, "127.0.0.1:6881".to_string(), 16 * 1024)
            .await;
        let stats = tracker.snapshot().await;

        assert_eq!(stats.blocks_completed, 1);
        assert_eq!(stats.blocks_pending, 63);
        assert!(stats.avg_peer_efficiency > 0.0);
    }

    #[tokio::test]
    async fn tracker_calculates_peer_efficiency() {
        let tracker = TorrentTracker::new("test123".to_string());
        let peer1 = PeerInfo::new("127.0.0.1:6881".to_string(), "peer-1".to_string(), true);
        tracker.add_peer(peer1).await;

        tracker
            .complete_block(0, "127.0.0.1:6881".to_string(), 1024)
            .await;
        tracker.fail_block(1, "127.0.0.1:6881".to_string()).await;

        let connected = tracker.connected_peers().await;
        assert_eq!(connected.len(), 1);
        let efficiency = connected[0].efficiency();
        assert!((efficiency - 0.5).abs() < f64::EPSILON); // 1 provided, 1 rejected
    }
}
