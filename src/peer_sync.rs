//! Archive Chain peer synchronization
//!
//! Handles synchronization with Archive Chain peers to download blocks
//! and verify Merkle roots against Main Chain snapshots.

use crate::error::Result;
use crate::storage::ArchiveStorage;
use crate::types::ArchiveBlock;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Archive Chain peer information
#[derive(Debug, Clone)]
pub struct ArchivePeer {
    pub peer_id: String,
    pub address: String,
    pub height: u64,
    pub last_seen: u64,
}

/// Peer synchronizer
pub struct PeerSynchronizer {
    storage: Arc<ArchiveStorage>,
    peers: Arc<parking_lot::RwLock<Vec<ArchivePeer>>>,
}

impl PeerSynchronizer {
    /// Create new peer synchronizer
    pub fn new(storage: Arc<ArchiveStorage>) -> Self {
        Self {
            storage,
            peers: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    /// Add peer
    pub fn add_peer(&self, peer: ArchivePeer) -> Result<()> {
        debug!("Adding Archive Chain peer: {}", peer.peer_id);

        let mut peers = self.peers.write();
        if !peers.iter().any(|p| p.peer_id == peer.peer_id) {
            peers.push(peer);
        }

        Ok(())
    }

    /// Remove peer
    pub fn remove_peer(&self, peer_id: &str) -> Result<()> {
        debug!("Removing Archive Chain peer: {}", peer_id);

        let mut peers = self.peers.write();
        peers.retain(|p| p.peer_id != peer_id);

        Ok(())
    }

    /// Get peers
    pub fn get_peers(&self) -> Vec<ArchivePeer> {
        self.peers.read().clone()
    }

    /// Get best peer (highest height)
    pub fn get_best_peer(&self) -> Option<ArchivePeer> {
        self.peers
            .read()
            .iter()
            .max_by_key(|p| p.height)
            .cloned()
    }

    /// Sync blocks from peer
    pub async fn sync_blocks_from_peer(
        &self,
        peer: &ArchivePeer,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<ArchiveBlock>> {
        debug!(
            "Syncing blocks {} - {} from peer {}",
            start_block, end_block, peer.peer_id
        );

        // In production, this would:
        // 1. Connect to peer
        // 2. Request blocks in range
        // 3. Verify each block
        // 4. Store blocks locally

        // For now, return empty vector
        Ok(vec![])
    }

    /// Verify block against Main Chain snapshot
    pub async fn verify_block_against_snapshot(
        &self,
        block: &ArchiveBlock,
        expected_merkle_root: &[u8; 64],
    ) -> Result<bool> {
        debug!(
            "Verifying Archive block {} against Main Chain snapshot",
            block.block_number
        );

        // Verify Merkle root matches
        if block.merkle_root != *expected_merkle_root {
            warn!(
                "Merkle root mismatch for block {}: expected {:?}, got {:?}",
                block.block_number, expected_merkle_root, block.merkle_root
            );
            return Ok(false);
        }

        // Verify we have validator signatures
        if block.validator_signatures.is_empty() {
            warn!("Block {} has no validator signatures", block.block_number);
            return Ok(false);
        }

        Ok(true)
    }

    /// Sync from genesis
    pub async fn sync_from_genesis(&self) -> Result<()> {
        info!("Starting Archive Chain sync from genesis");

        let current_height = self.storage.get_height().await?;
        info!("Current Archive Chain height: {}", current_height);

        // Get best peer
        let best_peer = self.get_best_peer();
        if best_peer.is_none() {
            warn!("No Archive Chain peers available");
            return Ok(());
        }

        let peer = best_peer.unwrap();
        info!(
            "Syncing from peer {} at height {}",
            peer.peer_id, peer.height
        );

        // Sync blocks from current height to peer height
        let mut current = current_height;
        while current < peer.height {
            let end = std::cmp::min(current + 100, peer.height);

            match self.sync_blocks_from_peer(&peer, current, end).await {
                Ok(blocks) => {
                    for block in blocks {
                        self.storage.store_block(&block).await?;
                        current = block.block_number + 1;
                    }
                }
                Err(e) => {
                    warn!("Failed to sync blocks from peer: {}", e);
                    break;
                }
            }
        }

        info!("Archive Chain sync complete at height {}", current);
        Ok(())
    }

    /// Handle chain reorganization
    pub async fn handle_reorganization(
        &self,
        fork_point: u64,
        new_blocks: Vec<ArchiveBlock>,
    ) -> Result<()> {
        info!(
            "Handling Archive Chain reorganization at fork point {}",
            fork_point
        );

        // In production, this would:
        // 1. Verify all new blocks
        // 2. Revert state to fork point
        // 3. Apply new blocks
        // 4. Verify consistency

        for block in new_blocks {
            self.storage.store_block(&block).await?;
        }

        info!("Archive Chain reorganization complete");
        Ok(())
    }

    /// Get sync status
    pub async fn get_sync_status(&self) -> Result<SyncStatus> {
        let local_height = self.storage.get_height().await?;
        let best_peer = self.get_best_peer();

        let (peer_height, is_synced) = if let Some(peer) = best_peer {
            (peer.height, local_height >= peer.height)
        } else {
            (0, false)
        };

        Ok(SyncStatus {
            local_height,
            peer_height,
            is_synced,
            peer_count: self.peers.read().len(),
        })
    }
}

/// Sync status information
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub local_height: u64,
    pub peer_height: u64,
    pub is_synced: bool,
    pub peer_count: usize,
}

/// Verify Merkle root against Main Chain snapshot
pub async fn verify_merkle_root_against_snapshot(
    storage: &ArchiveStorage,
    block_number: u64,
    expected_merkle_root: &[u8; 64],
) -> Result<bool> {
    let block = storage.get_block(block_number).await?;

    // Verify Merkle root matches
    Ok(block.merkle_root == *expected_merkle_root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_synchronizer_creation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(ArchiveStorage::new(dir.path().to_str().unwrap()).await.unwrap());
        let sync = PeerSynchronizer::new(storage);

        assert_eq!(sync.get_peers().len(), 0);
    }

    #[tokio::test]
    async fn test_add_peer() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(ArchiveStorage::new(dir.path().to_str().unwrap()).await.unwrap());
        let sync = PeerSynchronizer::new(storage);

        let peer = ArchivePeer {
            peer_id: "peer1".to_string(),
            address: "127.0.0.1:9000".to_string(),
            height: 100,
            last_seen: 0,
        };

        sync.add_peer(peer).unwrap();
        assert_eq!(sync.get_peers().len(), 1);
    }

    #[tokio::test]
    async fn test_get_best_peer() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(ArchiveStorage::new(dir.path().to_str().unwrap()).await.unwrap());
        let sync = PeerSynchronizer::new(storage);

        let peer1 = ArchivePeer {
            peer_id: "peer1".to_string(),
            address: "127.0.0.1:9000".to_string(),
            height: 100,
            last_seen: 0,
        };

        let peer2 = ArchivePeer {
            peer_id: "peer2".to_string(),
            address: "127.0.0.1:9001".to_string(),
            height: 200,
            last_seen: 0,
        };

        sync.add_peer(peer1).unwrap();
        sync.add_peer(peer2).unwrap();

        let best = sync.get_best_peer().unwrap();
        assert_eq!(best.peer_id, "peer2");
        assert_eq!(best.height, 200);
    }
}
