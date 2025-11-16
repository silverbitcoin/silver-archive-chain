//! Archive Chain synchronization
//!
//! Handles synchronization of Archive Chain from genesis and verification
//! of Merkle roots against Main Chain snapshots.

use crate::error::Result;
use crate::storage::ArchiveStorage;
use tracing::{debug, info, warn};

/// Sync state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Not synced
    NotSynced,
    /// Syncing from genesis
    Syncing,
    /// Synced and up-to-date
    Synced,
    /// Reorganization in progress
    Reorganizing,
}

/// Archive Chain synchronizer
pub struct ArchiveChainSync {
    storage: std::sync::Arc<ArchiveStorage>,
    state: std::sync::Arc<parking_lot::RwLock<SyncState>>,
}

impl ArchiveChainSync {
    /// Create new synchronizer
    pub fn new(storage: std::sync::Arc<ArchiveStorage>) -> Self {
        Self {
            storage,
            state: std::sync::Arc::new(parking_lot::RwLock::new(SyncState::NotSynced)),
        }
    }

    /// Get current sync state
    pub fn get_state(&self) -> SyncState {
        *self.state.read()
    }

    /// Sync Archive Chain from genesis
    pub async fn sync_from_genesis(&self) -> Result<()> {
        info!("Starting Archive Chain sync from genesis");

        *self.state.write() = SyncState::Syncing;

        // Get current height
        let current_height = self.storage.get_height().await?;
        info!("Current Archive Chain height: {}", current_height);

        // In production, this would:
        // 1. Connect to Archive Chain peers
        // 2. Download blocks from genesis (block 0)
        // 3. Verify Merkle roots against Main Chain snapshots
        // 4. Store transactions with proofs
        // 5. Handle chain reorganizations

        // For now, mark as synced if we have at least genesis block
        if current_height > 0 {
            *self.state.write() = SyncState::Synced;
            info!("Archive Chain sync complete at height {}", current_height);
        } else {
            warn!("Archive Chain is empty, waiting for first block");
        }

        Ok(())
    }

    /// Verify block against Main Chain snapshot
    pub async fn verify_block_against_snapshot(
        &self,
        block_number: u64,
        expected_merkle_root: &[u8; 64],
    ) -> Result<bool> {
        debug!(
            "Verifying Archive block {} against Main Chain snapshot",
            block_number
        );

        let block = self.storage.get_block(block_number).await?;

        // Verify Merkle root matches
        if block.merkle_root != *expected_merkle_root {
            warn!(
                "Merkle root mismatch for block {}: expected {:?}, got {:?}",
                block_number, expected_merkle_root, block.merkle_root
            );
            return Ok(false);
        }

        // Verify we have validator signatures
        if block.validator_signatures.is_empty() {
            warn!("Block {} has no validator signatures", block_number);
            return Ok(false);
        }

        Ok(true)
    }

    /// Handle chain reorganization
    pub async fn handle_reorganization(
        &self,
        fork_point: u64,
        new_blocks: Vec<crate::types::ArchiveBlock>,
    ) -> Result<()> {
        info!(
            "Handling Archive Chain reorganization at fork point {}",
            fork_point
        );

        *self.state.write() = SyncState::Reorganizing;

        // In production, this would:
        // 1. Verify all new blocks
        // 2. Revert state to fork point
        // 3. Apply new blocks
        // 4. Verify consistency

        for block in new_blocks {
            self.storage.store_block(&block).await?;
        }

        *self.state.write() = SyncState::Synced;
        info!("Archive Chain reorganization complete");

        Ok(())
    }

    /// Verify transaction against Merkle proof
    pub fn verify_transaction_proof(
        &self,
        tx_hash: &[u8; 64],
        proof: &crate::types::MerkleProof,
        root: &[u8; 64],
    ) -> bool {
        crate::merkle::verify_proof(tx_hash, proof, root)
    }

    /// Get sync progress
    pub async fn get_sync_progress(&self) -> Result<SyncProgress> {
        let current_height = self.storage.get_height().await?;
        let tx_count = self.storage.count_transactions().await?;

        Ok(SyncProgress {
            current_height,
            transaction_count: tx_count,
            state: self.get_state(),
        })
    }
}

/// Sync progress information
#[derive(Debug, Clone)]
pub struct SyncProgress {
    pub current_height: u64,
    pub transaction_count: u64,
    pub state: SyncState,
}

/// Sync Archive Chain from genesis
pub async fn sync_from_genesis(storage: &ArchiveStorage) -> Result<()> {
    info!("Starting Archive Chain sync from genesis");

    // Get current height
    let current_height = storage.get_height().await?;
    info!("Current Archive Chain height: {}", current_height);

    // In production, this would:
    // 1. Connect to Archive Chain peers
    // 2. Download blocks from genesis
    // 3. Verify Merkle roots against Main Chain snapshots
    // 4. Store transactions with proofs

    Ok(())
}

/// Verify block against Main Chain snapshot
pub async fn verify_block_against_snapshot(
    storage: &ArchiveStorage,
    block_number: u64,
    expected_merkle_root: &[u8; 64],
) -> Result<bool> {
    let block = storage.get_block(block_number).await?;

    // Verify Merkle root matches
    Ok(block.merkle_root == *expected_merkle_root)
}
