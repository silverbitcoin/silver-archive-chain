//! Archive Chain - Historical record with 3 TPS
//!
//! The Archive Chain maintains the complete historical record of all transactions
//! with Merkle proofs for verification. It operates at 3 TPS (vs 160,000 TPS Main Chain)
//! and stores approximately 47 GB/year.
//!
//! # Architecture
//!
//! ```text
//! Main Chain (160,000 TPS)
//!     ↓
//! Merkle Root (every 480ms)
//!     ↓
//! Archive Chain (3 TPS)
//!     ├─ Store transaction references
//!     ├─ Store Merkle proofs
//!     └─ Maintain full history
//! ```
//!
//! # Query Flow
//!
//! ```text
//! Light Node Query
//!     ↓
//! Archive Chain ParityDB
//!     ├─ Lookup by tx hash
//!     ├─ Generate Merkle proof
//!     └─ Return: [transactions] + [proof]
//!     ↓
//! Light Node Verification
//!     ├─ Verify Merkle proof
//!     ├─ Check validator signatures
//!     └─ Display results
//! ```

pub mod consensus;
pub mod error;
pub mod indexing;
pub mod merkle;
pub mod peer_sync;
pub mod proof_generator;
pub mod query;
pub mod schema;
pub mod storage;
pub mod sync;
pub mod types;

pub use consensus::{ArchiveChainStats, ArchiveConsensus, ArchiveValidator};
pub use error::{ArchiveChainError, Result};
pub use peer_sync::{ArchivePeer, PeerSynchronizer, SyncStatus};
pub use proof_generator::{
    BatchProofGenerator, BatchProofStats, MerkleTree, ProofGenerator, ProofGeneratorStats,
};
pub use sync::{ArchiveChainSync, SyncProgress, SyncState};
pub use types::{ArchiveBlock, ArchiveChainConfig, ArchiveTransaction, MerkleProof};

use std::sync::Arc;
use tracing::{debug, info};

/// Archive Chain node
pub struct ArchiveChain {
    storage: Arc<storage::ArchiveStorage>,
    consensus: Arc<consensus::ArchiveConsensus>,
    sync: Arc<sync::ArchiveChainSync>,
}

impl ArchiveChain {
    /// Create new Archive Chain
    pub async fn new(db_path: &str) -> Result<Self> {
        info!("Initializing Archive Chain at {}", db_path);

        let storage: Arc<storage::ArchiveStorage> = Arc::new(storage::ArchiveStorage::new(db_path).await?);
        let consensus = Arc::new(consensus::ArchiveConsensus::new(storage.clone()));
        let sync = Arc::new(sync::ArchiveChainSync::new(storage.clone()));

        Ok(Self {
            storage,
            consensus,
            sync,
        })
    }

    /// Add validator to Archive Chain
    pub fn add_validator(&self, validator: ArchiveValidator) -> Result<()> {
        self.consensus.add_validator(validator)
    }

    /// Remove validator from Archive Chain
    pub fn remove_validator(&self, address: &str) -> Result<()> {
        self.consensus.remove_validator(address)
    }

    /// Get Archive Chain validators
    pub fn get_validators(&self) -> Vec<ArchiveValidator> {
        self.consensus.get_validators()
    }

    /// Process Merkle root from Main Chain
    pub async fn process_merkle_root(
        &self,
        snapshot_number: u64,
        merkle_root: [u8; 64],
        validator_signatures: Vec<Vec<u8>>,
    ) -> Result<()> {
        debug!(
            "Processing Merkle root from Main Chain snapshot {}",
            snapshot_number
        );

        self.consensus
            .process_merkle_root(snapshot_number, merkle_root, validator_signatures)
            .await
    }

    /// Add pending transaction
    pub fn add_pending_transaction(&self, tx: ArchiveTransaction) -> Result<()> {
        self.consensus.add_pending_transaction(tx)
    }

    /// Get pending transactions
    pub fn get_pending_transactions(&self) -> Vec<ArchiveTransaction> {
        self.consensus.get_pending_transactions()
    }

    /// Query transactions by address
    pub async fn query_by_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
        debug!("Querying transactions for address: {}", address);
        query::query_by_address(&self.storage, address, limit).await
    }

    /// Query transaction by hash
    pub async fn query_by_hash(&self, tx_hash: &str) -> Result<(ArchiveTransaction, MerkleProof)> {
        debug!("Querying transaction: {}", tx_hash);
        query::query_by_hash(&self.storage, tx_hash).await
    }

    /// Query transactions in time range
    pub async fn query_by_time_range(
        &self,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
        debug!(
            "Querying transactions in range {} - {}",
            start_time, end_time
        );
        query::query_by_time_range(&self.storage, start_time, end_time, limit).await
    }

    /// Query transactions by recipient
    pub async fn query_by_recipient(
        &self,
        recipient: &str,
        limit: usize,
    ) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
        query::query_by_recipient(&self.storage, recipient, limit).await
    }

    /// Sync Archive Chain from genesis
    pub async fn sync_from_genesis(&self) -> Result<()> {
        info!("Starting Archive Chain sync from genesis");
        self.sync.sync_from_genesis().await
    }

    /// Get sync state
    pub fn get_sync_state(&self) -> SyncState {
        self.sync.get_state()
    }

    /// Get sync progress
    pub async fn get_sync_progress(&self) -> Result<SyncProgress> {
        self.sync.get_sync_progress().await
    }

    /// Get current height
    pub async fn get_height(&self) -> Result<u64> {
        self.storage.get_height().await
    }

    /// Get Archive Chain statistics
    pub fn get_stats(&self) -> ArchiveChainStats {
        self.consensus.get_stats()
    }

    /// Verify Merkle proof
    pub fn verify_merkle_proof(
        &self,
        tx_hash: &[u8; 64],
        proof: &MerkleProof,
        root: &[u8; 64],
    ) -> bool {
        merkle::verify_proof(tx_hash, proof, root)
    }

    /// Verify block against Main Chain snapshot
    pub async fn verify_block_against_snapshot(
        &self,
        block_number: u64,
        expected_merkle_root: &[u8; 64],
    ) -> Result<bool> {
        self.sync
            .verify_block_against_snapshot(block_number, expected_merkle_root)
            .await
    }

    /// Flush storage to disk
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    /// Compact storage
    pub async fn compact(&self) -> Result<()> {
        self.storage.compact().await
    }
}
