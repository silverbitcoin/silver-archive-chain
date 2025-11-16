//! Archive Chain storage layer using RocksDB

use crate::error::{ArchiveChainError, Result};
use crate::types::{ArchiveBlock, ArchiveTransaction};
use rocksdb::{DB, Options, IteratorMode};
use std::path::Path;
use tracing::{debug, info};

/// Archive Chain storage
pub struct ArchiveStorage {
    db: DB,
}

impl ArchiveStorage {
    /// Create new Archive Storage
    pub async fn new(db_path: &str) -> Result<Self> {
        let path = Path::new(db_path);
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);

        let db = DB::open(&opts, db_path)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        info!("Archive Storage initialized at {}", db_path);

        Ok(Self { db })
    }

    /// Store transaction
    pub async fn store_transaction(&self, tx: &ArchiveTransaction) -> Result<()> {
        let key = format!("tx:{}", hex::encode(&tx.hash));
        let value = serde_json::to_vec(tx)?;

        self.db
            .put(key.as_bytes(), &value)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        // Index by sender
        let sender_key = format!("sender:{}:{}", tx.sender, hex::encode(&tx.hash));
        self.db
            .put(sender_key.as_bytes(), &tx.hash)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        // Index by timestamp
        let time_key = format!("time:{}:{}", tx.timestamp, hex::encode(&tx.hash));
        self.db
            .put(time_key.as_bytes(), &tx.hash)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        debug!("Stored transaction: {}", hex::encode(&tx.hash));
        Ok(())
    }

    /// Get transaction by hash
    pub async fn get_transaction(&self, tx_hash: &str) -> Result<ArchiveTransaction> {
        let key = format!("tx:{}", tx_hash);
        let value = self
            .db
            .get(key.as_bytes())
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?
            .ok_or_else(|| ArchiveChainError::TransactionNotFound(tx_hash.to_string()))?;

        serde_json::from_slice(&value).map_err(|e| ArchiveChainError::SerializationError(e))
    }

    /// Get transactions by sender
    pub async fn get_transactions_by_sender(
        &self,
        sender: &str,
        limit: usize,
    ) -> Result<Vec<ArchiveTransaction>> {
        let prefix = format!("sender:{}:", sender);
        let mut transactions = Vec::new();

        let iter = self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));

        for result in iter {
            let (key, _) = result.map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }

            // Extract tx hash from key
            if let Some(tx_hash) = key_str.split(':').nth(2) {
                if let Ok(tx) = self.get_transaction(tx_hash).await {
                    transactions.push(tx);
                    if transactions.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(transactions)
    }

    /// Get transactions by time range
    pub async fn get_transactions_by_time_range(
        &self,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<Vec<ArchiveTransaction>> {
        let prefix = format!("time:");
        let mut transactions = Vec::new();

        let iter = self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));

        for result in iter {
            let (key, _) = result.map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with("time:") {
                break;
            }

            // Parse timestamp from key
            if let Some(time_str) = key_str.split(':').nth(1) {
                if let Ok(timestamp) = time_str.parse::<u64>() {
                    if timestamp >= start_time && timestamp <= end_time {
                        if let Some(tx_hash) = key_str.split(':').nth(2) {
                            if let Ok(tx) = self.get_transaction(tx_hash).await {
                                transactions.push(tx);
                                if transactions.len() >= limit {
                                    return Ok(transactions);
                                }
                            }
                        }
                    } else if timestamp > end_time {
                        break;
                    }
                }
            }
        }

        Ok(transactions)
    }

    /// Store block
    pub async fn store_block(&self, block: &ArchiveBlock) -> Result<()> {
        let key = format!("block:{}", block.block_number);
        let value = serde_json::to_vec(block)?;

        self.db
            .put(key.as_bytes(), &value)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        // Update height
        self.db
            .put(b"height", block.block_number.to_le_bytes().as_ref())
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        info!("Stored block: {}", block.block_number);
        Ok(())
    }

    /// Get block by number
    pub async fn get_block(&self, block_number: u64) -> Result<ArchiveBlock> {
        let key = format!("block:{}", block_number);
        let value = self
            .db
            .get(key.as_bytes())
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?
            .ok_or_else(|| ArchiveChainError::InvalidBlock(format!("Block {} not found", block_number)))?;

        serde_json::from_slice(&value).map_err(|e| ArchiveChainError::SerializationError(e))
    }

    /// Get current height
    pub async fn get_height(&self) -> Result<u64> {
        match self
            .db
            .get(b"height")
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?
        {
            Some(bytes) => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                Ok(u64::from_le_bytes(buf))
            }
            None => Ok(0),
        }
    }

    /// Store Merkle proof
    pub async fn store_merkle_proof(
        &self,
        tx_hash: &[u8; 64],
        proof: &crate::types::MerkleProof,
    ) -> Result<()> {
        let key = format!("proof:{}", hex::encode(tx_hash));
        let value = serde_json::to_vec(proof)?;

        self.db
            .put(key.as_bytes(), &value)
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;

        debug!("Stored Merkle proof for transaction: {}", hex::encode(tx_hash));
        Ok(())
    }

    /// Get Merkle proof
    pub async fn get_merkle_proof(
        &self,
        tx_hash: &[u8; 64],
    ) -> Result<crate::types::MerkleProof> {
        let key = format!("proof:{}", hex::encode(tx_hash));
        let value = self
            .db
            .get(key.as_bytes())
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?
            .ok_or_else(|| {
                ArchiveChainError::Unknown(format!(
                    "Merkle proof not found for transaction: {}",
                    hex::encode(tx_hash)
                ))
            })?;

        serde_json::from_slice(&value).map_err(|e| ArchiveChainError::SerializationError(e))
    }

    /// Get all transactions in a block
    pub async fn get_block_transactions(&self, block_number: u64) -> Result<Vec<ArchiveTransaction>> {
        let block = self.get_block(block_number).await?;
        Ok(block.transactions)
    }

    /// Count transactions
    pub async fn count_transactions(&self) -> Result<u64> {
        let prefix = "tx:";
        let mut count = 0u64;

        let iter = self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));

        for result in iter {
            let (key, _) = result.map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(prefix) {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Flush to disk
    pub async fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| ArchiveChainError::RocksDBError(e.to_string()))
    }

    /// Compact database
    pub async fn compact(&self) -> Result<()> {
        self.db
            .compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}
