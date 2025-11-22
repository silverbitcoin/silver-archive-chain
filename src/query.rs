//! Archive Chain query operations

use crate::error::Result;
use crate::storage::ArchiveStorage;
use crate::types::{ArchiveTransaction, MerkleProof};
use tracing::debug;

/// Query transactions by address
pub async fn query_by_address(
    storage: &ArchiveStorage,
    address: &str,
    limit: usize,
) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
    debug!("Querying transactions for address: {}", address);
    let transactions = storage.get_transactions_by_sender(address, limit).await?;

    let mut results = Vec::new();
    for tx in transactions {
        // Retrieve Merkle proof from storage
        let proof = storage.get_or_generate_merkle_proof(&tx.hash).await?;
        results.push((tx, proof));
    }

    Ok(results)
}

/// Query transaction by hash
pub async fn query_by_hash(
    storage: &ArchiveStorage,
    tx_hash: &str,
) -> Result<(ArchiveTransaction, MerkleProof)> {
    debug!("Querying transaction: {}", tx_hash);
    let tx = storage.get_transaction(tx_hash).await?;

    // Retrieve or generate Merkle proof
    let proof = storage.get_or_generate_merkle_proof(&tx.hash).await?;

    Ok((tx, proof))
}

/// Query transactions by time range
pub async fn query_by_time_range(
    storage: &ArchiveStorage,
    start_time: u64,
    end_time: u64,
    limit: usize,
) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
    debug!(
        "Querying transactions in range {} - {}",
        start_time, end_time
    );
    let transactions = storage
        .get_transactions_by_time_range(start_time, end_time, limit)
        .await?;

    let mut results = Vec::new();
    for tx in transactions {
        let proof = storage.get_or_generate_merkle_proof(&tx.hash).await?;
        };
        results.push((tx, proof));
    }

    Ok(results)
}

/// Query transactions by recipient
pub async fn query_by_recipient(
    _storage: &ArchiveStorage,
    recipient: &str,
    _limit: usize,
) -> Result<Vec<(ArchiveTransaction, MerkleProof)>> {
    debug!("Querying transactions for recipient: {}", recipient);
    
    // Note: This would require an additional index in storage
    // For now, we return empty results
    Ok(vec![])
}

/// Get transaction count
pub async fn get_transaction_count(storage: &ArchiveStorage) -> Result<u64> {
    storage.count_transactions().await
}
