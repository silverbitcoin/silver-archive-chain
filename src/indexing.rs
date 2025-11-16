//! Archive Chain indexing for efficient range queries
//!
//! This module provides utilities for efficient range queries on indexed data
//! stored in RocksDB.

use crate::error::Result;
use crate::storage::ArchiveStorage;
use crate::types::ArchiveTransaction;
use tracing::debug;

/// Index query builder for efficient range queries
pub struct IndexQuery {
    #[allow(dead_code)]
    prefix: String,
    limit: usize,
    reverse: bool,
}

impl IndexQuery {
    /// Create new index query with prefix
    pub fn new(prefix: String) -> Self {
        Self {
            prefix,
            limit: 1000,
            reverse: false,
        }
    }

    /// Set query limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Set reverse order
    pub fn reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }
}

/// Query results with pagination
pub struct QueryResults {
    pub items: Vec<ArchiveTransaction>,
    pub total_count: usize,
    pub has_more: bool,
}

/// Execute sender index query
pub async fn query_sender_index(
    storage: &ArchiveStorage,
    address: &str,
    limit: usize,
) -> Result<QueryResults> {
    debug!("Querying sender index for address: {}", address);

    let transactions = storage.get_transactions_by_sender(address, limit).await?;
    let total_count = transactions.len();
    let has_more = total_count >= limit;

    Ok(QueryResults {
        items: transactions,
        total_count,
        has_more,
    })
}

/// Execute recipient index query
pub async fn query_recipient_index(
    _storage: &ArchiveStorage,
    address: &str,
    _limit: usize,
) -> Result<QueryResults> {
    debug!("Querying recipient index for address: {}", address);

    // Note: This would require an additional index in storage
    // For now, return empty results
    Ok(QueryResults {
        items: vec![],
        total_count: 0,
        has_more: false,
    })
}

/// Execute time range index query
pub async fn query_time_range_index(
    storage: &ArchiveStorage,
    start_time: u64,
    end_time: u64,
    limit: usize,
) -> Result<QueryResults> {
    debug!(
        "Querying time range index: {} - {}",
        start_time, end_time
    );

    let transactions = storage
        .get_transactions_by_time_range(start_time, end_time, limit)
        .await?;
    let total_count = transactions.len();
    let has_more = total_count >= limit;

    Ok(QueryResults {
        items: transactions,
        total_count,
        has_more,
    })
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub sender_index_size: u64,
    pub recipient_index_size: u64,
    pub time_index_size: u64,
    pub total_transactions: u64,
}

/// Get index statistics
pub async fn get_index_stats(storage: &ArchiveStorage) -> Result<IndexStats> {
    let total_transactions = storage.count_transactions().await?;

    Ok(IndexStats {
        sender_index_size: total_transactions,
        recipient_index_size: 0, // Would be populated if recipient index exists
        time_index_size: total_transactions,
        total_transactions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_query_builder() {
        let query = IndexQuery::new("sender:0x123:".to_string())
            .limit(100)
            .reverse(true);

        assert_eq!(query.prefix, "sender:0x123:");
        assert_eq!(query.limit, 100);
        assert!(query.reverse);
    }
}
