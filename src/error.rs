//! Archive Chain error types

use thiserror::Error;

/// Archive Chain result type
pub type Result<T> = std::result::Result<T, ArchiveChainError>;

/// Archive Chain errors
#[derive(Error, Debug)]
pub enum ArchiveChainError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Invalid Merkle proof")]
    InvalidMerkleProof,

    #[error("Invalid validator signatures")]
    InvalidValidatorSignatures,

    #[error("Merkle root mismatch")]
    MerkleRootMismatch,

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("ParityDB error: {0}")]
    ParityDBError(String),

    #[error("Sync error: {0}")]
    SyncError(String),

    #[error("Invalid block: {0}")]
    InvalidBlock(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<parity_db::Error> for ArchiveChainError {
    fn from(err: parity_db::Error) -> Self {
        ArchiveChainError::ParityDBError(err.to_string())
    }
}
