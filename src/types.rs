//! Archive Chain types

use serde::{Deserialize, Serialize};

/// Archive transaction stored in Archive Chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveTransaction {
    /// Transaction hash (Blake3-512) - stored as hex string for serialization
    #[serde(
        serialize_with = "serialize_hash",
        deserialize_with = "deserialize_hash"
    )]
    pub hash: [u8; 64],
    /// Sender address
    pub sender: String,
    /// Recipient address (if applicable)
    pub recipient: Option<String>,
    /// Transaction amount
    pub amount: u64,
    /// Transaction timestamp
    pub timestamp: u64,
    /// Transaction type
    pub tx_type: String,
    /// Transaction data
    pub data: Vec<u8>,
}

/// Archive block containing Merkle root from Main Chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveBlock {
    /// Block number (from Main Chain snapshot)
    pub block_number: u64,
    /// Merkle root from Main Chain - stored as hex string for serialization
    #[serde(
        serialize_with = "serialize_hash",
        deserialize_with = "deserialize_hash"
    )]
    pub merkle_root: [u8; 64],
    /// Validator signatures (2/3+ stake)
    pub validator_signatures: Vec<Vec<u8>>,
    /// Block timestamp
    pub timestamp: u64,
    /// Transactions in this block
    pub transactions: Vec<ArchiveTransaction>,
}

/// Merkle proof for transaction verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleProof {
    /// Transaction hash - stored as hex string for serialization
    #[serde(
        serialize_with = "serialize_hash",
        deserialize_with = "deserialize_hash"
    )]
    pub tx_hash: [u8; 64],
    /// Merkle path (hashes from leaf to root) - stored as hex strings
    #[serde(
        serialize_with = "serialize_hash_vec",
        deserialize_with = "deserialize_hash_vec"
    )]
    pub path: Vec<[u8; 64]>,
    /// Position in tree (for reconstruction)
    pub position: u32,
    /// Root hash - stored as hex string for serialization
    #[serde(
        serialize_with = "serialize_hash",
        deserialize_with = "deserialize_hash"
    )]
    pub root: [u8; 64],
}

/// Serialize 64-byte hash as hex string
fn serialize_hash<S>(hash: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&hex::encode(hash))
}

/// Deserialize 64-byte hash from hex string
fn deserialize_hash<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    if bytes.len() != 64 {
        return Err(serde::de::Error::custom("Invalid hash length"));
    }
    let mut hash = [0u8; 64];
    hash.copy_from_slice(&bytes);
    Ok(hash)
}

/// Serialize vector of 64-byte hashes as hex strings
fn serialize_hash_vec<S>(hashes: &[[u8; 64]], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let hex_strings: Vec<String> = hashes.iter().map(|h| hex::encode(h)).collect();
    hex_strings.serialize(serializer)
}

/// Deserialize vector of 64-byte hashes from hex strings
fn deserialize_hash_vec<'de, D>(deserializer: D) -> Result<Vec<[u8; 64]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let hex_strings = Vec::<String>::deserialize(deserializer)?;
    let mut hashes = Vec::new();
    for s in hex_strings {
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        if bytes.len() != 64 {
            return Err(serde::de::Error::custom("Invalid hash length"));
        }
        let mut hash = [0u8; 64];
        hash.copy_from_slice(&bytes);
        hashes.push(hash);
    }
    Ok(hashes)
}

/// Query result with proof
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Transaction data
    pub transaction: ArchiveTransaction,
    /// Merkle proof for verification
    pub proof: MerkleProof,
    /// Validator signatures
    pub validator_signatures: Vec<Vec<u8>>,
}

/// Archive Chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveChainConfig {
    /// Database path
    pub db_path: String,
    /// Target TPS (typically 3)
    pub target_tps: u32,
    /// Snapshot interval in milliseconds
    pub snapshot_interval_ms: u64,
    /// Maximum transactions per block
    pub max_transactions_per_block: usize,
    /// Retention period in days (0 = unlimited)
    pub retention_days: u32,
}

impl Default for ArchiveChainConfig {
    fn default() -> Self {
        Self {
            db_path: "data/archive-chain".to_string(),
            target_tps: 3,
            snapshot_interval_ms: 480,
            max_transactions_per_block: 1440, // 3 TPS * 480ms = ~1440 tx per block
            retention_days: 0,                // Unlimited retention
        }
    }
}
