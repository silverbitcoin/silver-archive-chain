//! Merkle Proof Generation for Archive Chain
//!
//! Generates Merkle proofs for transactions stored in the Archive Chain.
//! Proofs include the path from transaction to snapshot root, optimized
//! for size (typically 1-10 KB).
//!
//! Merkle proofs enable light clients to verify transaction inclusion
//! without downloading the entire state tree.

use crate::types::MerkleProof;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Merkle tree node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MerkleNode {
    /// Node hash
    pub hash: [u8; 64],

    /// Node position in tree
    pub position: u32,

    /// Node level (0 = leaf)
    pub level: u32,
}

/// Merkle tree structure
#[derive(Debug, Clone)]
pub struct MerkleTree {
    /// Tree nodes by level
    levels: Vec<Vec<[u8; 64]>>,

    /// Total number of leaves
    leaf_count: usize,
}

impl MerkleTree {
    /// Build a Merkle tree from transaction hashes
    ///
    /// # Arguments
    /// * `tx_hashes` - Vector of transaction hashes (64 bytes each)
    ///
    /// # Returns
    /// A new Merkle tree
    pub fn build(tx_hashes: &[[u8; 64]]) -> Self {
        if tx_hashes.is_empty() {
            return Self {
                levels: vec![vec![]],
                leaf_count: 0,
            };
        }

        let mut levels = vec![tx_hashes.to_vec()];
        let mut current_level = tx_hashes.to_vec();

        // Build tree bottom-up
        while current_level.len() > 1 {
            let mut next_level = Vec::new();

            for i in (0..current_level.len()).step_by(2) {
                let left = current_level[i];
                let right = if i + 1 < current_level.len() {
                    current_level[i + 1]
                } else {
                    left // Duplicate if odd number
                };

                next_level.push(Self::hash_pair(&left, &right));
            }

            levels.push(next_level.clone());
            current_level = next_level;
        }

        Self {
            levels,
            leaf_count: tx_hashes.len(),
        }
    }

    /// Get the root hash of the tree
    pub fn root(&self) -> [u8; 64] {
        if self.levels.is_empty() || self.levels.last().unwrap().is_empty() {
            [0u8; 64]
        } else {
            self.levels.last().unwrap()[0]
        }
    }

    /// Get the depth of the tree
    pub fn depth(&self) -> usize {
        self.levels.len()
    }

    /// Get the number of leaves
    pub fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    /// Generate a Merkle proof for a transaction at the given index
    ///
    /// # Arguments
    /// * `index` - Index of the transaction in the leaf level
    ///
    /// # Returns
    /// A Merkle proof, or None if index is out of bounds
    pub fn generate_proof(&self, index: usize) -> Option<MerkleProof> {
        if index >= self.leaf_count {
            return None;
        }

        let tx_hash = self.levels[0][index];
        let mut path = Vec::new();
        let mut current_index = index;

        // Traverse from leaf to root
        for level in 0..self.levels.len() - 1 {
            let sibling_index = if current_index & 1 == 0 {
                current_index + 1
            } else {
                current_index - 1
            };

            if sibling_index < self.levels[level].len() {
                path.push(self.levels[level][sibling_index]);
            }

            current_index >>= 1;
        }

        let proof = MerkleProof {
            tx_hash,
            path,
            position: index as u32,
            root: self.root(),
        };

        Some(proof)
    }

    /// Generate proofs for multiple transactions
    ///
    /// # Arguments
    /// * `indices` - Indices of transactions to generate proofs for
    ///
    /// # Returns
    /// Vector of Merkle proofs
    pub fn generate_proofs(&self, indices: &[usize]) -> Vec<MerkleProof> {
        indices
            .iter()
            .filter_map(|&idx| self.generate_proof(idx))
            .collect()
    }

    /// Verify a Merkle proof
    pub fn verify_proof(&self, proof: &MerkleProof) -> bool {
        let mut current = proof.tx_hash;
        let mut position = proof.position;

        for &sibling in &proof.path {
            current = if position & 1 == 0 {
                // Left child
                Self::hash_pair(&current, &sibling)
            } else {
                // Right child
                Self::hash_pair(&sibling, &current)
            };
            position >>= 1;
        }

        current == proof.root
    }

    /// Hash two nodes together using Blake3
    fn hash_pair(left: &[u8; 64], right: &[u8; 64]) -> [u8; 64] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(left);
        hasher.update(right);

        let mut output = [0u8; 64];
        hasher.finalize_xof().fill(&mut output);
        output
    }

    /// Get the size of a proof in bytes
    pub fn proof_size(proof: &MerkleProof) -> usize {
        64 + (proof.path.len() * 64) + 4 + 64 // tx_hash + path + position + root
    }

    /// Get average proof size for this tree
    pub fn average_proof_size(&self) -> usize {
        if self.leaf_count == 0 {
            return 0;
        }

        let total_size: usize = (0..self.leaf_count)
            .filter_map(|i| self.generate_proof(i).map(|p| Self::proof_size(&p)))
            .sum();

        total_size / self.leaf_count
    }
}

/// Merkle proof generator for Archive Chain
pub struct ProofGenerator {
    /// Cache of recently generated trees
    tree_cache: HashMap<String, MerkleTree>,

    /// Maximum cache size
    max_cache_size: usize,
}

impl ProofGenerator {
    /// Create a new proof generator
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            tree_cache: HashMap::new(),
            max_cache_size,
        }
    }

    /// Generate a Merkle proof for a transaction
    ///
    /// # Arguments
    /// * `tx_hash` - Hash of the transaction
    /// * `tx_hashes` - All transaction hashes in the batch/snapshot
    ///
    /// # Returns
    /// A Merkle proof, or None if transaction not found
    pub fn generate_proof(
        &mut self,
        tx_hash: &[u8; 64],
        tx_hashes: &[[u8; 64]],
    ) -> Option<MerkleProof> {
        // Find transaction index
        let index = tx_hashes.iter().position(|h| h == tx_hash)?;

        // Build or retrieve tree
        let tree = self.get_or_build_tree(tx_hashes);

        // Generate proof
        tree.generate_proof(index)
    }

    /// Generate proofs for multiple transactions
    pub fn generate_proofs(
        &mut self,
        tx_hashes: &[[u8; 64]],
        all_tx_hashes: &[[u8; 64]],
    ) -> Vec<MerkleProof> {
        let tree = self.get_or_build_tree(all_tx_hashes);

        tx_hashes
            .iter()
            .filter_map(|tx_hash| {
                all_tx_hashes
                    .iter()
                    .position(|h| h == tx_hash)
                    .and_then(|idx| tree.generate_proof(idx))
            })
            .collect()
    }

    /// Generate a batch of proofs for a snapshot
    ///
    /// Optimizes proof generation for large batches by building
    /// the tree once and generating all proofs.
    pub fn generate_batch_proofs(&mut self, tx_hashes: &[[u8; 64]]) -> Vec<MerkleProof> {
        let tree = self.get_or_build_tree(tx_hashes);
        tree.generate_proofs(&(0..tx_hashes.len()).collect::<Vec<_>>())
    }

    /// Get or build a Merkle tree
    fn get_or_build_tree(&mut self, tx_hashes: &[[u8; 64]]) -> &MerkleTree {
        let cache_key = self.compute_cache_key(tx_hashes);

        if !self.tree_cache.contains_key(&cache_key) {
            // Evict oldest entry if cache is full
            if self.tree_cache.len() >= self.max_cache_size {
                if let Some(oldest_key) = self.tree_cache.keys().next().cloned() {
                    self.tree_cache.remove(&oldest_key);
                }
            }

            let tree = MerkleTree::build(tx_hashes);
            self.tree_cache.insert(cache_key.clone(), tree);
        }

        &self.tree_cache[&cache_key]
    }

    /// Compute cache key from transaction hashes
    fn compute_cache_key(&self, tx_hashes: &[[u8; 64]]) -> String {
        // Use first and last hash + count as cache key
        if tx_hashes.is_empty() {
            return "empty".to_string();
        }

        let first = hex::encode(&tx_hashes[0][..8]);
        let last = hex::encode(&tx_hashes[tx_hashes.len() - 1][..8]);
        let count = tx_hashes.len();

        format!("{}_{}_{}", first, last, count)
    }

    /// Clear the cache
    pub fn clear_cache(&mut self) {
        self.tree_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> ProofGeneratorStats {
        ProofGeneratorStats {
            cached_trees: self.tree_cache.len(),
            max_cache_size: self.max_cache_size,
        }
    }
}

/// Statistics for proof generator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofGeneratorStats {
    /// Number of cached trees
    pub cached_trees: usize,

    /// Maximum cache size
    pub max_cache_size: usize,
}

/// Batch proof generator for efficient proof generation
pub struct BatchProofGenerator {
    /// Transaction hashes
    tx_hashes: Vec<[u8; 64]>,

    /// Merkle tree
    tree: MerkleTree,

    /// Proof cache
    proof_cache: HashMap<usize, MerkleProof>,
}

impl BatchProofGenerator {
    /// Create a new batch proof generator
    pub fn new(tx_hashes: Vec<[u8; 64]>) -> Self {
        let tree = MerkleTree::build(&tx_hashes);

        Self {
            tx_hashes,
            tree,
            proof_cache: HashMap::new(),
        }
    }

    /// Get a proof for a transaction
    pub fn get_proof(&mut self, tx_hash: &[u8; 64]) -> Option<MerkleProof> {
        let index = self.tx_hashes.iter().position(|h| h == tx_hash)?;

        // Check cache first
        if let Some(proof) = self.proof_cache.get(&index) {
            return Some(proof.clone());
        }

        // Generate and cache
        let proof = self.tree.generate_proof(index)?;
        self.proof_cache.insert(index, proof.clone());

        Some(proof)
    }

    /// Get proofs for multiple transactions
    pub fn get_proofs(&mut self, tx_hashes: &[[u8; 64]]) -> Vec<MerkleProof> {
        tx_hashes.iter().filter_map(|h| self.get_proof(h)).collect()
    }

    /// Get all proofs
    pub fn get_all_proofs(&mut self) -> Vec<MerkleProof> {
        (0..self.tx_hashes.len())
            .filter_map(|i| {
                if let Some(proof) = self.proof_cache.get(&i) {
                    Some(proof.clone())
                } else {
                    let proof = self.tree.generate_proof(i)?;
                    self.proof_cache.insert(i, proof.clone());
                    Some(proof)
                }
            })
            .collect()
    }

    /// Get the root hash
    pub fn root(&self) -> [u8; 64] {
        self.tree.root()
    }

    /// Get the tree depth
    pub fn depth(&self) -> usize {
        self.tree.depth()
    }

    /// Get statistics
    pub fn stats(&self) -> BatchProofStats {
        BatchProofStats {
            total_transactions: self.tx_hashes.len(),
            cached_proofs: self.proof_cache.len(),
            tree_depth: self.tree.depth(),
            average_proof_size: self.tree.average_proof_size(),
        }
    }
}

/// Statistics for batch proof generator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProofStats {
    /// Total number of transactions
    pub total_transactions: usize,

    /// Number of cached proofs
    pub cached_proofs: usize,

    /// Tree depth
    pub tree_depth: usize,

    /// Average proof size in bytes
    pub average_proof_size: usize,
}
