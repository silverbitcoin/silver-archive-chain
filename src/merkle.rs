//! Merkle proof generation and verification

use crate::types::MerkleProof;

/// Generate Merkle proof for transaction
pub fn generate_proof(
    tx_hash: &[u8; 64],
    path: Vec<[u8; 64]>,
    position: u32,
    root: &[u8; 64],
) -> MerkleProof {
    MerkleProof {
        tx_hash: *tx_hash,
        path,
        position,
        root: *root,
    }
}

/// Verify Merkle proof
pub fn verify_proof(tx_hash: &[u8; 64], proof: &MerkleProof, root: &[u8; 64]) -> bool {
    // Reconstruct root from proof
    let mut current = *tx_hash;
    let mut position = proof.position;

    for &sibling in &proof.path {
        current = if position & 1 == 0 {
            // Left child
            hash_pair(&current, &sibling)
        } else {
            // Right child
            hash_pair(&sibling, &current)
        };
        position >>= 1;
    }

    // Compare with provided root
    current == *root
}

/// Hash two nodes together
fn hash_pair(left: &[u8; 64], right: &[u8; 64]) -> [u8; 64] {
    let mut combined = [0u8; 128];
    combined[..64].copy_from_slice(left);
    combined[64..].copy_from_slice(right);

    let hash = blake3::hash(&combined);
    let mut result = [0u8; 64];
    result.copy_from_slice(&hash.as_bytes()[..64]);
    result
}

/// Build Merkle tree from transactions
pub fn build_merkle_tree(tx_hashes: &[[u8; 64]]) -> ([u8; 64], Vec<Vec<[u8; 64]>>) {
    if tx_hashes.is_empty() {
        return ([0u8; 64], vec![]);
    }

    let mut current_level: Vec<[u8; 64]> = tx_hashes.to_vec();
    let mut tree = vec![current_level.clone()];

    while current_level.len() > 1 {
        let mut next_level = Vec::new();

        for i in (0..current_level.len()).step_by(2) {
            let left = current_level[i];
            let right = if i + 1 < current_level.len() {
                current_level[i + 1]
            } else {
                left // Duplicate if odd number
            };

            next_level.push(hash_pair(&left, &right));
        }

        tree.push(next_level.clone());
        current_level = next_level;
    }

    (current_level[0], tree)
}

/// Get Merkle proof for transaction at index
pub fn get_proof_for_index(tree: &[Vec<[u8; 64]>], index: usize) -> Vec<[u8; 64]> {
    let mut proof = Vec::new();
    let mut current_index = index;

    for level in tree.iter().take(tree.len() - 1) {
        let sibling_index = if current_index & 1 == 0 {
            current_index + 1
        } else {
            current_index - 1
        };

        if sibling_index < level.len() {
            proof.push(level[sibling_index]);
        }

        current_index >>= 1;
    }

    proof
}

/// Compute Merkle root from transaction hashes
pub fn compute_root(tx_hashes: &[[u8; 64]]) -> [u8; 64] {
    if tx_hashes.is_empty() {
        return [0u8; 64];
    }

    let (root, _) = build_merkle_tree(tx_hashes);
    root
}

/// Verify multiple proofs efficiently
pub fn verify_proofs(
    proofs: &[MerkleProof],
    root: &[u8; 64],
) -> bool {
    proofs.iter().all(|proof| verify_proof(&proof.tx_hash, proof, root))
}

/// Get proof size in bytes
pub fn proof_size(proof: &MerkleProof) -> usize {
    64 + (proof.path.len() * 64) + 4 + 64 // tx_hash + path + position + root
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_proof_verification() {
        let tx_hashes = vec![
            [1u8; 64],
            [2u8; 64],
            [3u8; 64],
            [4u8; 64],
        ];

        let (root, tree) = build_merkle_tree(&tx_hashes);
        let proof = get_proof_for_index(&tree, 0);

        assert!(verify_proof(&tx_hashes[0], &MerkleProof {
            tx_hash: tx_hashes[0],
            path: proof,
            position: 0,
            root,
        }, &root));
    }

    #[test]
    fn test_merkle_root_computation() {
        let tx_hashes = vec![
            [1u8; 64],
            [2u8; 64],
            [3u8; 64],
            [4u8; 64],
        ];

        let root = compute_root(&tx_hashes);
        assert_ne!(root, [0u8; 64]);
    }

    #[test]
    fn test_empty_merkle_tree() {
        let tx_hashes: Vec<[u8; 64]> = vec![];
        let root = compute_root(&tx_hashes);
        assert_eq!(root, [0u8; 64]);
    }

    #[test]
    fn test_single_transaction_merkle_tree() {
        let tx_hashes = vec![[1u8; 64]];
        let (root, _) = build_merkle_tree(&tx_hashes);
        assert_eq!(root, [1u8; 64]);
    }

    #[test]
    fn test_proof_size() {
        let proof = MerkleProof {
            tx_hash: [1u8; 64],
            path: vec![[2u8; 64], [3u8; 64]],
            position: 0,
            root: [4u8; 64],
        };

        let size = proof_size(&proof);
        assert!(size > 0);
    }
}
