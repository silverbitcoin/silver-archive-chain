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
    // Blake3 produces 32 bytes, copy to first 32 bytes of result
    result[..32].copy_from_slice(hash.as_bytes());
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
pub fn verify_proofs(proofs: &[MerkleProof], root: &[u8; 64]) -> bool {
    proofs
        .iter()
        .all(|proof| verify_proof(&proof.tx_hash, proof, root))
}

/// Get proof size in bytes
pub fn proof_size(proof: &MerkleProof) -> usize {
    64 + (proof.path.len() * 64) + 4 + 64 // tx_hash + path + position + root
}
