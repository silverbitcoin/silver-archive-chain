# Silver Archive Chain

Archive Chain maintains the complete historical record of all transactions with Merkle proofs for verification. It operates at 3 TPS (vs 160,000 TPS Main Chain) and stores approximately 47 GB/year.

## Architecture

```
Main Chain (160,000 TPS)
    ↓
Merkle Root (every 480ms)
    ↓
Archive Chain (3 TPS)
    ├─ Store transaction references
    ├─ Store Merkle proofs
    └─ Maintain full history
```

## Query Flow

```
Light Node Query
    ↓
Archive Chain ParityDB
    ├─ Lookup by tx hash
    ├─ Generate Merkle proof
    └─ Return: [transactions] + [proof]
    ↓
Light Node Verification
    ├─ Verify Merkle proof
    ├─ Check validator signatures
    └─ Display results
```

## Features

- **3 TPS Archive Chain**: Separate consensus for historical record
- **Merkle Proofs**: Cryptographic verification without full history
- **ParityDB Storage**: Efficient indexed storage (47 GB/year)
- **Query Interface**: By address, hash, or time range
- **Light Client Support**: Verify transactions with proofs

## Storage

Archive Chain stores:
- Transactions indexed by: hash, sender, timestamp
- Merkle proofs for verification
- Validator signatures (2/3+ stake)
- Blocks with Merkle roots from Main Chain

## Usage

```rust
use silver_archive_chain::ArchiveChain;

// Create Archive Chain
let archive = ArchiveChain::new("data/archive-chain").await?;

// Query by address
let txs = archive.query_by_address("0x...", 100).await?;

// Query by hash
let (tx, proof) = archive.query_by_hash("0x...").await?;

// Verify Merkle proof
let valid = archive.verify_merkle_proof(&tx.hash, &proof, &root);
```

## Performance

- Query latency: 5-50ms
- Proof size: 1-10 KB
- Verification time: 5-10ms
- Storage growth: ~47 GB/year

## No MongoDB

Unlike previous indexer implementations, Archive Chain uses:
- **ParityDB** for storage (not MongoDB)
- **Merkle proofs** for verification (not complex queries)
- **Simple indexes** (hash, sender, timestamp)
- **Cryptographic verification** (not database queries)

This eliminates the need for MongoDB while maintaining full query capability through cryptographic proofs.
