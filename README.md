# LithicDB

A distributed LSM-tree key-value storage engine written in Go.

LithicDB is built from scratch as a deep exploration of how storage engines work — from the skip list in memory to compaction on disk, MVCC transactions, and a distributed sharding layer.

"Lithic" comes from the Greek *lithos*, meaning stone. Writes arrive in layers, then compact over time into deeper, denser structures on disk — like sediment becoming rock.

📖 **[Read the blog series](https://ulixert.github.io/posts/building-lithicdb/)**

## Architecture

```
Client
  │
  ▼
gRPC Router (Phase 4)
  │
  ├──► Node A ──► LithicDB Engine
  ├──► Node B ──► LithicDB Engine
  └──► Node C ──► LithicDB Engine

Each LithicDB Engine:
┌────────────────────────────────────────────┐
│  Transaction Manager (Phase 3)             │
│    ├── Timestamp Oracle                    │
│    ├── Conflict Detection                  │
│    └── Snapshot Management                 │
│                                            │
│  LSM Core (Phases 1-2)                     │
│    ├── Active Memtable (SkipList)          │
│    ├── Immutable Memtables                 │
│    ├── WAL                                 │
│    ├── Block Cache                         │
│    ├── SSTable Manager                     │
│    │     ├── L0 (overlapping)              │
│    │     ├── L1 (sorted runs)              │
│    │     ├── L2 ...                        │
│    │     └── Bloom Filters (per SSTable)   │
│    ├── Compaction Worker                   │
│    └── Manifest                            │
└────────────────────────────────────────────┘
```

## Roadmap

- [x] **Phase 1** — Storage foundation: iterator contract, skip list memtable, WAL, SSTable format with bloom filters, read/write path
- [ ] **Phase 2** — Compaction and persistence: leveled compaction, manifest, block cache, write batches, benchmarking
- [ ] **Phase 3** — MVCC and transactions: versioned keys, snapshot isolation, conflict detection, MVCC-aware compaction
- [ ] **Phase 4** — Distributed layer: gRPC interface, consistent hashing, cluster membership, shard migration
- [ ] **Phase 5** — Performance hardening: block compression, prefix encoding, key-value separation, parallel compaction, rate limiting

## Build

```bash
make              # fmt, vet, lint, test with race detector
make test-race    # tests with race detection (default)
make bench        # run benchmarks
```

## References

- O'Neil et al. (1996). *The Log-Structured Merge-Tree (LSM-Tree)*
- Lu et al. (2016). *WiscKey: Separating Keys from Values in SSD-Conscious Storage*
- Peng & Dabek (2010). *Large-scale Incremental Processing Using Distributed Transactions and Notifications*