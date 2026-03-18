# LithicDB

A distributed LSM-tree key-value storage engine built from scratch in Go.

LithicDB is a from-scratch implementation of a log-structured merge-tree storage engine. Every core component — skip list memtable, write-ahead log, SSTable with bloom filters, merge iterator, leveled compaction, MVCC transactions — is built by hand, not imported. The engine is designed to scale from a single-node embedded store to a distributed, sharded cluster over gRPC.

> "Lithic" comes from the Greek *lithos*, meaning stone. Writes arrive in layers, then compact over time into deeper, denser structures on disk — like sediment becoming rock.

## Blog Posts

1. [Building LithicDB: A Distributed LSM Storage Engine from Scratch in Go](https://ulixert.github.io/posts/building-lithicdb/)
2. [The Storage Foundation: Memtable, WAL, and SSTables](https://ulixert.github.io/posts/lithicdb-storage-foundation/)
3. [Sequence Numbers, the Merge Iterator, and Wiring It All Together](https://ulixert.github.io/posts/lithicdb-wiring-it-together/)

## Getting Started


```go
package main

import "github.com/ulixert/lithicdb/db"

func main() {
    d, err := db.Open(db.DefaultOptions("./data"))
    if err != nil {
        panic(err)
    }
    defer d.Close()

    d.Put([]byte("hello"), []byte("world"))

    val, found := d.Get([]byte("hello"))
    // val.Data = []byte("world"), found = true

    d.Delete([]byte("hello"))

    iter := d.Scan()
    defer iter.Close()
    for iter.IsValid() {
        // use kv.UserKey(iter.Key()) for the user key
        iter.Next()
    }
}
```

## Design

LithicDB's design draws from several sources: the original LSM-tree paper by O'Neil et al., the WiscKey approach to key-value separation, and the internal key encoding strategy used by Pebble and Badger. The goals are:

- **Build every core component from scratch.** The skip list, WAL, SSTable format, bloom filter, merge iterator, and compaction logic are all implemented in this repository.
- **Encode for the future.** Every key carries a sequence number from day one. MVCC and snapshot isolation are a logic change, not a format rewrite.
- **Distribute eventually.** MVCC at the storage layer makes distributed single-shard transactions nearly free.

```
                          ┌─────────────────┐
                          │   Client / CLI  │
                          └────────┬────────┘
                                   │
                          ┌────────▼────────┐
                          │   gRPC Router   │
                          └──┬─────┬──────┬─┘
                             │     │      │
                    ┌────────▼┐ ┌──▼───┐ ┌▼────────┐
                    │ Node A  │ │Node B│ │ Node C  │
                    └────┬────┘ └──┬───┘ └────┬────┘
                         │         │          │
               ┌─────────▼─────────▼──────────▼───────────┐
               │          LithicDB Engine                 │
               │                                          │
               │  ┌───────────────────────────────────┐   │
               │  │  Transaction Manager              │   │
               │  │    Sequence Oracle · Snapshots    │   │
               │  │    Write-Write Conflict Detection │   │
               │  └───────────────┬───────────────────┘   │
               │                  │                       │
               │  ┌───────────────▼────────────────────┐  │
               │  │  LSM Core                          │  │
               │  │                                    │  │
               │  │  Active Memtable ◄── WAL           │  │
               │  │       │                            │  │
               │  │  Immutable Memtables               │  │
               │  │       │  (freeze + flush)          │  │
               │  │       ▼                            │  │
               │  │  L0  [SST][SST][SST] (overlapping) │  │
               │  │       │  (compaction)              │  │
               │  │  L1  [SST    ][SST    ] (sorted)   │  │
               │  │  L2  [SST        ][SST        ]    │  │
               │  │       ...                          │  │
               │  │                                    │  │
               │  │  Block Cache · Bloom Filters       │  │
               │  │  Manifest · Compaction Worker      │  │
               │  └────────────────────────────────────┘  │
               └──────────────────────────────────────────┘
```

## How a Read Works

```
Get("user:1234")
  │
  ├─► Active Memtable ──── found? ──► return
  │
  ├─► Immutable Memtables ── found? ──► return (newest first)
  │
  ├─► L0 SSTables (newest first)
  │     ├─ Bloom filter: "not here" ──► skip (99% of files)
  │     ├─ Bloom filter: "maybe"
  │     │    ├─ Binary search index ──► find block
  │     │    └─ Binary search block ──► found? return
  │     └─ ...
  │
  ├─► L1, L2, ... (one SSTable per level, non-overlapping)
  │
  └─► Not found
```

## Benchmarks

Measured on Apple M1, 64MB memtable, 4KB block size, 100-byte values.

| Operation | Throughput | Allocs/op |
|---|---|---|
| Put (sequential) | 266 K ops/sec | 5 |
| Put (random) | 262 K ops/sec | 6 |
| Get (hit) | 2.9 M ops/sec | 1 |
| Get (miss) | 4.6 M ops/sec | 2 |
| Scan (10K keys) | 900 scans/sec | 30,005 |

Get misses are faster than hits because the bloom filter rejects absent keys without reading any data blocks. Scan throughput will improve significantly once the block cache is implemented.

## Features

### Storage Engine
- [x] Skip list memtable (built from scratch, thread-safe)
- [x] Write-ahead log with CRC32 checksums and batch-aware format
- [x] SSTable format: ~4KB blocks, per-block checksums, offset tables for binary search
- [x] Bloom filters (10 bits/key, ~1% false positive rate, LevelDB-style)
- [x] Internal key encoding (`user_key + inverted_seq`) for MVCC-ready ordering via plain `bytes.Compare`
- [x] Merge iterator: fan-out min-heap, user-key deduplication across and within sources
- [x] Background flush with atomic SSTable writes (write → fsync → rename → fsync dir)
- [x] Crash recovery via WAL replay with sequence number restoration
- [x] Tombstone support with explicit typed flags

### Compaction & Persistence
- [x] Manifest file (source of truth for SSTable state, checksummed, periodic snapshots)
- [x] Leveled compaction (L0 → L1 → L2, 10x size ratio, background worker)
- [x] Reference-counted SSTables (safe deletion while iterators are active)
- [ ] Block cache (sharded LRU for decoded blocks)
- [ ] Write batch API (atomic multi-key writes)
- [ ] Write backpressure (block when flush can't keep up)

### MVCC & Transactions
- [ ] Snapshot isolation (`db.GetSnapshot()` for point-in-time reads)
- [ ] Optimistic transactions with write-write conflict detection
- [ ] MVCC-aware compaction (respect active snapshots during GC)

### Distributed Layer
- [ ] gRPC service (Put, Get, Delete, Scan, BatchWrite)
- [ ] Consistent hashing ring with virtual nodes
- [ ] Cluster membership and failure detection
- [ ] Shard migration on topology changes
- [ ] Replication (async, stretch: Raft per shard)

### Performance
- [ ] Block compression (Snappy / LZ4)
- [ ] Prefix encoding with restart points
- [ ] Key-value separation (WiscKey: large values in vLog)
- [ ] Parallel compaction
- [ ] I/O rate limiter for compaction
- [ ] Arena-based skip list allocation

## Project Structure

```
lithicdb/
  db/              engine: Put, Get, Delete, Scan, flush, recovery
  iterator/        Iterator interface, MergeIterator
  kv/              Value type, internal key encoding (user_key + seq)
  manifest/        LSM state persistence: SSTable tracking, ID counters
  memtable/        skip list (from scratch), thread-safe wrapper
  sstable/         block encoding, bloom filter, SSTable builder/reader
  wal/             write-ahead log: encoding, writing, crash recovery
```

## Build

```bash
make              # fmt, vet, lint, test with race detector
make test-race    # tests with race detection (default target)
make bench        # run benchmarks
make test-v       # verbose test output
```
### Reference
- O'Neil, P., Cheng, E., Gawlick, D., & O'Neil, E. (1996). *The Log-Structured Merge-Tree (LSM-Tree)*. Acta Informatica, 33(4), 351–385.
- Lu, L., Pillai, T. S., et al. (2016). *[WiscKey: Separating Keys from Values in SSD-Conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)*. FAST '16.
- Peng, D., & Dabek, F. (2010). *[Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://research.google/pubs/large-scale-incremental-processing-using-distributed-transactions-and-notifications/)*. OSDI '10.
- Dayan, N., & Idreos, S. (2018). *[Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores](https://www.cs.bu.edu/faculty/mathan/publications/sigmod18-dostoevsky.pdf)*. SIGMOD '18.
- Dayan, N., Athanassoulis, M., & Idreos, S. (2017). *[Monkey: Optimal Navigable Key-Value Store](https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf)*. SIGMOD '17.
- Luo, C., & Carey, M. J. (2020). *[LSM-based Storage Techniques: A Survey](https://arxiv.org/abs/1812.07527)*. VLDB Journal, 29(1).
- Petrov, A. (2019). *Database Internals: A Deep Dive into How Distributed Data Systems Work*. O'Reilly.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapters 3 (Storage) and 7 (Transactions).
- [LevelDB](https://github.com/google/leveldb) — Google's original LSM key-value store; LithicDB's bloom filter uses LevelDB's hash and rotated-probe design (C++)
- [Pebble](https://github.com/cockroachdb/pebble) — CockroachDB's LSM storage engine (Go)
- [Badger](https://github.com/dgraph-io/badger) — Dgraph's key-value store with WiscKey separation (Go)
- [mini-lsm](https://github.com/skyzh/mini-lsm) — LSM-tree course with week-by-week implementation (Rust)

## License

MIT