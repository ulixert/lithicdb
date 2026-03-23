# LithicDB

A distributed LSM-tree key-value storage engine built from scratch in Go.

Every core component — skip list, WAL, SSTable format, bloom filter, merge iterator, leveled compaction, snapshot isolation, optimistic transactions — is hand-built. The distributed layer — consistent hashing, hybrid logical clocks, SWIM gossip, quorum coordination, hinted handoff, merkle-tree anti-entropy — is equally from scratch. Only gRPC and protobuf use external libraries.

> "Lithic" comes from the Greek *lithos*, meaning stone. Writes arrive in layers, then compact over time into deeper, denser structures on disk — like sediment becoming rock.

## Blog Posts

1. [Building LithicDB: A Distributed LSM Storage Engine from Scratch in Go](https://ulixert.github.io/posts/building-lithicdb/)
2. [The Storage Foundation: Memtable, WAL, and SSTables](https://ulixert.github.io/posts/lithicdb-storage-foundation/)
3. [Sequence Numbers, the Merge Iterator, and Wiring It All Together](https://ulixert.github.io/posts/lithicdb-wiring-it-together/)
4. [Making the Engine Self-Maintaining: Compaction, Caching, and Durability](https://ulixert.github.io/posts/lithicdb-self-maintaining/)
5. [Snapshots, Transactions, and the Art of Not Blocking Writers](https://ulixert.github.io/posts/lithicdb-mvcc-transactions/)

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

    // Basic operations
    _ = d.Put([]byte("hello"), []byte("world"))

    val, found := d.Get([]byte("hello"))
    // val.Data = []byte("world"), found = true

    _ = d.Delete([]byte("hello"))

    // Point-in-time snapshots
    snap := d.GetSnapshot()
    defer snap.Close()
    val, found = snap.Get([]byte("hello")) // reads at snapshot time

    // Optimistic transactions
    tx := d.BeginTransaction()
    v, _ := tx.Get([]byte("counter"))
    tx.Put([]byte("counter"), append(v.Data, []byte("+1")...))
    if err := tx.Commit(); err == db.ErrConflict {
        // another writer modified "counter" — retry
    }

    // Iteration
    iter := d.Scan()
    defer iter.Close()
    for iter.IsValid() {
        // use kv.UserKey(iter.Key()) for the user key
        iter.Next()
    }
}
```

## Architecture

```
                        ┌─────────────────┐
                        │   Client / CLI  │
                        └────────┬────────┘
                                 │  gRPC
                        ┌────────▼────────┐
                        │   Coordinator   │  ◄── any node can coordinate
                        └──┬─────┬──────┬─┘
                           │     │      │      quorum fan-out
                  ┌────────▼┐ ┌──▼───┐ ┌▼────────┐
                  │ Node A  │ │Node B│ │ Node C  │
                  │(replica)│ │      │ │(replica)│
                  └────┬────┘ └──┬───┘ └────┬────┘
                       │         │          │
          SWIM gossip ◄┼─────────┼──────────┼► liveness detection
          HLC clocks  ◄┼─────────┼──────────┼► cross-node ordering
                       │         │          │
             ┌─────────▼─────────▼──────────▼────────────┐
             │          LithicDB Engine (per node)       │
             │                                           │
             │  ┌──────────────────────────────────────┐ │
             │  │  Transaction Manager                 │ │
             │  │    Sequence Oracle · Snapshots       │ │
             │  │    Write-Write Conflict Detection    │ │
             │  └────────────────┬─────────────────────┘ │
             │                   │                       │
             │  ┌────────────────▼─────────────────────┐ │
             │  │  LSM Core                            │ │
             │  │                                      │ │
             │  │  Active Memtable ◄── WAL             │ │
             │  │       │                              │ │
             │  │  Immutable Memtables                 │ │
             │  │       │  (freeze + flush)            │ │
             │  │       ▼                              │ │
             │  │  L0  [SST][SST][SST] (overlapping)   │ │
             │  │       │  (compaction)                │ │
             │  │  L1  [SST    ][SST    ] (sorted)     │ │
             │  │  L2  [SST        ][SST        ]      │ │
             │  │       ...                            │ │
             │  │                                      │ │
             │  │  Block Cache · Bloom Filters         │ │
             │  │  Manifest · Compaction Worker        │ │
             │  └──────────────────────────────────────┘ │
             │                                           │
             │  Hinted Handoff Store · Anti-Entropy Sync │
             └───────────────────────────────────────────┘
```

## How a Write Works

```
Put("user:1234", value)
  │
  ├─► Assign sequence number (monotonic uint64)
  │
  ├─► Append to WAL (CRC32 checksum per record)
  │     └─ fsync for durability
  │
  ├─► Insert into active memtable (skip list)
  │     key = [user_key | inverted_seq]
  │
  ├─► Memtable full?
  │     ├─ yes: freeze → push to immutable queue → signal flush
  │     └─ no: done
  │
  └─► Background flush (async)
        ├─ Build SSTable: sorted blocks + bloom filter + index
        ├─ Write to temp file → fsync → rename → fsync dir
        ├─ Add to L0 in manifest
        └─ L0 count ≥ 4? → trigger compaction
             └─ Leveled compaction: merge into L1, L2, ...
                (10x size ratio per level, MVCC-aware version GC)
```

## How a Read Works

```
Get("user:1234")
  │
  ├─► Active Memtable ──── found? ──► return
  │
  ├─► Immutable Memtables ── found? ──► return (newest first)
  │
  ├─► L0 SSTables (newest first, may overlap)
  │     ├─ Bloom filter: "not here" ──► skip (99% of files)
  │     ├─ Bloom filter: "maybe"
  │     │    ├─ Binary search index ──► find block
  │     │    └─ Binary search block ──► found? return
  │     └─ ...
  │
  ├─► L1, L2, ... (one SSTable per level, non-overlapping)
  │     └─ Binary search to find the single SSTable, then same path
  │
  └─► Not found
```

## Design Decisions

### Internal Key Format

Every key stored in LithicDB is an *internal key*: the user's key bytes followed by an 8-byte *inverted* sequence number (`math.MaxUint64 - seq`, big-endian). Inverting the sequence number means `bytes.Compare` on internal keys gives the right ordering for free: ascending by user key, then descending by sequence number (newest version first). This avoids a custom comparator — the entire read path (skip list, SSTable binary search, merge iterator) uses plain `bytes.Compare`, and MVCC comes from the key encoding, not from extra logic.

The sequence number is assigned at write time and never changes. This means MVCC and snapshot isolation were a logic change on top of the existing format, not a format migration. `GetAt(key, maxSeq)` simply seeks to the user key and scans forward until it finds a version with `seq ≤ maxSeq`.

### Leveled Compaction

LithicDB uses leveled compaction with a 10x size ratio (L1=256MB, L2=2.5GB, L3=25GB, up to 7 levels). Leveled was chosen over tiered (size-tiered) for two reasons:

1. **Bounded space amplification.** Leveled compaction guarantees at most ~10% space overhead beyond the logical data size, because each level has non-overlapping key ranges and at most one live version per key (modulo active snapshots). Tiered compaction can temporarily hold multiple copies of the same key across same-level runs, leading to higher space amplification.

2. **Better read amplification.** In levels L1+, each key exists in at most one SSTable per level, so a point lookup checks at most one SSTable per level (after bloom filter). Tiered compaction may require checking all runs within a level.

The tradeoff is higher write amplification — each byte may be rewritten ~10x as it moves through levels. For the workloads this project targets (moderate write volume, read-heavy), this is the right trade.

Compaction is MVCC-aware: the executor computes a GC watermark from the oldest active snapshot's sequence number and only drops old versions below that watermark. This prevents compaction from deleting versions that an active snapshot still needs to read.

### mmap SSTable Reader

SSTables are memory-mapped via `syscall.Mmap(PROT_READ, MAP_PRIVATE)` rather than read into heap memory with `os.ReadFile`. This matters because the LSM tree holds every SSTable open simultaneously — with default level sizing, that's potentially several GB of raw data. With mmap, the OS manages the page cache: only actively-read pages consume physical memory, and cold pages are evicted under memory pressure. The Go heap stays small.

The bloom filter (~1KB per SSTable) is copied out of the mmap'd region on open. It's checked on every point lookup, so keeping it on the heap avoids a page fault for cold SSTables.

### SSTable Format

Each SSTable is a self-contained file with four regions:

```
[data block 0][crc32]  [data block 1][crc32]  ...
[bloom filter]
[index block]
[footer: 33 bytes]
```

**Data blocks** (~4KB each) contain sorted key-value entries with an offset table for binary search within the block. Each block has a CRC32 checksum verified on read.

**Bloom filter** uses 10 bits per key with ~7 hash probes (LevelDB's rotated-hash design), giving ~1% false positive rate. The filter is built from user keys (not internal keys) so all versions of a key share one filter entry.

**Index block** maps each data block's last key to its file offset, enabling binary search to find the right block in O(log n).

**Footer** (33 bytes, fixed) stores offsets to the bloom filter and index block, a format version byte, a CRC32 of the footer fields, and a magic number (`0x4C544442` — "LTDB").

### Distributed Consistency Model

LithicDB's distributed layer provides **eventual consistency with tunable quorum** (Dynamo-style). The key design choices:

**Leaderless replication.** Any node can coordinate any request. No leader election, no single point of failure. The coordinator fans out to N replicas, waits for W acks (writes) or R responses (reads). With R + W > N, reads observe the most recent quorum-acknowledged write.

**Last-Writer-Wins (LWW) via HLC.** Concurrent writes to the same key are resolved by hybrid logical clock timestamps. HLC combines wall-clock time with a logical counter to provide causal ordering for related events and a deterministic total order for concurrent events. No vector clocks, no conflict resolution callbacks.

**Liveness decoupled from ownership.** SWIM gossip detects which nodes are reachable. The consistent hash ring determines who owns which data. These are independent: when a node dies, it stays in the ring (its keys don't move). The coordinator routes around it, storing hinted handoffs. When it recovers, it resumes ownership with zero ring churn. Ring changes only happen via explicit admin commands (`join`, `activate`, `remove`).

**Three convergence mechanisms.** (1) Async read repair — after returning the newest value, the coordinator fixes stale replicas in the background. (2) Hinted handoff — writes for a downed node are buffered and replayed on recovery. (3) Anti-entropy — periodic merkle tree comparison detects and repairs any remaining drift.

**Explicit non-guarantees.** No distributed snapshots, no distributed transactions, no cross-key causal consistency. These are deliberate scope boundaries, not oversights.

## Benchmarks

Measured on Apple M1, 4KB blocks, 8MB block cache, leveled compaction (v0.5.0, mmap reader).

| Operation | Throughput | ns/op |
|---|---|---|
| Put (sequential) | 263K ops/sec | 3,797 |
| Put (with flush + compaction) | 251K ops/sec | 3,984 |
| Get — memtable hit | 2.4M ops/sec | 418 |
| Get — SSTable hit (mmap) | 1.1M ops/sec | 1,092 |
| Get — SSTable hit (warm cache) | 1.2M ops/sec | 1,008 |
| Get — SSTable miss | 836K ops/sec | 1,461 |
| Scan (10K keys) | 541 scans/sec | 1.8M |
| **MVCC** | | |
| Snapshot Get | 3.2M ops/sec | 331 |
| Snapshot Scan (1K keys) | 10K scans/sec | 98,392 |
| Transaction (5 reads + 5 writes) | 289 txns/sec | 3.5M |

Snapshot Get is fast because `GetAt` uses the same skip list seek as regular Get — the only extra cost is comparing the sequence number. Transaction throughput is WAL-bound: each commit does one `fsync`. The conflict check (scanning active + immutable memtables) is negligible.

📊 **[Full benchmark analysis](BENCHMARKS.md)**

## Features

### Storage Engine
- [x] Skip list memtable (built from scratch, lock-free reads, mutex-guarded writes)
- [x] Write-ahead log with CRC32 checksums and batch-aware format
- [x] SSTable format: ~4KB blocks, per-block CRC32, offset tables for O(log n) lookup
- [x] Bloom filters (10 bits/key, ~1% false positive rate, LevelDB-style rotated hash)
- [x] Internal key encoding (`user_key | inverted_seq`) — `bytes.Compare` gives correct MVCC ordering
- [x] Merge iterator: min-heap fan-out, emits all versions in global sorted order
- [x] mmap'd SSTable reader (`syscall.Mmap`, OS-managed page cache)
- [x] Background flush with atomic writes (write → fsync → rename → fsync dir)
- [x] Crash recovery via WAL replay with sequence number restoration

### Compaction & Persistence
- [x] Leveled compaction (L0 → L1 → ... → L7, 10x size ratio, background worker)
- [x] MVCC-aware version GC (watermark from oldest active snapshot)
- [x] Manifest file (SSTable state, checksummed records, periodic snapshots)
- [x] Reference-counted SSTable handles (safe deletion while iterators are active)
- [x] Block cache (sharded LRU, keyed by SSTable ID + block offset)
- [x] Write batch API (atomic multi-key writes via single WAL entry)
- [x] Write backpressure (block writers when immutable memtable queue is full)

### MVCC & Transactions
- [x] Snapshot isolation (`db.GetSnapshot()` for point-in-time reads)
- [x] Optimistic transactions with write-write conflict detection
- [x] Snapshot iterator (seq filtering + user-key dedup, layered on merge iterator)
- [x] Version-aware point lookups (`GetAt`) through memtables and SSTables

### Distributed Layer
- [ ] Standalone gRPC server wrapping `db.DB` (Put, Get, Delete, Scan)
- [ ] Consistent hash ring with virtual nodes (150 vnodes/node, SHA-256)
- [ ] Hybrid logical clocks for cross-node timestamp ordering
- [ ] SWIM gossip protocol for decentralized failure detection
- [ ] Quorum coordinator with tunable R/W consistency (R + W > N)
- [ ] Async read repair on quorum reads
- [ ] Hinted handoff for temporary node failures
- [ ] Merkle-tree anti-entropy with tombstone GC
- [ ] Two-phase node join (JOINING → ACTIVE) with anti-entropy bootstrap
- [ ] Admin CLI for explicit topology management (join, activate, remove)
- [ ] Integration and chaos tests

## Project Structure

```
lithicdb/
  db/              engine: Put, Get, Delete, Scan, flush, recovery, compaction,
                   snapshots, transactions, MVCC-aware version GC
  compaction/      picker (L0 trigger + level size ratio), executor, level state
  iterator/        Iterator interface, MergeIterator, SnapshotIterator, WriteBufferIterator
  kv/              Value type, internal key encoding (user_key | inverted_seq)
  manifest/        LSM state persistence: SSTable tracking, ID counters, checksummed records
  memtable/        skip list (from scratch), thread-safe wrapper, GetAt/GetNewest
  sstable/         block encoding, bloom filter, SSTable builder/reader, mmap, block cache
  wal/             write-ahead log: CRC32 framing, batch encoding, crash recovery
```

## Build

```bash
make              # fmt, vet, lint, test with race detector
make test-race    # tests with race detection (default target)
make bench        # run benchmarks
make test-v       # verbose test output
```

## References

- O'Neil, P., Cheng, E., Gawlick, D., & O'Neil, E. (1996). *The Log-Structured Merge-Tree (LSM-Tree)*. Acta Informatica, 33(4), 351–385.
- DeCandia, G., Hastorun, D., Jampani, M., et al. (2007). *[Dynamo: Amazon's Highly Available Key-Value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)*. SOSP '07.
- Das, A., Gupta, I., & Motivala, A. (2002). *[SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)*. DSN '02.
- Kulkarni, S., Demirbas, M., et al. (2014). *[Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases](https://cse.buffalo.edu/tech-reports/2014-04.pdf)*. OPODIS '14.
- Luo, C., & Carey, M. J. (2020). *[LSM-based Storage Techniques: A Survey](https://arxiv.org/abs/1812.07527)*. VLDB Journal, 29(1).
- Lu, L., Pillai, T. S., et al. (2016). *[WiscKey: Separating Keys from Values in SSD-Conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)*. FAST '16.
- Dayan, N., & Idreos, S. (2018). *[Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores](https://www.cs.bu.edu/faculty/mathan/publications/sigmod18-dostoevsky.pdf)*. SIGMOD '18.
- Dayan, N., Athanassoulis, M., & Idreos, S. (2017). *[Monkey: Optimal Navigable Key-Value Store](https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf)*. SIGMOD '17.
- Petrov, A. (2019). *Database Internals: A Deep Dive into How Distributed Data Systems Work*. O'Reilly.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapters 3 (Storage), 5 (Replication), and 7 (Transactions).
- [The Apache Cassandra Architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/) — Dynamo-inspired distributed architecture, gossip protocol, consistent hashing, hinted handoff, anti-entropy repair
- [Scylla's Compaction Strategies](https://opensource.docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html) — Practical comparison of size-tiered vs leveled vs incremental compaction in production
- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) — Production LSM tuning: block cache sizing, bloom filter configuration, compaction triggers
- [LevelDB](https://github.com/google/leveldb) — Google's original LSM key-value store; LithicDB's bloom filter uses LevelDB's rotated-hash probe design (C++)
- [Pebble](https://github.com/cockroachdb/pebble) — CockroachDB's LSM storage engine; internal key encoding inspiration (Go)
- [Badger](https://github.com/dgraph-io/badger) — Dgraph's key-value store with WiscKey-style separation (Go)
- [mini-lsm](https://github.com/skyzh/mini-lsm) — LSM-tree course with week-by-week implementation (Rust)

## License

MIT
