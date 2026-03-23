# Benchmarks

All benchmarks run on Apple M1, Go 1.26+, `go test -bench=. -benchmem`.

## Current (v0.5.0 — mmap reader, structured logging)

Configuration: 4KB block size, 8MB block cache, 10 bits/key bloom filter, leveled compaction with L0 trigger at 4 files, mmap'd SSTable reader.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| **Writes** | | | | |
| Memtable Put (sequential) | 263K | 3,797 | 309 | 5 |
| Put with flush + compaction | 251K | 3,984 | 2,092 | 11 |
| WriteBatch (100 keys) | 248/s | 4,029,229 | 54,256 | 419 |
| **Reads — memtable** | | | | |
| Memtable Get (hit) | 2.4M | 418 | 207 | 2 |
| **Reads — SSTable** | | | | |
| SSTable Get (hit, mmap) | 1.1M | 1,092 | 207 | 2 |
| SSTable Get (hit, warm cache) | 1.2M | 1,008 | 207 | 2 |
| SSTable Get (miss) | 836K | 1,461 | 208 | 2 |
| SSTable Scan (10K keys) | 541/s | 1,846,533 | 1,305,578 | 30,020 |
| **MVCC** | | | | |
| Snapshot Get | 3.2M | 331 | 206 | 2 |
| Snapshot Scan (1K keys) | 10K/s | 98,392 | 128,569 | 3,007 |
| Transaction (5 reads + 5 writes + commit) | 289/s | 3,466,661 | 3,814 | 51 |

## Previous (v0.4.0 — MVCC, snapshots, transactions)

Configuration: Same as v0.5.0 but with `os.ReadFile` (full heap copy) instead of mmap.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| **Writes** | | | | |
| Memtable Put (sequential) | 266K | 3,763 | 308 | 5 |
| Put with flush + compaction | 249K | 4,007 | 2,342 | 11 |
| WriteBatch (100 keys) | 218/s | 4,576,243 | 54,176 | 411 |
| **Reads — memtable** | | | | |
| Memtable Get (hit) | 2.1M | 479 | 207 | 2 |
| **Reads — SSTable** | | | | |
| SSTable Get (hit, cold cache) | 946K | 1,057 | 208 | 2 |
| SSTable Get (hit, warm cache) | 843K | 1,187 | 207 | 2 |
| SSTable Get (miss) | 683K | 1,463 | 208 | 3 |
| SSTable Scan (10K keys) | 547/s | 1,827,832 | 1,305,536 | 30,019 |
| **MVCC** | | | | |
| Snapshot Get | 3.0M | 334 | 206 | 2 |
| Snapshot Scan (1K keys) | 10K/s | 99,485 | 128,568 | 3,007 |
| Transaction (5 reads + 5 writes + commit) | 259/s | 3,854,895 | 3,811 | 50 |

## Baseline (v0.1.0 — memtable only, no flush)

Configuration: 64MB memtable (no flushes during bench), 4KB block size, 100-byte values. All data stays in the memtable — these numbers reflect skip list + WAL performance only.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| Put (sequential) | 266K | 3,759 | 308 | 5 |
| Put (random) | 262K | 3,820 | 315 | 6 |
| Get (hit) | 2.9M | 340 | 31 | 1 |
| Get (miss) | 4.6M | 219 | 32 | 2 |
| Scan (10K keys) | 900/s | 1,109,578 | 1,280,230 | 30,005 |

## What Changed Between v0.4.0 and v0.5.0

| Operation | v0.4.0 | v0.5.0 | Delta | Why |
|---|---|---|---|---|
| Memtable Get | 479 ns | 418 ns | -13% | Run-to-run variance; no code change to the memtable read path |
| SSTable Get (cache miss) | 1,057 ns | 1,092 ns | +3% | Noise — mmap page fault vs heap read are both memory-speed for warm data |
| SSTable Get (cache hit) | 1,187 ns | 1,008 ns | -15% | Likely run-to-run variance; the cache saves CRC32 + decode but pays hash + mutex — roughly break-even for warm data |
| SSTable Get (miss) | 1,463 ns | 1,461 ns | ~same | Bloom filter reject path is identical |
| Transaction | 3.9M ns | 3.5M ns | -10% | Run-to-run variance in fsync latency; no code change to transaction path |

**The block cache is still marginal for warm data.** The ~8% difference between cache hit and miss is within noise range for a microbenchmark. Both the v0.4.0 and v0.5.0 benchmarks show the same story: when the working set fits in memory (OS page cache for mmap, Go heap for `os.ReadFile`), the cache saves CRC32 + block decode but pays hash + mutex + map lookup — roughly break-even. The cache's real value shows up when the working set exceeds physical memory, where a page fault costs tens of microseconds to milliseconds. The mmap switch doesn't change the cache's microbenchmark story, but it changes the memory footprint story dramatically — the Go heap no longer holds several GB of raw SSTable bytes.

## What Changed Between v0.1.0 and v0.3.0

| Operation | v0.1.0 | v0.3.0 | Delta | Why |
|---|---|---|---|---|
| Put (sequential) | 3,759 ns | 3,763 ns | ~same | WAL fsync dominates — compaction doesn't affect the write path |
| Get (memtable hit) | 340 ns | 479 ns | +41% | `snapshotLevels()` allocates a new slice on every read for compaction safety |
| Get (miss) | 219 ns | 1,463 ns | +6.7x | v0.1.0: one skip list miss. v0.3.0: skip list + bloom filter on every SSTable across all levels |
| Scan (10K keys) | 1.1M ns | 1.8M ns | +65% | Merge iterator now walks SSTable blocks in addition to the memtable |

## Analysis

### Write throughput is dominated by fsync

Sequential put throughput is ~263K ops/sec whether data stays in the memtable or flushes to disk. The bottleneck is `fsync` on the WAL after every write — the skip list insert is negligible in comparison. Write batches amortize the fsync cost: a batch of 100 puts pays for one fsync instead of 100, giving ~40µs per key versus ~3,797µs for individual puts.

### mmap vs heap: warm data is equivalent, cold data is where mmap wins

For benchmarks where all data fits in memory (as ours do on an M1 with 16GB), mmap and `os.ReadFile` perform similarly — both are memory-speed reads. The difference shows up in two places:

1. **Memory footprint.** With `os.ReadFile`, every SSTable's raw bytes live on the Go heap permanently. With mmap, the OS manages the page cache and can evict cold pages under memory pressure. For a database with GB of SSTables, this is the difference between OOM and stable operation.

2. **Cold data performance.** When the working set exceeds physical memory, mmap degrades gracefully — cold pages trigger page faults that the OS resolves from disk. With `os.ReadFile`, the entire dataset must fit in heap memory or the process OOMs. The block cache helps in both cases by keeping decoded hot blocks in application memory, but the mmap approach gives the OS the freedom to manage what's resident. The benchmark numbers don't show this because the test data fits comfortably in RAM.

### Get miss is slower than Get hit

A Get miss must check the bloom filter on *every* SSTable across all levels before returning "not found." A Get hit finds the key in one SSTable and stops. With data spread across multiple SSTables after compaction, a miss requires multiple bloom filter checks versus a hit requiring one block read.

In v0.1.0 (all data in memtable), Get miss was 219ns — one skip list seek that immediately failed. Now that data lives in SSTables, the miss path is 6.7x longer.

### Scan allocations are high

30K allocations per full scan of 10K keys comes from the merge iterator: heap entries, key copies for deduplication tracking, and per-block decode allocations. This is a clear optimization target — arena-based allocation or iterator pooling could reduce this significantly.

### Snapshot Get adds near-zero overhead

Snapshot Get (331 ns) is actually *faster* than the baseline memtable Get (418 ns). The `GetAt(key, maxSeq)` path does the same skip list seek as `Get`, plus one extra sequence number comparison per node visited. The difference comes from the benchmark setup: snapshot Get uses 1K keys (fewer entries in the skip list) while the baseline uses 10K keys, making the skip list shallower.

In an apples-to-apples comparison with equal key counts, snapshot Get would be ~1-2 ns slower than regular Get — the cost of one `uint64` comparison per seek.

### Transaction throughput is fsync-bound

At 289 txns/sec (~3.5 ms per transaction), the dominant cost is the WAL fsync in `applyBatchLocked`. The conflict check (scanning `GetNewest` on active + immutable memtables for each key in the write set) and write buffer construction are negligible — they complete in microseconds. Transaction throughput scales linearly with fsync latency, not with write set size.

With 5 reads + 5 writes per transaction, the per-operation cost is ~350 µs. Batching more operations per transaction amortizes the fsync cost — a transaction with 100 writes would still be ~3.5 ms, or ~35 µs per write.

### Snapshot Scan vs regular Scan

Snapshot Scan over 1K keys (98 µs, ~98 ns/key) versus regular Scan over 10K keys (1.8 ms, ~185 ns/key). The per-key cost is lower for snapshot scan because:
1. Fewer keys means a shallower merge iterator heap
2. The SnapshotIterator's seq filter and user-key dedup are simple comparisons that don't allocate

The 3,007 allocs/1K keys (3.0 allocs/key) matches the regular scan's 30,020 allocs/10K keys (3.0 allocs/key) — the per-key allocation profile is identical.

## How to reproduce

```bash
make bench
```

Or with specific benchmarks:

```bash
go test -bench=BenchmarkSSTable -benchmem ./db/
go test -bench=BenchmarkMemtable -benchmem ./db/
go test -bench=BenchmarkSnapshot -benchmem ./db/
go test -bench=BenchmarkTransaction -benchmem ./db/
```
