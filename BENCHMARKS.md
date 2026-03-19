# Benchmarks

All benchmarks run on Apple M1, Go 1.22+, `go test -bench=. -benchmem`.

## Current (v0.2.0 — compaction, block cache, manifest)

Configuration: 4KB block size, 8MB block cache, 10 bits/key bloom filter, leveled compaction with L0 trigger at 4 files.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| **Writes** | | | | |
| Memtable Put (sequential) | 266K | 3,763 | 308 | 5 |
| Put with flush + compaction | 249K | 4,007 | 2,342 | 11 |
| **Reads — memtable** | | | | |
| Memtable Get (hit) | 2.1M | 479 | 207 | 2 |
| **Reads — SSTable** | | | | |
| SSTable Get (hit, cold cache) | 946K | 1,057 | 208 | 2 |
| SSTable Get (hit, warm cache) | 843K | 1,187 | 207 | 2 |
| SSTable Get (miss) | 683K | 1,463 | 208 | 3 |
| SSTable Scan (10K keys) | 547/s | 1,827,832 | 1,305,536 | 30,019 |

## Baseline (v0.1.0 — memtable only, no flush)

Configuration: 64MB memtable (no flushes during bench), 4KB block size, 100-byte values. All data stays in the memtable — these numbers reflect skip list + WAL performance only.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| Put (sequential) | 266K | 3,759 | 308 | 5 |
| Put (random) | 262K | 3,820 | 315 | 6 |
| Get (hit) | 2.9M | 340 | 31 | 1 |
| Get (miss) | 4.6M | 219 | 32 | 2 |
| Scan (10K keys) | 900/s | 1,109,578 | 1,280,230 | 30,005 |

## What Changed Between v0.1.0 and v0.2.0

| Operation | v0.1.0 | v0.2.0 | Delta | Why |
|---|---|---|---|---|
| Put (sequential) | 3,759 ns | 3,763 ns | ~same | WAL fsync dominates — compaction doesn't affect the write path |
| Get (memtable hit) | 340 ns | 479 ns | +41% | `snapshotLevels()` allocates a new slice on every read for compaction safety |
| Get (miss) | 219 ns | 1,463 ns | +6.7x | v0.1.0: one skip list miss. v0.2.0: skip list + bloom filter on every SSTable across all levels |
| Scan (10K keys) | 1.1M ns | 1.8M ns | +65% | Merge iterator now walks SSTable blocks in addition to the memtable |

## Analysis

### Write throughput is dominated by fsync

Sequential put throughput is ~266K ops/sec whether data stays in the memtable or flushes to disk. The bottleneck is `fsync` on the WAL after every write — the skip list insert is negligible in comparison. Write batches amortize the fsync cost: a batch of 100 puts pays for one fsync instead of 100.

### SSTable reads are ~2x slower than memtable reads

A memtable Get does one skip list seek (~479ns). An SSTable Get does: bloom filter check → index binary search → block decode + binary search (~1,057ns). The extra cost comes from the binary searches and block decoding, not from disk I/O — the Reader loads the entire file into memory at open time.

### Cache hit ≈ cold hit (for now)

The block cache stores decoded `*Block` pointers, saving checksum verification and block decoding on hits. However, since the Reader already holds the entire file in memory (via `os.ReadFile`), the "cold" path is also an in-memory read. The cache saves ~100-200ns of CRC32 + decode overhead per block, but adds hash + mutex + map lookup overhead, making the difference negligible.

The cache will show a significant improvement when the Reader switches to `mmap` or lazy file reads, where cold block access actually hits the filesystem.

### Get miss is slower than Get hit

A Get miss must check the bloom filter on *every* SSTable across all levels before returning "not found." A Get hit finds the key in one SSTable and stops. With data spread across multiple SSTables after compaction, a miss requires ~12 bloom filter checks versus a hit requiring ~1 block read.

In v0.1.0 (all data in memtable), Get miss was 219ns — one skip list seek that immediately failed. Now that data lives in SSTables, the miss path is longer.

### Scan allocations are high

30K allocations per full scan of 10K keys comes from the merge iterator: heap entries, key copies for deduplication tracking, and per-block decode allocations. This is a clear optimization target — arena-based allocation or iterator pooling could reduce this significantly.

## How to reproduce

```bash
make bench
```

Or with specific benchmarks:

```bash
go test -bench=BenchmarkSSTable -benchmem ./db/
go test -bench=BenchmarkMemtable -benchmem ./db/
```