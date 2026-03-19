package db

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ulixert/lithicdb/compaction"
)

// --- Memtable-only benchmarks (no flush, all data in memory) ---

func memtableOnlyOpts(dir string) Options {
	return Options{
		Dir:          dir,
		MemtableSize: 256 * 1024 * 1024, // 256MB — no flushes
		BlockSize:    4096,
		Compaction:   compaction.DefaultConfig(),
	}
}

func BenchmarkMemtable_Put_Sequential(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(memtableOnlyOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	val := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		if err := d.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func BenchmarkMemtable_Get_Hit(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(memtableOnlyOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	n := 10000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		d.Put(fmt.Appendf(nil, "key-%016d", i), val)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
		d.Get(key)
	}
}

// --- SSTable benchmarks (data flushed to disk, exercises full read path) ---

func sstableOpts(dir string) Options {
	return Options{
		Dir:            dir,
		MemtableSize:   4096, // tiny — forces frequent flushes
		BlockSize:      4096,
		BlockCacheSize: 8 * 1024 * 1024, // 8MB cache
		Compaction: compaction.Config{
			L0CompactionTrigger: 4,
			LevelSizeBase:       256 * 1024 * 1024,
			LevelSizeMultiplier: 10,
			MaxLevels:           7,
		},
	}
}

// prepareSSTableDB writes n keys, waits for all flushes and compaction,
// then returns the open DB with all data in SSTables.
func prepareSSTableDB(b *testing.B, dir string, n int) *DB {
	b.Helper()

	d, err := Open(sstableOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}

	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		if err := d.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	// Force all data to SSTables
	d.Close()

	d2, err := Open(sstableOpts(dir))
	if err != nil {
		b.Fatalf("Reopen: %v", err)
	}

	return d2
}

func BenchmarkSSTable_Get_Hit(b *testing.B) {
	dir := b.TempDir()
	n := 10000
	d := prepareSSTableDB(b, dir, n)
	defer d.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
		d.Get(key)
	}
}

func BenchmarkSSTable_Get_Miss(b *testing.B) {
	dir := b.TempDir()
	d := prepareSSTableDB(b, dir, 10000)
	defer d.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "miss-%016d", i)
		d.Get(key)
	}
}

func BenchmarkSSTable_Get_CacheHit(b *testing.B) {
	dir := b.TempDir()
	n := 10000
	d := prepareSSTableDB(b, dir, n)
	defer d.Close()

	// Warm the cache by reading every key once
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		d.Get(key)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
		d.Get(key)
	}
}

func BenchmarkSSTable_Scan(b *testing.B) {
	dir := b.TempDir()
	d := prepareSSTableDB(b, dir, 10000)
	defer d.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := d.Scan()
		for iter.IsValid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		iter.Close()
	}
}

// --- Put with flushes (measures WAL + memtable + flush overhead) ---

func BenchmarkPut_WithFlush(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(sstableOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	val := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		if err := d.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}
