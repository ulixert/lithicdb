package db

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkSnapshot_Get(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(memtableOnlyOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	n := 1000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		d.Put(fmt.Appendf(nil, "key-%016d", i), val)
	}

	snap := d.GetSnapshot()
	defer snap.Close()

	// Write more data so the snapshot exercises version filtering.
	for i := 0; i < n; i++ {
		d.Put(fmt.Appendf(nil, "key-%016d", i), val)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
		snap.Get(key)
	}
}

func BenchmarkSnapshot_Scan(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(memtableOnlyOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	n := 1000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		d.Put(fmt.Appendf(nil, "key-%016d", i), val)
	}

	snap := d.GetSnapshot()
	defer snap.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := snap.Scan()
		for iter.IsValid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		iter.Close()
	}
}

func BenchmarkTransaction_ReadWrite(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(memtableOnlyOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	n := 1000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		d.Put(fmt.Appendf(nil, "key-%016d", i), val)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx := d.BeginTransaction()

		// Read 5 random keys.
		for r := 0; r < 5; r++ {
			key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
			tx.Get(key)
		}

		// Write 5 unique keys (unique to avoid self-conflicts across iterations).
		for w := 0; w < 5; w++ {
			key := fmt.Appendf(nil, "tx-%016d-%d", i, w)
			tx.Put(key, val)
		}

		if err := tx.Commit(); err != nil {
			// Conflicts are OK in benchmarks — just rollback.
			tx.Rollback()
		}
	}
}
