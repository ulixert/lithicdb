package db

import (
	"fmt"
	"math/rand"
	"testing"
)

func benchOpts(dir string) Options {
	return Options{
		Dir:          dir,
		MemtableSize: 64 * 1024 * 1024, // 64MB — large enough to avoid flushes during bench
		BlockSize:    4096,
	}
}

func BenchmarkPut_Sequential(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(benchOpts(dir))
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

func BenchmarkPut_Random(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(benchOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	val := make([]byte, 100)
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Int63())
		if err := d.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(benchOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Pre-populate with 10K keys
	n := 10000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		d.Put(key, val)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%016d", rng.Intn(n))
		d.Get(key)
	}
}

func BenchmarkGet_Miss(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(benchOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Pre-populate
	val := make([]byte, 100)
	for i := 0; i < 10000; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		d.Put(key, val)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "miss-%016d", i)
		d.Get(key)
	}
}

func BenchmarkScan(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(benchOpts(dir))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Pre-populate
	val := make([]byte, 100)
	for i := 0; i < 10000; i++ {
		key := fmt.Appendf(nil, "key-%016d", i)
		d.Put(key, val)
	}

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
