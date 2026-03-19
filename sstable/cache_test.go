package sstable

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

func TestBlockCache_PutAndGet(t *testing.T) {
	cache := NewBlockCache(1024)

	key := BlockCacheKey{SSTID: 1, BlockOffset: 0}
	block := &Block{numEntries: 5} // dummy block

	cache.Put(key, block, 100)

	got := cache.Get(key)
	if got == nil {
		t.Fatal("expected cache hit")
	}
	if got.numEntries != 5 {
		t.Errorf("numEntries = %d, want 5", got.numEntries)
	}
}

func TestBlockCache_Miss(t *testing.T) {
	cache := NewBlockCache(1024)

	key := BlockCacheKey{SSTID: 1, BlockOffset: 0}
	if cache.Get(key) != nil {
		t.Error("expected cache miss")
	}
}

func TestBlockCache_Eviction(t *testing.T) {
	// Cache fits exactly 2 entries of size 50 each (capacity=100)
	cache := NewBlockCache(100 * defaultNumShards) // per-shard capacity = 100

	// All keys go to the same shard for predictable eviction
	// (we can't easily control sharding, so use enough entries)
	keys := make([]BlockCacheKey, 10)
	for i := range keys {
		keys[i] = BlockCacheKey{SSTID: uint64(i), BlockOffset: 0}
	}

	// Fill cache beyond capacity
	for i, key := range keys {
		block := &Block{numEntries: i}
		cache.Put(key, block, 50)
	}

	// Most recent entries should still be present
	// (exact eviction depends on shard distribution, but at least
	// the very last one should be in the cache)
	last := keys[len(keys)-1]
	if cache.Get(last) == nil {
		t.Error("most recent entry should be in cache")
	}
}

func TestBlockCache_Update(t *testing.T) {
	cache := NewBlockCache(1024)

	key := BlockCacheKey{SSTID: 1, BlockOffset: 0}
	block1 := &Block{numEntries: 1}
	block2 := &Block{numEntries: 2}

	cache.Put(key, block1, 50)
	cache.Put(key, block2, 50) // update same key

	got := cache.Get(key)
	if got == nil {
		t.Fatal("expected cache hit")
	}
	if got.numEntries != 2 {
		t.Errorf("numEntries = %d, want 2 (should be updated)", got.numEntries)
	}
}

func TestBlockCache_DifferentSSTs(t *testing.T) {
	cache := NewBlockCache(1024)

	key1 := BlockCacheKey{SSTID: 1, BlockOffset: 100}
	key2 := BlockCacheKey{SSTID: 2, BlockOffset: 100}

	cache.Put(key1, &Block{numEntries: 10}, 50)
	cache.Put(key2, &Block{numEntries: 20}, 50)

	got1 := cache.Get(key1)
	got2 := cache.Get(key2)

	if got1 == nil || got1.numEntries != 10 {
		t.Errorf("SST 1 block: got %v", got1)
	}
	if got2 == nil || got2.numEntries != 20 {
		t.Errorf("SST 2 block: got %v", got2)
	}
}

func TestBlockCache_Concurrent(t *testing.T) {
	cache := NewBlockCache(64 * 1024)

	var wg sync.WaitGroup
	n := 100

	// Concurrent writers
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := BlockCacheKey{SSTID: uint64(id), BlockOffset: uint64(i * 4096)}
				cache.Put(key, &Block{numEntries: i}, 100)
			}
		}(g)
	}

	// Concurrent readers
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := BlockCacheKey{SSTID: uint64(id), BlockOffset: uint64(i * 4096)}
				cache.Get(key) // may hit or miss, just shouldn't panic
			}
		}(g)
	}

	wg.Wait()
}

func TestBlockCache_IntegrationWithReader(t *testing.T) {
	dir := t.TempDir()
	cache := NewBlockCache(1024 * 1024)

	// Build an SSTable
	b := NewBuilder(dir, 1, defaultBlockSize)
	for i := 0; i < 100; i++ {
		key := kv.MakeInternalKey([]byte(fmt.Sprintf("key-%04d", i)), uint64(i+1))
		b.Add(key, kv.NewValue([]byte(fmt.Sprintf("val-%04d", i))))
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Open with cache
	r, err := OpenReader(dir, 1, cache)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// First read: cache miss → populates cache
	val, found, err := r.Get([]byte("key-0050"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "val-0050" {
		t.Errorf("value = %q, want %q", val.Data, "val-0050")
	}

	// Second read: should be a cache hit (same block)
	val, found, err = r.Get([]byte("key-0050"))
	if err != nil {
		t.Fatalf("Get (cached): %v", err)
	}
	if !found {
		t.Fatal("expected found (cached)")
	}
	if string(val.Data) != "val-0050" {
		t.Errorf("cached value = %q, want %q", val.Data, "val-0050")
	}
}
