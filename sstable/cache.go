package sstable

import (
	"container/list"
	"sync"
)

// BlockCache is a sharded LRU cache for decoded SSTable blocks.
// It sits between Reader.readBlock and the filesystem - every block
// read goes through the cache.
//
// Sharding reduces lock contention: each shard has its own mutex
// and LRU list, and a block's shard is determined by hashing its
// cache key. With 16 shards, concurrent readers contend on the same
// mutex only 1/16 of the time.
type BlockCache struct {
	shards    []*cacheShard
	numShards uint64
	capacity  int64 // total capacity in bytes across all shards
}

// BlockCacheKey uniquely identifies a block within the cache.
type BlockCacheKey struct {
	SSTID       uint64
	BlockOffset uint64
}

type cacheEntry struct {
	key   BlockCacheKey
	block *Block
	size  int64 // approximate memory size of the block
}

type cacheShard struct {
	mu       sync.Mutex
	items    map[BlockCacheKey]*list.Element
	order    *list.List // front = most recent, back = least recent
	size     int64      // current size in bytes
	capacity int64      // max size in bytes for this shard
}

const defaultNumShards = 16

// NewBlockCache creates a cache with the given total capacity in bytes.
// Capacity is divided evenly across shards.
func NewBlockCache(capacity int64) *BlockCache {
	if capacity <= 0 {
		capacity = 8 * 1024 * 1024 // 8MB default
	}

	shards := make([]*cacheShard, defaultNumShards)
	shardCapacity := capacity / int64(defaultNumShards)
	if shardCapacity < 1 {
		shardCapacity = 1
	}

	for i := range shards {
		shards[i] = &cacheShard{
			items:    make(map[BlockCacheKey]*list.Element),
			order:    list.New(),
			capacity: shardCapacity,
		}
	}

	return &BlockCache{
		shards:    shards,
		numShards: uint64(defaultNumShards),
		capacity:  capacity,
	}
}
