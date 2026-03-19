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

// Get retrieves a block from the cache. Returns nil if not present.
func (c *BlockCache) Get(key BlockCacheKey) *Block {
	shard := c.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, ok := shard.items[key]
	if !ok {
		return nil
	}

	// Move to the front (most recently used)
	shard.order.MoveToFront(elem)
	return elem.Value.(*cacheEntry).block
}

// Put inserts a block into the cache. If the cache is full, the
// least recently used entries are evicted to make room.
func (c *BlockCache) Put(key BlockCacheKey, block *Block, size int64) {
	shard := c.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// If already present, update and move to the front
	if elem, ok := shard.items[key]; ok {
		old := elem.Value.(*cacheEntry)
		shard.size -= old.size
		old.block = block
		old.size = size
		shard.size += size
		shard.order.MoveToFront(elem)
		return
	}

	// Evict until there's room
	for shard.size+size > shard.capacity && shard.order.Len() > 0 {
		shard.evict()
	}

	entry := &cacheEntry{key: key, block: block, size: size}
	elem := shard.order.PushFront(entry)
	shard.items[key] = elem
	shard.size += size
}

func (s *cacheShard) evict() {
	back := s.order.Back()
	if back == nil {
		return
	}

	entry := back.Value.(*cacheEntry)
	delete(s.items, entry.key)
	s.order.Remove(back)
	s.size -= entry.size
}

// getShard returns the shard for a given key using a fast hash.
func (c *BlockCache) getShard(key BlockCacheKey) *cacheShard {
	// Mix both fields into a single hash. The multiply-xor-shift
	// is a standard fast hash for integer keys.
	h := key.SSTID*0x9E3779B97F4A7C15 ^ key.BlockOffset*0x517CC1B727220A95
	return c.shards[h%c.numShards]
}
