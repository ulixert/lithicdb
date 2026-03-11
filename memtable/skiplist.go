package memtable

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/ulixert/lithicdb/kv"
)

const (
	maxHeight   = 12   //supports ~16M entries with p=0.25
	probability = 0.25 // probability of promoting a node to the next level
)

// skipListNode is a single node in the skip list.
// Each node has forwarded pointers at multiple levels.
type skipListNode struct {
	key   []byte
	value kv.Value
	next  []*skipListNode // next[i] is the next node at level i
}

// SkipList is a probabilistic, ordered data structure that supports
// average O(log n) search, insert, and delete. It is the backing
// store for the memtable.
//
// A skip list is a layered linked list. The bottom level (level 0)
// contains all entries in sorted order. Higher levels act as express
// lanes, skipping over entries for faster traversal.
//
// SkipList is NOT thread-safe. The Memtable wrapper handles
// synchronization.
type SkipList struct {
	head     *skipListNode
	height   int   // current max level in use
	size     int   // number of entries
	dataSize int64 // approximate memory usage of keys plus values
	rng      *rand.Rand
}

// NewSkipList creates an empty skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		head: &skipListNode{
			next: make([]*skipListNode, maxHeight),
		},
		height: 1,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomHeight returns a random level for a new node.
// Each level has a 25% chance of being promoted to the next level.
// This gives a good balance between space overhead and search speed.
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && s.rng.Float64() < probability {
		h++
	}
	return h
}

// findGreaterOrEqual returns the first node with key >= target.
// If prev is non-nil, it records the predecessor node at each level,
// which is needed for insertion.
func (s *SkipList) findGreaterOrEqual(target []byte, prev []*skipListNode) *skipListNode {
	x := s.head

	for i := s.height - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, target) < 0 {
			x = x.next[i]
		}

		if prev != nil {
			prev[i] = x
		}
	}

	return x.next[0]
}

// Put inserts or updates a key-value pair in the skip list.
// If the key already exists, its value is overwritten.
func (s *SkipList) Put(key []byte, value kv.Value) {
	prev := make([]*skipListNode, maxHeight)
	found := s.findGreaterOrEqual(key, prev)

	// Key already exists - update in place
	if found != nil && bytes.Equal(found.key, key) {
		s.dataSize -= found.value.EncodedSize()
		found.value = value
		s.dataSize += value.EncodedSize()
		return
	}

	// Insert a new node
	h := s.randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	node := &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, h),
	}

	for i := 0; i < h; i++ {
		node.next[i] = prev[i].next[i]
		prev[i].next[i] = node
	}

	s.size++
	s.dataSize += int64(len(key)) + value.EncodedSize()
}

// Get returns the value for the given key and true if found,
// or an empty Value and false if not found.
func (s *SkipList) Get(key []byte) (kv.Value, bool) {
	found := s.findGreaterOrEqual(key, nil)
	if found != nil && bytes.Equal(found.key, key) {
		return found.value, true
	}
	return kv.Value{}, false
}

// Len returns the number of entries in the skip list.
func (s *SkipList) Len() int {
	return s.size
}

// ApproximateSize returns the approximate memory usage in bytes
// of all keys and values. Does not include node/pointer overhead.
func (s *SkipList) ApproximateSize() int64 {
	return s.dataSize
}
