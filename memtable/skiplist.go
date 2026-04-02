package memtable

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/ulixert/theseon/kv"
)

const (
	maxHeight   = 12   // supports ~16M entries with p=0.25
	probability = 0.25 // probability of promoting a node to the next level
)

// skipListNode is a single node in the skip list.
// Each node has forwarded pointers at multiple levels.
type skipListNode struct {
	key   []byte // internal key (user_key + inverted seq)
	value kv.Value
	next  []*skipListNode // next[i] is the next node at level i
}

// SkipList is a probabilistic, ordered data structure that supports
// average O(log n) search and insert. It is the backing
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

// Put inserts an internal key-value pair. Every internal key is unique
// (different sequence number), so this always inserts a new node.
func (s *SkipList) Put(internalKey []byte, value kv.Value) {
	prev := make([]*skipListNode, maxHeight)
	s.findGreaterOrEqual(internalKey, prev)

	h := s.randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	node := &skipListNode{
		key:   internalKey,
		value: value,
		next:  make([]*skipListNode, h),
	}

	for i := 0; i < h; i++ {
		node.next[i] = prev[i].next[i]
		prev[i].next[i] = node
	}

	s.size++
	s.dataSize += int64(len(internalKey)) + value.EncodedSize()
}

// Get finds the newest version of a user key by seeking to
// MakeSearchKey(userKey), which sorts before all real versions.
// Returns the value and true if found, or empty Value and false.
func (s *SkipList) Get(userKey []byte) (kv.Value, bool) {
	searchKey := kv.MakeSearchKey(userKey)
	found := s.findGreaterOrEqual(searchKey, nil)

	// The first entry >= searchKey is the newest version of this user key
	// (if it exists), because searchKey has MaxSeqNum, which inverts to
	// all zeros, sorting before any real inverted seq.
	if found != nil && bytes.Equal(kv.UserKey(found.key), userKey) {
		return found.value, true
	}

	return kv.Value{}, false
}

// GetNewest finds the newest version of a user key and also returns
// its sequence number. This is used for conflict detection in
// optimistic transactions: if the returned seq is greater than the
// transaction's snapshot seq, a write-write conflict has occurred.
func (s *SkipList) GetNewest(userKey []byte) (kv.Value, uint64, bool) {
	searchKey := kv.MakeSearchKey(userKey)
	found := s.findGreaterOrEqual(searchKey, nil)

	if found != nil && bytes.Equal(kv.UserKey(found.key), userKey) {
		return found.value, kv.SeqNum(found.key), true
	}

	return kv.Value{}, 0, false
}

// GetAt finds the newest version of a user key with seq <= maxSeq.
// This enables point-in-time reads: by passing a snapshot's sequence
// number, only versions visible to that snapshot are considered.
func (s *SkipList) GetAt(userKey []byte, maxSeq uint64) (kv.Value, bool) {
	searchKey := kv.MakeSearchKey(userKey)
	node := s.findGreaterOrEqual(searchKey, nil)

	// Walk forward through versions of this user key (newest first
	// due to inverted seq ordering) until we find one with seq <= maxSeq.
	for node != nil && bytes.Equal(kv.UserKey(node.key), userKey) {
		if kv.SeqNum(node.key) <= maxSeq {
			return node.value, true
		}
		node = node.next[0]
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

// Scan returns an iterator over all entries in sorted key order.
func (s *SkipList) Scan() *SkipListIterator {
	return newSkipListIterator(s.head.next[0], nil)
}

// ScanRange returns an iterator over entries in [start, end) by user key.
// If start is nil, the scan begins from the first key.
// If end is nil, the scan continues through the last key.
// Bounds are user keys, not internal keys.
func (s *SkipList) ScanRange(start, end []byte) *SkipListIterator {
	var startNode *skipListNode
	if start == nil {
		startNode = s.head.next[0]
	} else {
		// Seek to the newest version of the start key
		searchKey := kv.MakeSearchKey(start)
		startNode = s.findGreaterOrEqual(searchKey, nil)
	}

	// Convert the end user key to an internal key for comparison.
	// MakeSearchKey(end) sorts before all versions of the end, which
	// makes the bound exclusive on the user key.
	var endInternalKey []byte
	if end != nil {
		endInternalKey = kv.MakeSearchKey(end)
	}
	return newSkipListIterator(startNode, endInternalKey)
}
