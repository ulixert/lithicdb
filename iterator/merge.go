package iterator

import (
	"bytes"
	"container/heap"

	"github.com/ulixert/lithicdb/kv"
)

// MergeIterator merges multiple sorted iterators into a single sorted
// stream, deduplicating on the user key. When multiple iterators (or
// multiple versions within the same iterator) have the same user key,
// the entry with the smallest internal key wins - which is the newest
// version from the highest-priority iterator.
//
// This handles two deduplication cases:
// 1. The same user key across different iterators (e.g., memtable vs. SSTable)
// 2. The same user key within one iterator (e.g., multiple versions in a memtable)
type MergeIterator struct {
	iters       []Iterator
	h           mergeHeap
	cur         heapItem
	lastUserKey []byte // user key of the last emitted entry, for dedup
	err         error
}

type heapItem struct {
	key   []byte // copied on push so heap ordering remains stable across iterator.Next()
	idx   int    // index into iters - lower index = higher priority
	valid bool
}

// NewMergeIterator creates a merge iterator from the given iterators.
// Iterators at lower indices have higher priority (newer data).
// For example, iters[0] = active memtable, iters[1] = immutable
// memtable, iters[2..] = SSTables newest to oldest.
func NewMergeIterator(iters []Iterator) *MergeIterator {
	m := &MergeIterator{
		iters: iters,
	}

	// Initialize the heap with all valid iterators
	for i, it := range iters {
		if it.IsValid() {
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())
			m.h = append(m.h, heapItem{key: key, idx: i, valid: true})
		}
	}

	heap.Init(&m.h)
	m.advance()

	return m
}

// advance pops the smallest key from the heap, sets it as current,
// then skips any entries with the same user key - both from other
// iterators on the heap AND from older versions within the same
// iterator. Continues until it finds an entry with a different
// user key from the last emitted one (or the heap is exhausted).
func (m *MergeIterator) advance() {
	for {
		if len(m.h) == 0 {
			m.cur = heapItem{}
			return
		}

		m.cur = heap.Pop(&m.h).(heapItem)

		// Drain all heap entries with the same user key
		for len(m.h) > 0 && kv.SameUserKey(m.h[0].key, m.cur.key) {
			dup := heap.Pop(&m.h).(heapItem)
			m.pushNext(dup.idx)
			if m.err != nil {
				return
			}
		}

		// If this entry has the same user key as what we last emitted,
		// skip it - it's an older version from the same iterator that
		// got pushed after the previous advance.
		curUserKey := kv.UserKey(m.cur.key)
		if m.lastUserKey != nil && bytes.Equal(curUserKey, m.lastUserKey) {
			m.pushNext(m.cur.idx)
			if m.err != nil {
				return
			}
			continue // loop to find the next different user key
		}

		// Found a new user key - record it and return
		m.lastUserKey = append(m.lastUserKey[:0], curUserKey...)
		return
	}
}

// pushNext advances the iterator at the given index and pushes
// its next entry onto the heap if valid.
func (m *MergeIterator) pushNext(idx int) {
	it := m.iters[idx]
	it.Next()
	if err := it.Err(); err != nil {
		m.err = err
		return
	}
	if it.IsValid() {
		key := make([]byte, len(it.Key()))
		copy(key, it.Key())
		heap.Push(&m.h, heapItem{key: key, idx: idx, valid: true})
	}
}

func (m *MergeIterator) Key() []byte {
	return m.iters[m.cur.idx].Key()
}

func (m *MergeIterator) Value() []byte {
	return m.iters[m.cur.idx].Value()
}

func (m *MergeIterator) IsValid() bool {
	return m.cur.valid && m.err == nil
}

func (m *MergeIterator) Next() {
	if !m.IsValid() {
		return
	}

	// Advance the iterator that produced the current entry
	m.pushNext(m.cur.idx)
	if m.err != nil {
		return
	}

	m.advance()
}

func (m *MergeIterator) Err() error {
	return m.err
}

func (m *MergeIterator) Close() error {
	var firstErr error
	for _, it := range m.iters {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.cur = heapItem{}
	m.h = nil
	m.lastUserKey = nil
	return firstErr
}

// --- min-heap implementation for container/heap ---

type mergeHeap []heapItem

func (h mergeHeap) Len() int {
	return len(h)
}

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// With sequence numbers in the key encoding, two entries from
	// different iterators should never have byte-identical internal
	// keys (same user key → different seq → different bytes).
	// This tiebreaker is a defensive fallback for deterministic
	// ordering, not the mechanism for correctness. The actual
	// deduplication happens in advance() via kv.SameUserKey.
	return h[i].idx < h[j].idx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
