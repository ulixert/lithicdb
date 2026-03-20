package iterator

import (
	"bytes"
	"container/heap"
)

// MergeIterator merges multiple sorted iterators into a single sorted
// stream. It emits ALL versions of all keys in global sorted order
// (ascending user key, then descending sequence number within the same
// user key). No deduplication is performed - the caller is responsible
// for version selection (see SnapshotIterator).
//
// Iterators at lower indices have higher priority when two entries have
// byte-identical internal keys (defensive tiebreaker only - in practice,
// different sequence numbers make internal keys unique).
type MergeIterator struct {
	iters []Iterator
	h     mergeHeap
	cur   heapItem
	err   error
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

// advance pops the smallest key from the heap and sets it as current.
func (m *MergeIterator) advance() {
	if len(m.h) == 0 {
		m.cur = heapItem{}
		return
	}
	m.cur = heap.Pop(&m.h).(heapItem)
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
	// This tiebreaker is a defensive fallback for deterministic ordering.
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
