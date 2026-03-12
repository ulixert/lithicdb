package memtable

import (
	"bytes"
	"sync"
)

// SkipListIterator walks the skip list at level 0 in sorted key order.
// It implements iterator.Iterator.
//
// Unlike SSTable iterators, the slices returned by Key() and Value()
// remain valid after Next() because each skip list node owns its data.
// However, callers should still follow the Iterator contract and not
// rely on this — code written against the interface should assume
// slices are invalidated by Next().
//
// Callers must ensure no concurrent writes occur during iteration
// or use the Memtable wrapper which handles synchronization.
type SkipListIterator struct {
	current *skipListNode
	endKey  []byte // exclusive upper bound; nil means no bound
}

func newSkipListIterator(start *skipListNode, endKey []byte) *SkipListIterator {
	it := &SkipListIterator{
		current: start,
		endKey:  endKey,
	}
	it.checkBound()
	return it
}

// checkBound sets current to nil if we've reached or passed the end key.
func (it *SkipListIterator) checkBound() {
	if it.current != nil && it.endKey != nil {
		if bytes.Compare(it.current.key, it.endKey) >= 0 {
			it.current = nil
		}
	}
}

func (it *SkipListIterator) Key() []byte {
	return it.current.key
}

// Value returns the value at the current position.
// Returns nil for tombstone entries (deleted keys).
func (it *SkipListIterator) Value() []byte {
	if it.current.value.Tombstone {
		return nil
	}
	return it.current.value.Data
}

func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.current = it.current.next[0]
		it.checkBound()
	}
}

func (it *SkipListIterator) IsValid() bool {
	return it.current != nil
}

// Err always returns nil for memtable iterators since all data
// is in memory and no I/O errors can occur.
func (it *SkipListIterator) Err() error {
	return nil
}

func (it *SkipListIterator) Close() error {
	it.current = nil
	return nil
}

// IsTombstone returns true if the current entry is a deletion marker.
// This is not part of the base Iterator interface, but is needed by
// compaction and the read path to distinguish deletions from empty values.
func (it *SkipListIterator) IsTombstone() bool {
	if it.current == nil {
		return false
	}
	return it.current.value.Tombstone
}

// memtableIterator wraps a SkipListIterator and manages the
// read lock lifecycle. The lock is acquired before iteration
// begins and released when Close() is called.
type memtableIterator struct {
	inner *SkipListIterator
	mu    *sync.RWMutex
}

func (it *memtableIterator) Key() []byte   { return it.inner.Key() }
func (it *memtableIterator) Value() []byte { return it.inner.Value() }
func (it *memtableIterator) IsValid() bool { return it.inner.IsValid() }
func (it *memtableIterator) Next()         { it.inner.Next() }
func (it *memtableIterator) Err() error    { return it.inner.Err() }

func (it *memtableIterator) Close() error {
	err := it.inner.Close()
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return err
}

// IsTombstone returns true if the current entry is a deletion marker.
func (it *memtableIterator) IsTombstone() bool {
	return it.inner.IsTombstone()
}
