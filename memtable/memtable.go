package memtable

import (
	"sync"

	"github.com/ulixert/lithicdb/iterator"
	"github.com/ulixert/lithicdb/kv"
)

// Memtable is a thread-safe, ordered, in-memory key-value store backed
// by a skip list. It is the write target for the LSM engine.
//
// A Memtable starts as active (accepting writes). When it reaches its
// size threshold, it is frozen and becomes immutable, waiting to be
// flushed to an SSTable on disk.
type Memtable struct {
	sl     *SkipList
	mu     sync.RWMutex
	frozen bool
	id     uint64
}

// New creates an empty Memtable with the given ID.
// The ID is used to associate the memtable with its WAL file.
func New(id uint64) *Memtable {
	return &Memtable{
		sl: NewSkipList(),
		id: id,
	}
}

// ID returns the memtable's unique identifier.
func (m *Memtable) ID() uint64 {
	return m.id
}

// Put inserts or updates a key-value pair.
func (m *Memtable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, kv.NewValue(value))
}

// Delete marks a key as deleted by writing a tombstone.
func (m *Memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, kv.NewTombstone())
}

// Get returns the value for the given key. The second return value
// indicates whether the key was found. If the key was deleted
// (tombstone), found is true and Value.Tombstone is set.
func (m *Memtable) Get(key []byte) (kv.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(key)
}

// Scan returns an iterator over all entries in sorted key order.
//
// The returned iterator holds a read lock on the memtable.
// Callers MUST call Close() when done to release the lock.
// While the iterator is open, writes to this memtable will block.
func (m *Memtable) Scan() iterator.Iterator {
	m.mu.RLock()
	return &memtableIterator{
		inner: m.sl.Scan(),
		mu:    &m.mu,
	}
}

// ScanRange returns an iterator over entries in [start, end).
// If start is nil, the scan begins from the first key.
// If end is nil, the scan continues through the last key.
//
// The returned iterator holds a read lock on the memtable.
// Callers MUST call Close() when done to release the lock.
func (m *Memtable) ScanRange(start, end []byte) iterator.Iterator {
	m.mu.RLock()
	return &memtableIterator{
		inner: m.sl.ScanRange(start, end),
		mu:    &m.mu,
	}
}

// ApproximateSize returns the approximate memory usage in bytes.
func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.ApproximateSize()
}

// Len returns the number of entries in the memtable.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Len()
}

// Freeze marks the memtable as immutable. After freezing,
// no further writes should be made. Iteration over a frozen
// memtable is safe without holding any locks.
func (m *Memtable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
}

// IsFrozen returns whether the memtable has been frozen.
func (m *Memtable) IsFrozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.frozen
}
