package memtable

import (
	"sync"

	"github.com/ulixert/theseon/iterator"
	"github.com/ulixert/theseon/kv"
)

// Memtable is a thread-safe, ordered, in-memory key-value store backed
// by a skip list. It is the write target for the LSM engine.
//
// Keys stored in the memtable are internal keys (user_key + inverted
// sequence number). This means every write is a unique insert, and
// multiple versions of the same user key coexist in sorted order
// (newest first for the same user key).
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

// Put inserts an internal key with its value. The caller (the DB
// engine) is responsible for constructing the internal key via
// kv.MakeInternalKey(userKey, seq).
func (m *Memtable) Put(internalKey []byte, value kv.Value) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(internalKey, value)
}

// Get returns the newest version of a user key. The second return
//
//	value indicates whether the key was found. If the key was deleted
//
// (tombstone), found is true and Value.Tombstone is set.
func (m *Memtable) Get(userKey []byte) (kv.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(userKey)
}

// GetNewest returns the newest version of a user key along with its
// sequence number. Used for write-write conflict detection in transactions.
func (m *Memtable) GetNewest(userKey []byte) (kv.Value, uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.GetNewest(userKey)
}

// GetAt returns the newest version of a user key with seq <= maxSeq.
// This enables snapshot reads at a specific point in time.
func (m *Memtable) GetAt(userKey []byte, maxSeq uint64) (kv.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.GetAt(userKey, maxSeq)
}

// Scan returns an iterator over all entries in internal key order.
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

// ScanRange returns an iterator over entries whose user key is in
// [start, end). If start is nil, the scan begins from the first key.
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
