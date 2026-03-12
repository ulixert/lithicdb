package memtable

import (
	"sync"

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

// Put inserts or update a key-value pair.
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

type memtableIterator struct {
	inner *SkipListIterator
	mu    *sync.RWMutex
}

func (it *memtableIterator) Key() []byte {
	return it.inner.Key()
}

func (it *memtableIterator) Value() []byte {
	return it.inner.Value()
}

func (it *memtableIterator) IsValid() bool {
	return it.inner.IsValid()
}

func (it *memtableIterator) Next() {
	it.inner.Next()
}

func (it *memtableIterator) Err() error {
	return it.inner.Err()
}

func (it *memtableIterator) Close() error {
	err := it.inner.Close()

	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}

	return err
}
