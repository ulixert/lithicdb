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
}

func New(id uint64) *Memtable {
	return &Memtable{
		sl: NewSkipList(),
	}
}

func (m *Memtable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, kv.NewValue(value))
}

func (m *Memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, kv.NewTombstone())
}

func (m *Memtable) Get(key []byte) (kv.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(key)
}
