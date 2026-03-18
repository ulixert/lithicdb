package db

import (
	"fmt"

	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/memtable"
)

// Put inserts or updates a key-value pair.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.nextSeq.Add(1)

	if err := db.activeWAL.Put(key, value, seq); err != nil {
		return fmt.Errorf("db: WAL put: %w", err)
	}

	ikey := kv.MakeInternalKey(key, seq)
	db.active.Put(ikey, kv.NewValue(value))

	if db.active.ApproximateSize() >= db.opts.MemtableSize {
		if err := db.rotateMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Delete marks a key as deleted.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.nextSeq.Add(1)

	if err := db.activeWAL.Delete(key, seq); err != nil {
		return fmt.Errorf("db: WAL delete: %w", err)
	}

	ikey := kv.MakeInternalKey(key, seq)
	db.active.Put(ikey, kv.NewTombstone())

	if db.active.ApproximateSize() >= db.opts.MemtableSize {
		if err := db.rotateMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// rotateMemtable freezes the active memtable, creates a new one,
// and signals the flush goroutine. Must be called with mu held.
func (db *DB) rotateMemtable() error {
	db.active.Freeze()
	_ = db.activeWAL.Close()

	db.immutables = append([]*memtable.Memtable{db.active}, db.immutables...)

	if err := db.newMemtable(); err != nil {
		return err
	}

	select {
	case db.flushCh <- struct{}{}:
	default:
	}

	return nil
}
