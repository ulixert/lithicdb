package db

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ulixert/theseon/kv"
	"github.com/ulixert/theseon/memtable"
	"github.com/ulixert/theseon/metrics"
)

// Put inserts or updates a key-value pair.
// Blocks if there are too many unflushed memtables (backpressure).
func (db *DB) Put(key, value []byte) error {
	timer := prometheus.NewTimer(metrics.KVWriteLatency.WithLabelValues("put"))
	defer timer.ObserveDuration()

	db.mu.Lock()
	defer db.mu.Unlock()

	db.waitForFlushSpace()

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

	metrics.KVWrites.WithLabelValues("put", db.opts.Mode).Inc()
	return nil
}

// Delete marks a key as deleted.
// Blocks if there are too many unflushed memtables (backpressure).
func (db *DB) Delete(key []byte) error {
	timer := prometheus.NewTimer(metrics.KVWriteLatency.WithLabelValues("delete"))
	defer timer.ObserveDuration()

	db.mu.Lock()
	defer db.mu.Unlock()

	db.waitForFlushSpace()

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

	metrics.KVWrites.WithLabelValues("delete", db.opts.Mode).Inc()
	return nil
}

// waitForFlushSpace blocks until there's room for another immutable
// memtable. Called with mu.Lock held. cond.Wait releases the lock,
// allowing the flush goroutine to make progress, then re-acquires it.
func (db *DB) waitForFlushSpace() {
	for len(db.immutables) >= db.opts.MaxImmutableCount {
		db.flushDone.Wait()
	}
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
