package db

import (
	"slices"
	"sync/atomic"

	"github.com/ulixert/theseon/iterator"
	"github.com/ulixert/theseon/kv"
)

// txWrite holds a single buffered write in a transaction.
type txWrite struct {
	value kv.Value
}

// Transaction provides snapshot-isolation reads and optimistic
// write-write conflict detection. Reads see a consistent snapshot
// taken at transaction start. Writes are buffered locally until
// Commit, which checks for conflicts and applies them atomically.
//
// Usage:
//
//	tx := db.BeginTransaction()
//	val, ok := tx.Get([]byte("key"))
//	tx.Put([]byte("key"), []byte("new-value"))
//	if err := tx.Commit(); err != nil {
//	    // handle ErrConflict or other error
//	}
type Transaction struct {
	db       *DB
	snapshot *Snapshot
	writes   map[string]txWrite // keyed by string(userKey)
	closed   atomic.Bool
}

// Get returns the value for the given key. Buffered writes in this
// transaction take priority over the snapshot. If the key was deleted
// in this transaction, ok is false.
func (tx *Transaction) Get(key []byte) (kv.Value, bool) {
	// Check the local write buffer first.
	if w, ok := tx.writes[string(key)]; ok {
		if w.value.Tombstone {
			return kv.Value{}, false
		}
		return w.value, true
	}
	return tx.snapshot.Get(key)
}

// Put buffers a key-value write in the transaction.
func (tx *Transaction) Put(key, value []byte) error {
	if tx.closed.Load() {
		return ErrTxClosed
	}
	tx.writes[string(key)] = txWrite{value: kv.NewValue(value)}
	return nil
}

// Delete buffers a key deletion in the transaction.
func (tx *Transaction) Delete(key []byte) error {
	if tx.closed.Load() {
		return ErrTxClosed
	}
	tx.writes[string(key)] = txWrite{value: kv.NewTombstone()}
	return nil
}

// Commit checks for write-write conflicts and, if clean, applies
// all buffered writes atomically. Returns ErrConflict if another
// writer modified any key in the write set after this transaction's
// snapshot was taken.
//
// Conflict detection checks the active memtable and all immutable
// memtables. It does not check SSTables - this is the same trade-off
// as RocksDB's OptimisticTransactionDB, optimized for low-contention
// workloads where conflicts happen within a memtable lifetime.
func (tx *Transaction) Commit() error {
	if !tx.closed.CompareAndSwap(false, true) {
		return ErrTxClosed
	}
	defer tx.snapshot.Close()

	if len(tx.writes) == 0 {
		return nil
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Conflict check: look for any write to the keys with seq > snapshot.seq.
	if err := tx.checkConflicts(); err != nil {
		return err
	}

	// Build batch ops from the write buffer.
	ops := make([]batchOp, 0, len(tx.writes))
	for k, w := range tx.writes {
		ops = append(ops, batchOp{
			key:       []byte(k),
			value:     w.value.Data,
			tombstone: w.value.Tombstone,
		})
	}

	return tx.db.applyBatchLocked(ops)
}

// checkConflicts detects write-write conflicts by scanning the active
// and immutable memtables for any version of a key in the write set
// with seq > snapshot.seq. Must be called with db.mu held.
func (tx *Transaction) checkConflicts() error {
	for k := range tx.writes {
		userKey := []byte(k)

		// Check the active memtable.
		if _, seq, found := tx.db.active.GetNewest(userKey); found && seq > tx.snapshot.seq {
			return ErrConflict
		}

		// Check immutable memtables.
		for _, table := range tx.db.immutables {
			if _, seq, found := table.GetNewest(userKey); found && seq > tx.snapshot.seq {
				return ErrConflict
			}
		}
	}
	return nil
}

// Rollback discards all buffered writes and releases the snapshot.
// It is safe to call Rollback on an already-closed transaction.
func (tx *Transaction) Rollback() {
	if !tx.closed.CompareAndSwap(false, true) {
		return
	}
	tx.snapshot.Close()
	tx.writes = nil
}

// Scan returns an iterator over all key-value pairs visible to this
// transaction, including buffered writes. Buffered writes take
// priority over the snapshot for the same user key.
func (tx *Transaction) Scan() iterator.Iterator {
	return tx.mergedIterator(tx.db.rawScan())
}

// ScanRange returns an iterator over key-value pairs in [start, end)
// visible to this transaction, including buffered writes.
func (tx *Transaction) ScanRange(start, end []byte) iterator.Iterator {
	return tx.mergedIterator(tx.db.rawScanRange(start, end))
}

// mergedIterator builds an iterator that merges the transaction's
// write buffer with a raw database iterator, using the snapshot
// iterator to deduplicate and filter by visibility.
func (tx *Transaction) mergedIterator(rawIter iterator.Iterator) iterator.Iterator {
	// Build write buffer entries with seq = snapshot.seq + 1 so they
	// sort before (are "newer" than) any snapshot entry for the
	// same user key.
	bufSeq := tx.snapshot.seq + 1
	entries := make([]iterator.WriteBufferEntry, 0, len(tx.writes))
	for k, w := range tx.writes {
		entries = append(entries, iterator.WriteBufferEntry{
			Key:   kv.MakeInternalKey([]byte(k), bufSeq),
			Value: w.value,
		})
	}

	// Sort by internal key so the write buffer iterator is ordered.
	slices.SortFunc(entries, func(a, b iterator.WriteBufferEntry) int {
		return slices.Compare(a.Key, b.Key)
	})

	bufIter := iterator.NewWriteBufferIterator(entries)

	// Merge write buffer with raw DB iterator. The merge iterator
	// produces all versions in global sorted order. The snapshot
	// iterator then picks the newest visible version per user key.
	merged := iterator.NewMergeIterator([]iterator.Iterator{bufIter, rawIter})
	return iterator.NewSnapshotIterator(merged, bufSeq)
}
