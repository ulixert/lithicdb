package db

import (
	"fmt"

	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/wal"
)

// WriteBatch groups multiple Put and Delete operations into a single
// atomic unit. Either all operations in the batch are visible after
// Commit, or none are (on crash or error).
//
// Atomicity comes from the WAL: all entries are written as a single
// record and fsynced together. If the process crashes mid-commit,
// recovery either replays the complete batch or ignores the partial
// record (corrupt tail handling).
//
// Usage:
//
//	batch := db.NewWriteBatch()
//	batch.Put([]byte("a"), []byte("1"))
//	batch.Put([]byte("b"), []byte("2"))
//	batch.Delete([]byte("c"))
//	err := batch.Commit()
type WriteBatch struct {
	db   *DB
	ops  []batchOp
	size int // approximate byte size of all keys + values
}

type batchOp struct {
	key       []byte
	value     []byte // nil for deletes
	tombstone bool
}

// NewWriteBatch creates a new empty batch.
func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{db: db}
}

// Put stages a key-value write in the batch.
func (b *WriteBatch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{
		key:   key,
		value: value,
	})
	b.size += len(key) + len(value)
}

// Delete stages a key deletion in the batch.
func (b *WriteBatch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{
		key:       key,
		tombstone: true,
	})
	b.size += len(key)
}

// Count returns the number of operations in the batch.
func (b *WriteBatch) Count() int {
	return len(b.ops)
}

// Reset clears the batch for reuse.
func (b *WriteBatch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

// Commit writes all operations atomically. The entire batch is
// written to the WAL as a single record, then applied to the
// memtable. If the WAL write fails, no data is applied.
//
// Commit acquires the DB write lock for the duration. After commit,
// the batch is reset and can be reused.
func (b *WriteBatch) Commit() error {
	if len(b.ops) == 0 {
		return nil
	}

	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	// Assign a contiguous range of sequence numbers.
	// Each operation gets its own seq so that within the batch,
	// later operations on the same key win (correct for
	// Put("a","1") then Delete("a") in the same batch).
	baseSeq := b.db.nextSeq.Add(uint64(len(b.ops))) - uint64(len(b.ops)) + 1

	// Build WAL entries
	walEntries := make([]wal.Entry, len(b.ops))
	for i, op := range b.ops {
		seq := baseSeq + uint64(i)
		if op.tombstone {
			walEntries[i] = wal.Entry{
				Seq:   seq,
				Key:   op.key,
				Value: kv.NewTombstone(),
			}
		} else {
			walEntries[i] = wal.Entry{
				Seq:   seq,
				Key:   op.key,
				Value: kv.NewValue(op.value),
			}
		}
	}

	// Single WAL write + fsync - atomic on disk
	if err := b.db.activeWAL.WriteEntries(walEntries); err != nil {
		return fmt.Errorf("db: batch WAL write: %w", err)
	}

	// Apply to memtable (all operations, same order)
	for i, op := range b.ops {
		seq := baseSeq + uint64(i)
		ikey := kv.MakeInternalKey(op.key, seq)

		if op.tombstone {
			b.db.active.Put(ikey, kv.NewTombstone())
		} else {
			b.db.active.Put(ikey, kv.NewValue(op.value))
		}
	}

	// Check if the memtable needs rotation
	if b.db.active.ApproximateSize() >= b.db.opts.MemtableSize {
		if err := b.db.rotateMemtable(); err != nil {
			return err
		}
	}

	b.Reset()

	return nil
}
