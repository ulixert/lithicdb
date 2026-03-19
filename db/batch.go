package db

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
