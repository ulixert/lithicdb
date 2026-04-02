package iterator

// Iterator is the fundamental abstraction for reading ordered key-value data
// in Theseon. Every readable component — memtables, SSTable blocks, merge
// iterators, MVCC filtered iterators — implements this interface.
//
// Usage pattern:
//
//	for iter.IsValid() {
//	    key := iter.Key()
//	    value := iter.Value()
//	    // ... process ...
//	    iter.Next()
//	}
//	if err := iter.Err(); err != nil {
//	    // handle I/O or corruption error
//	}
type Iterator interface {
	// Key returns the key at the current position.
	// The returned slice is only valid until the next call to Next.
	// Caller must not modify the returned slice.
	Key() []byte

	// Value returns the value at the current position.
	// The returned slice is only valid until the next call to Next.
	// Caller must not modify the returned slice.
	Value() []byte

	// Next advances the iterator to the next key-value pair.
	// After Next returns, check IsValid to determine whether
	// the iterator has been exhausted.
	Next()

	// IsValid returns true if the iterator is positioned at a valid entry.
	// Returns false when the iterator is exhausted or after an error.
	IsValid() bool

	// Err returns the first error encountered during iteration, if any.
	// Callers should check Err after the iteration loop ends.
	// A non-nil Err means the iteration terminated early due to
	// an I/O or data corruption error, not normal exhaustion.
	Err() error

	// Close releases any resources held by the iterator, such as open
	// file handles or pinned cache blocks. Callers must call Close when
	// done with the iterator. It is safe to call Close on an already
	// closed iterator. Implementations that hold no resources should
	// return nil.
	Close() error
}
