package iterator

import "github.com/ulixert/lithicdb/kv"

// WriteBufferEntry is a single entry in a transaction's write buffer,
// stored as an internal key (user key + inverted seq) and a value.
type WriteBufferEntry struct {
	Key   []byte   // internal key: [userKey][inverted_seq]
	Value kv.Value // value or tombstone
}

// WriteBufferIterator implements Iterator over a pre-sorted slice of
// write buffer entries. It is used to merge a transaction's buffered
// writes with the database's snapshot view during Scan operations.
//
// The caller must sort entries by Key (internal key order) before
// constructing the iterator.
type WriteBufferIterator struct {
	entries []WriteBufferEntry
	pos     int
}

// NewWriteBufferIterator creates an iterator over the given entries.
// The entries must already be sorted by internal key order.
func NewWriteBufferIterator(entries []WriteBufferEntry) *WriteBufferIterator {
	return &WriteBufferIterator{
		entries: entries,
		pos:     0,
	}
}

func (it *WriteBufferIterator) Key() []byte {
	return it.entries[it.pos].Key
}

func (it *WriteBufferIterator) Value() []byte {
	if it.entries[it.pos].Value.Tombstone {
		return nil
	}
	return it.entries[it.pos].Value.Data
}

func (it *WriteBufferIterator) IsValid() bool {
	return it.pos < len(it.entries)
}

func (it *WriteBufferIterator) Next() {
	if it.pos < len(it.entries) {
		it.pos++
	}
}

func (it *WriteBufferIterator) Err() error {
	return nil
}

func (it *WriteBufferIterator) Close() error {
	it.entries = nil
	return nil
}
