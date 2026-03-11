package kv

// Value represents a value in the storage engine. A Value is either
// a regular data value or a tombstone marking a deleted key.
//
// Tombstones are how LSM trees handle deletes: instead of removing
// a key immediately, a tombstone is written. The actual removal
// happens later during compaction, once the tombstone has propagated
// below all older versions of the key.
type Value struct {
	Data      []byte
	Tombstone bool
}

// NewValue creates a regular (non-tombstone) value.
func NewValue(data []byte) Value {
	return Value{Data: data}
}

// NewTombstone creates a tombstone value indicating deletion.
func NewTombstone() Value {
	return Value{Tombstone: true}
}

// EncodedSize returns the approximate size in bytes for memory tracking.
func (v Value) EncodedSize() int64 {
	return int64(len(v.Data)) + 1
}
