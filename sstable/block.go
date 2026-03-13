package sstable

import (
	"encoding/binary"

	"github.com/ulixert/lithicdb/kv"
)

// Block format (on disk, without trailing checksum):
//
//	[entry_0][entry_1]...[entry_{n-1}]
//	[offset_0: 2]...[offset_{n-1}: 2]   ← byte offset of each entry
//	[num_entries: 2]
//
// Entry format:
//
//	[key_len: 2][value_len: 2][flag: 1][key][value]
//	flag: 0 = put, 1 = tombstone (value omitted, value_len = 0)
//
// The trailing checksum is appended by the SSTable builder and
// verified by the SSTable reader — not part of the block itself.

const (
	blockKeyLenSize   = 2
	blockValueLenSize = 2
	blockFlagSize     = 1
	blockEntryHeader  = blockKeyLenSize + blockValueLenSize + blockFlagSize // 5
	blockOffsetSize   = 2
	blockCountSize    = 2
)

// BlockBuilder accumulates sorted key-value entries and produces
// an encoded block. Entries must be added in sorted key order.
type BlockBuilder struct {
	data    []byte
	offsets []uint16
	size    int // target block size
}

// NewBlockBuilder creates a block builder with the given target size.
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{size: blockSize}
}

// Add appends a key-value entry. Returns false if adding the entry would
// exceed the target block size, in which case the entry is
// NOT added and the caller should finish this block and start a new one.
//
// The first entry is always accepted regardless of size, ensuring
// every block contains at least one entry.
func (b *BlockBuilder) Add(key []byte, value kv.Value) bool {
	entrySize := blockEntryHeader + len(key)
	if !value.Tombstone {
		entrySize += len(value.Data)
	}

	// Total size if we add this entry:
	// current data + new entry + all offsets (including the new one) + count
	newTotal := len(b.data) + entrySize + (len(b.offsets)+1)*blockOffsetSize + blockCountSize

	// Always accept the first entry (a block must have at least one entry)
	if len(b.offsets) > 0 && newTotal > b.size {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	// key_len
	b.data = binary.LittleEndian.AppendUint16(b.data, uint16(len(key)))

	// value_len
	if value.Tombstone {
		b.data = binary.LittleEndian.AppendUint16(b.data, 0)
	} else {
		b.data = binary.LittleEndian.AppendUint16(b.data, uint16(len(value.Data)))
	}

	// flag
	if value.Tombstone {
		b.data = append(b.data, blockFlagTombstone)
	} else {
		b.data = append(b.data, blockFlagPut)
	}

	// key
	b.data = append(b.data, key...)

	// value (omitted for tombstones)
	if !value.Tombstone {
		b.data = append(b.data, value.Data...)
	}

	return true
}

// IsEmpty returns true if no entries have been added.
func (b *BlockBuilder) IsEmpty() bool {
	return len(b.offsets) == 0
}

// Build serializes the block into its on-disk format (without checksum).
func (b *BlockBuilder) Build() []byte {
	size := len(b.data) + len(b.offsets)*blockOffsetSize + blockCountSize
	buf := make([]byte, size)

	// Entries
	copy(buf, b.data)
	offset := len(b.data)

	// Offset table
	for _, o := range b.offsets {
		binary.LittleEndian.PutUint16(buf[offset:], o)
		offset += blockOffsetSize
	}

	// Entry count
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(b.offsets)))

	return buf
}

// Reset clears the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.data = b.data[:0]
	b.offsets = b.offsets[:0]
}
