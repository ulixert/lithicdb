package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

// Block is a decoded data block. It holds the raw block bytes and
// a parsed offset table for efficient random access.
type Block struct {
	data       []byte   // raw block bytes (the full encoded block)
	offsets    []uint16 // entry offsets, parsed from the tail
	numEntries int
}

// DecodeBlock parses a raw block (without checksum) into a Block.
func DecodeBlock(data []byte) (*Block, error) {
	if len(data) < blockCountSize {
		return nil, fmt.Errorf("sstable: block too short (%d bytes)", len(data))
	}

	numEntries := int(binary.LittleEndian.Uint16(data[len(data)-blockCountSize:]))
	if numEntries == 0 {
		return nil, fmt.Errorf("sstable: block has zero entries")
	}

	offsetTableSize := numEntries*blockCountSize + blockCountSize
	if len(data) < offsetTableSize {
		return nil, fmt.Errorf("sstable: block too short for offset table")
	}

	offsetStart := len(data) - offsetTableSize
	offsets := make([]uint16, numEntries)
	for i := 0; i < numEntries; i++ {
		offsets[i] = binary.LittleEndian.Uint16(data[offsetStart+i*blockOffsetSize:])
	}

	return &Block{
		data:       data,
		offsets:    offsets,
		numEntries: numEntries,
	}, nil
}

// readEntry decodes the entry at the given offsets table index.
func (b *Block) readEntry(idx int) (key []byte, value kv.Value, err error) {
	if idx < 0 || idx >= b.numEntries {
		return nil, kv.Value{}, fmt.Errorf("sstable: block entry index %d out of range [0, %d)", idx, b.numEntries)
	}

	offset := int(b.offsets[idx])
	if offset+blockEntryHeader > len(b.data) {
		return nil, kv.Value{}, fmt.Errorf("sstable: block entry header truncated at index %d", idx)
	}

	keyLen := int(binary.LittleEndian.Uint16(b.data[offset:]))
	offset += blockKeyLenSize

	valueLen := int(binary.LittleEndian.Uint16(b.data[offset:]))
	offset += blockValueLenSize

	flag := b.data[offset]
	offset += blockFlagSize

	if offset+keyLen > len(b.data) {
		return nil, kv.Value{}, fmt.Errorf("sstable: block entry key truncated at index %d", idx)
	}

	key = b.data[offset : offset+keyLen]
	offset += keyLen

	switch flag {
	case blockFlagPut:
		if offset+valueLen > len(b.data) {
			return nil, kv.Value{}, fmt.Errorf("sstable: block entry value truncated at index: %d", idx)
		}
		value = kv.NewValue(b.data[offset : offset+valueLen])
	case blockFlagTombstone:
		value = kv.NewTombstone()
	default:
		return nil, kv.Value{}, fmt.Errorf("sstable: invalid block entry flag %d at index %d", flag, idx)
	}

	return key, value, nil
}

// Get searches for a key in the block using binary search.
// Returns the value and true if found, or empty value and false if not.
func (b *Block) Get(target []byte) (kv.Value, bool, error) {
	low, high := 0, b.numEntries-1

	for low <= high {
		mid := low + (high-low)/2
		key, _, err := b.readEntry(mid)
		if err != nil {
			return kv.Value{}, false, err
		}

		cmp := bytes.Compare(key, target)
		switch {
		case cmp < 0:
			low = mid + 1
		case cmp > 0:
			high = mid - 1
		default:
			_, value, err := b.readEntry(mid)
			if err != nil {
				return kv.Value{}, false, err
			}
			return value, true, nil
		}
	}

	return kv.Value{}, false, nil
}

// FirstKey returns the first (smallest) key in the block.
func (b *Block) FirstKey() ([]byte, error) {
	key, _, err := b.readEntry(0)
	return key, err
}

// LastKey returns the last (largest) key in the block.
func (b *Block) LastKey() ([]byte, error) {
	key, _, err := b.readEntry(b.numEntries - 1)
	return key, err
}

// NumEntries returns the number of entries in the block.
func (b *Block) NumEntries() int {
	return b.numEntries
}
