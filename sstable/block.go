package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
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
//	flag: 0 = put, 1 = tombstone (value_len must be 0, value omitted)
//
// The block checksum (CRC32) is appended and verified by the SSTable
// builder/reader, not by the block itself. See builder.go and reader.go.

const (
	blockKeyLenSize   = 2
	blockValueLenSize = 2
	blockFlagSize     = 1
	blockEntryHeader  = blockKeyLenSize + blockValueLenSize + blockFlagSize // 5
	blockOffsetSize   = 2
	blockCountSize    = 2

	maxKeyLen       = 1<<16 - 1 // 65,535, max value of uint16
	maxValueLen     = 1<<16 - 1 // 65,535, stored as uint16 in block entries
	maxBlockEntries = 1<<16 - 1 // 65,535
	maxBlockOffset  = 1<<16 - 1 // 65,535
)

var (
	ErrCorruptBlock    = errors.New("sstable: corrupt block")
	ErrKeyTooLarge     = errors.New("sstable: key exceeds maximum size")
	ErrValueTooLarge   = errors.New("sstable: value exceeds maximum size")
	ErrBlockEntryLimit = errors.New("sstable: block exceeds maximum entries")
)

// BlockBuilder accumulates sorted key-value entries and produces
// an encoded block. Entries must be added in sorted key order.
type BlockBuilder struct {
	data     []byte
	offsets  []uint16
	size     int // target block size
	firstKey []byte
	lastKey  []byte
}

// NewBlockBuilder creates a block builder with the given target size.
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{size: blockSize}
}

// Add appends a key-value entry. Returns false with a nil error if adding
// the entry would exceed the target block size, in which case the entry is
// NOT added and the caller should finish this block and start a new one.
//
// The first entry is always accepted regardless of size, ensuring every
// block contains at least one entry.
//
// Returns a non-nil error if the key, value, entry count, or entry offset
// exceeds the block format limits.
func (b *BlockBuilder) Add(key []byte, value kv.Value) (bool, error) {
	if len(key) > maxKeyLen {
		return false, fmt.Errorf("%w (%d bytes, max %d)", ErrKeyTooLarge, len(key), maxKeyLen)
	}
	if !value.Tombstone && len(value.Data) > maxValueLen {
		return false, fmt.Errorf("%w (%d bytes, max %d)", ErrValueTooLarge, len(value.Data), maxValueLen)
	}
	if len(b.offsets) >= maxBlockEntries {
		return false, fmt.Errorf("%w (%d)", ErrBlockEntryLimit, maxBlockEntries)
	}

	entrySize := blockEntryHeader + len(key)
	if !value.Tombstone {
		entrySize += len(value.Data)
	}

	// Total size if we add this entry:
	// current data + new entry + all offsets (including the new one) + count
	newTotal := len(b.data) + entrySize + (len(b.offsets)+1)*blockOffsetSize + blockCountSize

	// Always accept the first entry (a block must have at least one entry)
	if len(b.offsets) > 0 && newTotal > b.size {
		return false, nil
	}

	if len(b.data) > maxBlockOffset {
		return false, fmt.Errorf("%w: entry offset %d exceeds uint16 range", ErrCorruptBlock, len(b.data))
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	// Track the first and last key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	if b.firstKey == nil {
		b.firstKey = keyCopy
	}
	b.lastKey = keyCopy

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

	return true, nil
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
	b.firstKey = nil
	b.lastKey = nil
}

// FirstKey returns the first key added to this block, or nil if empty.
func (b *BlockBuilder) FirstKey() []byte {
	return b.firstKey
}

// LastKey returns the last key added to this block, or nil if empty.
func (b *BlockBuilder) LastKey() []byte {
	return b.lastKey
}

// Block is a decoded data block. It holds the raw block bytes and
// a parsed offset table for efficient random access within the
// entry data region.
type Block struct {
	data           []byte   // raw block bytes (the full encoded block)
	offsets        []uint16 // entry offsets, parsed from the tail
	numEntries     int
	entryRegionEnd int // byte offset where entry data ends and offset table begins
}

// DecodeBlock parses a raw block (without checksum) into a Block.
// Validates that the offset table is well-formed: all offsets must
// be within the entry data region and strictly ascending.
func DecodeBlock(data []byte) (*Block, error) {
	if len(data) < blockCountSize {
		return nil, fmt.Errorf("%w: too short (%d bytes)", ErrCorruptBlock, len(data))
	}

	numEntries := int(binary.LittleEndian.Uint16(data[len(data)-blockCountSize:]))
	if numEntries == 0 {
		return nil, fmt.Errorf("%w: zero entries", ErrCorruptBlock)
	}

	offsetTableSize := numEntries*blockOffsetSize + blockCountSize
	if len(data) < offsetTableSize {
		return nil, fmt.Errorf("%w: too short for offset table", ErrCorruptBlock)
	}

	entryRegionEnd := len(data) - offsetTableSize
	offsetStart := entryRegionEnd

	offsets := make([]uint16, numEntries)
	for i := 0; i < numEntries; i++ {
		offset := binary.LittleEndian.Uint16(data[offsetStart+i*blockOffsetSize:])

		// Validate: offset must be within the entry data region
		if int(offset) >= entryRegionEnd {
			return nil, fmt.Errorf("%w: offset[%d]=%d outside entry region [0, %d)", ErrCorruptBlock, i, offset, entryRegionEnd)
		}

		// Validate: offsets must be strictly ascending (except first)
		if i > 0 && offset <= offsets[i-1] {
			return nil, fmt.Errorf("%w: offset[%d]=%d not after offset[%d]=%d", ErrCorruptBlock, i, offset, i-1, offsets[i-1])
		}

		offsets[i] = offset
	}

	return &Block{
		data:           data,
		offsets:        offsets,
		numEntries:     numEntries,
		entryRegionEnd: entryRegionEnd,
	}, nil
}

// readEntry decodes the entry at the given offset table index.
// All bounds checks are against the entry data region, not the
// full block, so corrupted lengths cannot be read into the offset table.
func (b *Block) readEntry(idx int) (key []byte, value kv.Value, err error) {
	if idx < 0 || idx >= b.numEntries {
		return nil, kv.Value{}, fmt.Errorf("%w: entry index %d out of range [0, %d)", ErrCorruptBlock, idx, b.numEntries)
	}

	offset := int(b.offsets[idx])
	if offset+blockEntryHeader > b.entryRegionEnd {
		return nil, kv.Value{}, fmt.Errorf("%w: entry header truncated at index %d", ErrCorruptBlock, idx)
	}

	keyLen := int(binary.LittleEndian.Uint16(b.data[offset:]))
	offset += blockKeyLenSize

	valueLen := int(binary.LittleEndian.Uint16(b.data[offset:]))
	offset += blockValueLenSize

	flag := b.data[offset]
	offset += blockFlagSize

	if offset+keyLen > b.entryRegionEnd {
		return nil, kv.Value{}, fmt.Errorf("%w: entry key truncated at index %d", ErrCorruptBlock, idx)
	}

	key = b.data[offset : offset+keyLen]
	offset += keyLen

	switch flag {
	case blockFlagPut:
		if offset+valueLen > b.entryRegionEnd {
			return nil, kv.Value{}, fmt.Errorf("%w: entry value truncated at index %d", ErrCorruptBlock, idx)
		}
		value = kv.NewValue(b.data[offset : offset+valueLen])
	case blockFlagTombstone:
		if valueLen != 0 {
			return nil, kv.Value{}, fmt.Errorf("%w: tombstone at index %d has non-zero value_len %d", ErrCorruptBlock, idx, valueLen)
		}
		value = kv.NewTombstone()
	default:
		return nil, kv.Value{}, fmt.Errorf("%w: invalid flag %d at index %d", ErrCorruptBlock, flag, idx)
	}

	return key, value, nil
}

// Get searches for a key in the block using binary search.
// Returns the value and true if found, or empty value and false if not.
func (b *Block) Get(target []byte) (kv.Value, bool, error) {
	low, high := 0, b.numEntries-1

	for low <= high {
		mid := low + (high-low)/2
		key, value, err := b.readEntry(mid)
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
