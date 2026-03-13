package sstable

import "errors"

// Block layout (on disk):
//
//  [entry_0][entry_1]...[entry_n]
//  [restart_0: 4 bytes][restart_1: 4 bytes]...[restart_m: 4 bytes]
//  [num_restarts: 4 bytes]
//  [checksum:     4 bytes]
//
// Each entry:
//
//  [key_len:   2 bytes]
//  [value_len: 4 bytes]
//  [key]
//	[flag:      1 byte]   0x00 = regular value, 0x01 = tombstone
//  [value]
//
// Restart points store the offset of every restartInterval-th entry.
// At restart points, the full key is stored (no prefix compression yet).
// This enables binary search within the block for point lookups.
//
// A tombstone entry has value_len = 0 and 1-type value of 0x01.
// A regular empty value has value_len = 0 and no special marker.
// To distinguish: we use a flag byte approach - value starts with a
// flag byte: 0x00 = regular value, 0x01 = tombstone.

const (
	blockKeyLenSize   = 2
	blockValueLenSize = 4
	blockEntryHeader  = blockKeyLenSize + blockValueLenSize

	restartPointSize = 4 // uint32
	numRestartsSize  = 4 // uint32
	blockCRCSize     = 4 // CRC32

	// restartInterval is how many entries between restart points.
	// A restart point stores the full key and its offset, enabling
	// binary search within the block. 16 is the standard choice.
	restartInterval = 16

	// targetBlockSize is the approximate uncompressed block size.
	// The builder finished the current entry before checking, so
	// actual blocks may slightly exceed this.
	targetBlockSize = 4096

	// Tombstone encoding: we prepend a 1-byte flag to every value.
	flagValueRegular   byte = 0x00
	flagValueTombstone byte = 0x01
)

var (
	ErrEmptyBlock   = errors.New("sstable: block has no entries")
	ErrCorruptBlock = errors.New("sstable: block checksum mismatch")
	ErrShortBlock   = errors.New("sstable: block data too short")
)

// BlockBuilder accumulates key-value entries and produces an encoded block.
// Keys must be added in sorted order. The caller is responsible for
// ensuring this invariant.
type BlockBuilder struct {
	entries   []byte   // serialized entries
	restarts  []uint32 // offsets of restart-point entries
	count     int      // number of entries added
	firstKey  []byte   // the first key in this block (for index)
	lastKey   []byte   // the last key in this block (for index)
	estimated int      // estimated encoded size so far
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder() *BlockBuilder {
	return &BlockBuilder{
		restarts: []uint32{0}, // the first entry is always a restart point
	}
}
