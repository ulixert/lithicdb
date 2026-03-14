package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"github.com/ulixert/lithicdb/kv"
)

// Builder constructs an SSTable file from a sorted stream of
// key-value entries. Entries must be added in sorted key order.
//
// Usage:
//
//	b := sstable.NewBuilder(dir, id, blockSize)
//	b.Add(key1, val1)
//	b.Add(key2, val2)
//	err := b.Finish()
type Builder struct {
	dir       string
	id        uint64
	blockSize int

	block    *BlockBuilder
	metas    []blockMeta
	keys     [][]byte // all keys, for bloom filter
	firstKey []byte
	offset   uint64 // current write offset in the file
	buf      []byte // accumulated file bytes
}

// sstFileName returns the SSTable file name for a given ID.
func sstFileName(id uint64) string {
	return fmt.Sprintf("%06d.sst", id)
}

// SSTPath returns the full path for an SSTable file.
func SSTPath(dir string, id uint64) string {
	return filepath.Join(dir, sstFileName(id))
}

// NewBuilder creates a builder that will write an SSTable file
// to the given directory with the given ID and target block size.
func NewBuilder(dir string, id uint64, blockSize int) *Builder {
	if blockSize <= 0 {
		blockSize = defaultBlockSize
	}

	return &Builder{
		dir:       dir,
		id:        id,
		blockSize: blockSize,
		block:     NewBlockBuilder(blockSize),
	}
}

// Add appends a key-value pair to the SSTable. Keys must be added
// in strictly ascending sorted order. When the current block is full,
// it is flushed and a new block is started.
func (b *Builder) Add(key []byte, value kv.Value) error {
	// Track the first key of the entire SSTable
	if b.firstKey == nil {
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey, key)
	}

	// Collect key for bloom filter (make a copy since key may be reused)
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	b.keys = append(b.keys, keyCopy)

	ok, err := b.block.Add(key, value)
	if err != nil {
		return err
	}

	if !ok {
		// The current block is full - flush it and start a new one
		if err := b.flushBlock(); err != nil {
			return err
		}

		// Add to the new block (must succeed - first entry always accepted)
		if _, err := b.block.Add(key, value); err != nil {
			return err
		}
	}

	return nil
}

// flushBlock encodes the current block, appends it with a checksum
// to the output buffer, records its metadata, and resets the builder.
func (b *Builder) flushBlock() error {
	if b.block.IsEmpty() {
		return nil
	}

	blockData := b.block.Build()

	// Record metadata before writing
	lastKey := make([]byte, len(b.block.LastKey()))
	copy(lastKey, b.block.LastKey())

	b.metas = append(b.metas, blockMeta{
		offset:  b.offset,
		size:    uint32(len(blockData)),
		lastKey: lastKey,
	})

	// Append block data
	b.buf = append(b.buf, blockData...)
	b.offset += uint64(len(blockData))

	// Append block checksum
	checksum := crc32.ChecksumIEEE(blockData)
	b.buf = binary.LittleEndian.AppendUint32(b.buf, checksum)
	b.offset += blockChecksumSize

	b.block.Reset()

	return nil
}
