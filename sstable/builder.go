package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ulixert/lithicdb/kv"
)

// Builder constructs an SSTable file from a sorted stream of
// key-value entries. Entries must be added in strictly ascending
// sorted key order.
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
	keys     [][]byte // all keys, for bloom filter (see note below)
	firstKey []byte
	prevKey  []byte // prev key added, for sorted order validation
	offset   uint64 // current write offset in the file
	buf      []byte // accumulated file bytes
	finished bool

	// NOTE: keys hold a copy of every key for bloom filter construction.
	// For a 64MB memtable with 16-byte keys, this is ~64MB of copies.
	// A future optimization is to store only key hashes (4 bytes each)
	// and build the bloom filter incrementally.
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
	if b.finished {
		return fmt.Errorf("sstable: Add called after Finish")
	}

	// Verify sorted order
	if b.prevKey != nil && bytes.Compare(key, b.prevKey) <= 0 {
		return fmt.Errorf("sstable: keys not in sorted order: %q <= %q", key, b.prevKey)
	}

	// Track the first key of the entire SSTable
	if b.firstKey == nil {
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey, key)
	}

	// Copy key for bloom filter and sorted order validation.
	// keyCopy persists in b.keys, so b.prevKey can reference it
	// safely - it stays valid until Finish.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	b.keys = append(b.keys, keyCopy)
	b.prevKey = keyCopy

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

	// Validate block size fits in uint32
	if len(blockData) > math.MaxUint32 {
		return fmt.Errorf("sstable: block size %d exceeds uint32 max", len(blockData))
	}

	// Record metadata before writing
	b.metas = append(b.metas, blockMeta{
		offset:  b.offset,
		size:    uint32(len(blockData)),
		lastKey: b.block.LastKey(), // returns a copy
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

// Finish flushes the remaining block, writes the bloom filter,
// index block, and footer, and writes the complete SSTable to disk.
//
// The file is written atomically: data goes to a .tmp file first,
// which is fsynced, then renamed to the final .sst path. On crash,
// the .tmp file may be left behind and should be cleaned up on
// startup (see CleanupTempFiles).
//
// Finish must be called exactly once. Subsequent calls return an error.
func (b *Builder) Finish() error {
	if b.finished {
		return fmt.Errorf("sstable: Finish called twice")
	}
	// Finish flushes the remaining block, writes the bloom filter,
	// index block, and footer, and writes the complete SSTable to disk.
	//
	// Finish consumes the builder. After Finish is called, the builder
	// must not be reused, even if Finish returns an error.
	b.finished = true

	if b.firstKey == nil {
		return ErrEmptySSTable
	}

	// Flush the last block
	if err := b.flushBlock(); err != nil {
		return err
	}

	// Build and write bloom filter
	bloom := BuildBloomFilter(b.keys)
	bloomOffset := b.offset
	if bloom != nil {
		b.buf = append(b.buf, bloom...)
		b.offset += uint64(len(bloom))
	}

	// Build and write index block
	indexData, err := encodeIndex(b.firstKey, b.metas)
	if err != nil {
		return err
	}
	indexOffset := b.offset
	b.buf = append(b.buf, indexData...)
	b.offset += uint64(len(indexData))

	// Write footer
	f := footer{
		bloomOffset: bloomOffset,
		bloomLen:    uint32(len(bloom)),
		indexOffset: indexOffset,
		indexLen:    uint32(len(indexData)),
		version:     version1,
	}
	b.buf = append(b.buf, encodeFooter(f)...)

	// Write the file atomically with proper durability:
	// 1. Write to a temp file
	// 2. Fsync the temp file (data is durable on disk)
	// 3. Rename temp -> final (atomic metadata update)
	// 4. Fsync the directory (rename is durable)
	if err := os.MkdirAll(b.dir, 0o750); err != nil {
		return fmt.Errorf("sstable: create directory: %w", err)
	}

	finalPath := SSTPath(b.dir, b.id)
	tmpPath := finalPath + ".tmp"

	if err := writeDurable(tmpPath, b.buf); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sstable: rename: %w", err)
	}

	if err := syncDir(b.dir); err != nil {
		return fmt.Errorf("sstable: sync directory: %w", err)
	}

	return nil
}

// EstimatedSize returns the approximate size of the SSTable built so far.
// This is a lower bound - it does not include the bloom filter, index
// block, or footer, which are written during Finish(). Useful for
// deciding when to stop adding entries; being slightly under is safe
// (flushes a bit early, wastes minimal space).
func (b *Builder) EstimatedSize() uint64 {
	return b.offset + uint64(len(b.block.data))
}

// writeDurable writes data to path and fsyncs the file.
func writeDurable(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("sstable: create temp file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("sstable: write temp file: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sstable: sync temp file: %w", err)
	}

	return f.Close()
}

// syncDir fsyncs a directory to ensure that renames and file
// creations within it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		_ = d.Close()
	}()
	return d.Sync()
}

// CleanupTempFiles removes any leftover .sst.tmp files in dir.
// These are incomplete SSTable writes from a previous crash.
// Call this during engine startup before opening any SSTables.
func CleanupTempFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("sstable: read dir for cleanup: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sst.tmp") {
			path := filepath.Join(dir, e.Name())
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("sstable: remove temp file %s: %w", path, err)
			}
		}
	}

	return nil
}
