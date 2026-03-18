package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/ulixert/lithicdb/kv"
)

// Reader provides read access to an SSTable file. On open, it reads
// the footer, bloom filter, and index block into memory. Data blocks
// are read on demand during Get or Scan operations.
//
// Reader is safe for concurrent use by multiple goroutines.
type Reader struct {
	id       uint64
	data     []byte // mmap candidate - currently read into memory
	bloom    []byte
	firstKey []byte
	metas    []blockMeta
}

// OpenReader opens an SSTable file and reads its metadata.
func OpenReader(dir string, id uint64) (*Reader, error) {
	path := SSTPath(dir, id)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	if len(data) < footerSize {
		return nil, fmt.Errorf("sstable: file too small (%d bytes)", len(data))
	}

	// Decode footer
	f, err := decodeFooter(data)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	// Read bloom filter
	bloomEnd := f.bloomOffset + uint64(f.bloomLen)
	if bloomEnd > uint64(len(data)) {
		return nil, fmt.Errorf("sstable: bloom filter extends past file end")
	}
	var bloom []byte
	if f.bloomLen > 0 {
		bloom = data[f.bloomOffset:bloomEnd]
	}

	// Read and decode the index block
	indexEnd := f.indexOffset + uint64(f.indexLen)
	if indexEnd > uint64(len(data)) {
		return nil, fmt.Errorf("sstable: index block extends past file end")
	}

	firstKey, metas, err := decodeIndex(data[f.indexOffset:indexEnd])
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	return &Reader{
		id:       id,
		data:     data,
		bloom:    bloom,
		firstKey: firstKey,
		metas:    metas,
	}, nil
}

// ID returns the SSTable's unique identifier.
func (r *Reader) ID() uint64 {
	return r.id
}

// FirstKey returns a copy of the first (smallest) key in the SSTable.
func (r *Reader) FirstKey() []byte {
	out := make([]byte, len(r.firstKey))
	copy(out, r.firstKey)
	return out
}

// LastKey returns a copy of the last (largest) key in the SSTable.
func (r *Reader) LastKey() []byte {
	if len(r.metas) == 0 {
		return nil
	}
	last := r.metas[len(r.metas)-1].lastKey
	out := make([]byte, len(last))
	copy(out, last)
	return out
}

// NumBlocks returns the number of data blocks in the SSTable.
func (r *Reader) NumBlocks() int {
	return len(r.metas)
}

// FileSize returns the total size of the SSTable file in bytes.
func (r *Reader) FileSize() int {
	return len(r.data)
}

// MayContain checks the bloom filter for the given user key.
// Returns true if the key might be present, false if definitely absent.
// The argument must be a user key, not an internal key, because
// the bloom filter is built from user key hashes.
func (r *Reader) MayContain(userKey []byte) bool {
	return BloomMayContain(r.bloom, userKey)
}

// readBlock reads and verifies the data block at the given index.
func (r *Reader) readBlock(blockIdx int) (*Block, error) {
	if blockIdx < 0 || blockIdx >= len(r.metas) {
		return nil, fmt.Errorf("sstable: block index %d out of range [0, %d)", blockIdx, len(r.metas))
	}

	meta := r.metas[blockIdx]
	blockEnd := meta.offset + uint64(meta.size)

	// Read block data + checksum
	if blockEnd+blockChecksumSize > uint64(len(r.data)) {
		return nil, fmt.Errorf("%w: block %d extends past file end", ErrCorruptBlock, blockIdx)
	}

	blockData := r.data[meta.offset:blockEnd]

	// Verify checksum
	storedChecksum := binary.LittleEndian.Uint32(r.data[blockEnd : blockEnd+blockChecksumSize])
	actualChecksum := crc32.ChecksumIEEE(blockData)
	if storedChecksum != actualChecksum {
		return nil, fmt.Errorf("%w: block %d checksum mismatch", ErrInvalidChecksum, blockIdx)
	}

	return DecodeBlock(blockData)
}

// Get looks up the newest version of a user key in the SSTable. Returns the
// value and true if found, or empty value and false if not found.
//
// The lookup path:
//  1. Check bloom filter (hashed on the user key) - skip the entire SSTable
//     if the key is definitely absent
//  2. Binary search the index to find which block might contain the key
//  3. Read and verify that single block
//  4. Binary search within the block for the newest version of the user key
func (r *Reader) Get(userKey []byte) (value kv.Value, found bool, err error) {
	// Step 1: bloom filter (uses user key hash)
	if !r.MayContain(userKey) {
		return kv.Value{}, false, nil
	}

	// Step 2: find the block using an internal search key
	searchKey := kv.MakeSearchKey(userKey)
	blockIdx := r.findBlock(searchKey)
	if blockIdx < 0 {
		return kv.Value{}, false, nil
	}

	// Step 3: read and verify the block
	block, err := r.readBlock(blockIdx)
	if err != nil {
		return kv.Value{}, false, err
	}

	// Step 4: search within the block by user key
	return block.Get(userKey)
}

// findBlock returns the index of the block that might contain the key,
// or -1 if the key is outside the SSTable's range.
//
// Because blocks have non-overlapping, sorted key ranges, we find the
// first block whose lastKey >= target. If no such block exists, the
// key is larger than everything in the SSTable.
func (r *Reader) findBlock(key []byte) int {
	low, high := 0, len(r.metas)-1

	for low <= high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(r.metas[mid].lastKey, key)
		if cmp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if low >= len(r.metas) {
		return -1
	}

	return low
}

// Scan returns an iterator over all entries in the SSTable.
func (r *Reader) Scan() *SSTableIterator {
	return newSSTableIterator(r, 0, nil, nil)
}

// ScanRange returns an iterator over entries whose user key is in
// [start, end). If start is nil, the scan begins from the first key.
// If end is nil, the scan continues through the last key.
// Bounds are user keys, not internal keys.
func (r *Reader) ScanRange(start, end []byte) *SSTableIterator {
	startBlock := 0
	var startIKey []byte
	if start != nil {
		startIKey = kv.MakeSearchKey(start)
		startBlock = r.findBlock(startIKey)
		if startBlock < 0 {
			// start is past all keys - empty iterator
			return newSSTableIterator(r, len(r.metas), nil, nil)
		}
	}

	var endIKey []byte
	if end != nil {
		endIKey = kv.MakeSearchKey(end)
	}

	return newSSTableIterator(r, startBlock, startIKey, endIKey)
}
