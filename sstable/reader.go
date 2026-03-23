package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"syscall"

	"github.com/ulixert/lithicdb/kv"
)

// Reader provides read access to an SSTable file. On open, it mmap's
// the file and parses the footer, bloom filter, and index block.
// Data blocks are read on demand during Get or Scan operations,
// optionally via a shared BlockCache.
//
// The mmap'd region stays valid even after the file is deleted (Unix
// keeps the inode alive). Call Close() to munmap when the Reader is
// no longer needed.
//
// Reader is safe for concurrent use by multiple goroutines.
type Reader struct {
	id       uint64
	data     []byte // mmap'd file contents
	bloom    []byte // copied from mmap'd data on open
	firstKey []byte
	metas    []blockMeta
	cache    *BlockCache

	closeOnce sync.Once
}

// OpenReader opens an SSTable file via mmap and reads its metadata.
// If the cache is non-nil, block reads will go through the cache.
// The caller must call Close() when done to release the mmap.
func OpenReader(dir string, id uint64, cache ...*BlockCache) (*Reader, error) {
	path := SSTPath(dir, id)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}

	size := int(info.Size())
	if size < footerSize {
		return nil, fmt.Errorf("sstable: file too small (%d bytes)", size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("sstable: mmap %s: %w", path, err)
	}

	// From here on, if we fail, we must munmap.
	r, err := openFromData(id, data)
	if err != nil {
		_ = syscall.Munmap(data)
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	if len(cache) > 0 && cache[0] != nil {
		r.cache = cache[0]
	}

	return r, nil
}

// openFromData parses an SSTable from a byte slice (mmap'd or heap).
// The bloom filter is copied out of data so it survives munmap of
// the main data region (bloom is accessed on every lookup via
// MayContain, so it must remain valid).
func openFromData(id uint64, data []byte) (*Reader, error) {
	f, err := decodeFooter(data)
	if err != nil {
		return nil, err
	}

	// Copy bloom filter out of the mmap'd region - it's accessed on every
	// lookup and must survive even if the mmap is released.
	bloomEnd := f.bloomOffset + uint64(f.bloomLen)
	if bloomEnd > uint64(len(data)) {
		return nil, fmt.Errorf("bloom filter extends past file end")
	}
	var bloom []byte
	if f.bloomLen > 0 {
		bloom = make([]byte, f.bloomLen)
		copy(bloom, data[f.bloomOffset:bloomEnd])
	}

	// Read and decode the index block. decodeIndex already copies
	// firstKey and lastKey bytes, so they don't alias the mmap.
	indexEnd := f.indexOffset + uint64(f.indexLen)
	if indexEnd > uint64(len(data)) {
		return nil, fmt.Errorf("index block extends past file end")
	}

	firstKey, metas, err := decodeIndex(data[f.indexOffset:indexEnd])
	if err != nil {
		return nil, err
	}

	return &Reader{
		id:       id,
		data:     data,
		bloom:    bloom,
		firstKey: firstKey,
		metas:    metas,
	}, nil
}

// Close releases the mmap'd region. After Close, the Reader must not
// be used. Close is idempotent.
func (r *Reader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		if r.data != nil {
			err = syscall.Munmap(r.data)
			r.data = nil
		}
	})
	return err
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
// If a cache is configured, checks the cache first and populates
// it on a miss.
func (r *Reader) readBlock(blockIdx int) (*Block, error) {
	if blockIdx < 0 || blockIdx >= len(r.metas) {
		return nil, fmt.Errorf("sstable: block index %d out of range [0, %d)", blockIdx, len(r.metas))
	}

	meta := r.metas[blockIdx]

	// Check cache
	if r.cache != nil {
		cacheKey := BlockCacheKey{
			SSTID:       r.id,
			BlockOffset: meta.offset,
		}
		if block := r.cache.Get(cacheKey); block != nil {
			return block, nil
		}

		// Cache miss - read, verify, decode, then populate cache
		block, err := r.readBlockFromData(blockIdx, meta)
		if err != nil {
			return nil, err
		}

		r.cache.Put(cacheKey, block, int64(meta.size))
		return block, nil
	}

	return r.readBlockFromData(blockIdx, meta)
}

// readBlockFromData reads a block directly from the mmap'd file data,
// verifies its checksum, and decodes it.
func (r *Reader) readBlockFromData(blockIdx int, meta blockMeta) (*Block, error) {
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

// GetAt looks up the newest version of a user key with seq <= maxSeq.
// This enables snapshot reads through SSTables.
//
// A user key's versions may span multiple blocks (e.g., the newest
// versions fill the end of block N, and older versions spill into
// block N+1). If the first block contains only versions with seq >
// maxSeq, we continue to the next block.
func (r *Reader) GetAt(userKey []byte, maxSeq uint64) (value kv.Value, found bool, err error) {
	if !r.MayContain(userKey) {
		return kv.Value{}, false, nil
	}

	searchKey := kv.MakeSearchKey(userKey)
	blockIdx := r.findBlock(searchKey)
	if blockIdx < 0 {
		return kv.Value{}, false, nil
	}

	for blockIdx < len(r.metas) {
		block, err := r.readBlock(blockIdx)
		if err != nil {
			return kv.Value{}, false, err
		}

		val, found, err := block.GetAt(userKey, maxSeq)
		if err != nil {
			return kv.Value{}, false, err
		}
		if found {
			return val, true, nil
		}

		// Check if the block's last entry still has our user key.
		// If so, older versions may continue in the next block.
		lastKey, err := block.LastKey()
		if err != nil {
			return kv.Value{}, false, err
		}
		if !bytes.Equal(kv.UserKey(lastKey), userKey) {
			// The user key ended within this block - no match.
			return kv.Value{}, false, nil
		}

		blockIdx++
	}

	return kv.Value{}, false, nil
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
