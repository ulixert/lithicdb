package sstable

import (
	"bytes"

	"github.com/ulixert/theseon/kv"
)

// SSTableIterator walks through an SSTable's entries in sorted key
// order, loading data blocks on demand.
//
// Key() and Value() return slices into the current block's raw buffer.
// These slices are invalidated when Next() advances to a new block.
// Callers must copy if they need to retain the data.
type SSTableIterator struct {
	reader   *Reader
	blockIdx int    // current block index in reader.metas
	entryIdx int    // current entry index within the block
	block    *Block // current decoded block (nil if exhausted)
	startKey []byte // inclusive start bound (nil = no lower bound)
	endKey   []byte // exclusive end bound (nil = no upper bound)

	// Cached current entry to avoid double decode
	curKey   []byte
	curValue kv.Value
	err      error
}

func newSSTableIterator(r *Reader, startBlock int, startKey, endKey []byte) *SSTableIterator {
	it := &SSTableIterator{
		reader:   r,
		blockIdx: startBlock,
		startKey: startKey,
		endKey:   endKey,
	}

	it.loadBlock()
	if it.err != nil {
		return it
	}

	// If we have a start key, advance to the first entry >= startKey
	if startKey != nil && it.block != nil {
		it.seekToStart()
	} else if it.block != nil {
		it.loadEntry()
	}

	it.checkEnd()

	return it
}

// loadBlock loads the block at blockIdx. Sets block to nil if
// blockIdx is out of range (iterator exhausted).
func (it *SSTableIterator) loadBlock() {
	if it.blockIdx >= it.reader.NumBlocks() {
		it.block = nil
		return
	}

	block, err := it.reader.readBlock(it.blockIdx)
	if err != nil {
		it.err = err
		it.invalidate()
		return
	}

	it.block = block
	it.entryIdx = 0
}

// loadEntry reads the current entry into curKey/curValue.
func (it *SSTableIterator) loadEntry() {
	if it.block == nil {
		return
	}

	key, value, err := it.block.readEntry(it.entryIdx)
	if err != nil {
		it.err = err
		it.invalidate()
		return
	}

	it.curKey = key
	it.curValue = value
}

// seekToStart advances to the first entry >= startKey within the
// current block. If no such entry exists, moves to the next block.
func (it *SSTableIterator) seekToStart() {
	for it.block != nil {
		for it.entryIdx < it.block.numEntries {
			it.loadEntry()
			if it.err != nil {
				return
			}
			if bytes.Compare(it.curKey, it.startKey) >= 0 {
				return
			}
			it.entryIdx++
		}

		// No matching entry in this block - try next
		it.blockIdx++
		it.loadBlock()
		if it.err != nil {
			return
		}
	}
}

// checkEnd marks the iterator as exhausted if the current key
// is at or past the end bound and releases references to block data.
func (it *SSTableIterator) checkEnd() {
	if it.block == nil || it.endKey == nil {
		return
	}
	if bytes.Compare(it.curKey, it.endKey) >= 0 {
		it.invalidate()
	}
}

func (it *SSTableIterator) Key() []byte {
	return it.curKey
}

// Value returns the value at the current position.
// Returns nil for tombstone entries.
func (it *SSTableIterator) Value() []byte {
	if it.curValue.Tombstone {
		return nil
	}
	return it.curValue.Data
}

func (it *SSTableIterator) IsValid() bool {
	return it.block != nil && it.err == nil
}

func (it *SSTableIterator) Next() {
	if !it.IsValid() {
		return
	}

	it.entryIdx++

	// If we've exhausted the current block, move to the next one
	if it.entryIdx >= it.block.numEntries {
		it.blockIdx++
		it.loadBlock()
		if it.err != nil || it.block == nil {
			return
		}
	}

	it.loadEntry()
	if it.err != nil {
		return
	}

	it.checkEnd()
}

func (it *SSTableIterator) Err() error {
	return it.err
}

func (it *SSTableIterator) Close() error {
	it.invalidate()
	return nil
}

// IsTombstone returns true if the current entry is a deletion marker.
func (it *SSTableIterator) IsTombstone() bool {
	return it.curValue.Tombstone
}

func (it *SSTableIterator) invalidate() {
	it.block = nil
	it.curKey = nil
	it.curValue = kv.Value{}
}
