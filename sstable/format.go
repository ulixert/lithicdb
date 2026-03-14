package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// SSTable file layout:
//
//	[data block 0][crc32: 4]
//	[data block 1][crc32: 4]
//	...
//	[bloom filter bytes]
//	[index block bytes]
//	[footer: 33 bytes]
//
// Block checksums are appended during SSTable construction (builder.go)
// and verified on read (reader.go). The block itself (block.go) does
// not know about its checksum.
// Footer layout (fixed 33 bytes):
//
//	[bloom_offset:  8]
//	[bloom_len:     4]
//	[index_offset:  8]
//	[index_len:     4]
//	[version:       1]
//	[checksum:      4]  CRC32 of the 25 bytes above
//	[magic:         4]  0x4C544442 ("LTDB")
//
// Index block layout:
//
//	[first_key_len: 2][first_key]
//	[index entries...]
//
// Index entry:
//
//	[last_key_len: 2][last_key][block_offset: 8][block_size: 4]

const (
	defaultBlockSize  = 4096
	blockChecksumSize = 4

	footerSize = 33
	magicValue = uint32(0x4C544442) // "LTDB"
	version1   = byte(1)

	indexKeyLenSize    = 2
	indexOffsetSize    = 8
	indexBlockSizeSize = 4

	blockFlagPut       byte = 0
	blockFlagTombstone byte = 1
)

var (
	ErrInvalidMagic    = errors.New("sstable: invalid magic number")
	ErrInvalidChecksum = errors.New("sstable: checksum mismatch")
	ErrInvalidVersion  = errors.New("sstable: unsupported version")
	ErrEmptySSTable    = errors.New("sstable: cannot build empty SSTable")
	ErrCorruptIndex    = errors.New("sstable: corrupt index block")
)

// footer holds the metadata stored at the end of every SSTable file.
type footer struct {
	bloomOffset uint64
	bloomLen    uint32
	indexOffset uint64
	indexLen    uint32
	version     byte
}

func encodeFooter(f footer) []byte {
	buf := make([]byte, footerSize)

	binary.LittleEndian.PutUint64(buf[0:], f.bloomOffset)
	binary.LittleEndian.PutUint32(buf[8:], f.bloomLen)
	binary.LittleEndian.PutUint64(buf[12:], f.indexOffset)
	binary.LittleEndian.PutUint32(buf[20:], f.indexLen)
	buf[24] = f.version

	// Checksum covers the first 25 bytes
	checksum := crc32.ChecksumIEEE(buf[:25])
	binary.LittleEndian.PutUint32(buf[25:], checksum)

	binary.LittleEndian.PutUint32(buf[29:], magicValue)

	return buf
}

func decodeFooter(data []byte) (footer, error) {
	if len(data) < footerSize {
		return footer{}, fmt.Errorf("sstable: footer too short (%d bytes)", len(data))
	}

	d := data[len(data)-footerSize:]

	if m := binary.LittleEndian.Uint32(d[29:]); m != magicValue {
		return footer{}, ErrInvalidMagic
	}

	stored := binary.LittleEndian.Uint32(d[25:29])
	actual := crc32.ChecksumIEEE(d[:25])
	if stored != actual {
		return footer{}, fmt.Errorf("%w: footer", ErrInvalidChecksum)
	}

	f := footer{
		bloomOffset: binary.LittleEndian.Uint64(d[0:]),
		bloomLen:    binary.LittleEndian.Uint32(d[8:]),
		indexOffset: binary.LittleEndian.Uint64(d[12:]),
		indexLen:    binary.LittleEndian.Uint32(d[20:]),
		version:     d[24],
	}

	if f.version != version1 {
		return footer{}, fmt.Errorf("%w: %d", ErrInvalidVersion, f.version)
	}

	return f, nil
}

// blockMeta describes a single data block's position in the SSTable file.
type blockMeta struct {
	offset  uint64
	size    uint32 // size of block data (without checksum)
	lastKey []byte
}

func encodeIndex(firstKey []byte, metas []blockMeta) ([]byte, error) {
	if len(firstKey) > maxKeyLen {
		return nil, fmt.Errorf("%w: first key too large (%d bytes)", ErrCorruptIndex, len(firstKey))
	}

	for i, m := range metas {
		if len(m.lastKey) > maxKeyLen {
			return nil, fmt.Errorf("%w: last key too large at block %d (%d bytes)", ErrCorruptIndex, i, len(m.lastKey))
		}
	}

	// Calculate size: first_key header + entries
	size := indexKeyLenSize + len(firstKey)
	for _, m := range metas {
		size += indexKeyLenSize + len(m.lastKey) + indexOffsetSize + indexBlockSizeSize
	}

	buf := make([]byte, 0, size)

	// First key
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(firstKey)))
	buf = append(buf, firstKey...)

	// Block entries
	for _, m := range metas {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(m.lastKey)))
		buf = append(buf, m.lastKey...)
		buf = binary.LittleEndian.AppendUint64(buf, m.offset)
		buf = binary.LittleEndian.AppendUint32(buf, m.size)
	}

	return buf, nil
}

func decodeIndex(data []byte) (firstKey []byte, metas []blockMeta, err error) {
	if len(data) < indexKeyLenSize {
		return nil, nil, fmt.Errorf("%w: too short", ErrCorruptIndex)
	}

	offset := 0

	// First key
	fkLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += indexKeyLenSize

	if offset+fkLen > len(data) {
		return nil, nil, fmt.Errorf("%w: first key truncated", ErrCorruptIndex)
	}
	firstKey = make([]byte, fkLen)
	copy(firstKey, data[offset:offset+fkLen])
	offset += fkLen

	// Block entries
	var prevLastKey []byte
	var prevEndOffset uint64

	for offset < len(data) {
		if offset+indexKeyLenSize > len(data) {
			return nil, nil, fmt.Errorf("%w: entry truncated at offset %d", ErrCorruptIndex, offset)
		}

		lkLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += indexKeyLenSize

		if offset+lkLen+indexOffsetSize+indexBlockSizeSize > len(data) {
			return nil, nil, fmt.Errorf("%w: entry truncated at offset %d", ErrCorruptIndex, offset)
		}

		lastKey := make([]byte, lkLen)
		copy(lastKey, data[offset:offset+lkLen])
		offset += lkLen

		blockOffset := binary.LittleEndian.Uint64(data[offset:])
		offset += indexOffsetSize

		blockSize := binary.LittleEndian.Uint32(data[offset:])
		offset += indexBlockSizeSize

		// Validate: block size must be non-zero
		if blockSize == 0 {
			return nil, nil, fmt.Errorf("%w: block  %d has zero size", ErrCorruptIndex, len(metas))
		}

		// Validate: block offsets must be non-overlapping and increasing
		if len(metas) > 0 {
			if blockOffset < prevEndOffset {
				return nil, nil, fmt.Errorf("%w: block %d offset %d overlaps previous block ending at %d",
					ErrCorruptIndex, len(metas), blockOffset, prevEndOffset)
			}
		}

		// Validate: last keys must be strictly increasing
		if prevLastKey != nil && bytes.Compare(lastKey, prevLastKey) <= 0 {
			return nil, nil, fmt.Errorf("%w: block %d last key %q not after previous %q",
				ErrCorruptIndex, len(metas), lastKey, prevLastKey)
		}

		metas = append(metas, blockMeta{
			offset:  blockOffset,
			size:    blockSize,
			lastKey: lastKey,
		})

		prevLastKey = lastKey
		prevEndOffset = blockOffset + uint64(blockSize) + blockChecksumSize
	}

	return firstKey, metas, nil
}
