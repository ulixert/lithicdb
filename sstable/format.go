package sstable

import (
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
	defaultBlockSize = 4096
	checksumSize     = 4

	footerSize = 33
	magicValue = uint32(0x4C544442) // "LTDB"
	version1   = byte(1)

	indexKeyLenSize     = 2
	indexOffsetSize     = 8
	indexBlockSizeLen   = 4
	indexEntryFixedSize = indexKeyLenSize + indexOffsetSize + indexBlockSizeLen

	blockFlagPut       byte = 0
	blockFlagTombstone byte = 1
)

var (
	ErrInvalidMagic    = errors.New("sstable: invalid magic number")
	ErrInvalidChecksum = errors.New("sstable: checksum mismatch")
	ErrInvalidVersion  = errors.New("sstable: unsupported version")
	ErrEmptySSTable    = errors.New("sstable: cannot build empty SSTable")
	ErrKeyNotFound     = errors.New("sstable: key not found")
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
		return footer{}, ErrInvalidChecksum
	}

	f := footer{
		bloomOffset: binary.LittleEndian.Uint64(d[0:]),
		bloomLen:    binary.LittleEndian.Uint32(d[8:]),
		indexOffset: binary.LittleEndian.Uint64(d[12:]),
		indexLen:    binary.LittleEndian.Uint32(d[20:]),
		version:     d[24],
	}

	if f.version != version1 {
		return footer{}, ErrInvalidVersion
	}

	return f, nil
}

// blockMeta describes a single data block's position in the SSTable file.
type blockMeta struct {
	offset  uint64
	size    uint32
	lastKey []byte
}

func encodeIndex(firstKey []byte, metas []blockMeta) []byte {
	// Calculate size: first_key header + entries
	size := indexKeyLenSize + len(firstKey)
	for _, m := range metas {
		size += indexEntryFixedSize + len(m.lastKey)
	}

	buf := make([]byte, 0, size)

	// First key
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(firstKey)))
	buf = append(buf, firstKey...)

	// Block entries
	for _, m := range metas {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(m.lastKey)))
		buf = append(buf, m.lastKey...)
		buf = appendUint64(buf, m.offset)
		buf = appendUint32(buf, m.size)
	}

	return buf
}

// Help functions to avoid the verbosity of binary.LittleEndian
func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func appendUint32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}
