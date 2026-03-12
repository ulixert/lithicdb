package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/ulixert/lithicdb/kv"
)

// WAL record format:
//
//	[checksum: 4 bytes]  CRC32 of everything after checksum
//	[length:   4 bytes]  byte length of (count + entries)
//	[count:    2 bytes]  number of entries
//	[entries...]
//
// Entry format:
//
//	[flag:      1 byte]   0 = put, 1 = tombstone
//	[key_len:   2 bytes]  max key size: 64KB
//	[value_len: 4 bytes]  max value size: 4GB (0 for tombstones)
//	[key]
//	[value]               omitted for tombstones

const (
	checksumSize = 4
	lengthSize   = 4
	countSize    = 2
	headerSize   = checksumSize + lengthSize // 8 bytes before payload

	flagSize     = 1
	keyLenSize   = 2
	valueLenSize = 4
	entryHeader  = flagSize + keyLenSize + valueLenSize // 7 bytes per entry

	flagPut       byte = 0
	flagTombstone byte = 1
)

var (
	ErrCorruptRecord = errors.New("wal: corrupt record (checksum mismatch)")
	ErrShortRecord   = errors.New("wal: record too short")
	ErrInvalidFlag   = errors.New("wal: invalid entry flag")
)

// Entry represents a single key-value operation in the WAL.
type Entry struct {
	Key   []byte
	Value kv.Value
}

// encodeRecord serializes a batch of entries into a single WAL record.
// The returned byte slice includes the checksum and length header.
func encodeRecord(entries []Entry) []byte {
	// Calculate payload size: count + all entries
	payloadSize := countSize
	for _, e := range entries {
		payloadSize += entryHeader + len(e.Key)
		if !e.Value.Tombstone {
			payloadSize += len(e.Value.Data)
		}
	}

	buf := make([]byte, headerSize+payloadSize)

	// Skip checksum for now, write length
	binary.LittleEndian.PutUint32(buf[checksumSize:], uint32(payloadSize))

	// Write entry count
	offset := headerSize
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(entries)))
	offset += countSize

	// Write each entry
	for _, e := range entries {
		if e.Value.Tombstone {
			buf[offset] = flagTombstone
		} else {
			buf[offset] = flagPut
		}
		offset += flagSize

		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(e.Key)))
		offset += keyLenSize

		if e.Value.Tombstone {
			binary.LittleEndian.PutUint32(buf[offset:], 0)
		} else {
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Value.Data)))
		}
		offset += valueLenSize

		copy(buf[offset:], e.Key)
		offset += len(e.Key)

		if !e.Value.Tombstone {
			copy(buf[offset:], e.Value.Data)
			offset += len(e.Value.Data)
		}
	}

	// Compute checksum over everything after the checksum field
	checksum := crc32.ChecksumIEEE(buf[checksumSize:])
	binary.LittleEndian.PutUint32(buf[:checksumSize], checksum)

	return buf
}
