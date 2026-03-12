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
//	[count:    2 bytes]  number of entries in this record
//	[entries...]
//
// Entry format:
//
//	[flag:      1 byte]   0 = put, 1 = tombstone
//	[key_len:   2 bytes]  max key size: 64KB
//	[value_len: 4 bytes]  max value size: 4GB (0 for tombstones)
//	[key]
//	[value]               omitted when flag = tombstone

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

// decodeRecord deserializes a WAL record from a byte slice that
// includes the checksum and length header.
// Returns the entries and the number of bytes consumed.
func decodeRecord(data []byte) ([]Entry, int, error) {
	if len(data) < headerSize {
		return nil, 0, ErrShortRecord
	}

	// Read and verify checksum
	storedChecksum := binary.LittleEndian.Uint32(data[:checksumSize])
	payloadLen := binary.LittleEndian.Uint32(data[checksumSize:headerSize])

	totalLen := headerSize + int(payloadLen)
	if len(data) < totalLen {
		return nil, 0, ErrShortRecord
	}

	actualChecksum := crc32.ChecksumIEEE(data[checksumSize:totalLen])
	if storedChecksum != actualChecksum {
		return nil, 0, ErrCorruptRecord
	}

	// Read entry count
	offset := headerSize
	count := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += countSize

	// Decode entries
	entries := make([]Entry, count)
	for i := 0; i < count; i++ {
		if offset+entryHeader > totalLen {
			return nil, 0, ErrShortRecord
		}

		flag := data[offset]
		offset += flagSize

		keyLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += keyLenSize

		valueLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += valueLenSize

		if offset+keyLen > totalLen {
			return nil, 0, ErrShortRecord
		}

		key := make([]byte, keyLen)
		copy(key, data[offset:offset+keyLen])
		offset += keyLen

		var value kv.Value
		switch flag {
		case flagPut:
			if offset+valueLen > totalLen {
				return nil, 0, ErrShortRecord
			}
			valData := make([]byte, valueLen)
			copy(valData, data[offset:offset+valueLen])
			offset += valueLen
			value = kv.NewValue(valData)
		case flagTombstone:
			value = kv.NewTombstone()
		default:
			return nil, 0, ErrInvalidFlag
		}

		entries[i] = Entry{Key: key, Value: value}
	}

	return entries, totalLen, nil
}
