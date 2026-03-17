package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Manifest record format:
//
//	[checksum: 4 bytes]  CRC32 of everything after checksum
//	[length:   4 bytes]  byte length of (type + payload)
//	[type:     1 byte]
//	[payload...]
//
// Record types:
//
//	1 = SSTableAdded   { id: 8, level: 1, first_key_len: 2, first_key, last_key_len: 2, last_key }
//	2 = SSTableRemoved { id: 8, level: 1 }
//	3 = Snapshot       { count: 4, entries[]{ id: 8, level: 1, first_key_len: 2, first_key, last_key_len: 2, last_key } }
//	4 = NextIDs        { next_mem_id: 8, next_seq: 8 }

const (
	recordHeaderSize = 8 // checksum(4) + length(4)
	recordTypeSize   = 1

	typeSSTableAdded   byte = 1
	typeSSTableRemoved byte = 2
	typeSnapshot       byte = 3
	typeNextIDs        byte = 4

	idSize     = 8  // uint64 SSTable ID
	levelSize  = 1  // uint8 level number
	keyLenSize = 2  // uint16 key length prefix
	nextIDSize = 16 // two uint64s (nextMemID + nextSeq)
)

var (
	ErrCorruptRecord = errors.New("manifest: corrupt record (checksum mismatch)")
	ErrShortRecord   = errors.New("manifest: record too short")
	ErrUnknownType   = errors.New("manifest: unknown record type")
)

// SSTableInfo describes an SSTable's identity and key range.
type SSTableInfo struct {
	ID       uint64
	Level    uint8
	FirstKey []byte
	LastKey  []byte
}

// Record is a decoded manifest record.
type Record struct {
	Type byte

	// SSTableAdded / SSTableRemoved
	SSTable SSTableInfo

	// Snapshot
	SSTables []SSTableInfo

	// NextIDs
	NextMemID uint64
	NextSeq   uint64
}

func encodeRecord(r Record) ([]byte, error) {
	var payload []byte

	switch r.Type {
	case typeSSTableAdded:
		payload = encodeSSTableAdded(r.SSTable)
	case typeSSTableRemoved:
		payload = encodeSSTableRemoved(r.SSTable)
	case typeSnapshot:
		payload = encodeSnapshot(r.SSTables)
	case typeNextIDs:
		payload = encodeNextIDs(r.NextMemID, r.NextSeq)
	default:
		return nil, fmt.Errorf("manifest: unknown record type %d", r.Type)
	}

	totalPayload := recordTypeSize + len(payload)
	buf := make([]byte, recordHeaderSize+totalPayload)

	// Skip checksum, write length
	binary.LittleEndian.PutUint32(buf[4:8], uint32(totalPayload))

	// Write type
	buf[recordHeaderSize] = r.Type

	// Write payload
	copy(buf[recordHeaderSize+recordTypeSize:], payload)

	// Compute checksum over everything after the checksum field
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], checksum)

	return buf, nil
}

func decodeRecord(data []byte) (Record, int, error) {
	if len(data) < recordHeaderSize {
		return Record{}, 0, ErrShortRecord
	}

	payloadLen := binary.LittleEndian.Uint32(data[4:8])
	if payloadLen < recordTypeSize {
		return Record{}, 0, ErrShortRecord
	}

	totalLen := recordHeaderSize + int(payloadLen)
	if len(data) < totalLen {
		return Record{}, 0, ErrShortRecord
	}

	storedChecksum := binary.LittleEndian.Uint32(data[:4])
	actualChecksum := crc32.ChecksumIEEE(data[4:totalLen])
	if storedChecksum != actualChecksum {
		return Record{}, 0, ErrCorruptRecord
	}

	recType := data[recordHeaderSize]
	payload := data[recordHeaderSize+recordTypeSize : totalLen]

	var r Record
	r.Type = recType

	var err error
	switch recType {
	case typeSSTableAdded:
		r.SSTable, err = decodeSSTableAdded(payload)
	case typeSSTableRemoved:
		r.SSTable, err = decodeSSTableRemoved(payload)
	case typeSnapshot:
		r.SSTables, err = decodeSnapshot(payload)
	case typeNextIDs:
		r.NextMemID, r.NextSeq, err = decodeNextIDs(payload)
	default:
		return Record{}, 0, ErrUnknownType
	}

	if err != nil {
		return Record{}, 0, err
	}

	return r, totalLen, nil
}

// --- SSTableAdded ---

func encodeSSTableAdded(info SSTableInfo) []byte {
	size := idSize + levelSize + keyLenSize + len(info.FirstKey) + keyLenSize + len(info.LastKey)
	buf := make([]byte, 0, size)

	buf = binary.LittleEndian.AppendUint64(buf, info.ID)
	buf = append(buf, info.Level)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(info.FirstKey)))
	buf = append(buf, info.FirstKey...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(info.LastKey)))
	buf = append(buf, info.LastKey...)

	return buf
}

func decodeSSTableAdded(data []byte) (SSTableInfo, error) {
	if len(data) < idSize+levelSize+keyLenSize {
		return SSTableInfo{}, ErrShortRecord
	}

	offset := 0
	id := binary.LittleEndian.Uint64(data[offset:])
	offset += idSize

	level := data[offset]
	offset += levelSize

	fkLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+fkLen > len(data) {
		return SSTableInfo{}, ErrShortRecord
	}
	firstKey := make([]byte, fkLen)
	copy(firstKey, data[offset:offset+fkLen])
	offset += fkLen

	if offset+keyLenSize > len(data) {
		return SSTableInfo{}, ErrShortRecord
	}
	lkLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+lkLen > len(data) {
		return SSTableInfo{}, ErrShortRecord
	}
	lastKey := make([]byte, lkLen)
	copy(lastKey, data[offset:offset+lkLen])

	return SSTableInfo{
		ID:       id,
		Level:    level,
		FirstKey: firstKey,
		LastKey:  lastKey,
	}, nil
}
