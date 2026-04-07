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
//	5 = HNSWSnapshot   { collection_name_len: 2, collection_name, seq: 8, filename_len: 2, filename }

const (
	// Record header layout
	checksumSize     = 4
	lengthSize       = 4
	recordHeaderSize = checksumSize + lengthSize // 8
	recordTypeSize   = 1

	// Record types
	typeSSTableAdded   byte = 1
	typeSSTableRemoved byte = 2
	typeSnapshot       byte = 3
	typeNextIDs        byte = 4
	typeHNSWSnapshot   byte = 5

	// Field sizes within payloads
	idSize            = 8  // uint64 SSTable ID
	levelSize         = 1  // uint8 level number
	keyLenSize        = 2  // uint16 key length prefix
	nextIDSize        = 16 // two uint64s (nextMemID + nextSeq)
	snapshotCountSize = 4  // uint32 entry count

	// Minimum size of an SSTableInfo entry (no key data)
	minInfoSize = idSize + levelSize + keyLenSize
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

	// Snapshot (type 3): captures the full state
	SSTables      []SSTableInfo
	HNSWSnapshots map[string]HNSWSnapshotInfo

	// NextIDs
	NextMemID uint64
	NextSeq   uint64

	// HNSWSnapshot
	HNSWCollection string
	HNSWSeq        uint64
	HNSWFilename   string
}

func encodeRecord(r Record) ([]byte, error) {
	var payload []byte

	switch r.Type {
	case typeSSTableAdded:
		payload = encodeSSTableInfo(r.SSTable)
	case typeSSTableRemoved:
		payload = encodeSSTableRemoved(r.SSTable)
	case typeSnapshot:
		payload = encodeSnapshot(r.SSTables, r.HNSWSnapshots)
	case typeNextIDs:
		payload = encodeNextIDs(r.NextMemID, r.NextSeq)
	case typeHNSWSnapshot:
		payload = encodeHNSWSnapshot(r.HNSWCollection, r.HNSWSeq, r.HNSWFilename)
	default:
		return nil, fmt.Errorf("manifest: unknown record type %d", r.Type)
	}

	totalPayload := recordTypeSize + len(payload)
	buf := make([]byte, recordHeaderSize+totalPayload)

	// Length field (after checksum)
	binary.LittleEndian.PutUint32(buf[checksumSize:recordHeaderSize], uint32(totalPayload))

	// Type
	buf[recordHeaderSize] = r.Type

	// Payload
	copy(buf[recordHeaderSize+recordTypeSize:], payload)

	// Checksum over everything after the checksum field
	checksum := crc32.ChecksumIEEE(buf[checksumSize:])
	binary.LittleEndian.PutUint32(buf[:checksumSize], checksum)

	return buf, nil
}

func decodeRecord(data []byte) (Record, int, error) {
	if len(data) < recordHeaderSize {
		return Record{}, 0, ErrShortRecord
	}

	payloadLen := binary.LittleEndian.Uint32(data[checksumSize:recordHeaderSize])
	if payloadLen < recordTypeSize {
		return Record{}, 0, ErrShortRecord
	}

	totalLen := recordHeaderSize + int(payloadLen)
	if len(data) < totalLen {
		return Record{}, 0, ErrShortRecord
	}

	storedChecksum := binary.LittleEndian.Uint32(data[:checksumSize])
	actualChecksum := crc32.ChecksumIEEE(data[checksumSize:totalLen])
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
		r.SSTable, err = decodeSSTableInfo(payload)
	case typeSSTableRemoved:
		r.SSTable, err = decodeSSTableRemoved(payload)
	case typeSnapshot:
		r.SSTables, r.HNSWSnapshots, err = decodeSnapshot(payload)
	case typeNextIDs:
		r.NextMemID, r.NextSeq, err = decodeNextIDs(payload)
	case typeHNSWSnapshot:
		r.HNSWCollection, r.HNSWSeq, r.HNSWFilename, err = decodeHNSWSnapshot(payload)
	default:
		return Record{}, 0, ErrUnknownType
	}

	if err != nil {
		return Record{}, 0, err
	}

	return r, totalLen, nil
}

// --- SSTableInfo encode/decode (shared by SSTableAdded and Snapshot) ---

// encodeSSTableInfo appends a single SSTableInfo to buf and returns
// the extended slice. Used by both SSTableAdded and Snapshot encoding.
func encodeSSTableInfo(info SSTableInfo) []byte {
	size := idSize + levelSize + keyLenSize + len(info.FirstKey) + keyLenSize + len(info.LastKey)
	buf := make([]byte, 0, size)
	return appendSSTableInfo(buf, info)
}

func appendSSTableInfo(buf []byte, info SSTableInfo) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, info.ID)
	buf = append(buf, info.Level)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(info.FirstKey)))
	buf = append(buf, info.FirstKey...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(info.LastKey)))
	buf = append(buf, info.LastKey...)
	return buf
}

// decodeSSTableInfo reads a single SSTableInfo starting at data[0].
// Returns the decoded info and the number of bytes consumed.
func decodeSSTableInfo(data []byte) (SSTableInfo, error) {
	info, _, err := decodeSSTableInfoAt(data)
	return info, err
}

func decodeSSTableInfoAt(data []byte) (SSTableInfo, int, error) {
	if len(data) < minInfoSize {
		return SSTableInfo{}, 0, ErrShortRecord
	}

	offset := 0
	id := binary.LittleEndian.Uint64(data[offset:])
	offset += idSize

	level := data[offset]
	offset += levelSize

	fkLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+fkLen > len(data) {
		return SSTableInfo{}, 0, ErrShortRecord
	}
	firstKey := make([]byte, fkLen)
	copy(firstKey, data[offset:offset+fkLen])
	offset += fkLen

	if offset+keyLenSize > len(data) {
		return SSTableInfo{}, 0, ErrShortRecord
	}
	lkLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+lkLen > len(data) {
		return SSTableInfo{}, 0, ErrShortRecord
	}
	lastKey := make([]byte, lkLen)
	copy(lastKey, data[offset:offset+lkLen])
	offset += lkLen

	return SSTableInfo{
		ID:       id,
		Level:    level,
		FirstKey: firstKey,
		LastKey:  lastKey,
	}, offset, nil
}

// --- SSTableRemoved ---

func encodeSSTableRemoved(info SSTableInfo) []byte {
	buf := make([]byte, 0, idSize+levelSize)
	buf = binary.LittleEndian.AppendUint64(buf, info.ID)
	buf = append(buf, info.Level)
	return buf
}

func decodeSSTableRemoved(data []byte) (SSTableInfo, error) {
	if len(data) < idSize+levelSize {
		return SSTableInfo{}, ErrShortRecord
	}
	return SSTableInfo{
		ID:    binary.LittleEndian.Uint64(data[:idSize]),
		Level: data[idSize],
	}, nil
}

// --- Snapshot ---

// encodeSnapshot encodes the full manifest state: SSTables + HNSW snapshots.
// Format: [sstable_count:4][sstables...][hnsw_count:4][hnsw_entries...]
func encodeSnapshot(tables []SSTableInfo, hnswSnaps map[string]HNSWSnapshotInfo) []byte {
	size := snapshotCountSize
	for _, t := range tables {
		size += idSize + levelSize + keyLenSize + len(t.FirstKey) + keyLenSize + len(t.LastKey)
	}
	// HNSW section
	size += snapshotCountSize
	for _, h := range hnswSnaps {
		size += keyLenSize + len(h.Collection) + idSize + keyLenSize + len(h.Filename)
	}

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(tables)))
	for _, t := range tables {
		buf = appendSSTableInfo(buf, t)
	}

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(hnswSnaps)))
	for _, h := range hnswSnaps {
		buf = append(buf, encodeHNSWSnapshot(h.Collection, h.Seq, h.Filename)...)
	}

	return buf
}

// decodeSnapshot decodes the full manifest state. Backward-compatible:
// if no HNSW section is present (old format), returns an empty map.
func decodeSnapshot(data []byte) ([]SSTableInfo, map[string]HNSWSnapshotInfo, error) {
	if len(data) < snapshotCountSize {
		return nil, nil, ErrShortRecord
	}

	count := int(binary.LittleEndian.Uint32(data[:snapshotCountSize]))
	offset := snapshotCountSize

	tables := make([]SSTableInfo, count)
	for i := 0; i < count; i++ {
		info, n, err := decodeSSTableInfoAt(data[offset:])
		if err != nil {
			return nil, nil, err
		}
		tables[i] = info
		offset += n
	}

	// Parse HNSW section if present (backward-compatible with old snapshots).
	hnswSnaps := make(map[string]HNSWSnapshotInfo)
	if offset+snapshotCountSize <= len(data) {
		hnswCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += snapshotCountSize
		for i := 0; i < hnswCount; i++ {
			col, seq, filename, err := decodeHNSWSnapshot(data[offset:])
			if err != nil {
				return nil, nil, err
			}
			hnswSnaps[col] = HNSWSnapshotInfo{
				Collection: col,
				Seq:        seq,
				Filename:   filename,
			}
			// Advance offset past this entry.
			offset += keyLenSize + len(col) + idSize + keyLenSize + len(filename)
		}
	}

	return tables, hnswSnaps, nil
}

// --- NextIDs ---

func encodeNextIDs(nextMemID, nextSeq uint64) []byte {
	buf := make([]byte, nextIDSize)
	binary.LittleEndian.PutUint64(buf[:idSize], nextMemID)
	binary.LittleEndian.PutUint64(buf[idSize:], nextSeq)
	return buf
}

func decodeNextIDs(data []byte) (nextMemID, nextSeq uint64, err error) {
	if len(data) < nextIDSize {
		return 0, 0, ErrShortRecord
	}
	return binary.LittleEndian.Uint64(data[:idSize]),
		binary.LittleEndian.Uint64(data[idSize:]),
		nil
}

// --- HNSWSnapshot ---
// Payload: [collection_name_len:2][collection_name][seq:8][filename_len:2][filename]

func encodeHNSWSnapshot(collection string, seq uint64, filename string) []byte {
	size := keyLenSize + len(collection) + idSize + keyLenSize + len(filename)
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(collection)))
	buf = append(buf, collection...)
	buf = binary.LittleEndian.AppendUint64(buf, seq)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(filename)))
	buf = append(buf, filename...)
	return buf
}

func decodeHNSWSnapshot(data []byte) (collection string, seq uint64, filename string, err error) {
	const minSize = keyLenSize + idSize + keyLenSize // 2 + 8 + 2 = 12
	if len(data) < minSize {
		return "", 0, "", ErrShortRecord
	}

	offset := 0
	colLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+colLen > len(data) {
		return "", 0, "", ErrShortRecord
	}
	collection = string(data[offset : offset+colLen])
	offset += colLen

	if offset+idSize > len(data) {
		return "", 0, "", ErrShortRecord
	}
	seq = binary.LittleEndian.Uint64(data[offset:])
	offset += idSize

	if offset+keyLenSize > len(data) {
		return "", 0, "", ErrShortRecord
	}
	fnLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += keyLenSize
	if offset+fnLen > len(data) {
		return "", 0, "", ErrShortRecord
	}
	filename = string(data[offset : offset+fnLen])

	return collection, seq, filename, nil
}
