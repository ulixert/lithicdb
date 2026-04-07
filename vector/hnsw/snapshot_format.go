package hnsw

import "encoding/binary"

// HNSW snapshot binary format (all integers little-endian).
//
// File:
//   [node_0][node_1]...[node_n-1][header][footer]
//
// Node:
//   [id:8 LE]
//   [external_id:16]
//   [level:1]
//   [dim:2 LE]
//   [vector_data: dim * 4 bytes float32 LE]
//   per layer:
//     [num_neighbors:2 LE]
//     [neighbor_ids: num_neighbors * 8 bytes LE]
//
// Header:
//   [num_nodes:8 LE]
//   [entry_point:8 LE]
//   [max_level:1]
//   [M:2 LE]
//   [dim:2 LE]
//   [metric:1]
//   [snapshot_seq:8 LE]
//
// Footer (33 bytes):
//   [header_offset:8 LE]            // absolute file offset of header start
//   [header_len:4 LE]               // header size in bytes
//   [reserved:12]                   // zero for now
//   [version:1]                     // snapshot format version
//   [crc32:4 LE]                    // CRC32 over [node_0 ... node_n-1][header]
//   [magic:4 LE]                    // 0x484E5357 = "HNSW"

const (
	snapshotVersion byte   = 1
	snapshotMagic   uint32 = 0x484E5357 // "HNSW"

	// Footer layout: headerOffset(8) + headerLen(4) + reserved(12) + version(1) + crc32(4) + magic(4)
	footerSize = 33

	// Header layout: numNodes(8) + entryPoint(8) + maxLevel(1) + M(2) + dim(2) + metric(1) + seq(8)
	headerFixedSize = 30
)

// snapshotHeader is the metadata block written after all nodes.
type snapshotHeader struct {
	numNodes   uint64
	entryPoint uint64
	maxLevel   uint8
	m          uint16
	dim        uint16
	metric     uint8
	seq        uint64
}

func (h *snapshotHeader) encode(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:], h.numNodes)
	binary.LittleEndian.PutUint64(buf[8:], h.entryPoint)
	buf[16] = h.maxLevel
	binary.LittleEndian.PutUint16(buf[17:], h.m)
	binary.LittleEndian.PutUint16(buf[19:], h.dim)
	buf[21] = h.metric
	binary.LittleEndian.PutUint64(buf[22:], h.seq)
}

func decodeSnapshotHeader(buf []byte) snapshotHeader {
	return snapshotHeader{
		numNodes:   binary.LittleEndian.Uint64(buf[0:]),
		entryPoint: binary.LittleEndian.Uint64(buf[8:]),
		maxLevel:   buf[16],
		m:          binary.LittleEndian.Uint16(buf[17:]),
		dim:        binary.LittleEndian.Uint16(buf[19:]),
		metric:     buf[21],
		seq:        binary.LittleEndian.Uint64(buf[22:]),
	}
}

// snapshotFooter is the last 33 bytes of a snapshot file.
type snapshotFooter struct {
	headerOffset uint64
	headerLen    uint32
	// 12 bytes reserved
	version  byte
	crc32Val uint32
	magic    uint32
}

func (f *snapshotFooter) encode(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:], f.headerOffset)
	binary.LittleEndian.PutUint32(buf[8:], f.headerLen)
	// reserved bytes [12:24] stay zero
	buf[24] = f.version
	binary.LittleEndian.PutUint32(buf[25:], f.crc32Val)
	binary.LittleEndian.PutUint32(buf[29:], f.magic)
}

func decodeSnapshotFooter(buf []byte) snapshotFooter {
	return snapshotFooter{
		headerOffset: binary.LittleEndian.Uint64(buf[0:]),
		headerLen:    binary.LittleEndian.Uint32(buf[8:]),
		version:      buf[24],
		crc32Val:     binary.LittleEndian.Uint32(buf[25:]),
		magic:        binary.LittleEndian.Uint32(buf[29:]),
	}
}
