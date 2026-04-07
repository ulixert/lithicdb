package hnsw

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
)

var (
	ErrCorruptSnapshot = errors.New("hnsw: corrupt snapshot (checksum mismatch)")
	ErrInvalidSnapshot = errors.New("hnsw: invalid snapshot format")
	ErrSnapshotVersion = errors.New("hnsw: unsupported snapshot version")
)

// SnapshotData holds the deserialized contents of a snapshot file.
type SnapshotData struct {
	Nodes      []*Node
	EntryPoint uint64
	MaxLevel   int
	M          int
	Dim        int
	Metric     uint8
	Seq        uint64
}

// WriteSnapshot serializes the graph to w in the snapshot binary format.
// The graph's RLock is held for the duration of the write (blocks writers,
// not readers). The caller is responsible for file I/O durability (fsync,
// atomic rename). CRC32 covers all bytes before the footer.
func (g *Graph) WriteSnapshot(w io.Writer, seq uint64, metric uint8) error {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.nodes) == 0 {
		return ErrEmptyGraph
	}

	// Collect and sort node IDs for deterministic output.
	ids := make([]uint64, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	// Write nodes.
	dim := g.opts.Dim
	buf := make([]byte, 8) // reusable scratch for writing integers
	for _, id := range ids {
		node := g.nodes[id]
		if err := writeNode(mw, node, dim, buf); err != nil {
			return fmt.Errorf("hnsw: write node %d: %w", id, err)
		}
	}

	// Write header.
	hdr := snapshotHeader{
		numNodes:   uint64(len(g.nodes)),
		entryPoint: g.entryPoint,
		maxLevel:   uint8(g.maxLevel),
		m:          uint16(g.opts.M),
		dim:        uint16(dim),
		metric:     metric,
		seq:        seq,
	}
	var hdrBuf [headerFixedSize]byte
	hdr.encode(hdrBuf[:])
	if _, err := mw.Write(hdrBuf[:]); err != nil {
		return fmt.Errorf("hnsw: write header: %w", err)
	}

	// Recompute headerOffset: sum of all node byte sizes.
	var offset uint64
	for _, id := range ids {
		offset += uint64(nodeByteSize(dim, g.nodes[id]))
	}

	// Write footer (not through hasher - CRC covers only nodes+header).
	checksum := hasher.Sum32()
	var ftr [footerSize]byte
	(&snapshotFooter{
		headerOffset: offset,
		headerLen:    headerFixedSize,
		version:      snapshotVersion,
		crc32Val:     checksum,
		magic:        snapshotMagic,
	}).encode(ftr[:])

	if _, err := w.Write(ftr[:]); err != nil {
		return fmt.Errorf("hnsw: write footer: %w", err)
	}

	return nil
}

// writeNode serializes a single node to w. buf must be at least 8 bytes.
func writeNode(w io.Writer, node *Node, dim int, buf []byte) error {
	// id(8)
	binary.LittleEndian.PutUint64(buf, node.ID)
	if _, err := w.Write(buf[:8]); err != nil {
		return err
	}
	// externalID(16)
	if _, err := w.Write(node.ExternalID[:]); err != nil {
		return err
	}
	// level(1)
	if _, err := w.Write([]byte{byte(node.Level)}); err != nil {
		return err
	}
	// dim(2)
	binary.LittleEndian.PutUint16(buf, uint16(dim))
	if _, err := w.Write(buf[:2]); err != nil {
		return err
	}
	// vector: dim * 4 bytes
	for _, f := range node.Vector {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		if _, err := w.Write(buf[:4]); err != nil {
			return err
		}
	}
	// per layer: num_neighbors(2) + neighbor_ids(N*8)
	for l := 0; l <= node.Level; l++ {
		neighbors := node.Neighbors[l]
		binary.LittleEndian.PutUint16(buf, uint16(len(neighbors)))
		if _, err := w.Write(buf[:2]); err != nil {
			return err
		}
		for _, nid := range neighbors {
			binary.LittleEndian.PutUint64(buf, nid)
			if _, err := w.Write(buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

// nodeByteSize returns the serialized byte size of a single node.
func nodeByteSize(dim int, node *Node) int {
	// id(8) + externalID(16) + level(1) + dim(2) + vector(dim*4)
	size := 8 + 16 + 1 + 2 + dim*4
	for l := 0; l <= node.Level; l++ {
		// num_neighbors(2) + neighbor_ids(N*8)
		size += 2 + len(node.Neighbors[l])*8
	}
	return size
}
