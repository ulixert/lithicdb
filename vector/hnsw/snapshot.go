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

// ReadSnapshot deserializes a snapshot from r. It validates the magic,
// version, and CRC32 checksum. Returns ErrCorruptSnapshot if the
// checksum doesn't match, ErrInvalidSnapshot for structural errors.
func ReadSnapshot(r io.ReaderAt, size int64) (*SnapshotData, error) {
	if size < int64(footerSize) {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", ErrInvalidSnapshot, size)
	}

	// Read footer.
	var ftrBuf [footerSize]byte
	if _, err := r.ReadAt(ftrBuf[:], size-int64(footerSize)); err != nil {
		return nil, fmt.Errorf("%w: read footer: %v", ErrInvalidSnapshot, err)
	}
	ftr := decodeSnapshotFooter(ftrBuf[:])

	if ftr.magic != snapshotMagic {
		return nil, fmt.Errorf("%w: bad magic 0x%08X", ErrInvalidSnapshot, ftr.magic)
	}
	if ftr.version != snapshotVersion {
		return nil, fmt.Errorf("%w: version %d", ErrSnapshotVersion, ftr.version)
	}

	// Validate footer consistency.
	expectedSize := int64(ftr.headerOffset) + int64(ftr.headerLen) + int64(footerSize)
	if expectedSize != size {
		return nil, fmt.Errorf("%w: size mismatch (expected %d, got %d)", ErrInvalidSnapshot, expectedSize, size)
	}

	// Read all data covered by CRC (nodes + header).
	crcLen := int64(ftr.headerOffset) + int64(ftr.headerLen)
	data := make([]byte, crcLen)
	if _, err := r.ReadAt(data, 0); err != nil {
		return nil, fmt.Errorf("%w: read data: %v", ErrInvalidSnapshot, err)
	}

	// Validate CRC.
	if crc32.ChecksumIEEE(data) != ftr.crc32Val {
		return nil, ErrCorruptSnapshot
	}

	// Parse header.
	if int64(ftr.headerLen) < headerFixedSize {
		return nil, fmt.Errorf("%w: header too small", ErrInvalidSnapshot)
	}
	hdr := decodeSnapshotHeader(data[ftr.headerOffset:])

	// Parse nodes.
	dim := int(hdr.dim)
	nodes := make([]*Node, 0, hdr.numNodes)
	offset := 0
	nodeData := data[:ftr.headerOffset]

	for i := uint64(0); i < hdr.numNodes; i++ {
		node, n, err := readNode(nodeData[offset:], dim)
		if err != nil {
			return nil, fmt.Errorf("%w: node %d: %v", ErrInvalidSnapshot, i, err)
		}
		nodes = append(nodes, node)
		offset += n
	}

	if offset != int(ftr.headerOffset) {
		return nil, fmt.Errorf("%w: node data size mismatch (read %d, expected %d)", ErrInvalidSnapshot, offset, ftr.headerOffset)
	}

	return &SnapshotData{
		Nodes:      nodes,
		EntryPoint: hdr.entryPoint,
		MaxLevel:   int(hdr.maxLevel),
		M:          int(hdr.m),
		Dim:        dim,
		Metric:     hdr.metric,
		Seq:        hdr.seq,
	}, nil
}

// readNode deserializes a single node from buf. Returns the node and
// the number of bytes consumed.
func readNode(buf []byte, dim int) (*Node, int, error) {
	// Minimum: id(8) + externalID(16) + level(1) + dim(2) + vector(dim*4)
	minSize := 8 + 16 + 1 + 2 + dim*4
	if len(buf) < minSize {
		return nil, 0, fmt.Errorf("truncated node (need %d, have %d)", minSize, len(buf))
	}

	off := 0

	id := binary.LittleEndian.Uint64(buf[off:])
	off += 8

	var externalID [16]byte
	copy(externalID[:], buf[off:off+16])
	off += 16

	level := int(buf[off])
	off++

	nodeDim := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2
	if nodeDim != dim {
		return nil, 0, fmt.Errorf("dim mismatch (node %d, header %d)", nodeDim, dim)
	}

	vec := make([]float32, dim)
	for j := 0; j < dim; j++ {
		vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
	}

	numLayers := level + 1
	neighbors := make([][]uint64, numLayers)
	for l := 0; l < numLayers; l++ {
		if off+2 > len(buf) {
			return nil, 0, fmt.Errorf("truncated neighbor count at layer %d", l)
		}
		count := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+count*8 > len(buf) {
			return nil, 0, fmt.Errorf("truncated neighbors at layer %d", l)
		}
		nbs := make([]uint64, count)
		for k := 0; k < count; k++ {
			nbs[k] = binary.LittleEndian.Uint64(buf[off:])
			off += 8
		}
		neighbors[l] = nbs
	}

	return &Node{
		ID:         id,
		ExternalID: externalID,
		Vector:     vec,
		Level:      level,
		Neighbors:  neighbors,
	}, off, nil
}

// RestoreFromSnapshot populates the graph from deserialized snapshot data.
// Must be called before the graph is shared (no lock acquired). Validates
// that dim and M match the graph's options.
func (g *Graph) RestoreFromSnapshot(data *SnapshotData) error {
	if data.Dim != g.opts.Dim {
		return fmt.Errorf("hnsw: snapshot dim %d != graph dim %d", data.Dim, g.opts.Dim)
	}
	if data.M != g.opts.M {
		return fmt.Errorf("hnsw: snapshot M %d != graph M %d", data.M, g.opts.M)
	}

	g.nodes = make(map[uint64]*Node, len(data.Nodes))
	g.memoryBytes = 0
	for _, node := range data.Nodes {
		g.nodes[node.ID] = node
		g.memoryBytes += g.estimateNodeMemory(node.Level)
	}
	g.entryPoint = data.EntryPoint
	g.maxLevel = data.MaxLevel
	g.tombstones = make(map[uint64]bool)

	return nil
}
