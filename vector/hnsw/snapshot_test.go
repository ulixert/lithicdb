package hnsw

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func newTestGraph(t *testing.T, dim, m, n int) *Graph {
	t.Helper()
	g, err := New(Options{
		M:           m,
		EfConstruct: 100,
		EfSearch:    50,
		Dim:         dim,
		Dist:        DistanceL2Squared,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		var extID [16]byte
		extID[0] = byte(i)
		extID[1] = byte(i >> 8)
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = float32(i*dim + j)
		}
		if err := g.Insert(uint64(i), extID, vec); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	return g
}

func snapshotRoundTrip(t *testing.T, g *Graph, seq uint64, metric uint8) *SnapshotData {
	t.Helper()
	var buf bytes.Buffer
	if err := g.WriteSnapshot(&buf, seq, metric); err != nil {
		t.Fatal("WriteSnapshot:", err)
	}
	data, err := ReadSnapshot(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal("ReadSnapshot:", err)
	}
	return data
}

func TestSnapshot_RoundTrip(t *testing.T) {
	const dim, m, n = 4, 8, 100
	g := newTestGraph(t, dim, m, n)

	data := snapshotRoundTrip(t, g, 42, 1)

	if len(data.Nodes) != n {
		t.Fatalf("node count: got %d, want %d", len(data.Nodes), n)
	}
	if data.Seq != 42 {
		t.Fatalf("seq: got %d, want 42", data.Seq)
	}
	if data.M != m {
		t.Fatalf("M: got %d, want %d", data.M, m)
	}
	if data.Dim != dim {
		t.Fatalf("dim: got %d, want %d", data.Dim, dim)
	}
	if data.Metric != 1 {
		t.Fatalf("metric: got %d, want 1", data.Metric)
	}

	// Restore into a fresh graph and verify search works.
	g2, err := New(Options{M: m, EfConstruct: 100, EfSearch: 50, Dim: dim, Dist: DistanceL2Squared})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.RestoreFromSnapshot(data); err != nil {
		t.Fatal("RestoreFromSnapshot:", err)
	}

	query := make([]float32, dim)
	results, err := g2.Search(query, 5, nil)
	if err != nil {
		t.Fatal("Search:", err)
	}
	if len(results) != 5 {
		t.Fatalf("search results: got %d, want 5", len(results))
	}
}

func TestSnapshot_NodeDataPreserved(t *testing.T) {
	const dim, m, n = 4, 8, 20
	g := newTestGraph(t, dim, m, n)

	data := snapshotRoundTrip(t, g, 1, 2)

	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, restored := range data.Nodes {
		orig, ok := g.nodes[restored.ID]
		if !ok {
			t.Fatalf("missing node %d in original graph", restored.ID)
		}
		if restored.ExternalID != orig.ExternalID {
			t.Errorf("node %d: externalID mismatch", restored.ID)
		}
		if restored.Level != orig.Level {
			t.Errorf("node %d: level %d != %d", restored.ID, restored.Level, orig.Level)
		}
		if len(restored.Vector) != len(orig.Vector) {
			t.Fatalf("node %d: vector length %d != %d", restored.ID, len(restored.Vector), len(orig.Vector))
		}
		for i := range restored.Vector {
			if restored.Vector[i] != orig.Vector[i] {
				t.Errorf("node %d: vector[%d] = %f, want %f", restored.ID, i, restored.Vector[i], orig.Vector[i])
			}
		}
		if len(restored.Neighbors) != len(orig.Neighbors) {
			t.Fatalf("node %d: neighbor layers %d != %d", restored.ID, len(restored.Neighbors), len(orig.Neighbors))
		}
		for l := range restored.Neighbors {
			if len(restored.Neighbors[l]) != len(orig.Neighbors[l]) {
				t.Errorf("node %d layer %d: neighbors %d != %d", restored.ID, l, len(restored.Neighbors[l]), len(orig.Neighbors[l]))
			}
			for k := range restored.Neighbors[l] {
				if restored.Neighbors[l][k] != orig.Neighbors[l][k] {
					t.Errorf("node %d layer %d neighbor %d: %d != %d", restored.ID, l, k, restored.Neighbors[l][k], orig.Neighbors[l][k])
				}
			}
		}
	}
}

func TestSnapshot_CorruptionDetected(t *testing.T) {
	const dim, m, n = 4, 8, 50
	g := newTestGraph(t, dim, m, n)

	var buf bytes.Buffer
	if err := g.WriteSnapshot(&buf, 1, 1); err != nil {
		t.Fatal(err)
	}

	// Flip a byte in the node data region.
	raw := buf.Bytes()
	raw[len(raw)/2] ^= 0xFF

	_, err := ReadSnapshot(bytes.NewReader(raw), int64(len(raw)))
	if err == nil {
		t.Fatal("expected error for corrupt data")
	}
	if !isCorruptOrInvalid(err) {
		t.Fatalf("expected corrupt/invalid error, got: %v", err)
	}
}

func TestSnapshot_Truncated(t *testing.T) {
	const dim, m, n = 4, 8, 20
	g := newTestGraph(t, dim, m, n)

	var buf bytes.Buffer
	if err := g.WriteSnapshot(&buf, 1, 1); err != nil {
		t.Fatal(err)
	}

	// Truncate last 10 bytes.
	raw := buf.Bytes()[:buf.Len()-10]
	_, err := ReadSnapshot(bytes.NewReader(raw), int64(len(raw)))
	if err == nil {
		t.Fatal("expected error for truncated file")
	}
}

func TestSnapshot_BadMagic(t *testing.T) {
	const dim, m, n = 4, 8, 10
	g := newTestGraph(t, dim, m, n)

	var buf bytes.Buffer
	if err := g.WriteSnapshot(&buf, 1, 1); err != nil {
		t.Fatal(err)
	}

	// Overwrite magic bytes at the very end.
	raw := buf.Bytes()
	raw[len(raw)-1] = 0x00
	raw[len(raw)-2] = 0x00

	_, err := ReadSnapshot(bytes.NewReader(raw), int64(len(raw)))
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestSnapshot_DimMismatch(t *testing.T) {
	const dim, m, n = 4, 8, 10
	g := newTestGraph(t, dim, m, n)
	data := snapshotRoundTrip(t, g, 1, 1)

	g2, _ := New(Options{M: m, EfConstruct: 100, EfSearch: 50, Dim: 8, Dist: DistanceL2Squared})
	if err := g2.RestoreFromSnapshot(data); err == nil {
		t.Fatal("expected error for dim mismatch")
	}
}

func TestSnapshot_MMismatch(t *testing.T) {
	const dim, m, n = 4, 8, 10
	g := newTestGraph(t, dim, m, n)
	data := snapshotRoundTrip(t, g, 1, 1)

	g2, _ := New(Options{M: 32, EfConstruct: 100, EfSearch: 50, Dim: dim, Dist: DistanceL2Squared})
	if err := g2.RestoreFromSnapshot(data); err == nil {
		t.Fatal("expected error for M mismatch")
	}
}

func TestSnapshot_EmptyGraph(t *testing.T) {
	g, _ := New(Options{M: 8, EfConstruct: 100, EfSearch: 50, Dim: 4, Dist: DistanceL2Squared})
	var buf bytes.Buffer
	if err := g.WriteSnapshot(&buf, 1, 1); err != ErrEmptyGraph {
		t.Fatalf("expected ErrEmptyGraph, got: %v", err)
	}
}

func TestSnapshot_Deterministic(t *testing.T) {
	const dim, m, n = 4, 8, 30
	g := newTestGraph(t, dim, m, n)

	var buf1, buf2 bytes.Buffer
	if err := g.WriteSnapshot(&buf1, 99, 2); err != nil {
		t.Fatal(err)
	}
	if err := g.WriteSnapshot(&buf2, 99, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("snapshots are not byte-identical")
	}
}

func TestSnapshot_TombstonedNodesPresent(t *testing.T) {
	const dim, m, n = 4, 8, 20
	g := newTestGraph(t, dim, m, n)

	// Tombstone a few nodes.
	g.MarkDeleted(0)
	g.MarkDeleted(5)

	data := snapshotRoundTrip(t, g, 1, 1)

	// Tombstoned nodes should still be present in the snapshot
	// (they remain in g.nodes).
	if len(data.Nodes) != n {
		t.Fatalf("node count: got %d, want %d (tombstoned nodes should be included)", len(data.Nodes), n)
	}

	// After restore, tombstones map should be empty (caller must re-detect).
	g2, _ := New(Options{M: m, EfConstruct: 100, EfSearch: 50, Dim: dim, Dist: DistanceL2Squared})
	if err := g2.RestoreFromSnapshot(data); err != nil {
		t.Fatal(err)
	}
	g2.mu.RLock()
	if len(g2.tombstones) != 0 {
		t.Errorf("expected empty tombstones after restore, got %d", len(g2.tombstones))
	}
	g2.mu.RUnlock()
}

func TestSnapshot_LargeExternalIDs(t *testing.T) {
	g, _ := New(Options{M: 8, EfConstruct: 100, EfSearch: 50, Dim: 4, Dist: DistanceL2Squared})

	// Use random external IDs to test full 16-byte range.
	for i := 0; i < 10; i++ {
		var extID [16]byte
		rand.Read(extID[:])
		vec := make([]float32, 4)
		for j := range vec {
			vec[j] = float32(i*4 + j)
		}
		if err := g.Insert(uint64(i), extID, vec); err != nil {
			t.Fatal(err)
		}
	}

	data := snapshotRoundTrip(t, g, 1, 1)

	g.mu.RLock()
	for _, restored := range data.Nodes {
		orig := g.nodes[restored.ID]
		if restored.ExternalID != orig.ExternalID {
			t.Errorf("node %d: externalID not preserved", restored.ID)
		}
	}
	g.mu.RUnlock()
}

func isCorruptOrInvalid(err error) bool {
	return errors.Is(err, ErrCorruptSnapshot) || errors.Is(err, ErrInvalidSnapshot)
}
