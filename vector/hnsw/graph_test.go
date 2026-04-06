package hnsw

import (
	"math/rand/v2"
	"testing"
)

func TestNew_ValidOptions(t *testing.T) {
	g, err := New(DefaultOptions(128))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if g == nil {
		t.Fatal("New returned nil graph")
	}
}

func TestNew_InvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"dim 0", Options{M: 16, EfConstruct: 200, EfSearch: 50, Dim: 0}},
		{"M 1", Options{M: 1, EfConstruct: 200, EfSearch: 50, Dim: 128}},
		{"efConstruct 0", Options{M: 16, EfConstruct: 0, EfSearch: 50, Dim: 128}},
		{"efSearch 0", Options{M: 16, EfConstruct: 200, EfSearch: 0, Dim: 128}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.opts)
			if err == nil {
				t.Error("expected error for invalid options")
			}
		})
	}
}

func TestInsert_FirstNode(t *testing.T) {
	g := testGraph(t, 4)
	if err := g.Insert(1, [16]byte{}, []float32{1, 2, 3, 4}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if g.Len() != 1 {
		t.Errorf("Len: got %d, want 1", g.Len())
	}
}

func TestInsert_DimensionMismatch(t *testing.T) {
	g := testGraph(t, 4)
	err := g.Insert(1, [16]byte{}, []float32{1, 2, 3}) // wrong dim
	if err != ErrDimensionMismatch {
		t.Errorf("got %v, want ErrDimensionMismatch", err)
	}
}

func TestInsert_DuplicateID(t *testing.T) {
	g := testGraph(t, 4)
	if err := g.Insert(1, [16]byte{}, []float32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	err := g.Insert(1, [16]byte{}, []float32{5, 6, 7, 8})
	if err != ErrDuplicateID {
		t.Errorf("got %v, want ErrDuplicateID", err)
	}
}

func TestInsert_MemoryLimit(t *testing.T) {
	opts := DefaultOptions(4)
	opts.MaxMemoryBytes = 1 // absurdly small
	g, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	// First insert may succeed if the estimate allows it, but eventually it should fail.
	for i := uint64(0); i < 1000; i++ {
		err = g.Insert(i, [16]byte{}, randomVec(4))
		if err == ErrMemoryLimitExceeded {
			return // success
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	t.Error("expected ErrMemoryLimitExceeded, but 1000 inserts succeeded")
}

func TestInsert_MultipleNodes(t *testing.T) {
	g := testGraph(t, 32)
	for i := uint64(0); i < 100; i++ {
		if err := g.Insert(i, [16]byte{}, randomVec(32)); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	if g.Len() != 100 {
		t.Errorf("Len: got %d, want 100", g.Len())
	}
}

func TestMarkDeleted(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 10; i++ {
		if err := g.Insert(i, [16]byte{}, randomVec(4)); err != nil {
			t.Fatal(err)
		}
	}
	g.MarkDeleted(3)
	g.MarkDeleted(7)
	if g.Len() != 8 {
		t.Errorf("Len: got %d, want 8", g.Len())
	}
	stats := g.Stats()
	if stats.TotalNodes != 10 {
		t.Errorf("TotalNodes: got %d, want 10", stats.TotalNodes)
	}
	if stats.Tombstoned != 2 {
		t.Errorf("Tombstoned: got %d, want 2", stats.Tombstoned)
	}
}

func TestStats_MemoryTracking(t *testing.T) {
	g := testGraph(t, 128)
	for i := uint64(0); i < 50; i++ {
		if err := g.Insert(i, [16]byte{}, randomVec(128)); err != nil {
			t.Fatal(err)
		}
	}
	stats := g.Stats()
	if stats.MemoryBytes <= 0 {
		t.Errorf("MemoryBytes should be > 0, got %d", stats.MemoryBytes)
	}
}

func TestLevelDistribution(t *testing.T) {
	// Verify that level assignment follows roughly geometric distribution.
	// With M=16, ~64% of nodes should be at level 0.
	g := testGraph(t, 4)
	levels := make(map[int]int)
	for i := uint64(0); i < 10000; i++ {
		if err := g.Insert(i, [16]byte{}, randomVec(4)); err != nil {
			t.Fatal(err)
		}
	}
	g.mu.RLock()
	for _, node := range g.nodes {
		levels[node.Level]++
	}
	g.mu.RUnlock()

	level0Pct := float64(levels[0]) / 10000.0
	// With M=16, P(level 0) = 1 - 1/M = 0.9375. Allow bounds for randomness.
	if level0Pct < 0.90 || level0Pct > 0.97 {
		t.Errorf("level 0 percentage: %.2f, expected ~0.9375 (0.90-0.97 range)", level0Pct)
	}
	// Should have some nodes at level >= 1
	if levels[1] == 0 {
		t.Error("no nodes at level 1")
	}
}

// testGraph creates a graph with default options for the given dimension.
func testGraph(t *testing.T, dim int) *Graph {
	t.Helper()
	g, err := New(DefaultOptions(dim))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

func randomVec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rand.Float32()*2 - 1 // [-1, 1]
	}
	return v
}
