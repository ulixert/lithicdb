package hnsw

import (
	"math/rand/v2"
	"sort"
	"testing"
)

func TestSearch_EmptyGraph(t *testing.T) {
	g := testGraph(t, 4)
	_, err := g.Search([]float32{1, 2, 3, 4}, 5, nil)
	if err != ErrEmptyGraph {
		t.Errorf("got %v, want ErrEmptyGraph", err)
	}
}

func TestSearch_DimensionMismatch(t *testing.T) {
	g := testGraph(t, 4)
	g.Insert(0, []float32{1, 2, 3, 4})
	_, err := g.Search([]float32{1, 2, 3}, 1, nil)
	if err != ErrDimensionMismatch {
		t.Errorf("got %v, want ErrDimensionMismatch", err)
	}
}

func TestSearch_SingleNode(t *testing.T) {
	g := testGraph(t, 4)
	g.Insert(42, []float32{1, 2, 3, 4})
	results, err := g.Search([]float32{1, 2, 3, 4}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].ID != 42 {
		t.Errorf("got ID %d, want 42", results[0].ID)
	}
	if results[0].Distance != 0 {
		t.Errorf("got distance %v, want 0", results[0].Distance)
	}
}

func TestSearch_BruteForceEquivalence(t *testing.T) {
	// With ef >= N, HNSW search should be exact.
	const (
		n   = 500
		dim = 32
		k   = 10
	)
	g := testGraph(t, dim)
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = randomVec(dim)
		if err := g.Insert(uint64(i), vecs[i]); err != nil {
			t.Fatal(err)
		}
	}

	query := randomVec(dim)
	results, err := g.Search(query, k, &SearchOptions{EfSearch: n})
	if err != nil {
		t.Fatal(err)
	}

	// Brute force ground truth.
	type idDist struct {
		id   uint64
		dist float32
	}
	all := make([]idDist, n)
	for i := 0; i < n; i++ {
		all[i] = idDist{uint64(i), DistanceL2Squared(query, vecs[i])}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })

	trueIDs := make(map[uint64]bool, k)
	for i := 0; i < k; i++ {
		trueIDs[all[i].id] = true
	}
	hits := 0
	for _, r := range results {
		if trueIDs[r.ID] {
			hits++
		}
	}
	recall := float64(hits) / float64(k)
	if recall < 1.0 {
		t.Errorf("with ef=N, expected perfect recall, got %.2f", recall)
	}
}

func TestSearch_TombstoneFiltering(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 100; i++ {
		if err := g.Insert(i, randomVec(4)); err != nil {
			t.Fatal(err)
		}
	}
	// Tombstone IDs 10-19.
	tombstoned := make(map[uint64]bool)
	for i := uint64(10); i < 20; i++ {
		g.MarkDeleted(i)
		tombstoned[i] = true
	}

	results, err := g.Search(randomVec(4), 20, &SearchOptions{EfSearch: 200})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if tombstoned[r.ID] {
			t.Errorf("tombstoned ID %d appeared in results", r.ID)
		}
	}
}

func TestSearch_TombstoneTraversal(t *testing.T) {
	// Build a small graph where tombstoned nodes serve as bridges.
	// Insert nodes along a line, tombstone the middle ones, and verify
	// that we can still find nodes beyond the tombstoned region.
	const dim = 2
	g, err := New(Options{
		M:           4,
		EfConstruct: 100,
		EfSearch:    100,
		Dim:         dim,
		Dist:        DistanceL2Squared,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a line of 20 nodes at (i, 0).
	for i := 0; i < 20; i++ {
		if err := g.Insert(uint64(i), []float32{float32(i), 0}); err != nil {
			t.Fatal(err)
		}
	}

	// Tombstone the middle section (5-14).
	for i := 5; i < 15; i++ {
		g.MarkDeleted(uint64(i))
	}

	// Search near the far end. Should still find nodes 15-19 despite the gap.
	results, err := g.Search([]float32{17, 0}, 5, nil)
	if err != nil {
		t.Fatal(err)
	}

	foundFar := false
	for _, r := range results {
		if r.ID >= 15 && r.ID <= 19 {
			foundFar = true
			break
		}
	}
	if !foundFar {
		t.Error("failed to find nodes beyond tombstoned region; traversal may be broken")
	}
}

func TestSearch_EfSearchOverride(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 50; i++ {
		if err := g.Insert(i, randomVec(4)); err != nil {
			t.Fatal(err)
		}
	}
	// Should not panic with different ef values.
	for _, ef := range []int{1, 5, 10, 50} {
		_, err := g.Search(randomVec(4), 5, &SearchOptions{EfSearch: ef})
		if err != nil {
			t.Errorf("ef=%d: %v", ef, err)
		}
	}
}

func TestSearch_KLargerThanGraph(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 5; i++ {
		if err := g.Insert(i, randomVec(4)); err != nil {
			t.Fatal(err)
		}
	}
	results, err := g.Search(randomVec(4), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Errorf("got %d results, want 5 (all nodes)", len(results))
	}
}

func TestRecall_10K_128dim(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large recall test in short mode")
	}

	const (
		n       = 10000
		dim     = 128
		k       = 10
		queries = 100
	)

	g, err := New(Options{
		M:           16,
		EfConstruct: 200,
		EfSearch:    250, // 128-dim random data needs higher ef than structured datasets
		Dim:         dim,
		Dist:        DistanceL2Squared,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Build index.
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = randomVec(dim)
		if err := g.Insert(uint64(i), vecs[i]); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Query and measure recall.
	totalRecall := 0.0
	for q := 0; q < queries; q++ {
		query := randomVec(dim)
		results, err := g.Search(query, k, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Brute force ground truth.
		truth := bruteForceKNN(vecs, query, k)
		trueSet := make(map[uint64]bool, k)
		for _, id := range truth {
			trueSet[id] = true
		}

		hits := 0
		for _, r := range results {
			if trueSet[r.ID] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}

	meanRecall := totalRecall / float64(queries)
	t.Logf("mean recall@%d: %.4f (n=%d, dim=%d, M=16, efC=200, efS=250)", k, meanRecall, n, dim)
	if meanRecall < 0.95 {
		t.Errorf("recall@%d = %.4f, want >= 0.95", k, meanRecall)
	}
}

// bruteForceKNN returns the k nearest neighbor IDs by L2 squared distance.
func bruteForceKNN(vecs [][]float32, query []float32, k int) []uint64 {
	type idDist struct {
		id   uint64
		dist float32
	}
	all := make([]idDist, len(vecs))
	for i, v := range vecs {
		all[i] = idDist{uint64(i), DistanceL2Squared(query, v)}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	result := make([]uint64, k)
	for i := 0; i < k && i < len(all); i++ {
		result[i] = all[i].id
	}
	return result
}

func BenchmarkSearch_1K_128dim(b *testing.B) {
	const (
		n   = 1000
		dim = 128
		k   = 10
	)
	g, _ := New(DefaultOptions(dim))
	for i := 0; i < n; i++ {
		g.Insert(uint64(i), randomVecBench(dim))
	}
	query := randomVecBench(dim)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		g.Search(query, k, nil)
	}
}

func randomVecBench(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rand.Float32()*2 - 1
	}
	return v
}
