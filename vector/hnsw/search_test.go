package hnsw

import (
	"container/heap"
	"math/rand/v2"
	"sort"
	"sync"
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
	g.Insert(0, [16]byte{}, []float32{1, 2, 3, 4})
	_, err := g.Search([]float32{1, 2, 3}, 1, nil)
	if err != ErrDimensionMismatch {
		t.Errorf("got %v, want ErrDimensionMismatch", err)
	}
}

func TestSearch_SingleNode(t *testing.T) {
	g := testGraph(t, 4)
	g.Insert(42, [16]byte{}, []float32{1, 2, 3, 4})
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
		if err := g.Insert(uint64(i), [16]byte{}, vecs[i]); err != nil {
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
		if err := g.Insert(i, [16]byte{}, randomVec(4)); err != nil {
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
		if err := g.Insert(uint64(i), [16]byte{}, []float32{float32(i), 0}); err != nil {
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
		if err := g.Insert(i, [16]byte{}, randomVec(4)); err != nil {
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
		if err := g.Insert(i, [16]byte{}, randomVec(4)); err != nil {
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
		if err := g.Insert(uint64(i), [16]byte{}, vecs[i]); err != nil {
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
		g.Insert(uint64(i), [16]byte{}, randomVecBench(dim))
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

// TestSearch_AllocsBounded locks in the alloc-free contract on Search.
// Pre-refactor this would report ~440+; post-refactor target is ≤8.
// (Two pool Gets are recycled allocation-free; the residual allocs are the
// SearchResult return slice in Search and the heapItem copy out of
// searchLayer.)
func TestSearch_AllocsBounded(t *testing.T) {
	const (
		n   = 200
		dim = 32
		k   = 10
	)
	g, err := New(DefaultOptions(dim))
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := g.Insert(uint64(i), [16]byte{}, randomVec(dim)); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	query := randomVec(dim)

	// Warm up the pools so the steady-state alloc count is what we measure.
	for range 100 {
		_, _ = g.Search(query, k, nil)
	}

	allocs := testing.AllocsPerRun(200, func() {
		_, _ = g.Search(query, k, nil)
	})
	const allowedAllocs = 8
	if allocs > allowedAllocs {
		t.Fatalf("Search allocs/op = %v, want <= %d", allocs, allowedAllocs)
	}
	t.Logf("Search allocs/op = %v", allocs)
}

// TestSearch_Concurrent exercises the per-call pool ownership: many goroutines
// hammer Search on the same graph at once. With -race this catches any
// accidentally shared mutable state across the visited / heap pools.
func TestSearch_Concurrent(t *testing.T) {
	const (
		n           = 500
		dim         = 32
		k           = 10
		goroutines  = 16
		searchesPer = 50
	)
	g, err := New(DefaultOptions(dim))
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := g.Insert(uint64(i), [16]byte{}, randomVec(dim)); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Pre-build a stable set of queries so all goroutines see the same workload
	// and pool-corruption bugs would manifest as differing result sets.
	queries := make([][]float32, goroutines)
	for i := range queries {
		queries[i] = randomVec(dim)
	}

	// Serial baseline.
	want := make([][]SearchResult, goroutines)
	for i, q := range queries {
		r, err := g.Search(q, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		want[i] = r
	}

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range searchesPer {
				r, err := g.Search(queries[i], k, nil)
				if err != nil {
					t.Errorf("goroutine %d: %v", i, err)
					return
				}
				if len(r) != len(want[i]) {
					t.Errorf("goroutine %d: len mismatch %d != %d", i, len(r), len(want[i]))
					return
				}
				for j, got := range r {
					if got.ID != want[i][j].ID || got.Distance != want[i][j].Distance {
						t.Errorf("goroutine %d pos %d: got %+v want %+v", i, j, got, want[i][j])
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

// refHeap is a container/heap.Interface implementation used solely as the
// reference oracle for TestDistHeap_Parity. Independent of the production
// distHeap so the parity test is meaningful.
type refHeap struct {
	items []heapItem
	max   bool
}

func (h refHeap) Len() int { return len(h.items) }
func (h refHeap) Less(i, j int) bool {
	if h.max {
		return h.items[i].dist > h.items[j].dist
	}
	return h.items[i].dist < h.items[j].dist
}
func (h refHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *refHeap) Push(x any)   { h.items = append(h.items, x.(heapItem)) }
func (h *refHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// TestDistHeap_Parity drives the typed distHeap and a container/heap-backed
// refHeap with the same random Push/Pop sequence and asserts identical pop
// order. Catches sift-up / sift-down / max-vs-min bugs in the typed
// implementation.
func TestDistHeap_Parity(t *testing.T) {
	for _, isMax := range []bool{false, true} {
		for seed := uint64(1); seed <= 5; seed++ {
			t.Run("", func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, seed*31))
				typed := &distHeap{max: isMax}
				ref := &refHeap{max: isMax}

				const ops = 5000
				for range ops {
					switch op := rng.IntN(3); op {
					case 0, 1: // bias toward push so the heap grows
						item := heapItem{
							id:   rng.Uint64(),
							dist: rng.Float32() * 1000,
						}
						typed.push(item)
						heap.Push(ref, item)
					case 2:
						if len(typed.items) == 0 {
							continue
						}
						gotItem := typed.pop()
						wantItem := heap.Pop(ref).(heapItem)
						if gotItem != wantItem {
							t.Fatalf("max=%v seed=%d: pop mismatch got=%+v want=%+v", isMax, seed, gotItem, wantItem)
						}
					}
				}
				// Drain both and compare order.
				for len(typed.items) > 0 {
					gotItem := typed.pop()
					wantItem := heap.Pop(ref).(heapItem)
					if gotItem != wantItem {
						t.Fatalf("max=%v seed=%d (drain): got=%+v want=%+v", isMax, seed, gotItem, wantItem)
					}
				}
				if ref.Len() != 0 {
					t.Fatalf("ref still has %d items after drain", ref.Len())
				}
			})
		}
	}
}
