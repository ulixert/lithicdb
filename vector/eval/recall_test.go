package eval

import (
	"math/rand/v2"
	"testing"

	"github.com/ulixert/theseon/vector/hnsw"
)

func TestRecallAtK_Perfect(t *testing.T) {
	trueNN := []uint64{1, 2, 3, 4, 5}
	approxNN := []uint64{1, 2, 3, 4, 5}
	if r := RecallAtK(trueNN, approxNN, 5); r != 1.0 {
		t.Errorf("got %v, want 1.0", r)
	}
}

func TestRecallAtK_Zero(t *testing.T) {
	trueNN := []uint64{1, 2, 3, 4, 5}
	approxNN := []uint64{6, 7, 8, 9, 10}
	if r := RecallAtK(trueNN, approxNN, 5); r != 0.0 {
		t.Errorf("got %v, want 0.0", r)
	}
}

func TestRecallAtK_Partial(t *testing.T) {
	trueNN := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	approxNN := []uint64{1, 2, 3, 4, 5, 6, 7, 99, 98, 97}
	r := RecallAtK(trueNN, approxNN, 10)
	if r != 0.7 {
		t.Errorf("got %v, want 0.7", r)
	}
}

func TestRecallAtK_KTruncation(t *testing.T) {
	trueNN := []uint64{1, 2, 3}
	approxNN := []uint64{1, 2, 99}
	// k=5 but only 3 true neighbors: recall = 2/3
	r := RecallAtK(trueNN, approxNN, 5)
	if r < 0.66 || r > 0.67 {
		t.Errorf("got %v, want ~0.667", r)
	}
}

func TestMeanRecallAtK(t *testing.T) {
	trueNNs := [][]uint64{{1, 2, 3}, {4, 5, 6}}
	approxNNs := [][]uint64{{1, 2, 3}, {4, 5, 99}}
	// Query 0: 3/3 = 1.0, Query 1: 2/3 = 0.667, mean = 0.833
	r := MeanRecallAtK(trueNNs, approxNNs, 3)
	if r < 0.83 || r > 0.84 {
		t.Errorf("got %v, want ~0.833", r)
	}
}

func TestBruteForceKNN(t *testing.T) {
	// 5 vectors in 2D, query at origin.
	vecs := &Vectors{
		Data: []float32{
			1, 0, // id=0, dist=1
			0, 2, // id=1, dist=4
			3, 0, // id=2, dist=9
			0, 0.5, // id=3, dist=0.25
			-1, -1, // id=4, dist=2
		},
		N:   5,
		Dim: 2,
	}
	query := []float32{0, 0}
	ids := BruteForceKNN(vecs, query, 3, hnsw.DistanceL2Squared)
	// Expected order by L2 squared: id=3 (0.25), id=0 (1), id=4 (2)
	want := []uint64{3, 0, 4}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("ids[%d] = %d, want %d", i, id, want[i])
		}
	}
}

func TestBruteForceKNN_KLargerThanN(t *testing.T) {
	vecs := &Vectors{
		Data: []float32{1, 2, 3, 4},
		N:    2,
		Dim:  2,
	}
	ids := BruteForceKNN(vecs, []float32{0, 0}, 10, hnsw.DistanceL2Squared)
	if len(ids) != 2 {
		t.Errorf("got %d ids, want 2", len(ids))
	}
}

func TestBruteForceKNN_RandomData(t *testing.T) {
	const (
		n   = 1000
		dim = 32
		k   = 10
	)
	data := make([]float32, n*dim)
	for i := range data {
		data[i] = rand.Float32()*2 - 1
	}
	vecs := &Vectors{Data: data, N: n, Dim: dim}
	query := make([]float32, dim)
	for i := range query {
		query[i] = rand.Float32()*2 - 1
	}

	ids := BruteForceKNN(vecs, query, k, hnsw.DistanceL2Squared)
	if len(ids) != k {
		t.Fatalf("got %d ids, want %d", len(ids), k)
	}

	// Verify ordering: distances should be non-decreasing.
	prev := float32(0)
	for i, id := range ids {
		d := hnsw.DistanceL2Squared(query, vecs.Vec(int(id)))
		if i > 0 && d < prev {
			t.Errorf("ids[%d] dist=%v < ids[%d] dist=%v", i, d, i-1, prev)
		}
		prev = d
	}
}
