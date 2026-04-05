package eval

import (
	"container/heap"

	"github.com/ulixert/theseon/vector/hnsw"
)

// RecallAtK computes the fraction of true top-k neighbors found in approxNN.
// Both slices are truncated to k if longer.
func RecallAtK(trueNN, approxNN []uint64, k int) float64 {
	if k > len(trueNN) {
		k = len(trueNN)
	}
	if k == 0 {
		return 0
	}

	trueSet := make(map[uint64]struct{}, k)
	for _, id := range trueNN[:k] {
		trueSet[id] = struct{}{}
	}

	top := k
	if top > len(approxNN) {
		top = len(approxNN)
	}
	hits := 0
	for _, id := range approxNN[:top] {
		if _, ok := trueSet[id]; ok {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

// MeanRecallAtK computes the mean recall@k across multiple query results.
func MeanRecallAtK(trueNNs, approxNNs [][]uint64, k int) float64 {
	if len(trueNNs) == 0 {
		return 0
	}
	total := 0.0
	for i := range trueNNs {
		total += RecallAtK(trueNNs[i], approxNNs[i], k)
	}
	return total / float64(len(trueNNs))
}

// BruteForceKNN computes the exact k nearest neighbors of a query among
// all vectors in vecs, using the given distance function.
//
// ID mapping convention: vector at row index i has ID uint64(i).
// This convention is used consistently across the eval package.
func BruteForceKNN(vecs *Vectors, query []float32, k int, dist hnsw.DistanceFunc) []uint64 {
	if k <= 0 || vecs.N == 0 {
		return nil
	}
	if k > vecs.N {
		k = vecs.N
	}

	// Max-heap of size k to track the k closest.
	h := &knnHeap{}
	for i := 0; i < vecs.N; i++ {
		d := dist(query, vecs.Vec(i))
		if h.Len() < k {
			heap.Push(h, knnItem{id: uint64(i), dist: d})
		} else if d < h.items[0].dist {
			h.items[0] = knnItem{id: uint64(i), dist: d}
			heap.Fix(h, 0)
		}
	}

	// Extract in ascending distance order.
	result := make([]uint64, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(knnItem).id
	}
	return result
}

type knnItem struct {
	id   uint64
	dist float32
}

// knnHeap is a max-heap by distance (farthest at top).
type knnHeap struct {
	items []knnItem
}

func (h knnHeap) Len() int {
	return len(h.items)
}

func (h knnHeap) Less(i, j int) bool {
	return h.items[i].dist > h.items[j].dist
}

func (h knnHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *knnHeap) Push(x any) {
	h.items = append(h.items, x.(knnItem))
}

func (h *knnHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}
