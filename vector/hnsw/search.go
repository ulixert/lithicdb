package hnsw

import (
	"cmp"
	"slices"
	"sync"
)

// SearchOptions overrides default search parameters for a single query.
type SearchOptions struct {
	EfSearch int // 0 => use graph default
}

// SearchResult is a single vector result with its distance to the query.
type SearchResult struct {
	ID         uint64
	ExternalID [16]byte
	Distance   float32
}

// Search finds the k nearest neighbors of the query in the graph.
// Returns results sorted by ascending distance.
func (g *Graph) Search(query []float32, k int, sopts *SearchOptions) ([]SearchResult, error) {
	if len(query) != g.opts.Dim {
		return nil, ErrDimensionMismatch
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.nodes) == 0 {
		return nil, ErrEmptyGraph
	}

	ef := g.opts.EfSearch
	if sopts != nil && sopts.EfSearch > 0 {
		ef = sopts.EfSearch
	}
	if ef < k {
		ef = k
	}

	ep := g.entryPoint
	dist := g.opts.Dist

	// Phase 1: Greedy descent from maxLevel to layer 1.
	for l := g.maxLevel; l >= 1; l-- {
		ep = g.greedyClosest(query, ep, l, dist)
	}

	// Phase 2: Beam search on layer 0.
	candidates := g.searchLayer(query, []uint64{ep}, ef, 0)

	// Sort by distance and take top k.
	slices.SortFunc(candidates, func(a, b heapItem) int {
		return cmp.Compare(a.dist, b.dist)
	})
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	results := make([]SearchResult, len(candidates))
	for i, c := range candidates {
		results[i] = SearchResult{
			ID:         c.id,
			ExternalID: g.nodes[c.id].ExternalID,
			Distance:   c.dist,
		}
	}
	return results, nil
}

// heapItem is an element in candidate/result heaps.
type heapItem struct {
	id   uint64
	dist float32
}

// distHeap is a typed binary heap over heapItem. When max is true the largest
// distance is at index 0 (max-heap); otherwise the smallest is at index 0
// (min-heap). Methods are direct (no container/heap interface dispatch and
// no any-boxing on Push/Pop), so comparisons and swaps inline.
type distHeap struct {
	items []heapItem
	max   bool
}

func (h *distHeap) less(i, j int) bool {
	if h.max {
		return h.items[i].dist > h.items[j].dist
	}
	return h.items[i].dist < h.items[j].dist
}

func (h *distHeap) push(item heapItem) {
	h.items = append(h.items, item)
	h.up(len(h.items) - 1)
}

func (h *distHeap) pop() heapItem {
	n := len(h.items) - 1
	h.items[0], h.items[n] = h.items[n], h.items[0]
	h.down(0, n)
	item := h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *distHeap) peek() heapItem {
	return h.items[0]
}

func (h *distHeap) up(i int) {
	for {
		parent := (i - 1) / 2
		if i == parent || !h.less(i, parent) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *distHeap) down(i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, i) {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}

// visitedPool hands out reusable map[uint64]bool sets for searchLayer.
// Concurrent searches each Get one and Put it back; clear() resets state
// without freeing the underlying buckets.
var visitedPool = sync.Pool{
	New: func() any {
		return new(make(map[uint64]bool, 256))
	},
}

// heapPool hands out reusable []heapItem backing slices for the candidate
// and result heaps. The pointer-to-slice wrapper avoids any-boxing the
// slice header on Put. Capacity is preserved across Get/Put so the
// steady-state search path performs no slice growths.
var heapPool = sync.Pool{
	New: func() any {
		return new(make([]heapItem, 0, 256))
	},
}

// searchLayer runs beam search on a single layer of the graph.
// It returns up to ef non-tombstoned results. Tombstoned nodes are
// traversed (their neighbors explored) but excluded from results.
//
// This method is called by both Insert and Search. The caller must
// hold at least an RLock on g.mu.
func (g *Graph) searchLayer(query []float32, entryIDs []uint64, ef int, layer int) []heapItem {
	dist := g.opts.Dist

	visitedP := visitedPool.Get().(*map[uint64]bool)
	visited := *visitedP
	clear(visited)
	defer visitedPool.Put(visitedP)

	candP := heapPool.Get().(*[]heapItem)
	candidates := &distHeap{items: (*candP)[:0], max: false} // min-heap: closest first
	defer func() {
		*candP = candidates.items
		heapPool.Put(candP)
	}()

	resP := heapPool.Get().(*[]heapItem)
	results := &distHeap{items: (*resP)[:0], max: true} // max-heap: farthest at top
	defer func() {
		*resP = results.items
		heapPool.Put(resP)
	}()

	for _, epID := range entryIDs {
		if visited[epID] {
			continue
		}
		visited[epID] = true
		d := dist(query, g.nodes[epID].Vector)
		candidates.push(heapItem{epID, d})
		if !g.tombstones[epID] {
			results.push(heapItem{epID, d})
		}
	}

	for len(candidates.items) > 0 {
		c := candidates.pop() // closest unvisited candidate

		// If we have enough results and the closest candidate is farther
		// than our farthest result, remaining candidates will only be farther.
		if len(results.items) >= ef {
			if c.dist > results.peek().dist {
				break
			}
		}

		node := g.nodes[c.id]
		if layer >= len(node.Neighbors) {
			continue
		}
		for _, nid := range node.Neighbors[layer] {
			if visited[nid] {
				continue
			}
			visited[nid] = true

			n, ok := g.nodes[nid]
			if !ok {
				continue
			}
			d := dist(query, n.Vector)

			shouldAdd := len(results.items) < ef
			if !shouldAdd && d < results.peek().dist {
				shouldAdd = true
			}

			if shouldAdd {
				candidates.push(heapItem{nid, d})
				if !g.tombstones[nid] {
					results.push(heapItem{nid, d})
					if len(results.items) > ef {
						results.pop() // evict farthest
					}
				}
			}
		}
	}

	// Copy results out so the pooled backing slice can be returned without
	// dangling references in caller-held memory. Allocates one slice of
	// at most ef heapItems.
	out := make([]heapItem, len(results.items))
	copy(out, results.items)
	return out
}

// selectClosest returns up to maxN items from candidates sorted by
// ascending distance (SELECT-NEIGHBORS-SIMPLE from the HNSW paper).
func selectClosest(candidates []heapItem, maxN int) []heapItem {
	slices.SortFunc(candidates, func(a, b heapItem) int {
		return cmp.Compare(a.dist, b.dist)
	})
	if len(candidates) > maxN {
		candidates = candidates[:maxN]
	}
	return candidates
}
