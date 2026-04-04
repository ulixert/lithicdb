package hnsw

import "container/heap"

// heapItem is an element in candidate/result heaps.
type heapItem struct {
	id   uint64
	dist float32
}

// distHeap implements container/heap.Interface.
// When max is true, the largest distance is at index 0 (max-heap).
// When max is false, the smallest distance is at index 0 (min-heap).
type distHeap struct {
	items []heapItem
	max   bool
}

func (h distHeap) Len() int {
	return len(h.items)
}

func (h distHeap) Less(i, j int) bool {
	if h.max {
		return h.items[i].dist > h.items[j].dist
	}
	return h.items[i].dist < h.items[j].dist
}

func (h distHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *distHeap) Push(x any) {
	h.items = append(h.items, x.(heapItem))
}

func (h *distHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

func (h distHeap) peek() heapItem {
	return h.items[0]
}

// searchLayer runs beam search on a single layer of the graph.
// It returns up to ef non-tombstoned results. Tombstoned nodes are
// traversed (their neighbors explored) but excluded from results.
//
// This method is called by both Insert and Search. The caller must
// hold at least an RLock on g.mu.
func (g *Graph) searchLayer(query []float32, entryIDs []uint64, ef int, layer int) []heapItem {
	dist := g.opts.Dist
	visited := make(map[uint64]bool, ef*2)

	candidates := &distHeap{max: false} // min-heap: closest first
	results := &distHeap{max: true}     // max-heap: farthest at top

	for _, epID := range entryIDs {
		if visited[epID] {
			continue
		}
		visited[epID] = true
		d := dist(query, g.nodes[epID].Vector)
		heap.Push(candidates, heapItem{epID, d})
		if !g.tombstones[epID] {
			heap.Push(results, heapItem{epID, d})
		}
	}

	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(heapItem) // closest unvisited candidate

		// If we have enough results and the closest candidate is farther
		// than our farthest result, remaining candidates will only be farther.
		if results.Len() >= ef {
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

			shouldAdd := results.Len() < ef
			if !shouldAdd && d < results.peek().dist {
				shouldAdd = true
			}

			if shouldAdd {
				heap.Push(candidates, heapItem{nid, d})
				if !g.tombstones[nid] {
					heap.Push(results, heapItem{nid, d})
					if results.Len() > ef {
						heap.Pop(results) // evict farthest
					}
				}
			}
		}
	}

	return results.items
}
