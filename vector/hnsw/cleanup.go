package hnsw

// Cleanup physically removes all tombstoned nodes from the graph.
// It updates neighbor lists of remaining nodes to remove references
// to deleted nodes and recalculates the entry point and max level
// if needed. Returns the number of nodes removed.
//
// No neighbor repair is performed in v1: when a tombstoned node is
// removed, its former neighbors lose a connection but are not
// reconnected. The eval framework measures recall degradation;
// data drives whether repair is added later.
func (g *Graph) Cleanup() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.tombstones) == 0 {
		return 0
	}

	// Build the set of IDs to remove.
	removed := make(map[uint64]struct{}, len(g.tombstones))
	for id := range g.tombstones {
		removed[id] = struct{}{}
	}

	// Delete tombstoned nodes and reclaim estimated memory.
	for id := range removed {
		node := g.nodes[id]
		g.memoryBytes -= g.estimateNodeMemory(node.Level)
		delete(g.nodes, id)
	}
	count := len(g.tombstones)
	g.tombstones = make(map[uint64]bool)

	// Scrub removed IDs from every remaining node's neighbor lists.
	for _, node := range g.nodes {
		for l := range node.Neighbors {
			filtered := node.Neighbors[l][:0]
			for _, nid := range node.Neighbors[l] {
				if _, ok := removed[nid]; !ok {
					filtered = append(filtered, nid)
				}
			}
			node.Neighbors[l] = filtered
		}
	}

	// Recalculate entry point and max level.
	if len(g.nodes) == 0 {
		g.maxLevel = 0
		g.entryPoint = 0
		return count
	}

	newMaxLevel := 0
	bestEntry := uint64(0)
	bestLevel := -1
	for id, node := range g.nodes {
		if node.Level > newMaxLevel {
			newMaxLevel = node.Level
		}
		if node.Level > bestLevel {
			bestLevel = node.Level
			bestEntry = id
		}
	}
	g.maxLevel = newMaxLevel
	g.entryPoint = bestEntry

	return count
}
