package hnsw

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
)

var (
	ErrMemoryLimitExceeded = errors.New("hnsw: memory limit exceeded")
	ErrDimensionMismatch   = errors.New("hnsw: dimension mismatch")
	ErrEmptyGraph          = errors.New("hnsw: graph is empty")
	ErrDuplicateID         = errors.New("hnsw: duplicate node ID")
)

// Options configure an HNSW graph.
type Options struct {
	M              int          // max connections per upper layer (layer 0 gets 2*M). Default: 16
	EfConstruct    int          // beam width during insertion. Default: 200
	EfSearch       int          // default beam width for queries. Default: 50
	Dim            int          // vector dimensionality. Required, >= 1
	Dist           DistanceFunc // distance function. Default: DistanceL2Squared
	Logger         *slog.Logger // nil => slog.Default()
	MaxMemoryBytes int64        // soft OOM cap; 0 = unlimited
}

// DefaultOptions returns reasonable defaults for the given dimensionality.
func DefaultOptions(dim int) Options {
	return Options{
		M:           16,
		EfConstruct: 200,
		EfSearch:    50,
		Dim:         dim,
		Dist:        DistanceL2Squared,
	}
}

// Node is a single vector in the HNSW graph.
type Node struct {
	ID        uint64
	Vector    []float32
	Level     int
	Neighbors [][]uint64 // neighbors[layer] = neighbor IDs at that layer
}

// Stats report the current state of the graph.
type Stats struct {
	TotalNodes  int
	Tombstoned  int
	MaxLevel    int
	MemoryBytes int64
}

// Graph is a Hierarchical Navigable Small World index.
//
// Concurrency model (for now, correctness-first):
//   - Search acquires RLock for the entire traversal.
//   - Insert, MarkDeleted, and Cleanup acquire Lock.
//   - A single graph-level RWMutex is used. Fine-grained locking
//     will be implemented in the future.
type Graph struct {
	mu         sync.RWMutex
	nodes      map[uint64]*Node
	entryPoint uint64
	maxLevel   int

	opts   Options
	logger *slog.Logger

	// Tombstones track soft-deleted nodes. They remain in the graph
	// for navigability but are excluded from search results.
	tombstones map[uint64]bool

	// memoryBytes is an estimated accounting metric, not actual Go heap.
	// Includes: vector float32s + neighbor slice capacity + per-node overhead.
	memoryBytes int64

	// mL is the level multiplier: 1 / ln(M). Cached at construction.
	mL float64
}

// New creates an HNSW graph with the given options. Returns an error
// if the options are invalid.
func New(opts Options) (*Graph, error) {
	if opts.Dim < 1 {
		return nil, fmt.Errorf("hnsw: Dim must be >= 1, got %d", opts.Dim)
	}
	if opts.M < 2 {
		return nil, fmt.Errorf("hnsw: M must be >= 2, got %d", opts.M)
	}
	if opts.EfConstruct < 1 {
		return nil, fmt.Errorf("hnsw: EfConstruct must be >= 1, got %d", opts.EfConstruct)
	}
	if opts.EfSearch < 1 {
		return nil, fmt.Errorf("hnsw: EfSearch must be >= 1, got %d", opts.EfSearch)
	}
	if opts.Dist == nil {
		opts.Dist = DistanceL2Squared
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Graph{
		nodes:      make(map[uint64]*Node),
		tombstones: make(map[uint64]bool),
		opts:       opts,
		logger:     logger,
		mL:         1.0 / math.Log(float64(opts.M)),
	}, nil
}

// maxM returns the neighbor capacity for upper layers (>= 1).
func (g *Graph) maxM() int {
	return g.opts.M
}

// maxM0 returns the neighbor capacity for layer 0.
func (g *Graph) maxM0() int {
	return 2 * g.opts.M
}

// neighborCap returns the maximum neighbor count for a given layer.
func (g *Graph) neighborCap(layer int) int {
	if layer == 0 {
		return g.maxM0()
	}
	return g.maxM()
}

// estimateNodeMemory returns the estimated memory in bytes for a node
// at the given level. This is an operational guardrail, not a precise heap.
func (g *Graph) estimateNodeMemory(level int) int64 {
	const nodeOverhead = 64 // struct + map entry overhead
	vecBytes := int64(g.opts.Dim) * 4
	var neighborBytes int64
	for l := 0; l <= level; l++ {
		neighborBytes += int64(g.neighborCap(l)) * 8
	}
	return vecBytes + neighborBytes + nodeOverhead
}

// randomLevel generates a random level for a new node using the
// geometric distribution from the HNSW paper.
func (g *Graph) randomLevel() int {
	return int(math.Floor(-math.Log(rand.Float64()) * g.mL))
}

// Insert adds a vector to the graph. Returns ErrDuplicateID if the id
// already exists, ErrDimensionMismatch if the vector length is wrong,
// or ErrMemoryLimitExceeded if the soft memory cap would be exceeded.
func (g *Graph) Insert(id uint64, vec []float32) error {
	if len(vec) != g.opts.Dim {
		return ErrDimensionMismatch
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.nodes[id]; exists {
		return ErrDuplicateID
	}

	level := g.randomLevel()
	mem := g.estimateNodeMemory(level)
	if g.opts.MaxMemoryBytes > 0 && g.memoryBytes+mem > g.opts.MaxMemoryBytes {
		return ErrMemoryLimitExceeded
	}

	node := &Node{
		ID:        id,
		Vector:    make([]float32, len(vec)),
		Level:     level,
		Neighbors: make([][]uint64, level+1),
	}
	copy(node.Vector, vec)
	for l := 0; l <= level; l++ {
		node.Neighbors[l] = make([]uint64, 0, g.neighborCap(l))
	}

	g.nodes[id] = node
	g.memoryBytes += mem

	// First node becomes the entry point.
	if len(g.nodes) == 1 {
		g.entryPoint = id
		g.maxLevel = level
		return nil
	}

	ep := g.entryPoint
	dist := g.opts.Dist

	// Phase 1: Greedy descent from maxLevel to level+1.
	// Find the single closest node at each layer above the new node's level.
	for l := g.maxLevel; l > level; l-- {
		ep = g.greedyClosest(vec, ep, l, dist)
	}

	// Phase 2: Search-and-connect from min(level, maxLevel) down to layer 0.
	topLayer := level
	if g.maxLevel < topLayer {
		topLayer = g.maxLevel
	}
	entryIDs := []uint64{ep}
	for l := topLayer; l >= 0; l-- {
		candidates := g.searchLayer(vec, entryIDs, g.opts.EfConstruct, l)
		neighbors := g.selectNeighborsHeuristic(vec, candidates, g.neighborCap(l))

		// Connect the new node to selected neighbors.
		node.Neighbors[l] = make([]uint64, len(neighbors))
		for i, n := range neighbors {
			node.Neighbors[l][i] = n.id
		}

		// Add reverse edges and shrink if overflowed.
		for _, n := range neighbors {
			neighbor := g.nodes[n.id]
			neighbor.Neighbors[l] = append(neighbor.Neighbors[l], id)
			capacity := g.neighborCap(l)
			if len(neighbor.Neighbors[l]) > capacity {
				g.shrinkNeighbors(neighbor, l, capacity)
			}
		}

		// Carry forward ALL search results as entry points for the next layer.
		// The paper uses W (full search result set), not just the selected neighbors,
		// so the lower-layer search starts from a broader set of positions.
		entryIDs = make([]uint64, len(candidates))
		for i, c := range candidates {
			entryIDs[i] = c.id
		}
	}

	// Update entry point if the new node has a higher level.
	if level > g.maxLevel {
		g.entryPoint = id
		g.maxLevel = level
	}

	return nil
}

// greedyClosest walks from ep through the given layer, always moving to
// the neighbor closest to query, until no improvement is found.
func (g *Graph) greedyClosest(query []float32, ep uint64, layer int, dist DistanceFunc) uint64 {
	bestID := ep
	bestDist := dist(query, g.nodes[ep].Vector)
	for {
		improved := false
		for _, nid := range g.nodes[bestID].Neighbors[layer] {
			if n, ok := g.nodes[nid]; ok {
				d := dist(query, n.Vector)
				if d < bestDist {
					bestID = nid
					bestDist = d
					improved = true
				}
			}
		}
		if !improved {
			return bestID
		}
	}
}

// selectNeighborsHeuristic implements Algorithm 4 from the HNSW paper.
// It selects up to maxN neighbors that are diverse: a candidate is only
// selected if it is closer to the query than to any already-selected neighbor.
// This prevents selecting clusters of nearby nodes as neighbors, improving
// graph navigability and recall.
func (g *Graph) selectNeighborsHeuristic(query []float32, candidates []heapItem, maxN int) []heapItem {
	slices.SortFunc(candidates, func(a, b heapItem) int {
		return cmp.Compare(a.dist, b.dist)
	})

	selected := make([]heapItem, 0, maxN)
	for _, c := range candidates {
		if len(selected) >= maxN {
			break
		}
		// Check if c is closer to the query than to any already-selected neighbor.
		good := true
		cNode := g.nodes[c.id]
		for _, s := range selected {
			sNode := g.nodes[s.id]
			if g.opts.Dist(cNode.Vector, sNode.Vector) < c.dist {
				good = false
				break
			}
		}
		if good {
			selected = append(selected, c)
		}
	}

	// If the heuristic didn't fill up to maxN, add discarded candidates
	// by distance order (keepPrunedConnections = true).
	if len(selected) < maxN {
		selectedSet := make(map[uint64]bool, len(selected))
		for _, s := range selected {
			selectedSet[s.id] = true
		}
		for _, c := range candidates {
			if len(selected) >= maxN {
				break
			}
			if !selectedSet[c.id] {
				selected = append(selected, c)
			}
		}
	}

	return selected
}

// shrinkNeighbors trims the node's neighbor list at the given layer to maxN
// by keeping the closest neighbors to the node itself.
func (g *Graph) shrinkNeighbors(node *Node, layer int, maxN int) {
	nbs := node.Neighbors[layer]
	type nd struct {
		id   uint64
		dist float32
	}
	ranked := make([]nd, 0, len(nbs))
	for _, nid := range nbs {
		if n, ok := g.nodes[nid]; ok {
			ranked = append(ranked, nd{nid, g.opts.Dist(node.Vector, n.Vector)})
		}
	}
	slices.SortFunc(ranked, func(a, b nd) int {
		return cmp.Compare(a.dist, b.dist)
	})
	if len(ranked) > maxN {
		ranked = ranked[:maxN]
	}
	node.Neighbors[layer] = node.Neighbors[layer][:len(ranked)]
	for i, n := range ranked {
		node.Neighbors[layer][i] = n.id
	}
}

// MarkDeleted soft-deletes a node. It remains in the graph for navigability
// (its neighbors are still explored during search) but is excluded from
// search results. Use Cleanup to physically remove tombstoned nodes.
func (g *Graph) MarkDeleted(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.nodes[id]; ok {
		g.tombstones[id] = true
	}
}

// Len returns the number of live (non-tombstoned) nodes.
func (g *Graph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes) - len(g.tombstones)
}

// Stats returns a snapshot of the graph's current state.
func (g *Graph) Stats() Stats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return Stats{
		TotalNodes:  len(g.nodes),
		Tombstoned:  len(g.tombstones),
		MaxLevel:    g.maxLevel,
		MemoryBytes: g.memoryBytes,
	}
}
