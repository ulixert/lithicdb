package hnsw

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
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
