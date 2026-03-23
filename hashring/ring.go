// Package hashring implements a consistent hash ring with virtual nodes.
// It maps arbitrary keys to a set of physical nodes with minimal disruption
// when nodes are added or removed - only ~1/N of keys move on a topology
// change, compared to full rehashing.
//
// This is a pure data structure with no networking or I/O. It is safe
// for concurrent use by multiple goroutines.
package hashring

import (
	"sort"
	"sync"
)

// Node represents a physical node in the cluster.
type Node struct {
	ID   string // unique identifier, e.g. "node-1"
	Addr string // network address, e.g. "10.0.0.1:9090"
}

// vnode is a point on the hash ring owned by a physical node.
type vnode struct {
	hash   uint64
	nodeID string
}

// Ring is a consistent hash ring with virtual nodes. Each physical node
// is mapped to multiple points (virtual nodes) on the ring for uniform
// key distribution. Lookups find the first vnode clockwise from the
// key's hash position.
type Ring struct {
	mu           sync.RWMutex
	vnodes       []vnode         // sorted by hash
	nodes        map[string]Node // nodeID → Node
	virtualNodes int             // vnodes per physical node
}

// New creates an empty consistent hash ring. Each physical node added
// will be represented by virtualNodes points on the ring. A typical
// value is 150 - with 3 physical nodes that give 450 vnodes, enough
// for good distribution without excessive memory.
func New(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 150
	}
	return &Ring{
		nodes:        make(map[string]Node),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a physical node to the ring. If a node with the same ID
// already exists, this is a no-op.
func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node.ID]; exists {
		return
	}

	r.nodes[node.ID] = node

	for i := range r.virtualNodes {
		h := hashVnode(node.ID, i)
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeID: node.ID})
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the
// ring. If the node doesn't exist, this is a no-op.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}

	delete(r.nodes, nodeID)

	// Filter out all vnodes belonging to the removed node.
	filtered := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.nodeID != nodeID {
			filtered = append(filtered, v)
		}
	}
	r.vnodes = filtered
}
