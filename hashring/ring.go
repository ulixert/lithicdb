// Package hashring implements a consistent hash ring with virtual nodes.
// It maps arbitrary keys to a set of physical nodes with minimal disruption
// when nodes are added or removed - only ~1/N of keys move on a topology
// change, compared to full rehashing.
//
// This is a pure data structure with no networking or I/O. It is safe
// for concurrent use by multiple goroutines.
package hashring

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"strconv"
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
	mu            sync.RWMutex
	vnodes        []vnode         // sorted by hash
	nodes         map[string]Node // nodeID → Node
	vnodesPerNode int             // virtual nodes per physical node
}

// New creates an empty consistent hash ring. Each physical node added
// will be represented by vnodesPerNode points on the ring. A typical
// value is 150 - with 3 physical nodes that give 450 vnodes, enough
// for good distribution without excessive memory.
func New(vnodesPerNode int) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 150
	}
	return &Ring{
		nodes:         make(map[string]Node),
		vnodesPerNode: vnodesPerNode,
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

	for i := range r.vnodesPerNode {
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

// GetNode returns the primary owner of the given key - the first node
// found clockwise from the key's hash position on the ring.
// Returns a zero Node if the ring is empty.
func (r *Ring) GetNode(key []byte) Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return Node{}
	}

	h := hashKey(key)
	idx := r.search(h)
	return r.nodes[r.vnodes[idx].nodeID]
}

// GetNodes returns up to n distinct physical nodes responsible for the
// given key, walking clockwise from the key's hash position. If fewer
// than n physical nodes exist, all nodes are returned.
// Returns nil if the ring is empty.
func (r *Ring) GetNodes(key []byte, n int) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}

	// Cap n to the number of physical nodes.
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := hashKey(key)
	idx := r.search(h)

	result := make([]Node, 0, n)
	seen := make(map[string]struct{}, n)

	for range r.vnodes {
		vn := r.vnodes[idx]
		if _, ok := seen[vn.nodeID]; !ok {
			seen[vn.nodeID] = struct{}{}
			result = append(result, r.nodes[vn.nodeID])
			if len(result) == n {
				break
			}
		}
		idx = (idx + 1) % len(r.vnodes)
	}

	return result
}

// Members returns all physical nodes currently in the ring.
// The order is not guaranteed.
func (r *Ring) Members() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		members = append(members, node)
	}
	return members
}

// ReplaceMembers atomically rebuilds the ring with the given set of nodes.
// The new vnodes slice and nodes map are built entirely before taking the
// write lock, so concurrent readers never observe an empty or half-built ring.
func (r *Ring) ReplaceMembers(nodes []Node) {
	newNodes := make(map[string]Node, len(nodes))
	newVnodes := make([]vnode, 0, len(nodes)*r.vnodesPerNode)
	for _, n := range nodes {
		newNodes[n.ID] = n
		for i := range r.vnodesPerNode {
			newVnodes = append(newVnodes, vnode{hash: hashVnode(n.ID, i), nodeID: n.ID})
		}
	}
	slices.SortFunc(newVnodes, func(a, b vnode) int {
		return cmp.Compare(a.hash, b.hash)
	})

	r.mu.Lock()
	r.nodes = newNodes
	r.vnodes = newVnodes
	r.mu.Unlock()
}

// Size returns the number of physical nodes in the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// search returns the index of the first vnode with hash >= h.
// If no such vnode exists (h is past the last vnode), wraps to 0.
// Caller must hold at least r.mu.RLock.
func (r *Ring) search(h uint64) int {
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		return 0
	}
	return idx
}

// hashVnode computes the ring position for a virtual node.
// Format: SHA-256("{nodeID}-{index}"), truncated to uint64.
func hashVnode(nodeID string, index int) uint64 {
	// Build "{nodeID}-{index}" without fmt.Sprintf allocation.
	buf := make([]byte, 0, len(nodeID)+1+10) // nodeID + '-' + up to 10 digits
	buf = append(buf, nodeID...)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, int64(index), 10)
	sum := sha256.Sum256(buf)
	return binary.BigEndian.Uint64(sum[:8])
}

// hashKey computes the ring position for an arbitrary key.
// SHA-256 of raw key bytes, truncated to uint64.
func hashKey(key []byte) uint64 {
	sum := sha256.Sum256(key)
	return binary.BigEndian.Uint64(sum[:8])
}
