package hashring

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestDeterministicPlacement(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	key := []byte("test-key")
	first := r.GetNode(key)

	for range 100 {
		got := r.GetNode(key)
		if got.ID != first.ID {
			t.Fatalf("non-deterministic: got %q then %q for same key", first.ID, got.ID)
		}
	}
}

func TestSingleNode(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "only", Addr: "10.0.0.1:9090"})

	for i := range 100 {
		key := []byte(fmt.Sprintf("key-%d", i))
		node := r.GetNode(key)
		if node.ID != "only" {
			t.Fatalf("key %d: got %q, want %q", i, node.ID, "only")
		}
	}
}

func TestGetNodesDistinct(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	nodes := r.GetNodes([]byte("some-key"), 3)
	if len(nodes) != 3 {
		t.Fatalf("GetNodes(3): got %d nodes, want 3", len(nodes))
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n.ID] {
			t.Fatalf("duplicate node %q in GetNodes result", n.ID)
		}
		seen[n.ID] = true
	}
}

func TestGetNodesExceedsClusterSize(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})

	nodes := r.GetNodes([]byte("key"), 5)
	if len(nodes) != 2 {
		t.Fatalf("GetNodes(5) with 2 nodes: got %d, want 2", len(nodes))
	}
}

func TestGetNodesConsistentWithGetNode(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	for i := range 100 {
		key := []byte(fmt.Sprintf("key-%d", i))
		primary := r.GetNode(key)
		nodes := r.GetNodes(key, 1)
		if len(nodes) != 1 {
			t.Fatalf("key %d: GetNodes(1) returned %d nodes", i, len(nodes))
		}
		if nodes[0].ID != primary.ID {
			t.Fatalf("key %d: GetNode=%q, GetNodes(1)=%q", i, primary.ID, nodes[0].ID)
		}
	}
}

func TestAddNodeMinimalDisruption(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	const numKeys = 10000
	before := make(map[string]string, numKeys) // key → nodeID
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		before[key] = r.GetNode([]byte(key)).ID
	}

	r.AddNode(Node{ID: "node-4", Addr: "10.0.0.4:9090"})

	moved := 0
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		after := r.GetNode([]byte(key)).ID
		if after != before[key] {
			moved++
		}
	}

	// With 3→4 nodes, expect ~25% keys to move. Allow 15-40% range
	// to account for hash distribution variance.
	movedPct := float64(moved) / numKeys * 100
	if movedPct < 15 || movedPct > 40 {
		t.Fatalf("%.1f%% keys moved (want 15-40%% for 3→4 node transition)", movedPct)
	}
}

func TestRemoveNodeRestoresPlacement(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	const numKeys = 1000
	before := make(map[string]string, numKeys)
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		before[key] = r.GetNode([]byte(key)).ID
	}

	// Add then remove — should return to original state.
	r.AddNode(Node{ID: "node-4", Addr: "10.0.0.4:9090"})
	r.RemoveNode("node-4")

	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		after := r.GetNode([]byte(key)).ID
		if after != before[key] {
			t.Fatalf("key %q: was %q, after add+remove got %q", key, before[key], after)
		}
	}
}

func TestDistributionUniformity(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})
	r.AddNode(Node{ID: "node-3", Addr: "10.0.0.3:9090"})

	const numKeys = 10000
	counts := make(map[string]int)
	for i := range numKeys {
		key := []byte(fmt.Sprintf("key-%d", i))
		node := r.GetNode(key)
		counts[node.ID]++
	}

	mean := float64(numKeys) / 3.0
	var variance float64
	for _, count := range counts {
		diff := float64(count) - mean
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / 3.0)
	cvPct := stddev / mean * 100

	if cvPct > 15 {
		t.Fatalf("distribution too skewed: stddev=%.0f, mean=%.0f, CV=%.1f%% (want <15%%)\ncounts: %v",
			stddev, mean, cvPct, counts)
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(150)

	node := r.GetNode([]byte("key"))
	if node.ID != "" || node.Addr != "" {
		t.Fatalf("GetNode on empty ring: got %+v, want zero Node", node)
	}

	nodes := r.GetNodes([]byte("key"), 3)
	if nodes != nil {
		t.Fatalf("GetNodes on empty ring: got %v, want nil", nodes)
	}

	if r.Size() != 0 {
		t.Fatalf("Size on empty ring: got %d, want 0", r.Size())
	}
}

func TestAddNodeIdempotent(t *testing.T) {
	r := New(150)
	node := Node{ID: "node-1", Addr: "10.0.0.1:9090"}

	r.AddNode(node)
	vnodesBefore := len(r.vnodes)

	r.AddNode(node)
	vnodesAfter := len(r.vnodes)

	if vnodesBefore != vnodesAfter {
		t.Fatalf("double AddNode: vnodes went from %d to %d", vnodesBefore, vnodesAfter)
	}
}

func TestRemoveNodeIdempotent(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})

	r.RemoveNode("node-1")
	r.RemoveNode("node-1") // should not panic

	if r.Size() != 0 {
		t.Fatalf("Size after double remove: got %d, want 0", r.Size())
	}
}

func TestMembers(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})

	members := r.Members()
	if len(members) != 2 {
		t.Fatalf("Members: got %d, want 2", len(members))
	}

	ids := make(map[string]bool)
	for _, m := range members {
		ids[m.ID] = true
	}
	if !ids["node-1"] || !ids["node-2"] {
		t.Fatalf("Members: missing expected nodes, got %v", members)
	}
}

func TestConcurrentSafety(t *testing.T) {
	r := New(150)
	r.AddNode(Node{ID: "node-1", Addr: "10.0.0.1:9090"})
	r.AddNode(Node{ID: "node-2", Addr: "10.0.0.2:9090"})

	var wg sync.WaitGroup
	const goroutines = 8
	const ops = 1000

	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range ops {
				key := []byte(fmt.Sprintf("key-%d-%d", id, i))
				switch i % 4 {
				case 0:
					r.GetNode(key)
				case 1:
					r.GetNodes(key, 3)
				case 2:
					nodeID := fmt.Sprintf("temp-%d-%d", id, i)
					r.AddNode(Node{ID: nodeID, Addr: "127.0.0.1:0"})
				case 3:
					nodeID := fmt.Sprintf("temp-%d-%d", id, i-1)
					r.RemoveNode(nodeID)
				}
			}
		}(g)
	}

	wg.Wait()
}
