package antientropy

import (
	"fmt"
	"testing"

	"github.com/ulixert/theseon/hashring"
)

func newRingWith(ids ...string) *hashring.Ring {
	r := hashring.New(150)
	for _, id := range ids {
		r.AddNode(hashring.Node{ID: id, Addr: id + ":0"})
	}
	return r
}

func TestOwnedPeers_EmptyRing(t *testing.T) {
	t.Parallel()
	ring := hashring.New(150)
	peers := OwnedPeers(ring, "self", 3)
	if peers != nil {
		t.Errorf("empty ring should yield nil, got %v", peers)
	}
}

func TestOwnedPeers_SingleNode(t *testing.T) {
	t.Parallel()
	ring := newRingWith("self")
	peers := OwnedPeers(ring, "self", 3)
	if peers != nil {
		t.Errorf("single-node ring has no peers, got %v", peers)
	}
}

func TestOwnedPeers_TwoNodes(t *testing.T) {
	t.Parallel()
	ring := newRingWith("a", "b")
	peers := OwnedPeers(ring, "a", 2)
	if len(peers) != 1 || peers[0] != "b" {
		t.Errorf("expected [b], got %v", peers)
	}
}

func TestOwnedPeers_ThreeNodes_N3(t *testing.T) {
	t.Parallel()
	// With N=3 and 3 nodes, every node co-replicates with the other two
	// on at least some keys.
	ring := newRingWith("a", "b", "c")
	peers := OwnedPeers(ring, "a", 3)
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %v", peers)
	}
	saw := map[string]bool{}
	for _, p := range peers {
		saw[p] = true
	}
	if !saw["b"] || !saw["c"] {
		t.Errorf("peers should be {b,c}, got %v", peers)
	}
}

func TestOwnedPeers_FiveNodes_N3(t *testing.T) {
	t.Parallel()
	// With 5 nodes and N=3, any given key replicates to exactly 3 of 5
	// nodes. Over many keys the co-replica set for 'a' typically covers
	// all other 4 nodes — assert that explicitly.
	ring := newRingWith("a", "b", "c", "d", "e")
	peers := OwnedPeers(ring, "a", 3)
	// Must be subset of {b, c, d, e}, no self, no duplicates.
	seen := make(map[string]int)
	for _, p := range peers {
		if p == "a" {
			t.Errorf("peers must exclude self")
		}
		seen[p]++
	}
	for k, count := range seen {
		if count > 1 {
			t.Errorf("peer %s appears %d times", k, count)
		}
	}
	if len(peers) == 0 || len(peers) > 4 {
		t.Errorf("expected 1..4 peers, got %d: %v", len(peers), peers)
	}
}

func TestShouldReconcile_Symmetric(t *testing.T) {
	t.Parallel()
	ring := newRingWith("a", "b", "c")

	// For any probe key, ShouldReconcile(ring, key, a, b, 3) must equal
	// ShouldReconcile(ring, key, b, a, 3) — co-ownership is a symmetric
	// relation on keys.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("test-key-%d", i))
		ab := ShouldReconcile(ring, key, "a", "b", 3)
		ba := ShouldReconcile(ring, key, "b", "a", 3)
		if ab != ba {
			t.Errorf("ShouldReconcile is not symmetric for key %q: a-b=%v b-a=%v", key, ab, ba)
		}
	}
}

func TestShouldReconcile_N3_3Nodes_AlwaysTrue(t *testing.T) {
	t.Parallel()
	// With 3 nodes and N=3, every key replicates to ALL nodes, so any
	// pair co-replicates every key.
	ring := newRingWith("a", "b", "c")
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		if !ShouldReconcile(ring, key, "a", "b", 3) {
			t.Errorf("with N=len(ring)=3, every pair should co-own every key; failed for %q", key)
		}
	}
}

func TestShouldReconcile_NilRing(t *testing.T) {
	t.Parallel()
	if ShouldReconcile(nil, []byte("k"), "a", "b", 3) {
		t.Error("nil ring should return false")
	}
}
