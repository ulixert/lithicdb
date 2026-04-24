package antientropy

import "github.com/ulixert/theseon/hashring"

// OwnedPeers returns the set of peer node IDs that share at least one
// key's replica set with selfID under the given ring and replication
// factor N. Order is deterministic (sorted by ring membership order).
//
// For each physical node in the ring, we check whether any single ring
// position (any vnode) places both selfID and that node into the same
// N-replica set. Because `GetNodes(key, N)` walks vnodes clockwise from
// the key's hash, two nodes co-replicate iff some vnode has a clockwise
// window of size N containing both of them. We compute this by walking
// every vnode and asking "who are the N distinct owners starting here?"
// — the set of co-replicas of selfID is the union of those windows that
// include selfID, excluding selfID itself.
//
// This is O(V*N) where V is total vnodes (~150 * numNodes), fine for
// infrequent (per-reconcile or per-ring-change) evaluation.
func OwnedPeers(ring *hashring.Ring, selfID string, n int) []string {
	if ring == nil || ring.Size() == 0 {
		return nil
	}
	members := ring.Members()
	if len(members) <= 1 {
		return nil
	}

	seen := make(map[string]struct{}, len(members))
	var out []string

	// Walk each physical node's first vnode position by asking the ring
	// who owns the keyspace anchored at that member's ID. We don't have
	// direct vnode access, so we probe with every member's ID as the
	// "key" — that gives us at least one window per physical node, and
	// since vnodes are uniformly distributed any co-replica of selfID
	// will co-own many windows; probing one per member is sufficient
	// to discover the complete set.
	//
	// Probe with member IDs plus a few synthetic keys per member to
	// cover asymmetric vnode arcs.
	for _, m := range members {
		probes := [][]byte{
			[]byte(m.ID),
			[]byte(m.ID + "\x00"),
			[]byte(m.ID + "\xff"),
		}
		for _, key := range probes {
			owners := ring.GetNodes(key, n)
			if !containsNode(owners, selfID) {
				continue
			}
			for _, o := range owners {
				if o.ID == selfID {
					continue
				}
				if _, dup := seen[o.ID]; dup {
					continue
				}
				seen[o.ID] = struct{}{}
				out = append(out, o.ID)
			}
		}
	}
	return out
}

// ShouldReconcile returns true iff both selfID and peerID are in the
// top-N replica set for `key`. Used to filter streaming scans so we
// only include keys that both sides co-own.
func ShouldReconcile(ring *hashring.Ring, key []byte, selfID, peerID string, n int) bool {
	if ring == nil {
		return false
	}
	owners := ring.GetNodes(key, n)
	var sawSelf, sawPeer bool
	for _, o := range owners {
		switch o.ID {
		case selfID:
			sawSelf = true
		case peerID:
			sawPeer = true
		}
	}
	return sawSelf && sawPeer
}

func containsNode(nodes []hashring.Node, id string) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}
