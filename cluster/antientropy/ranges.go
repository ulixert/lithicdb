package antientropy

import "github.com/ulixert/theseon/hashring"

// OwnedPeers returns the peer node IDs that share at least one replica
// set with selfID under the given replication factor N. Result is sorted
// and excludes selfID.
func OwnedPeers(ring *hashring.Ring, selfID string, n int) []string {
	if ring == nil {
		return nil
	}
	return ring.CoReplicas(selfID, n)
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
