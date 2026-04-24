// Package antientropy implements background Merkle-tree reconciliation
// between co-replicas, catching silent divergence that read repair and
// hinted handoff miss: cold keys, expired hints, and transient failures
// where a write silently missed a replica.
//
// Data flow:
//  1. A Manager ticks, or receives an on-recovery or admin trigger.
//  2. For each shared range with a peer, both sides build a Merkle tree
//     over the (key, hlc_timestamp, deleted) triples of owned entries.
//  3. Roots are exchanged; on mismatch, the initiator descends the tree
//     via RPCs to localize divergent leaves.
//  4. Divergent leaves are reconciled by streaming (key, ts, deleted)
//     triples and applying LWW repairs through the shared ApplyRepair
//     helper in the cluster package, preserving source HLC bit-for-bit.
//
// Correctness invariants enforced here:
//   - Tree build iterates snapshot-filtered storage (the caller passes a
//     Source derived from db.ScanRange, which wraps SnapshotIterator).
//   - Bucket leaves are commutative XOR accumulators, independent of
//     insertion order.
//   - Grace period filter is symmetric: both sides use the same
//     GraceCutoffWall so in-flight writes don't appear as divergence.
package antientropy

import (
	"errors"
	"fmt"

	"github.com/ulixert/theseon/hlc"
)

// ErrInvalidTreeParams indicates fanout or depth were out of range.
var ErrInvalidTreeParams = errors.New("antientropy: invalid tree fanout/depth")

// Entry is a single (key, hlc, deleted) triple fed into a Merkle tree.
// The value bytes are intentionally excluded: LWW conflict resolution
// looks at (timestamp, deleted) alone, and including value in the hash
// would waste CPU on data the caller already knows matches when the
// envelope metadata matches.
type Entry struct {
	Key       []byte
	Timestamp hlc.Timestamp
	Deleted   bool
}

// Source iterates a set of entries that should participate in the tree.
// Implementations typically wrap db.ScanRange and filter to entries the
// local node co-replicates with the target peer.
type Source interface {
	// Next returns the next entry. ok=false signals end of iteration.
	Next() (Entry, bool, error)
	Close() error
}

// Tree is a fixed-fanout, fixed-depth Merkle tree over Entries, stored
// level-by-level as flat []uint64 slices.
//
//	levels[0] holds the root (1 node).
//	levels[depth] holds fanout^depth leaves (bucket hashes).
//
// Leaf hashes are commutative XOR accumulators; internal hashes are
// xxhash64 over the concatenation of child hashes in child-index order.
type Tree struct {
	fanout int
	depth  int
	levels [][]uint64 // levels[0]=root, levels[depth]=leaves
}

// NewTree creates an empty tree with the given fanout and depth.
// A 2-argument validation enforces: fanout >= 2, depth >= 1, and the
// bucket count stays within an int (fanout^depth).
func NewTree(fanout, depth int) (*Tree, error) {
	if fanout < 2 {
		return nil, fmt.Errorf("%w: fanout=%d (must be >= 2)", ErrInvalidTreeParams, fanout)
	}
	if depth < 1 || depth > 8 {
		return nil, fmt.Errorf("%w: depth=%d (must be 1..8)", ErrInvalidTreeParams, depth)
	}

	t := &Tree{
		fanout: fanout,
		depth:  depth,
		levels: make([][]uint64, depth+1),
	}
	size := 1
	for i := 0; i <= depth; i++ {
		t.levels[i] = make([]uint64, size)
		// Guard against int overflow on absurd configs (fanout=16, depth=9 = 16^9 ~ 7*10^10).
		if i < depth {
			next := size * fanout
			if next/fanout != size {
				return nil, fmt.Errorf("%w: fanout^depth overflow", ErrInvalidTreeParams)
			}
			size = next
		}
	}
	return t, nil
}
