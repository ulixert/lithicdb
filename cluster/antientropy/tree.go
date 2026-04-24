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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/ulixert/theseon/hlc"
)

// ErrInvalidTreeParams indicates fanout or depth were out of range.
var ErrInvalidTreeParams = errors.New("anti entropy: invalid tree fanout/depth")

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

// Fanout returns the configured fanout.
func (t *Tree) Fanout() int { return t.fanout }

// Depth returns the configured depth.
func (t *Tree) Depth() int { return t.depth }

// NumLeaves returns fanout^depth.
func (t *Tree) NumLeaves() int { return len(t.levels[t.depth]) }

// Root returns the root hash. Only meaningful after Finalize().
func (t *Tree) Root() uint64 { return t.levels[0][0] }

// LeafHash returns the hash of the leaf at the given bucket index.
func (t *Tree) LeafHash(idx int) uint64 { return t.levels[t.depth][idx] }

// BucketFor returns the leaf bucket index for the given user key.
// The mapping is deterministic and identical across nodes with matching
// fanout/depth parameters.
func (t *Tree) BucketFor(key []byte) int {
	return int(xxhash.Sum64(key) % uint64(t.NumLeaves()))
}

// PathForBucket decomposes a bucket index into a child-index path from
// the root. len(path) == depth.
func (t *Tree) PathForBucket(bucketIdx int) []int {
	path := make([]int, t.depth)
	n := bucketIdx
	for i := t.depth - 1; i >= 0; i-- {
		path[i] = n % t.fanout
		n /= t.fanout
	}
	return path
}

// ChildrenAt returns the fanout child hashes of the internal node at
// `path`. The path is a list of child indices from the root (length
// 0 = root itself, returning its direct children).
//
// Returns an error if the path is invalid or points at a leaf.
func (t *Tree) ChildrenAt(path []int) ([]uint64, error) {
	if len(path) < 0 || len(path) >= t.depth+1 {
		return nil, fmt.Errorf("anti entropy: path len %d exceeds depth %d", len(path), t.depth)
	}
	if len(path) == t.depth {
		return nil, errors.New("anti entropy: leaves have no children")
	}
	// Compute the node's flat index within its level from the root-to-node path.
	idx := 0
	for _, p := range path {
		if p < 0 || p >= t.fanout {
			return nil, fmt.Errorf("anti entropy: path element %d out of [0,%d)", p, t.fanout)
		}
		idx = idx*t.fanout + p
	}

	childLevel := t.levels[len(path)+1]
	start := idx * t.fanout
	return childLevel[start : start+t.fanout], nil
}

// AccumulateEntry mixes an entry into its bucket leaf via commutative
// XOR. Safe to call in any order; the resulting leaf hash depends only
// on the set of entries, not insertion order.
func (t *Tree) AccumulateEntry(e Entry) {
	bucket := t.BucketFor(e.Key)
	t.levels[t.depth][bucket] ^= entryHash(e)
}

// Finalize propagates leaf hashes up the tree to compute internal
// nodes and root. Must be called before Root/ChildrenAt return
// meaningful values.
func (t *Tree) Finalize() {
	for level := t.depth - 1; level >= 0; level-- {
		parent := t.levels[level]
		child := t.levels[level+1]
		for i := range parent {
			parent[i] = hashChildren(child[i*t.fanout : (i+1)*t.fanout])
		}
	}
}

// BuildTree drains a Source, filtering out entries whose HLC wall time
// is at or after graceCutoffWall (exclude in-flight writes from
// divergence detection). The caller is responsible for supplying a
// Source backed by snapshot-filtered storage.
//
// graceCutoffWall is in nanoseconds since epoch, matching hlc.Timestamp.WallTime.
// Callers typically pass `time.Now().Add(-grace).UnixNano()` - entries
// with WallTime < graceCutoffWall participate; younger ones are filtered.
func BuildTree(src Source, graceCutoffWall int64, fanout, depth int) (*Tree, error) {
	t, err := NewTree(fanout, depth)
	if err != nil {
		return nil, err
	}
	for {
		e, ok, err := src.Next()
		if err != nil {
			_ = src.Close()
			return nil, err
		}
		if !ok {
			break
		}
		if e.Timestamp.WallTime >= graceCutoffWall {
			continue
		}
		t.AccumulateEntry(e)
	}
	if err := src.Close(); err != nil {
		return nil, err
	}
	t.Finalize()
	return t, nil
}

// entryHash hashes a (key, hlc, deleted) triple into a uint64 suitable
// for XOR accumulation into a bucket. Value bytes are intentionally
// excluded; LWW compares (ts, deleted) only.
//
// The hash must be stable across nodes and across time, so it uses
// encoded HLC bytes rather than the Go struct layout.
func entryHash(e Entry) uint64 {
	keyH := xxhash.Sum64(e.Key)

	// Encode the HLC timestamp into its canonical byte form so nodes
	// with different Go padding / struct layouts still hash identically.
	// HLC encode is only fallible on NodeID length > uint16 max — never
	// true in practice. Ignore the error at this layer to keep the hot
	// path allocation-light; callers validate upstream.
	tsBytes, _ := e.Timestamp.Encode()
	tsH := xxhash.Sum64(tsBytes)

	var delByte uint64
	if e.Deleted {
		delByte = 1
	}

	// XOR-combine so each leaf bucket can accumulate entries in any
	// order and still converge on the same hash.
	return keyH ^ tsH ^ delByte
}

// hashChildren hashes the concatenation of child hashes (big-endian,
// 8 bytes each) in order. This is position-sensitive: swapping two
// children changes the parent hash, which is what we want for internal
// nodes (to localize divergence to a specific child index).
func hashChildren(children []uint64) uint64 {
	buf := make([]byte, 8*len(children))
	for i, h := range children {
		binary.BigEndian.PutUint64(buf[i*8:], h)
	}
	return xxhash.Sum64(buf)
}
