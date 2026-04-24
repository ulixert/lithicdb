package antientropy

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ulixert/theseon/hlc"
)

// sliceSource is an in-memory Source for tests.
type sliceSource struct {
	entries []Entry
	i       int
	closed  bool
}

func (s *sliceSource) Next() (Entry, bool, error) {
	if s.i >= len(s.entries) {
		return Entry{}, false, nil
	}
	e := s.entries[s.i]
	s.i++
	return e, true, nil
}

func (s *sliceSource) Close() error {
	s.closed = true
	return nil
}

func makeEntry(key string, wall int64, logical uint32, nodeID string, deleted bool) Entry {
	return Entry{
		Key: []byte(key),
		Timestamp: hlc.Timestamp{
			WallTime: wall,
			Logical:  logical,
			NodeID:   nodeID,
		},
		Deleted: deleted,
	}
}

func TestNewTree_ValidatesParams(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		fanout, depth int
		wantErr       bool
	}{
		{"ok_16_4", 16, 4, false},
		{"ok_2_1", 2, 1, false},
		{"fanout_too_small", 1, 4, true},
		{"depth_zero", 16, 0, true},
		{"depth_too_large", 16, 9, true},
	}
	for _, c := range cases {
		_, err := NewTree(c.fanout, c.depth)
		if (err != nil) != c.wantErr {
			t.Errorf("NewTree(%d, %d): err=%v, wantErr=%v", c.fanout, c.depth, err, c.wantErr)
		}
	}
}

func TestTree_Determinism(t *testing.T) {
	t.Parallel()
	// Same entries in two different orders must produce the same root.
	entries := []Entry{
		makeEntry("alpha", 1000, 0, "n1", false),
		makeEntry("beta", 2000, 5, "n2", false),
		makeEntry("gamma", 3000, 0, "n1", true),
		makeEntry("delta", 4000, 1, "n3", false),
	}

	srcA := &sliceSource{entries: append([]Entry{}, entries...)}
	treeA, err := BuildTree(srcA, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("BuildTree A: %v", err)
	}

	// Reverse order.
	reversed := append([]Entry{}, entries...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	srcB := &sliceSource{entries: reversed}
	treeB, err := BuildTree(srcB, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("BuildTree B: %v", err)
	}

	if treeA.Root() != treeB.Root() {
		t.Errorf("root should be order-independent: A=%x B=%x", treeA.Root(), treeB.Root())
	}
	if !srcA.closed || !srcB.closed {
		t.Error("BuildTree must Close the source")
	}
}

func TestTree_Commutativity(t *testing.T) {
	t.Parallel()
	// Fuzz: random permutations must all produce the same root.
	r := rand.New(rand.NewSource(42))

	entries := make([]Entry, 100)
	for i := range entries {
		entries[i] = makeEntry(fmt.Sprintf("k%d", i), int64(1000+i), uint32(i), "n1", i%7 == 0)
	}

	srcFirst := &sliceSource{entries: append([]Entry{}, entries...)}
	first, err := BuildTree(srcFirst, 1_000_000_000, 4, 2)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	expected := first.Root()

	for trial := 0; trial < 5; trial++ {
		shuffled := append([]Entry{}, entries...)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		src := &sliceSource{entries: shuffled}
		tree, err := BuildTree(src, 1_000_000_000, 4, 2)
		if err != nil {
			t.Fatalf("trial %d: %v", trial, err)
		}
		if tree.Root() != expected {
			t.Errorf("trial %d root %x != expected %x", trial, tree.Root(), expected)
		}
	}
}

func TestTree_DivergenceLocalization(t *testing.T) {
	t.Parallel()
	// Changing one key's timestamp must flip exactly one leaf bucket and
	// the root-to-leaf path of internal nodes. Siblings along the path
	// must be unchanged.
	base := []Entry{
		makeEntry("k1", 1000, 0, "n1", false),
		makeEntry("k2", 2000, 0, "n1", false),
		makeEntry("k3", 3000, 0, "n1", false),
		makeEntry("k4", 4000, 0, "n1", false),
	}

	srcA := &sliceSource{entries: append([]Entry{}, base...)}
	treeA, err := BuildTree(srcA, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("treeA: %v", err)
	}

	mutated := append([]Entry{}, base...)
	mutated[1] = makeEntry("k2", 2001, 0, "n1", false) // bump wall time by 1
	srcB := &sliceSource{entries: mutated}
	treeB, err := BuildTree(srcB, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("treeB: %v", err)
	}

	if treeA.Root() == treeB.Root() {
		t.Fatal("roots should differ when one entry changed")
	}

	// The mutated key maps to exactly one bucket. Count differing leaves.
	diffLeaves := 0
	for i := 0; i < treeA.NumLeaves(); i++ {
		if treeA.LeafHash(i) != treeB.LeafHash(i) {
			diffLeaves++
		}
	}
	if diffLeaves != 1 {
		t.Errorf("exactly one leaf should differ, got %d", diffLeaves)
	}

	// The bucket path's sibling children should be identical at every level.
	bucket := treeA.BucketFor([]byte("k2"))
	path := treeA.PathForBucket(bucket)
	for L := 0; L < len(path); L++ {
		childrenA, _ := treeA.ChildrenAt(path[:L])
		childrenB, _ := treeB.ChildrenAt(path[:L])
		if len(childrenA) != len(childrenB) {
			t.Fatalf("level %d: child count mismatch", L)
		}
		for i := range childrenA {
			if i == path[L] {
				continue // the child on the divergent path
			}
			if childrenA[i] != childrenB[i] {
				t.Errorf("level %d sibling idx %d diverged unexpectedly", L, i)
			}
		}
	}
}

func TestTree_GracePeriodFilter(t *testing.T) {
	t.Parallel()
	// Entries with wall time >= cutoff must not contribute to the tree.
	entries := []Entry{
		makeEntry("old", 100, 0, "n1", false),    // contributes
		makeEntry("future", 500, 0, "n1", false), // filtered
	}

	src := &sliceSource{entries: entries}
	treeFiltered, err := BuildTree(src, 400, 4, 2) // cutoff excludes "future"
	if err != nil {
		t.Fatalf("treeFiltered: %v", err)
	}

	// Build a tree with only the one entry that should survive the filter.
	srcOnlyOld := &sliceSource{entries: []Entry{entries[0]}}
	treeOnlyOld, err := BuildTree(srcOnlyOld, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("treeOnlyOld: %v", err)
	}

	if treeFiltered.Root() != treeOnlyOld.Root() {
		t.Errorf("grace filter should exclude entry at wall=500 with cutoff=400; roots differ")
	}
}

// TestTree_SnapshotSemantics covers invariant #1: the tree must be built
// from a snapshot-filtered iterator (only latest version per key). This
// test simulates a correct Source — a single latest Entry per key — and
// verifies that a broken Source emitting an older stale version along
// with the newer one produces a DIFFERENT tree. The invariant is that
// the Source filters to latest versions; this test documents what
// happens when it doesn't, so future code that swaps in a raw iterator
// would visibly break.
func TestTree_SnapshotSemantics(t *testing.T) {
	t.Parallel()
	// Good source: one latest entry per key.
	good := &sliceSource{entries: []Entry{
		makeEntry("k1", 2000, 0, "n1", false), // latest
	}}
	goodTree, err := BuildTree(good, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("good: %v", err)
	}

	// Bad source: emits both old and latest for the same key (simulates
	// accidentally using a raw all-versions iterator).
	bad := &sliceSource{entries: []Entry{
		makeEntry("k1", 1000, 0, "n1", false), // old — should have been filtered
		makeEntry("k1", 2000, 0, "n1", false), // latest
	}}
	badTree, err := BuildTree(bad, 1_000_000, 4, 2)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}

	if goodTree.Root() == badTree.Root() {
		t.Fatal("a non-snapshot source should produce a different tree; otherwise the invariant would not be observable")
	}
}

func TestTree_PathForBucket_RoundTrip(t *testing.T) {
	t.Parallel()
	tree, err := NewTree(16, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Recompose bucket from path for every bucket and check.
	for b := 0; b < tree.NumLeaves(); b++ {
		path := tree.PathForBucket(b)
		if len(path) != tree.Depth() {
			t.Fatalf("bucket %d: path len %d want %d", b, len(path), tree.Depth())
		}
		recon := 0
		for _, p := range path {
			recon = recon*tree.Fanout() + p
		}
		if recon != b {
			t.Errorf("bucket %d round-tripped to %d via path %v", b, recon, path)
		}
	}
}

func TestTree_ChildrenAt(t *testing.T) {
	t.Parallel()
	tree, err := NewTree(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Populate distinct leaf hashes.
	for i := 0; i < tree.NumLeaves(); i++ {
		tree.levels[tree.depth][i] = uint64(i) + 1 // avoid zero
	}
	tree.Finalize()

	root := tree.Root()
	rootChildren, err := tree.ChildrenAt(nil)
	if err != nil {
		t.Fatalf("ChildrenAt([]): %v", err)
	}
	if len(rootChildren) != 4 {
		t.Errorf("want 4 root children, got %d", len(rootChildren))
	}
	// Hashing root-children must equal root.
	recomputed := hashChildren(rootChildren)
	if recomputed != root {
		t.Errorf("hashChildren(root children) = %x, want root %x", recomputed, root)
	}

	// Leaves have no children.
	if _, err := tree.ChildrenAt([]int{0, 0}); err == nil {
		t.Error("ChildrenAt at depth should error")
	}
}
