package hnsw

import "testing"

func TestCleanup_Empty(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 10; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}
	if n := g.Cleanup(); n != 0 {
		t.Errorf("Cleanup with no tombstones removed %d nodes", n)
	}
}

func TestCleanup_RemovesNodes(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 20; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}
	for i := uint64(0); i < 5; i++ {
		g.MarkDeleted(i)
	}
	n := g.Cleanup()
	if n != 5 {
		t.Errorf("Cleanup removed %d, want 5", n)
	}
	if g.Len() != 15 {
		t.Errorf("Len: got %d, want 15", g.Len())
	}
	stats := g.Stats()
	if stats.TotalNodes != 15 {
		t.Errorf("TotalNodes: got %d, want 15", stats.TotalNodes)
	}
	if stats.Tombstoned != 0 {
		t.Errorf("Tombstoned: got %d, want 0", stats.Tombstoned)
	}
}

func TestCleanup_NeighborListsScrubbed(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 50; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}
	// Tombstone and clean up.
	for i := uint64(10); i < 20; i++ {
		g.MarkDeleted(i)
	}
	g.Cleanup()

	// Verify no remaining node references a deleted ID.
	removed := make(map[uint64]bool)
	for i := uint64(10); i < 20; i++ {
		removed[i] = true
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	for id, node := range g.nodes {
		for l, nbs := range node.Neighbors {
			for _, nid := range nbs {
				if removed[nid] {
					t.Errorf("node %d layer %d still references deleted node %d", id, l, nid)
				}
			}
		}
	}
}

func TestCleanup_EntryPointUpdated(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 50; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}

	g.mu.RLock()
	oldEntry := g.entryPoint
	g.mu.RUnlock()

	g.MarkDeleted(oldEntry)
	g.Cleanup()

	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, ok := g.nodes[g.entryPoint]; !ok && len(g.nodes) > 0 {
		t.Error("entry point refers to a non-existent node after cleanup")
	}
}

func TestCleanup_AllDeleted(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 10; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}
	for i := uint64(0); i < 10; i++ {
		g.MarkDeleted(i)
	}
	n := g.Cleanup()
	if n != 10 {
		t.Errorf("Cleanup removed %d, want 10", n)
	}
	if g.Len() != 0 {
		t.Errorf("Len: got %d, want 0", g.Len())
	}
}

func TestCleanup_SearchAfterCleanup(t *testing.T) {
	g := testGraph(t, 4)
	for i := uint64(0); i < 50; i++ {
		g.Insert(i, [16]byte{}, randomVec(4))
	}
	for i := uint64(0); i < 10; i++ {
		g.MarkDeleted(i)
	}
	g.Cleanup()

	results, err := g.Search(randomVec(4), 5, nil)
	if err != nil {
		t.Fatalf("Search after cleanup: %v", err)
	}
	if len(results) == 0 {
		t.Error("Search returned no results after cleanup")
	}
	// Verify no deleted IDs in results.
	for _, r := range results {
		if r.ID < 10 {
			t.Errorf("deleted ID %d in search results", r.ID)
		}
	}
}

func TestCleanup_MemoryDecreases(t *testing.T) {
	g := testGraph(t, 128)
	for i := uint64(0); i < 100; i++ {
		g.Insert(i, [16]byte{}, randomVec(128))
	}
	before := g.Stats().MemoryBytes
	for i := uint64(0); i < 50; i++ {
		g.MarkDeleted(i)
	}
	g.Cleanup()
	after := g.Stats().MemoryBytes
	if after >= before {
		t.Errorf("memory did not decrease after cleanup: before=%d, after=%d", before, after)
	}
}
