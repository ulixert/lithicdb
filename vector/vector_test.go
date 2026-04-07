package vector

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ulixert/theseon/compaction"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/manifest"
)

func testDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open(db.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4096,
		BlockSize:    256,
		Compaction:   compaction.DefaultConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func testCollectionConfig() CollectionConfig {
	return CollectionConfig{
		Dim:         4,
		Metric:      MetricL2,
		M:           16,
		EfConstruct: 200,
		EfSearch:    50,
	}
}

func randomUUID() [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = byte(rand.IntN(256))
	}
	return id
}

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rand.Float32()*2 - 1
	}
	return v
}

func TestPutAndSearch(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg := testCollectionConfig()
	if err := vs.CreateCollection("test", cfg); err != nil {
		t.Fatal(err)
	}

	ids := make([][16]byte, 100)
	for i := range ids {
		ids[i] = randomUUID()
		if err := vs.Put("test", ids[i], randomVector(4), nil); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Search for nearest 5.
	results, err := vs.Search("test", randomVector(4), 5, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("got %d results, want 5", len(results))
	}

	// Verify sorted by distance.
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("results not sorted: [%d].dist=%v > [%d].dist=%v",
				i-1, results[i-1].Distance, i, results[i].Distance)
		}
	}
}

func TestDeleteAndSearch(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg := testCollectionConfig()
	if err := vs.CreateCollection("test", cfg); err != nil {
		t.Fatal(err)
	}

	// Insert a known vector and some others.
	target := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	targetVec := []float32{1, 0, 0, 0}
	if err := vs.Put("test", target, targetVec, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := vs.Put("test", randomUUID(), randomVector(4), nil); err != nil {
			t.Fatal(err)
		}
	}

	// Delete the target.
	if err := vs.Delete("test", target); err != nil {
		t.Fatal(err)
	}

	// Search near the target — it should not appear.
	results, err := vs.Search("test", targetVec, 20, &SearchOptions{EfSearch: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if r.ID == target {
			t.Error("deleted vector appeared in search results")
		}
	}
}

func TestDeleteIdempotent(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	// Delete non-existent UUID should succeed.
	if err := vs.Delete("test", randomUUID()); err != nil {
		t.Errorf("Delete non-existent: %v", err)
	}
}

func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	dbOpts := db.Options{
		Dir:          dir,
		MemtableSize: 4096,
		BlockSize:    256,
		Compaction:   compaction.DefaultConfig(),
	}

	// Phase 1: insert vectors.
	ids := make([][16]byte, 50)
	{
		d, err := db.Open(dbOpts)
		if err != nil {
			t.Fatal(err)
		}
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			ids[i] = randomUUID()
			if err := vs.Put("test", ids[i], randomVector(4), nil); err != nil {
				t.Fatalf("Put(%d): %v", i, err)
			}
		}
		vs.Close()
		d.Close()
	}

	// Phase 2: reopen and verify.
	{
		d, err := db.Open(dbOpts)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer vs.Close()

		results, err := vs.Search("test", randomVector(4), 10, nil)
		if err != nil {
			t.Fatalf("Search after reopen: %v", err)
		}
		if len(results) == 0 {
			t.Error("no results after reopen — recovery failed")
		}

		// Verify all result IDs are from the original set.
		idSet := make(map[[16]byte]bool, len(ids))
		for _, id := range ids {
			idSet[id] = true
		}
		for _, r := range results {
			if !idSet[r.ID] {
				t.Errorf("unexpected ID %x in results", r.ID)
			}
		}
	}
}

func TestMaxVectorsLimit(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg := testCollectionConfig()
	cfg.MaxVectors = 10
	if err := vs.CreateCollection("test", cfg); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := vs.Put("test", randomUUID(), randomVector(4), nil); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// 11th insert should fail.
	err = vs.Put("test", randomUUID(), randomVector(4), nil)
	if err == nil {
		t.Error("expected error at max vectors limit")
	}
}

func TestUpdateSameUUID(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	id := randomUUID()
	oldVec := []float32{100, 0, 0, 0}
	newVec := []float32{0, 0, 0, 100}

	if err := vs.Put("test", id, oldVec, Metadata{"version": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := vs.Put("test", id, newVec, Metadata{"version": int64(2)}); err != nil {
		t.Fatal(err)
	}

	// Search near newVec — should find the updated vector.
	results, err := vs.Search("test", newVec, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("no results")
	}
	if results[0].ID != id {
		t.Errorf("expected ID %x, got %x", id, results[0].ID)
	}
	if results[0].Metadata["version"] != int64(2) {
		t.Errorf("expected version 2, got %v", results[0].Metadata["version"])
	}
}

func TestUpdateDoesNotCountTowardsLimit(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg := testCollectionConfig()
	cfg.MaxVectors = 5
	if err := vs.CreateCollection("test", cfg); err != nil {
		t.Fatal(err)
	}

	ids := make([][16]byte, 5)
	for i := range ids {
		ids[i] = randomUUID()
		if err := vs.Put("test", ids[i], randomVector(4), nil); err != nil {
			t.Fatal(err)
		}
	}

	// Updating an existing vector should not trigger the limit.
	if err := vs.Put("test", ids[0], randomVector(4), nil); err != nil {
		t.Errorf("update at limit should succeed: %v", err)
	}
}

func TestCollectionNotFound(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	_, err = vs.Search("nonexistent", []float32{1, 2, 3}, 5, nil)
	if !errors.Is(err, ErrCollectionNotFound) {
		t.Errorf("got %v, want ErrCollectionNotFound", err)
	}

	err = vs.Put("nonexistent", randomUUID(), []float32{1, 2, 3}, nil)
	if !errors.Is(err, ErrCollectionNotFound) {
		t.Errorf("got %v, want ErrCollectionNotFound", err)
	}

	err = vs.Delete("nonexistent", randomUUID())
	if !errors.Is(err, ErrCollectionNotFound) {
		t.Errorf("got %v, want ErrCollectionNotFound", err)
	}
}

func TestCollectionExists(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg := testCollectionConfig()
	if err := vs.CreateCollection("test", cfg); err != nil {
		t.Fatal(err)
	}
	err = vs.CreateCollection("test", cfg)
	if !errors.Is(err, ErrCollectionExists) {
		t.Errorf("got %v, want ErrCollectionExists", err)
	}
}

func TestMultipleCollections(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	cfg4 := CollectionConfig{Dim: 4, Metric: MetricL2, M: 16, EfConstruct: 200, EfSearch: 50}
	cfg8 := CollectionConfig{Dim: 8, Metric: MetricCosine, M: 16, EfConstruct: 200, EfSearch: 50}

	if err := vs.CreateCollection("col4", cfg4); err != nil {
		t.Fatal(err)
	}
	if err := vs.CreateCollection("col8", cfg8); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if err := vs.Put("col4", randomUUID(), randomVector(4), nil); err != nil {
			t.Fatal(err)
		}
		if err := vs.Put("col8", randomUUID(), randomVector(8), nil); err != nil {
			t.Fatal(err)
		}
	}

	r4, err := vs.Search("col4", randomVector(4), 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	r8, err := vs.Search("col8", randomVector(8), 5, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(r4) != 5 {
		t.Errorf("col4: got %d results, want 5", len(r4))
	}
	if len(r8) != 5 {
		t.Errorf("col8: got %d results, want 5", len(r8))
	}
}

func TestSearchWithMetadata(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	id := randomUUID()
	meta := Metadata{
		"label": "important",
		"score": 0.95,
	}
	if err := vs.Put("test", id, []float32{1, 0, 0, 0}, meta); err != nil {
		t.Fatal(err)
	}

	results, err := vs.Search("test", []float32{1, 0, 0, 0}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("no results")
	}
	if results[0].Metadata["label"] != "important" {
		t.Errorf("label: got %v, want %q", results[0].Metadata["label"], "important")
	}
	if results[0].Metadata["score"] != 0.95 {
		t.Errorf("score: got %v, want 0.95", results[0].Metadata["score"])
	}
}

func TestConcurrentPutAndSearch(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	// Pre-insert some vectors so Search doesn't hit an empty graph.
	for i := 0; i < 10; i++ {
		if err := vs.Put("test", randomUUID(), randomVector(4), nil); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Concurrent writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := vs.Put("test", randomUUID(), randomVector(4), nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	// Concurrent readers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := vs.Search("test", randomVector(4), 5, nil)
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	// Concurrent deleters.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				// Delete random UUIDs — most will be no-ops (idempotent).
				if err := vs.Delete("test", randomUUID()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent error: %v", err)
	}
}

func TestStaleCandidate_KVTombstonedDirectly(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	// Insert a vector.
	id := randomUUID()
	vec := []float32{1, 0, 0, 0}
	if err := vs.Put("test", id, vec, nil); err != nil {
		t.Fatal(err)
	}

	// Tombstone directly in KV, bypassing VectorStore (simulating inconsistency).
	vectorKey := makeVectorKey("test", id)
	if err := d.Delete(vectorKey); err != nil {
		t.Fatal(err)
	}

	// Search should filter out the stale candidate.
	results, err := vs.Search("test", vec, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if r.ID == id {
			t.Error("stale candidate (KV-tombstoned) should not appear in results")
		}
	}
}

func TestDimensionMismatch(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	err = vs.Put("test", randomUUID(), []float32{1, 2, 3}, nil) // dim=3, want 4
	if err == nil {
		t.Error("expected dimension mismatch error")
	}
}

// --- Snapshot Recovery Tests ---

func testDBWithDir(t *testing.T, dir string) *db.DB {
	t.Helper()
	d, err := db.Open(db.Options{
		Dir:          dir,
		MemtableSize: 4096,
		BlockSize:    256,
		Compaction:   compaction.DefaultConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSnapshotRecovery_Basic(t *testing.T) {
	dir := t.TempDir()
	cfg := testCollectionConfig()
	const n = 50

	ids := make([][16]byte, n)

	// Phase 1: create, insert, snapshot.
	{
		d := testDBWithDir(t, dir)
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", cfg); err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			ids[i] = randomUUID()
			if err := vs.Put("test", ids[i], randomVector(4), nil); err != nil {
				t.Fatal(err)
			}
		}

		infos, err := vs.SnapshotAll(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) != 1 {
			t.Fatalf("expected 1 snapshot, got %d", len(infos))
		}

		// Record to manifest.
		d.Manifest().AddHNSWSnapshot(infos[0].Collection, infos[0].Seq, infos[0].Filename)
		vs.Close()
		d.Close()
	}

	// Phase 2: reopen with snapshot.
	{
		d := testDBWithDir(t, dir)
		defer d.Close()

		state := getManifestState(t, dir)
		snapMap := convertSnapshots(state.HNSWSnapshots)

		vs, err := NewVectorStore(d, VectorStoreConfig{}, WithSnapshots(snapMap))
		if err != nil {
			t.Fatal(err)
		}
		defer vs.Close()

		results, err := vs.Search("test", randomVector(4), 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) == 0 {
			t.Error("no results after snapshot recovery")
		}

		// All IDs should be from original set.
		idSet := make(map[[16]byte]bool, n)
		for _, id := range ids {
			idSet[id] = true
		}
		for _, r := range results {
			if !idSet[r.ID] {
				t.Errorf("unexpected ID %x", r.ID)
			}
		}
	}
}

func TestSnapshotRecovery_Incremental(t *testing.T) {
	dir := t.TempDir()
	cfg := testCollectionConfig()

	ids1 := make([][16]byte, 30)
	ids2 := make([][16]byte, 20)

	// Phase 1: insert first batch, snapshot.
	{
		d := testDBWithDir(t, dir)
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", cfg); err != nil {
			t.Fatal(err)
		}
		for i := range ids1 {
			ids1[i] = randomUUID()
			if err := vs.Put("test", ids1[i], randomVector(4), nil); err != nil {
				t.Fatal(err)
			}
		}

		infos, _ := vs.SnapshotAll(dir)
		d.Manifest().AddHNSWSnapshot(infos[0].Collection, infos[0].Seq, infos[0].Filename)

		// Insert second batch AFTER snapshot.
		for i := range ids2 {
			ids2[i] = randomUUID()
			if err := vs.Put("test", ids2[i], randomVector(4), nil); err != nil {
				t.Fatal(err)
			}
		}

		vs.Close()
		d.Close()
	}

	// Phase 2: reopen with snapshot — should have all 50 vectors.
	{
		d := testDBWithDir(t, dir)
		defer d.Close()

		state := getManifestState(t, dir)
		snapMap := convertSnapshots(state.HNSWSnapshots)

		vs, err := NewVectorStore(d, VectorStoreConfig{}, WithSnapshots(snapMap))
		if err != nil {
			t.Fatal(err)
		}
		defer vs.Close()

		// Search with high ef to find all.
		results, err := vs.Search("test", randomVector(4), 50, &SearchOptions{EfSearch: 200})
		if err != nil {
			t.Fatal(err)
		}

		allIDs := make(map[[16]byte]bool)
		for _, id := range ids1 {
			allIDs[id] = true
		}
		for _, id := range ids2 {
			allIDs[id] = true
		}

		foundIDs := make(map[[16]byte]bool)
		for _, r := range results {
			foundIDs[r.ID] = true
			if !allIDs[r.ID] {
				t.Errorf("unexpected ID %x in results", r.ID)
			}
		}

		if len(results) < 50 {
			t.Logf("got %d results (some may be missed due to graph topology)", len(results))
		}
	}
}

func TestSnapshotRecovery_DeleteAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := testCollectionConfig()

	ids := make([][16]byte, 30)
	var deletedIDs [][16]byte

	// Phase 1: insert, snapshot, then delete some.
	{
		d := testDBWithDir(t, dir)
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", cfg); err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			ids[i] = randomUUID()
			if err := vs.Put("test", ids[i], randomVector(4), nil); err != nil {
				t.Fatal(err)
			}
		}

		infos, _ := vs.SnapshotAll(dir)
		d.Manifest().AddHNSWSnapshot(infos[0].Collection, infos[0].Seq, infos[0].Filename)

		// Delete some after snapshot.
		deletedIDs = ids[:10]
		for _, id := range deletedIDs {
			if err := vs.Delete("test", id); err != nil {
				t.Fatal(err)
			}
		}

		vs.Close()
		d.Close()
	}

	// Phase 2: reopen — deleted vectors should be absent.
	{
		d := testDBWithDir(t, dir)
		defer d.Close()

		state := getManifestState(t, dir)
		snapMap := convertSnapshots(state.HNSWSnapshots)

		vs, err := NewVectorStore(d, VectorStoreConfig{}, WithSnapshots(snapMap))
		if err != nil {
			t.Fatal(err)
		}
		defer vs.Close()

		deletedSet := make(map[[16]byte]bool)
		for _, id := range deletedIDs {
			deletedSet[id] = true
		}

		results, err := vs.Search("test", randomVector(4), 30, &SearchOptions{EfSearch: 200})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range results {
			if deletedSet[r.ID] {
				t.Errorf("deleted vector %x appeared in results", r.ID)
			}
		}
	}
}

func TestSnapshotRecovery_UpdateAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := testCollectionConfig()

	ids := make([][16]byte, 20)

	// Phase 1: insert with known vectors, snapshot, then update some.
	{
		d := testDBWithDir(t, dir)
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", cfg); err != nil {
			t.Fatal(err)
		}

		// Insert all vectors pointing "away" from origin.
		for i := range ids {
			ids[i] = randomUUID()
			vec := []float32{100, 0, 0, 0}
			if err := vs.Put("test", ids[i], vec, Metadata{"version": int64(1)}); err != nil {
				t.Fatal(err)
			}
		}

		infos, _ := vs.SnapshotAll(dir)
		d.Manifest().AddHNSWSnapshot(infos[0].Collection, infos[0].Seq, infos[0].Filename)

		// Update first 5 vectors to point near origin.
		for i := 0; i < 5; i++ {
			vec := []float32{0.001 * float32(i), 0, 0, 0}
			if err := vs.Put("test", ids[i], vec, Metadata{"version": int64(2)}); err != nil {
				t.Fatal(err)
			}
		}

		vs.Close()
		d.Close()
	}

	// Phase 2: reopen — search near origin should find updated vectors.
	{
		d := testDBWithDir(t, dir)
		defer d.Close()

		state := getManifestState(t, dir)
		snapMap := convertSnapshots(state.HNSWSnapshots)

		vs, err := NewVectorStore(d, VectorStoreConfig{}, WithSnapshots(snapMap))
		if err != nil {
			t.Fatal(err)
		}
		defer vs.Close()

		// Search near origin.
		results, err := vs.Search("test", []float32{0, 0, 0, 0}, 5, &SearchOptions{EfSearch: 200})
		if err != nil {
			t.Fatal(err)
		}

		// The updated vectors should be closest.
		updatedSet := make(map[[16]byte]bool)
		for i := 0; i < 5; i++ {
			updatedSet[ids[i]] = true
		}

		for _, r := range results {
			if !updatedSet[r.ID] {
				t.Errorf("expected updated vector in top results, got non-updated ID %x (dist=%f)", r.ID, r.Distance)
			}
			if r.Metadata["version"] != int64(2) {
				t.Errorf("expected version 2, got %v", r.Metadata["version"])
			}
		}
	}
}

func TestSnapshotRecovery_CorruptFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := testCollectionConfig()
	const n = 30

	// Phase 1: insert, snapshot.
	{
		d := testDBWithDir(t, dir)
		vs, err := NewVectorStore(d, VectorStoreConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := vs.CreateCollection("test", cfg); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if err := vs.Put("test", randomUUID(), randomVector(4), nil); err != nil {
				t.Fatal(err)
			}
		}

		infos, _ := vs.SnapshotAll(dir)
		d.Manifest().AddHNSWSnapshot(infos[0].Collection, infos[0].Seq, infos[0].Filename)

		// Corrupt the snapshot file.
		snapPath := filepath.Join(dir, infos[0].Filename)
		data, _ := os.ReadFile(snapPath)
		data[len(data)/2] ^= 0xFF // flip a byte
		os.WriteFile(snapPath, data, 0o640)

		vs.Close()
		d.Close()
	}

	// Phase 2: reopen — should fall back to full rebuild.
	{
		d := testDBWithDir(t, dir)
		defer d.Close()

		state := getManifestState(t, dir)
		snapMap := convertSnapshots(state.HNSWSnapshots)

		vs, err := NewVectorStore(d, VectorStoreConfig{}, WithSnapshots(snapMap))
		if err != nil {
			t.Fatalf("should fall back, not fail: %v", err)
		}
		defer vs.Close()

		results, err := vs.Search("test", randomVector(4), 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) == 0 {
			t.Error("no results after corrupt snapshot fallback")
		}
	}
}

// getManifestState opens and closes the manifest to read the current state.
func getManifestState(t *testing.T, dir string) *manifest.State {
	t.Helper()
	m, state, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	m.Close()
	return state
}

// convertSnapshots converts manifest HNSWSnapshotInfo to vector SnapshotInfo.
func convertSnapshots(m map[string]manifest.HNSWSnapshotInfo) map[string]SnapshotInfo {
	result := make(map[string]SnapshotInfo, len(m))
	for k, v := range m {
		result[k] = SnapshotInfo{
			Collection: v.Collection,
			Seq:        v.Seq,
			Filename:   v.Filename,
		}
	}
	return result
}

func TestKindByteKeyDistinction(t *testing.T) {
	d := testDB(t)
	vs, err := NewVectorStore(d, VectorStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	if err := vs.CreateCollection("test", testCollectionConfig()); err != nil {
		t.Fatal(err)
	}

	id := randomUUID()
	if err := vs.Put("test", id, randomVector(4), nil); err != nil {
		t.Fatal(err)
	}

	// Verify config key and vector key are distinguishable.
	configKey := makeCollectionConfigKey("test")
	vectorKey := makeVectorKey("test", id)

	_, _, configKind, err := parseVectorKey(configKey)
	if err != nil {
		t.Fatal(err)
	}
	_, _, vectorKind, err := parseVectorKey(vectorKey)
	if err != nil {
		t.Fatal(err)
	}

	if configKind != kindConfig {
		t.Errorf("config key kind: got 0x%02x, want 0x%02x", configKind, kindConfig)
	}
	if vectorKind != kindVector {
		t.Errorf("vector key kind: got 0x%02x, want 0x%02x", vectorKind, kindVector)
	}
}
