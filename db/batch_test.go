package db

import (
	"fmt"
	"testing"
)

func TestWriteBatch_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	batch := d.NewWriteBatch()
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	batch.Put([]byte("c"), []byte("3"))

	if batch.Count() != 3 {
		t.Errorf("Count = %d, want 3", batch.Count())
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for _, tc := range []struct{ key, val string }{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	} {
		val, found := d.Get([]byte(tc.key))
		if !found {
			t.Fatalf("key %q not found", tc.key)
		}
		if string(val.Data) != tc.val {
			t.Errorf("Get(%q) = %q, want %q", tc.key, val.Data, tc.val)
		}
	}
}

func TestWriteBatch_Delete(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("value"))

	batch := d.NewWriteBatch()
	batch.Delete([]byte("key"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected tombstone to be found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestWriteBatch_PutThenDeleteSameKey(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Within the same batch: put then delete.
	// The delete has a higher seq number, so it should win.
	batch := d.NewWriteBatch()
	batch.Put([]byte("key"), []byte("value"))
	batch.Delete([]byte("key"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected tombstone to be found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone (delete should win over put in same batch)")
	}
}

func TestWriteBatch_DeleteThenPutSameKey(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("old"))

	// Delete then re-put in same batch — put should win
	batch := d.NewWriteBatch()
	batch.Delete([]byte("key"))
	batch.Put([]byte("key"), []byte("new"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected found")
	}
	if val.Tombstone {
		t.Fatal("expected non-tombstone (put after delete should win)")
	}
	if string(val.Data) != "new" {
		t.Errorf("value = %q, want %q", val.Data, "new")
	}
}

func TestWriteBatch_Empty(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	batch := d.NewWriteBatch()
	if err := batch.Commit(); err != nil {
		t.Fatalf("empty Commit should succeed: %v", err)
	}
}

func TestWriteBatch_Reset(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	batch := d.NewWriteBatch()
	batch.Put([]byte("a"), []byte("1"))
	batch.Reset()

	if batch.Count() != 0 {
		t.Errorf("Count after Reset = %d, want 0", batch.Count())
	}

	// Commit after reset should be a no-op
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit after Reset: %v", err)
	}

	_, found := d.Get([]byte("a"))
	if found {
		t.Error("key 'a' should not exist (batch was reset)")
	}
}

func TestWriteBatch_Reuse(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	batch := d.NewWriteBatch()

	// First commit
	batch.Put([]byte("a"), []byte("1"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit 1: %v", err)
	}

	// Batch should be reset after commit
	if batch.Count() != 0 {
		t.Errorf("Count after Commit = %d, want 0", batch.Count())
	}

	// Second commit
	batch.Put([]byte("b"), []byte("2"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	val, found := d.Get([]byte("a"))
	if !found || string(val.Data) != "1" {
		t.Errorf("a: found=%v val=%q", found, val.Data)
	}
	val, found = d.Get([]byte("b"))
	if !found || string(val.Data) != "2" {
		t.Errorf("b: found=%v val=%q", found, val.Data)
	}
}

func TestWriteBatch_Recovery(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 1024 * 1024 // large, no flush

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := d.NewWriteBatch()
	batch.Put([]byte("x"), []byte("10"))
	batch.Put([]byte("y"), []byte("20"))
	batch.Delete([]byte("z"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	d.Close()

	// Reopen — WAL replay should recover the entire batch
	d2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	val, found := d2.Get([]byte("x"))
	if !found || string(val.Data) != "10" {
		t.Errorf("x: found=%v val=%q", found, val.Data)
	}
	val, found = d2.Get([]byte("y"))
	if !found || string(val.Data) != "20" {
		t.Errorf("y: found=%v val=%q", found, val.Data)
	}
	val, found = d2.Get([]byte("z"))
	if !found || !val.Tombstone {
		t.Errorf("z: found=%v tombstone=%v", found, val.Tombstone)
	}
}

func TestWriteBatch_Large(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	batch := d.NewWriteBatch()
	n := 1000
	for i := 0; i < n; i++ {
		batch.Put(
			[]byte(fmt.Sprintf("key-%04d", i)),
			[]byte(fmt.Sprintf("val-%04d", i)),
		)
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify all keys
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val, found := d.Get([]byte(key))
		if !found {
			t.Fatalf("key %q not found", key)
		}
		want := fmt.Sprintf("val-%04d", i)
		if string(val.Data) != want {
			t.Errorf("Get(%q) = %q, want %q", key, val.Data, want)
		}
	}
}

func TestWriteBatch_MixedWithSingleWrites(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Single write first
	d.Put([]byte("a"), []byte("single"))

	// Batch write
	batch := d.NewWriteBatch()
	batch.Put([]byte("b"), []byte("batch"))
	batch.Put([]byte("a"), []byte("overwritten"))
	batch.Commit()

	// Another single write
	d.Put([]byte("c"), []byte("single2"))

	val, _ := d.Get([]byte("a"))
	if string(val.Data) != "overwritten" {
		t.Errorf("a = %q, want %q", val.Data, "overwritten")
	}
	val, _ = d.Get([]byte("b"))
	if string(val.Data) != "batch" {
		t.Errorf("b = %q, want %q", val.Data, "batch")
	}
	val, _ = d.Get([]byte("c"))
	if string(val.Data) != "single2" {
		t.Errorf("c = %q, want %q", val.Data, "single2")
	}
}

func TestWriteBatch_ScanDeduplicates(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Put same key twice in one batch
	batch := d.NewWriteBatch()
	batch.Put([]byte("key"), []byte("v1"))
	batch.Put([]byte("key"), []byte("v2"))
	batch.Commit()

	// Scan should deduplicate — only the newest (v2) should appear
	keys := collectUserKeys(t, d.Scan())
	if len(keys) != 1 {
		t.Fatalf("scan returned %d keys, want 1", len(keys))
	}

	val, _ := d.Get([]byte("key"))
	if string(val.Data) != "v2" {
		t.Errorf("value = %q, want %q", val.Data, "v2")
	}
}
