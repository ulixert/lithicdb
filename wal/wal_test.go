package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ulixert/theseon/kv"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestWAL_PutAndRecover(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("hello"), []byte("world"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("foo"), []byte("bar"), 2); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("recovered %d entries, want 2", len(entries))
	}

	assertEntry(t, entries[0], 1, "hello", "world", false)
	assertEntry(t, entries[1], 2, "foo", "bar", false)
}

func TestWAL_DeleteAndRecover(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("key"), []byte("value"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Delete([]byte("key"), 2); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("recovered %d entries, want 2", len(entries))
	}

	assertEntry(t, entries[0], 1, "key", "value", false)
	assertEntry(t, entries[1], 2, "key", "", true)
}

func TestWAL_EmptyValue(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("key"), []byte{}, 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(entries))
	}

	if entries[0].Value.Tombstone {
		t.Fatal("empty value should not be a tombstone")
	}
	if len(entries[0].Value.Data) != 0 {
		t.Errorf("expected empty data, got %q", entries[0].Value.Data)
	}
}

func TestWAL_BatchWrite(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	batch := []Entry{
		{Seq: 1, Key: []byte("a"), Value: kv.NewValue([]byte("1"))},
		{Seq: 2, Key: []byte("b"), Value: kv.NewValue([]byte("2"))},
		{Seq: 3, Key: []byte("c"), Value: kv.NewTombstone()},
	}

	if err := w.WriteEntries(batch); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(entries))
	}

	assertEntry(t, entries[0], 1, "a", "1", false)
	assertEntry(t, entries[1], 2, "b", "2", false)
	assertEntry(t, entries[2], 3, "c", "", true)
}

func TestWAL_SeqNumberPreserved(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("a"), []byte("1"), 100); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("b"), []byte("2"), 200); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Delete([]byte("c"), 300); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if entries[0].Seq != 100 {
		t.Errorf("entry 0 seq = %d, want 100", entries[0].Seq)
	}
	if entries[1].Seq != 200 {
		t.Errorf("entry 1 seq = %d, want 200", entries[1].Seq)
	}
	if entries[2].Seq != 300 {
		t.Errorf("entry 2 seq = %d, want 300", entries[2].Seq)
	}
}

func TestWAL_RecoverCorruptTail(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("good1"), []byte("v1"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("good2"), []byte("v2"), 2); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(w.Path(), os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.Write([]byte("this is corrupt garbage")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("recovered %d entries, want 2", len(entries))
	}

	assertEntry(t, entries[0], 1, "good1", "v1", false)
	assertEntry(t, entries[1], 2, "good2", "v2", false)
}

func TestWAL_RecoverTruncatedRecord(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("survived"), []byte("yes"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	partial := make([]byte, len(data)+4)
	copy(partial, data)
	partial[len(data)] = 0xFF
	partial[len(data)+1] = 0xFF
	partial[len(data)+2] = 0xFF
	partial[len(data)+3] = 0xFF

	if err := os.WriteFile(w.Path(), partial, 0o640); err != nil {
		t.Fatalf("write: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(entries))
	}

	assertEntry(t, entries[0], 1, "survived", "yes", false)
}

func TestWAL_RecoverEmpty(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("recovered %d entries from empty WAL, want 0", len(entries))
	}
}

func TestWAL_Remove(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w.Close()

	if err := w.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := os.Stat(w.Path()); !os.IsNotExist(err) {
		t.Fatalf("expected file to be deleted, got err: %v", err)
	}
}

func TestWAL_FindFiles(t *testing.T) {
	dir := tempDir(t)

	for _, id := range []uint64{3, 1, 5} {
		w, err := Create(dir, id)
		if err != nil {
			t.Fatalf("Create(%d): %v", id, err)
		}
		w.Close()
	}

	nonWAL := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(nonWAL, []byte("ignore me"), 0o640); err != nil {
		t.Fatalf("write non-WAL: %v", err)
	}

	ids, err := FindWALFiles(dir)
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if len(ids) != 3 {
		t.Fatalf("found %d WAL files, want 3", len(ids))
	}

	if ids[0] != 1 || ids[1] != 3 || ids[2] != 5 {
		t.Errorf("ids = %v, want [1, 3, 5]", ids)
	}
}

func TestWAL_FindFiles_EmptyDir(t *testing.T) {
	dir := tempDir(t)

	ids, err := FindWALFiles(dir)
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if len(ids) != 0 {
		t.Fatalf("found %d WAL files in empty dir, want 0", len(ids))
	}
}

func TestWAL_FindFiles_NonExistentDir(t *testing.T) {
	ids, err := FindWALFiles("/nonexistent/path")
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestWAL_RecoverDir(t *testing.T) {
	dir := tempDir(t)

	w1, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create(1): %v", err)
	}
	w1.Put([]byte("a"), []byte("1"), 1)
	w1.Put([]byte("b"), []byte("2"), 2)
	w1.Close()

	w3, err := Create(dir, 3)
	if err != nil {
		t.Fatalf("Create(3): %v", err)
	}
	w3.Put([]byte("c"), []byte("3"), 3)
	w3.Delete([]byte("a"), 4)
	w3.Close()

	result, err := RecoverDir(dir)
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("recovered %d WALs, want 2", len(result))
	}

	if result[0].ID != 1 || result[1].ID != 3 {
		t.Fatalf("WAL IDs = [%d, %d], want [1, 3]", result[0].ID, result[1].ID)
	}

	if len(result[0].Entries) != 2 {
		t.Fatalf("WAL 1: %d entries, want 2", len(result[0].Entries))
	}
	assertEntry(t, result[0].Entries[0], 1, "a", "1", false)
	assertEntry(t, result[0].Entries[1], 2, "b", "2", false)

	if len(result[1].Entries) != 2 {
		t.Fatalf("WAL 3: %d entries, want 2", len(result[1].Entries))
	}
	assertEntry(t, result[1].Entries[0], 3, "c", "3", false)
	assertEntry(t, result[1].Entries[1], 4, "a", "", true)
}

func TestWAL_RecoverDir_Empty(t *testing.T) {
	dir := tempDir(t)

	result, err := RecoverDir(dir)
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 WALs, got %d", len(result))
	}
}

func TestWAL_ManyEntries(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	n := 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := w.Put(key, val, uint64(i+1)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != n {
		t.Fatalf("recovered %d entries, want %d", len(entries), n)
	}

	// Verify seq numbers survived
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entry %d: seq = %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestWAL_LargeValues(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	largeVal := make([]byte, 1<<20)
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}

	if err := w.Put([]byte("big"), largeVal, 1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(entries))
	}

	if len(entries[0].Value.Data) != len(largeVal) {
		t.Fatalf("value size = %d, want %d", len(entries[0].Value.Data), len(largeVal))
	}

	for i := range largeVal {
		if entries[0].Value.Data[i] != largeVal[i] {
			t.Fatalf("value mismatch at byte %d", i)
		}
	}
}

// --- helpers ---

func assertEntry(t *testing.T, e Entry, seq uint64, key, value string, tombstone bool) {
	t.Helper()

	if e.Seq != seq {
		t.Errorf("seq = %d, want %d", e.Seq, seq)
	}

	if string(e.Key) != key {
		t.Errorf("key = %q, want %q", e.Key, key)
	}

	if e.Value.Tombstone != tombstone {
		t.Errorf("tombstone = %v, want %v", e.Value.Tombstone, tombstone)
	}

	if !tombstone {
		if string(e.Value.Data) != value {
			t.Errorf("value = %q, want %q", e.Value.Data, value)
		}
	}
}
