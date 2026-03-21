package db

import (
	"errors"
	"os"
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "lithic-tx-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestTransaction_ReadOwnWrites(t *testing.T) {
	d := openTestDB(t)

	tx := d.BeginTransaction()
	tx.Put([]byte("a"), []byte("v1"))

	val, ok := tx.Get([]byte("a"))
	if !ok {
		t.Fatal("expected key 'a' to be found")
	}
	if string(val.Data) != "v1" {
		t.Errorf("a = %q, want v1", val.Data)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify it's in the DB.
	val, ok = d.Get([]byte("a"))
	if !ok {
		t.Fatal("expected key 'a' in DB after commit")
	}
	if string(val.Data) != "v1" {
		t.Errorf("db: a = %q, want v1", val.Data)
	}
}

func TestTransaction_ReadOwnDelete(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("v1"))

	tx := d.BeginTransaction()
	tx.Delete([]byte("a"))

	_, ok := tx.Get([]byte("a"))
	if ok {
		t.Error("expected 'a' to be not found after delete in tx")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// DB should also not find it (tombstone).
	val, ok := d.Get([]byte("a"))
	if ok && !val.Tombstone {
		t.Error("expected 'a' to be deleted in DB after commit")
	}
}

func TestTransaction_SnapshotIsolation(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("v1"))

	tx := d.BeginTransaction()

	// Concurrent write after tx started.
	d.Put([]byte("a"), []byte("v2"))
	d.Put([]byte("b"), []byte("new"))

	// Transaction should still see the old value.
	val, ok := tx.Get([]byte("a"))
	if !ok {
		t.Fatal("expected 'a' in tx")
	}
	if string(val.Data) != "v1" {
		t.Errorf("tx: a = %q, want v1", val.Data)
	}

	// Transaction should not see the new key.
	_, ok = tx.Get([]byte("b"))
	if ok {
		t.Error("tx should not see key 'b' written after snapshot")
	}

	tx.Rollback()
}

func TestTransaction_ConflictDetection(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("v1"))

	tx1 := d.BeginTransaction()
	tx2 := d.BeginTransaction()

	tx1.Put([]byte("a"), []byte("tx1"))
	tx2.Put([]byte("a"), []byte("tx2"))

	// First commit succeeds.
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	// Second commit should conflict.
	err := tx2.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("tx2 commit: got %v, want ErrConflict", err)
	}
}

func TestTransaction_NoConflictDifferentKeys(t *testing.T) {
	d := openTestDB(t)

	tx1 := d.BeginTransaction()
	tx2 := d.BeginTransaction()

	tx1.Put([]byte("a"), []byte("v1"))
	tx2.Put([]byte("b"), []byte("v2"))

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("tx2: %v", err)
	}

	// Both keys should be in the DB.
	val, ok := d.Get([]byte("a"))
	if !ok || string(val.Data) != "v1" {
		t.Errorf("a = %v/%q, want found/v1", ok, val.Data)
	}
	val, ok = d.Get([]byte("b"))
	if !ok || string(val.Data) != "v2" {
		t.Errorf("b = %v/%q, want found/v2", ok, val.Data)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	d := openTestDB(t)

	tx := d.BeginTransaction()
	tx.Put([]byte("a"), []byte("v1"))
	tx.Rollback()

	// Key should not exist in DB.
	_, ok := d.Get([]byte("a"))
	if ok {
		t.Error("rolled-back write should not be visible in DB")
	}
}

func TestTransaction_CommitAfterClose(t *testing.T) {
	d := openTestDB(t)

	tx := d.BeginTransaction()
	tx.Put([]byte("a"), []byte("v1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Second commit should fail.
	err := tx.Commit()
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("second commit: got %v, want ErrTxClosed", err)
	}

	// Put after close should fail.
	err = tx.Put([]byte("b"), []byte("v2"))
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("put after close: got %v, want ErrTxClosed", err)
	}
}

func TestTransaction_EmptyCommit(t *testing.T) {
	d := openTestDB(t)

	tx := d.BeginTransaction()
	if err := tx.Commit(); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
}

func TestTransaction_ScanIncludesWriteBuffer(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("db-a"))
	d.Put([]byte("c"), []byte("db-c"))

	tx := d.BeginTransaction()
	tx.Put([]byte("b"), []byte("tx-b"))

	iter := tx.Scan()
	defer iter.Close()

	expected := []struct {
		key   string
		value string
	}{
		{"a", "db-a"},
		{"b", "tx-b"},
		{"c", "db-c"},
	}

	for i, exp := range expected {
		if !iter.IsValid() {
			t.Fatalf("entry %d: expected valid", i)
		}
		gotKey := string(kv.UserKey(iter.Key()))
		if gotKey != exp.key {
			t.Errorf("entry %d: key = %q, want %q", i, gotKey, exp.key)
		}
		if string(iter.Value()) != exp.value {
			t.Errorf("entry %d: value = %q, want %q", i, iter.Value(), exp.value)
		}
		iter.Next()
	}

	if iter.IsValid() {
		t.Errorf("expected exhausted, got extra key %q", kv.UserKey(iter.Key()))
	}

	tx.Rollback()
}

func TestTransaction_ScanWriteBufferOverrides(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("old"))

	tx := d.BeginTransaction()
	tx.Put([]byte("a"), []byte("new"))

	iter := tx.Scan()
	defer iter.Close()

	if !iter.IsValid() {
		t.Fatal("expected valid")
	}
	if string(kv.UserKey(iter.Key())) != "a" {
		t.Errorf("key = %q, want a", kv.UserKey(iter.Key()))
	}
	if string(iter.Value()) != "new" {
		t.Errorf("value = %q, want new", iter.Value())
	}

	iter.Next()
	if iter.IsValid() {
		t.Errorf("expected exhausted, got extra key %q", kv.UserKey(iter.Key()))
	}

	tx.Rollback()
}

func TestTransaction_DeleteInTxScan(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("v1"))
	d.Put([]byte("b"), []byte("v2"))
	d.Put([]byte("c"), []byte("v3"))

	tx := d.BeginTransaction()
	tx.Delete([]byte("b"))

	iter := tx.Scan()
	defer iter.Close()

	// Should see a and c, but b should appear as a tombstone.
	// The snapshot iterator emits tombstones — the caller filters them.
	// In a real application, tombstones would be filtered by a higher layer.
	// Here we just verify that b's value is nil (tombstone).
	var keys []string
	for iter.IsValid() {
		key := string(kv.UserKey(iter.Key()))
		keys = append(keys, key)
		if key == "b" && iter.Value() != nil {
			t.Errorf("b should be tombstone (nil value), got %q", iter.Value())
		}
		iter.Next()
	}

	if len(keys) != 3 {
		t.Errorf("got keys %v, want [a b c] (b as tombstone)", keys)
	}
}
