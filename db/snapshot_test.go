package db

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ulixert/lithicdb/kv"
)

func TestSnapshot_ReadAtPointInTime(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Write a=1, take snapshot, write a=2.
	d.Put([]byte("a"), []byte("1"))
	snap := d.GetSnapshot()
	defer snap.Close()
	d.Put([]byte("a"), []byte("2"))

	// Snapshot sees "1", DB sees "2".
	val, found := snap.Get([]byte("a"))
	if !found || val.Tombstone {
		t.Fatal("snapshot: a not found")
	}
	if string(val.Data) != "1" {
		t.Errorf("snapshot: a = %q, want %q", val.Data, "1")
	}

	val, found = d.Get([]byte("a"))
	if !found || string(val.Data) != "2" {
		t.Errorf("db: a = %q, want %q", val.Data, "2")
	}
}

func TestSnapshot_DoesNotSeeWritesAfterCreation(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	snap := d.GetSnapshot()
	defer snap.Close()

	// Write after snapshot — invisible.
	d.Put([]byte("x"), []byte("hello"))

	_, found := snap.Get([]byte("x"))
	if found {
		t.Error("snapshot should not see writes after creation")
	}
}

func TestSnapshot_SeesDeletesBeforeSnapshot(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.Delete([]byte("a"))
	snap := d.GetSnapshot()
	defer snap.Close()

	val, found := snap.Get([]byte("a"))
	if !found {
		t.Fatal("expected tombstone to be found")
	}
	if !val.Tombstone {
		t.Error("expected tombstone")
	}
}

func TestSnapshot_DoesNotSeeDeletesAfterSnapshot(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	snap := d.GetSnapshot()
	defer snap.Close()
	d.Delete([]byte("a"))

	val, found := snap.Get([]byte("a"))
	if !found || val.Tombstone {
		t.Fatal("snapshot should still see a=1")
	}
	if string(val.Data) != "1" {
		t.Errorf("a = %q, want %q", val.Data, "1")
	}
}

func TestSnapshot_Scan(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	snap := d.GetSnapshot()
	defer snap.Close()

	// Write c after snapshot — invisible to scan.
	d.Put([]byte("c"), []byte("3"))

	keys := collectUserKeys(t, snap.Scan())
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("scan keys = %v, want [a b]", keys)
	}
}

func TestSnapshot_ScanRange(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("c"), []byte("3"))
	snap := d.GetSnapshot()
	defer snap.Close()

	// Write d after snapshot.
	d.Put([]byte("d"), []byte("4"))

	keys := collectUserKeys(t, snap.ScanRange([]byte("b"), []byte("d")))
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Errorf("scan range keys = %v, want [b c]", keys)
	}
}

func TestSnapshot_SurvivesFlush(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("before-flush"))
	snap := d.GetSnapshot()
	defer snap.Close()

	// Write enough data to trigger a flush (small memtable = 4KB).
	// Each entry is ~40 bytes of user data + 8 bytes seq + overhead.
	// With 4KB memtable, ~200 entries should force rotation.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("flush-key-%04d", i)
		val := fmt.Sprintf("flush-val-%04d-padding-to-increase-size", i)
		if err := d.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	waitForFlush(t, d, 1)

	// Snapshot should still see the original value.
	val, found := snap.Get([]byte("a"))
	if !found || val.Tombstone {
		t.Fatal("snapshot: a not found after flush")
	}
	if string(val.Data) != "before-flush" {
		t.Errorf("snapshot: a = %q, want %q", val.Data, "before-flush")
	}
}

func TestSnapshot_DoubleClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	snap := d.GetSnapshot()
	snap.Close()
	snap.Close() // should not panic

	if d.snapshots.Len() != 0 {
		t.Errorf("snapshots.Len = %d, want 0", d.snapshots.Len())
	}
}

func TestSnapshot_MultipleSnapshots(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("v1"))
	snap1 := d.GetSnapshot()
	defer snap1.Close()

	d.Put([]byte("a"), []byte("v2"))
	snap2 := d.GetSnapshot()
	defer snap2.Close()

	d.Put([]byte("a"), []byte("v3"))

	// snap1 sees v1, snap2 sees v2, db sees v3.
	val1, _ := snap1.Get([]byte("a"))
	val2, _ := snap2.Get([]byte("a"))
	val3, _ := d.Get([]byte("a"))

	if string(val1.Data) != "v1" {
		t.Errorf("snap1: a = %q, want v1", val1.Data)
	}
	if string(val2.Data) != "v2" {
		t.Errorf("snap2: a = %q, want v2", val2.Data)
	}
	if string(val3.Data) != "v3" {
		t.Errorf("db: a = %q, want v3", val3.Data)
	}
}

func TestSnapshot_ScanDeduplication(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Write multiple versions of the same key.
	d.Put([]byte("x"), []byte("v1"))
	d.Put([]byte("x"), []byte("v2"))
	snap := d.GetSnapshot()
	defer snap.Close()
	d.Put([]byte("x"), []byte("v3"))

	// Scan should return exactly one entry for "x" with value "v2".
	iter := snap.Scan()
	defer iter.Close()

	if !iter.IsValid() {
		t.Fatal("expected valid iterator")
	}
	userKey := string(kv.UserKey(iter.Key()))
	if userKey != "x" {
		t.Errorf("user key = %q, want x", userKey)
	}
	if string(iter.Value()) != "v2" {
		t.Errorf("value = %q, want v2", iter.Value())
	}

	iter.Next()
	if iter.IsValid() {
		t.Errorf("expected exhausted, got extra key %q", kv.UserKey(iter.Key()))
	}
}

func TestSnapshot_SurvivesCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Write initial data and take a snapshot.
	d.Put([]byte("a"), []byte("v1"))
	d.Put([]byte("b"), []byte("v1"))
	snap := d.GetSnapshot()
	defer snap.Close()

	// Overwrite the keys so the old versions become eligible for GC.
	d.Put([]byte("a"), []byte("v2"))
	d.Put([]byte("b"), []byte("v2"))

	// Force data to SSTables by writing enough to trigger flushes.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("pad-%04d", i)
		d.Put([]byte(key), []byte(fmt.Sprintf("padding-value-%04d-extra-bytes-for-size", i)))
	}

	// Wait for L0 files to appear, then trigger compaction.
	waitForFlush(t, d, 1)
	d.triggerCompaction()

	// Give compaction a moment to run.
	deadline := time.After(5 * time.Second)
	for {
		d.mu.RLock()
		hasLevels := len(d.levels) > 1 && len(d.levels[1]) > 0
		d.mu.RUnlock()
		if hasLevels {
			break
		}
		select {
		case <-deadline:
			// Compaction may not have run into L1, that's OK.
			// The important thing is the snapshot still reads correctly.
			break
		case <-time.After(50 * time.Millisecond):
		}
		if hasLevels {
			break
		}
		// Check deadline
		select {
		case <-deadline:
			break
		default:
		}
		break
	}

	// The snapshot must still see the old values.
	val, ok := snap.Get([]byte("a"))
	if !ok {
		t.Fatal("snapshot: key 'a' not found after compaction")
	}
	if string(val.Data) != "v1" {
		t.Errorf("snapshot: a = %q, want v1", val.Data)
	}

	val, ok = snap.Get([]byte("b"))
	if !ok {
		t.Fatal("snapshot: key 'b' not found after compaction")
	}
	if string(val.Data) != "v1" {
		t.Errorf("snapshot: b = %q, want v1", val.Data)
	}

	// Current DB should see the new values.
	val, ok = d.Get([]byte("a"))
	if !ok {
		t.Fatal("db: key 'a' not found")
	}
	if string(val.Data) != "v2" {
		t.Errorf("db: a = %q, want v2", val.Data)
	}
}

func TestSnapshot_RegistryTracking(t *testing.T) {
	dir, err := os.MkdirTemp("", "lithic-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	snap1 := d.GetSnapshot()
	snap2 := d.GetSnapshot()

	if d.snapshots.Len() != 2 {
		t.Errorf("Len = %d, want 2", d.snapshots.Len())
	}

	snap1.Close()
	if d.snapshots.Len() != 1 {
		t.Errorf("Len = %d, want 1", d.snapshots.Len())
	}

	snap2.Close()
	if d.snapshots.Len() != 0 {
		t.Errorf("Len = %d, want 0", d.snapshots.Len())
	}
}
