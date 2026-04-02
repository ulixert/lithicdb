package db

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMVCC_SnapshotDuringFlush verifies that a snapshot taken before a
// flush still reads the correct data after the flush completes.
func TestMVCC_SnapshotDuringFlush(t *testing.T) {
	d := openTestDB(t)

	// Write initial data.
	for i := 0; i < 10; i++ {
		key := fmt.Appendf(nil, "key-%04d", i)
		d.Put(key, []byte(fmt.Sprintf("v1-%04d", i)))
	}

	snap := d.GetSnapshot()
	defer snap.Close()

	// Write enough to trigger at least one flush (4KB memtable).
	for i := 0; i < 200; i++ {
		key := fmt.Appendf(nil, "key-%04d", i)
		d.Put(key, []byte(fmt.Sprintf("v2-%04d-padding-to-increase-size", i)))
	}
	waitForFlush(t, d, 1)

	// Snapshot should still see the old values.
	for i := 0; i < 10; i++ {
		key := fmt.Appendf(nil, "key-%04d", i)
		val, ok := snap.Get(key)
		if !ok {
			t.Fatalf("snapshot: key %q not found after flush", key)
		}
		want := fmt.Sprintf("v1-%04d", i)
		if string(val.Data) != want {
			t.Errorf("snapshot: %s = %q, want %q", key, val.Data, want)
		}
	}

	// Verify scan also returns correct values.
	iter := snap.Scan()
	count := 0
	for iter.IsValid() {
		count++
		iter.Next()
	}
	iter.Close()
	if count != 10 {
		t.Errorf("snapshot scan: got %d keys, want 10", count)
	}
}

// TestMVCC_SnapshotDuringCompaction verifies that a snapshot remains
// readable after compaction merges and potentially drops versions.
func TestMVCC_SnapshotDuringCompaction(t *testing.T) {
	d := openTestDB(t)

	// Write enough data to create multiple L0 files.
	for round := 0; round < 5; round++ {
		for i := 0; i < 50; i++ {
			key := fmt.Appendf(nil, "key-%04d", i)
			d.Put(key, []byte(fmt.Sprintf("round%d-padding-for-size", round)))
		}
	}
	waitForFlush(t, d, 1)

	snap := d.GetSnapshot()
	defer snap.Close()

	// Overwrite all keys and trigger compaction.
	for i := 0; i < 50; i++ {
		key := fmt.Appendf(nil, "key-%04d", i)
		d.Put(key, []byte("after-snapshot-padding-for-size"))
	}
	waitForAllFlushes(t, d)
	d.triggerCompaction()
	time.Sleep(200 * time.Millisecond) // let compaction run

	// Snapshot should still see pre-compaction values.
	val, ok := snap.Get([]byte("key-0000"))
	if !ok {
		t.Fatal("snapshot: key-0000 not found after compaction")
	}
	if string(val.Data) != "round4-padding-for-size" {
		t.Errorf("snapshot: key-0000 = %q, want round4-padding-for-size", val.Data)
	}

	// DB should see the new value.
	val, ok = d.Get([]byte("key-0000"))
	if !ok {
		t.Fatal("db: key-0000 not found")
	}
	if string(val.Data) != "after-snapshot-padding-for-size" {
		t.Errorf("db: key-0000 = %q, want after-snapshot-padding-for-size", val.Data)
	}
}

// TestMVCC_ConcurrentSnapshotsAndWrites verifies that multiple
// goroutines can take snapshots and write concurrently without
// data corruption. Each snapshot sees a consistent view.
func TestMVCC_ConcurrentSnapshotsAndWrites(t *testing.T) {
	d := openTestDB(t)

	// Pre-populate some data so snapshots aren't empty.
	for i := 0; i < 20; i++ {
		d.Put(fmt.Appendf(nil, "base-%04d", i), []byte("base-value"))
	}

	const goroutines = 8
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Take a snapshot.
			snap := d.GetSnapshot()
			defer snap.Close()

			// Verify snapshot can read base keys.
			for i := 0; i < 20; i++ {
				key := fmt.Appendf(nil, "base-%04d", i)
				_, ok := snap.Get(key)
				if !ok {
					errs <- fmt.Errorf("goroutine %d: base key %q not found in snapshot", id, key)
					return
				}
			}

			// Write keys unique to this goroutine.
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Appendf(nil, "g%d-%04d", id, i)
				if err := d.Put(key, []byte(fmt.Sprintf("val-%d-%d", id, i))); err != nil {
					errs <- fmt.Errorf("goroutine %d: Put: %v", id, err)
					return
				}
			}

			// The snapshot should NOT see this goroutine's new keys.
			key := fmt.Appendf(nil, "g%d-0000", id)
			_, ok := snap.Get(key)
			if ok {
				errs <- fmt.Errorf("goroutine %d: snapshot saw its own write %q", id, key)
				return
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	// After all goroutines finish, DB should see all keys.
	for g := 0; g < goroutines; g++ {
		for i := 0; i < opsPerGoroutine; i++ {
			key := fmt.Appendf(nil, "g%d-%04d", g, i)
			_, ok := d.Get(key)
			if !ok {
				t.Errorf("key %q not found in DB after concurrent writes", key)
			}
		}
	}
}

// TestMVCC_SnapshotPreventsTombstoneGC verifies that an active snapshot
// prevents compaction from dropping tombstones it needs to see.
func TestMVCC_SnapshotPreventsTombstoneGC(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("a"), []byte("v1"))

	// Delete the key.
	d.Delete([]byte("a"))

	// Take snapshot — it should see the delete (tombstone).
	snap := d.GetSnapshot()
	defer snap.Close()

	val, ok := snap.Get([]byte("a"))
	if ok && !val.Tombstone {
		t.Fatal("snapshot should see 'a' as deleted")
	}

	// Write more data to trigger flush + compaction.
	for i := 0; i < 200; i++ {
		d.Put(fmt.Appendf(nil, "pad-%04d", i), []byte(fmt.Sprintf("padding-value-%04d-extra-bytes-for-size", i)))
	}
	waitForFlush(t, d, 1)
	waitForAllFlushes(t, d)
	d.triggerCompaction()
	time.Sleep(200 * time.Millisecond)

	// Snapshot should still see 'a' as deleted (tombstone preserved).
	val, ok = snap.Get([]byte("a"))
	if ok && !val.Tombstone {
		t.Error("snapshot should still see 'a' as deleted after compaction")
	}
}

// TestMVCC_AllSnapshotsClosedAllowsGC verifies that once all snapshots
// are closed, compaction can drop old versions.
func TestMVCC_AllSnapshotsClosedAllowsGC(t *testing.T) {
	dir, err := os.MkdirTemp("", "theseon-gc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple versions of the same key.
	for v := 0; v < 5; v++ {
		d.Put([]byte("x"), []byte(fmt.Sprintf("v%d", v)))
	}

	snap := d.GetSnapshot()

	// Write even more versions.
	for v := 5; v < 10; v++ {
		d.Put([]byte("x"), []byte(fmt.Sprintf("v%d", v)))
	}

	// Close the snapshot — now watermark becomes 0 (no snapshots).
	snap.Close()

	if d.snapshots.Len() != 0 {
		t.Fatalf("expected 0 snapshots, got %d", d.snapshots.Len())
	}

	// Write padding to trigger flush.
	for i := 0; i < 200; i++ {
		d.Put(fmt.Appendf(nil, "pad-%04d", i), []byte(fmt.Sprintf("padding-value-%04d-extra-bytes-for-size", i)))
	}
	waitForFlush(t, d, 1)
	waitForAllFlushes(t, d)
	d.triggerCompaction()
	time.Sleep(200 * time.Millisecond)

	d.Close()

	// Reopen and verify only the newest version of "x" survives.
	d2, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	val, ok := d2.Get([]byte("x"))
	if !ok {
		t.Fatal("key 'x' not found after reopen")
	}
	if string(val.Data) != "v9" {
		t.Errorf("x = %q, want v9", val.Data)
	}
}

// TestMVCC_TransactionConflictUnderLoad verifies that concurrent
// transactions on the same key correctly detect conflicts, and that
// the final value is consistent.
func TestMVCC_TransactionConflictUnderLoad(t *testing.T) {
	d := openTestDB(t)

	d.Put([]byte("counter"), []byte("0"))

	const goroutines = 10
	const attemptsPerGoroutine = 20

	var successCount atomic.Int64
	var conflictCount atomic.Int64
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for attempt := 0; attempt < attemptsPerGoroutine; attempt++ {
				tx := d.BeginTransaction()

				// Read current value.
				val, ok := tx.Get([]byte("counter"))
				if !ok {
					tx.Rollback()
					continue
				}

				// "Increment" by appending the goroutine id.
				newVal := fmt.Sprintf("%s+%d", val.Data, id)
				tx.Put([]byte("counter"), []byte(newVal))

				err := tx.Commit()
				if errors.Is(err, ErrConflict) {
					conflictCount.Add(1)
				} else if err != nil {
					t.Errorf("goroutine %d: unexpected commit error: %v", id, err)
				} else {
					successCount.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	total := successCount.Load() + conflictCount.Load()
	if total != goroutines*attemptsPerGoroutine {
		t.Errorf("total = %d, want %d", total, goroutines*attemptsPerGoroutine)
	}

	if successCount.Load() == 0 {
		t.Error("expected at least one successful commit")
	}
	if conflictCount.Load() == 0 {
		t.Error("expected at least one conflict")
	}

	// Final value should be readable.
	val, ok := d.Get([]byte("counter"))
	if !ok {
		t.Fatal("counter key not found")
	}
	t.Logf("successes=%d conflicts=%d final=%q", successCount.Load(), conflictCount.Load(), val.Data)
}

// TestMVCC_LargeWriteBatchInTransaction verifies that a transaction
// with many buffered writes can commit successfully and all writes
// are visible afterward.
func TestMVCC_LargeWriteBatchInTransaction(t *testing.T) {
	d := openTestDB(t)

	tx := d.BeginTransaction()

	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		val := fmt.Appendf(nil, "val-%06d", i)
		tx.Put(key, val)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify all keys are readable.
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		val, ok := d.Get(key)
		if !ok {
			t.Fatalf("key %q not found after large tx commit", key)
		}
		want := fmt.Sprintf("val-%06d", i)
		if string(val.Data) != want {
			t.Errorf("%s = %q, want %q", key, val.Data, want)
		}
	}

	// Verify scan returns all keys.
	keys := collectUserKeys(t, d.Scan())
	if len(keys) != n {
		t.Errorf("scan: got %d keys, want %d", len(keys), n)
	}
}

// TestMVCC_SnapshotAfterRecovery verifies that after a crash and
// recovery, snapshots work correctly with the restored sequence numbers.
func TestMVCC_SnapshotAfterRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "theseon-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// First session: write data.
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}

	d.Put([]byte("a"), []byte("v1"))
	d.Put([]byte("b"), []byte("v1"))
	d.Put([]byte("a"), []byte("v2"))

	d.Close()

	// Second session: reopen, verify snapshot reads correct data.
	d2, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	snap := d2.GetSnapshot()
	defer snap.Close()

	// Write more data in the new session.
	d2.Put([]byte("a"), []byte("v3"))
	d2.Put([]byte("c"), []byte("v1"))

	// Snapshot should see v2 for "a" (the latest before snapshot).
	val, ok := snap.Get([]byte("a"))
	if !ok {
		t.Fatal("snapshot: 'a' not found after recovery")
	}
	if string(val.Data) != "v2" {
		t.Errorf("snapshot: a = %q, want v2", val.Data)
	}

	// Snapshot should see "b".
	val, ok = snap.Get([]byte("b"))
	if !ok {
		t.Fatal("snapshot: 'b' not found after recovery")
	}
	if string(val.Data) != "v1" {
		t.Errorf("snapshot: b = %q, want v1", val.Data)
	}

	// Snapshot should NOT see "c" (written after snapshot).
	_, ok = snap.Get([]byte("c"))
	if ok {
		t.Error("snapshot should not see 'c' written after snapshot creation")
	}

	// DB should see the latest.
	val, ok = d2.Get([]byte("a"))
	if !ok {
		t.Fatal("db: 'a' not found")
	}
	if string(val.Data) != "v3" {
		t.Errorf("db: a = %q, want v3", val.Data)
	}
}
