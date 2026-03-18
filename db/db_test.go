package db

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ulixert/lithicdb/compaction"
	"github.com/ulixert/lithicdb/iterator"
	"github.com/ulixert/lithicdb/kv"
)

func testOpts(dir string) Options {
	return Options{
		Dir:          dir,
		MemtableSize: 4096,
		BlockSize:    256,
		Compaction:   compaction.DefaultConfig(),
	}
}

func waitForFlush(t *testing.T, d *DB, expectedL0 int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		d.mu.RLock()
		n := len(d.l0)
		d.mu.RUnlock()
		if n >= expectedL0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for flush: have %d L0 SSTables, want %d", n, expectedL0)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// waitForAllFlushes waits until all immutable memtables have been
// flushed (immutables list is empty).
func waitForAllFlushes(t *testing.T, d *DB) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		d.mu.RLock()
		n := len(d.immutables)
		d.mu.RUnlock()
		if n == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for all flushes: %d immutables remaining", n)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// collectUserKeys reads an iterator and returns user keys in order.
func collectUserKeys(t *testing.T, iter iterator.Iterator) []string {
	t.Helper()
	defer iter.Close()

	var keys []string
	for iter.IsValid() {
		keys = append(keys, string(kv.UserKey(iter.Key())))
		iter.Next()
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return keys
}

func TestDB_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, found := d.Get([]byte("hello"))
	if !found {
		t.Fatal("expected key to be found")
	}
	if string(val.Data) != "world" {
		t.Errorf("value = %q, want %q", val.Data, "world")
	}
}

func TestDB_GetMissing(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	_, found := d.Get([]byte("missing"))
	if found {
		t.Fatal("expected not found")
	}
}

func TestDB_Delete(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("value"))
	d.Delete([]byte("key"))

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected tombstone to be found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestDB_Overwrite(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("first"))
	d.Put([]byte("key"), []byte("second"))

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "second" {
		t.Errorf("value = %q, want %q", val.Data, "second")
	}
}

func TestDB_Scan(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("c"), []byte("3"))
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))

	keys := collectUserKeys(t, d.Scan())
	if len(keys) != 3 {
		t.Fatalf("got %d keys, want 3", len(keys))
	}
	if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("keys = %v, want [a b c]", keys)
	}
}

func TestDB_Scan_OverwriteDeduplicates(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("v1"))
	d.Put([]byte("a"), []byte("v2"))
	d.Put([]byte("b"), []byte("v1"))

	// Scan should deduplicate — only one entry per user key
	keys := collectUserKeys(t, d.Scan())
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2 (deduped)", len(keys))
	}
	if keys[0] != "a" || keys[1] != "b" {
		t.Errorf("keys = %v, want [a b]", keys)
	}
}

func TestDB_ScanRange(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("c"), []byte("3"))
	d.Put([]byte("d"), []byte("4"))

	keys := collectUserKeys(t, d.ScanRange([]byte("b"), []byte("d")))
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2", len(keys))
	}
	if keys[0] != "b" || keys[1] != "c" {
		t.Errorf("keys = %v, want [b c]", keys)
	}
}

func TestDB_FlushTriggered(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := d.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	waitForFlush(t, d, 1)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val, found := d.Get([]byte(key))
		if !found {
			t.Fatalf("key %q not found after flush", key)
		}
		want := fmt.Sprintf("val-%04d", i)
		if string(val.Data) != want {
			t.Errorf("Get(%q) = %q, want %q", key, val.Data, want)
		}
	}
}

func TestDB_ReadAcrossMemtableAndSSTable(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		d.Put([]byte(key), []byte(val))
	}

	waitForFlush(t, d, 1)

	for i := 50; i < 60; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		d.Put([]byte(key), []byte(val))
	}

	keys := collectUserKeys(t, d.Scan())
	if len(keys) < 60 {
		t.Errorf("scan returned %d keys, want at least 60", len(keys))
	}

	// Verify sorted order
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("keys not sorted at index %d: %q <= %q", i, keys[i], keys[i-1])
		}
	}
}

func TestDB_OverwriteAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("v1"))
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("pad-%04d", i)), []byte("x"))
	}

	waitForFlush(t, d, 1)

	d.Put([]byte("key"), []byte("v2"))

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "v2" {
		t.Errorf("value = %q, want %q", val.Data, "v2")
	}

	// Scan should also show v2 (deduplicated)
	iter := d.Scan()
	defer iter.Close()
	for iter.IsValid() {
		if string(kv.UserKey(iter.Key())) == "key" {
			if string(iter.Value()) != "v2" {
				t.Errorf("scan value for key = %q, want %q", iter.Value(), "v2")
			}
			break
		}
		iter.Next()
	}
}

func TestDB_DeleteAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("key"), []byte("value"))
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("pad-%04d", i)), []byte("x"))
	}

	waitForFlush(t, d, 1)

	d.Delete([]byte("key"))

	val, found := d.Get([]byte("key"))
	if !found {
		t.Fatal("expected tombstone found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestDB_Recovery(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 1024 * 1024

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Delete([]byte("c"))
	d.Close()

	d2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	val, found := d2.Get([]byte("a"))
	if !found || string(val.Data) != "1" {
		t.Errorf("recovered a: found=%v, val=%q", found, val.Data)
	}

	val, found = d2.Get([]byte("b"))
	if !found || string(val.Data) != "2" {
		t.Errorf("recovered b: found=%v, val=%q", found, val.Data)
	}

	val, found = d2.Get([]byte("c"))
	if !found || !val.Tombstone {
		t.Errorf("recovered c: found=%v, tombstone=%v", found, val.Tombstone)
	}
}

func TestDB_RecoveryPreservesSeqNumbers(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 1024 * 1024

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	d.Put([]byte("key"), []byte("v1"))
	d.Put([]byte("key"), []byte("v2"))
	d.Close()

	d2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	// After recovery, the newest version should be visible
	val, found := d2.Get([]byte("key"))
	if !found {
		t.Fatal("expected found after recovery")
	}
	if string(val.Data) != "v2" {
		t.Errorf("value = %q, want %q", val.Data, "v2")
	}

	// New writes should get higher seq numbers
	d2.Put([]byte("key"), []byte("v3"))
	val, found = d2.Get([]byte("key"))
	if !found || string(val.Data) != "v3" {
		t.Errorf("after new write: found=%v, val=%q", found, val.Data)
	}
}

func TestDB_ScanWithTombstones(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(testOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Delete([]byte("b"))
	d.Put([]byte("c"), []byte("3"))

	// Scan deduplicates — "b" should appear once with its tombstone
	iter := d.Scan()
	defer iter.Close()

	if !iter.IsValid() || string(kv.UserKey(iter.Key())) != "a" {
		t.Fatalf("expected 'a', got %q", kv.UserKey(iter.Key()))
	}
	iter.Next()

	if !iter.IsValid() || string(kv.UserKey(iter.Key())) != "b" {
		t.Fatalf("expected 'b', got %q", kv.UserKey(iter.Key()))
	}
	// Tombstone is newest version — value should be nil
	if iter.Value() != nil {
		t.Error("expected nil value for tombstone 'b'")
	}
	iter.Next()

	if !iter.IsValid() || string(kv.UserKey(iter.Key())) != "c" {
		t.Fatalf("expected 'c', got %q", kv.UserKey(iter.Key()))
	}
}

func TestDB_FlushCreatesSSTableAndDeletesWAL(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Write enough to trigger at least one flush
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		d.Put([]byte(key), []byte("value"))
	}

	// Wait for ALL flushes to complete, not just one
	waitForAllFlushes(t, d)

	// Check that SSTable files were created
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	var sstCount, walCount int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			sstCount++
		}
		if strings.HasSuffix(e.Name(), ".wal") {
			walCount++
		}
	}

	if sstCount == 0 {
		t.Error("expected at least one .sst file after flush")
	}

	// There should be exactly one WAL remaining — the active memtable's.
	// Flushed memtables' WALs should be deleted.
	if walCount != 1 {
		t.Errorf("expected 1 WAL (active memtable), got %d", walCount)
	}
}

func TestDB_FlushThenScanMergesCorrectly(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Write keys 0-49, triggering flush(es)
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("v1-%04d", i)))
	}

	waitForFlush(t, d, 1)

	// Overwrite some keys in the active memtable
	for i := 0; i < 10; i++ {
		d.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("v2-%04d", i)))
	}

	// Scan should merge SSTable + memtable, showing v2 for keys 0-9
	iter := d.Scan()
	defer iter.Close()

	for iter.IsValid() {
		userKey := string(kv.UserKey(iter.Key()))
		val := string(iter.Value())

		// Extract index from key
		var idx int
		fmt.Sscanf(userKey, "key-%04d", &idx)

		if idx < 10 {
			expected := fmt.Sprintf("v2-%04d", idx)
			if val != expected {
				t.Errorf("key %q: value = %q, want %q (overwritten)", userKey, val, expected)
			}
		} else if idx < 50 {
			expected := fmt.Sprintf("v1-%04d", idx)
			if val != expected {
				t.Errorf("key %q: value = %q, want %q (original)", userKey, val, expected)
			}
		}

		iter.Next()
	}
}

func TestDB_RecoveryAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write enough to trigger flush
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte("value"))
	}
	waitForFlush(t, d, 1)

	// Write a few more into the active memtable (unflushed)
	d.Put([]byte("after-flush"), []byte("yes"))
	d.Close()

	// Reopen — flushed data should be recovered from manifest + SSTables,
	// unflushed data from WAL replay.
	d2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	// Unflushed write recovered from WAL
	val, found := d2.Get([]byte("after-flush"))
	if !found {
		t.Fatal("expected 'after-flush' to be recoverable from WAL")
	}
	if string(val.Data) != "yes" {
		t.Errorf("value = %q, want %q", val.Data, "yes")
	}

	// Flushed writes recovered from manifest + SSTable files
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val, found := d2.Get([]byte(key))
		if !found {
			t.Fatalf("key %q not found after recovery (should be in SSTable)", key)
		}
		if string(val.Data) != "value" {
			t.Errorf("Get(%q) = %q, want %q", key, val.Data, "value")
		}
	}
}

func TestDB_RecoveryPreservesSeqAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MemtableSize = 128

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write and flush — this deletes the WAL
	for i := 0; i < 50; i++ {
		d.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte("v1"))
	}
	waitForAllFlushes(t, d)
	d.Close()

	// Reopen — nextSeq should come from manifest, not WAL
	// (WALs were deleted after flush)
	d2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	// New writes should get sequence numbers higher than anything
	// in the SSTables, so overwrites work correctly
	d2.Put([]byte("key-0000"), []byte("v2"))

	val, found := d2.Get([]byte("key-0000"))
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "v2" {
		t.Errorf("value = %q, want %q (new write should win over SSTable)", val.Data, "v2")
	}
}
