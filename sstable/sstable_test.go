package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ulixert/lithicdb/iterator/itertest"
	"github.com/ulixert/lithicdb/kv"
)

// --- Builder + Reader roundtrip tests ---

func TestSSTable_BuildAndGet(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	entries := []struct {
		key, val string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
	}

	for _, e := range entries {
		if err := b.Add([]byte(e.key), kv.NewValue([]byte(e.val))); err != nil {
			t.Fatalf("Add(%q): %v", e.key, err)
		}
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	for _, e := range entries {
		val, found, err := r.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", e.key)
		}
		if string(val.Data) != e.val {
			t.Errorf("Get(%q) = %q, want %q", e.key, val.Data, e.val)
		}
	}
}

func TestSSTable_GetMissing(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("b"), kv.NewValue([]byte("2")))
	b.Add([]byte("d"), kv.NewValue([]byte("4")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	for _, key := range []string{"a", "c", "e"} {
		_, found, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if found {
			t.Errorf("Get(%q): expected not found", key)
		}
	}
}

func TestSSTable_Tombstone(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("alive"), kv.NewValue([]byte("yes")))
	b.Add([]byte("dead"), kv.NewTombstone())
	b.Add([]byte("exists"), kv.NewValue([]byte("yep")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// Point lookup: tombstone found
	val, found, err := r.Get([]byte("dead"))
	if err != nil {
		t.Fatalf("Get(dead): %v", err)
	}
	if !found {
		t.Fatal("Get(dead): expected found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone")
	}

	// Iterator: tombstone has nil value
	iter := r.Scan()
	defer iter.Close()

	iter.Next() // skip "alive"
	if string(iter.Key()) != "dead" {
		t.Fatalf("key = %q, want %q", iter.Key(), "dead")
	}
	if iter.Value() != nil {
		t.Errorf("tombstone Value() = %q, want nil", iter.Value())
	}
}

func TestSSTable_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("key"), kv.NewValue([]byte{}))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	val, found, err := r.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found")
	}
	if val.Tombstone {
		t.Fatal("empty value should not be tombstone")
	}
	if len(val.Data) != 0 {
		t.Errorf("expected empty data, got %q", val.Data)
	}
}

func TestSSTable_EmptyBuild(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	err := b.Finish()
	if err != ErrEmptySSTable {
		t.Fatalf("expected ErrEmptySSTable, got: %v", err)
	}
}

func TestSSTable_UnsortedKeys(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	if err := b.Add([]byte("b"), kv.NewValue([]byte("2"))); err != nil {
		t.Fatalf("Add(b): %v", err)
	}

	err := b.Add([]byte("a"), kv.NewValue([]byte("1")))
	if err == nil {
		t.Fatal("expected error for unsorted keys")
	}
}

func TestSSTable_DuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	if err := b.Add([]byte("a"), kv.NewValue([]byte("1"))); err != nil {
		t.Fatalf("Add(a): %v", err)
	}

	err := b.Add([]byte("a"), kv.NewValue([]byte("2")))
	if err == nil {
		t.Fatal("expected error for duplicate key")
	}
}

func TestSSTable_DoubleFinish(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))

	if err := b.Finish(); err != nil {
		t.Fatalf("first Finish: %v", err)
	}

	err := b.Finish()
	if err == nil {
		t.Fatal("expected error on second Finish")
	}
}

func TestSSTable_AddAfterFinish(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	err := b.Add([]byte("b"), kv.NewValue([]byte("2")))
	if err == nil {
		t.Fatal("expected error on Add after Finish")
	}
}

func TestSSTable_CleanupTempFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some temp files and a real SSTable
	os.WriteFile(filepath.Join(dir, "000001.sst.tmp"), []byte("incomplete"), 0o640)
	os.WriteFile(filepath.Join(dir, "000002.sst.tmp"), []byte("also incomplete"), 0o640)
	os.WriteFile(filepath.Join(dir, "000003.sst"), []byte("real file"), 0o640)

	if err := CleanupTempFiles(dir); err != nil {
		t.Fatalf("CleanupTempFiles: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst.tmp") {
			t.Errorf("temp file %s was not cleaned up", e.Name())
		}
	}

	// Real SSTable should still exist
	if _, err := os.Stat(filepath.Join(dir, "000003.sst")); err != nil {
		t.Errorf("real SSTable was incorrectly removed: %v", err)
	}
}

func TestSSTable_CleanupTempFiles_NonExistentDir(t *testing.T) {
	err := CleanupTempFiles("/nonexistent/path")
	if err != nil {
		t.Fatalf("expected nil error for non-existent dir, got: %v", err)
	}
}

// --- Scan tests ---

func TestSSTable_Scan(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))
	b.Add([]byte("b"), kv.NewValue([]byte("2")))
	b.Add([]byte("c"), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	itertest.AssertIterator(t, r.Scan(), []itertest.Entry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
}

func TestSSTable_ScanRange(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))
	b.Add([]byte("b"), kv.NewValue([]byte("2")))
	b.Add([]byte("c"), kv.NewValue([]byte("3")))
	b.Add([]byte("d"), kv.NewValue([]byte("4")))
	b.Add([]byte("e"), kv.NewValue([]byte("5")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// [b, d) should return b, c
	itertest.AssertIterator(t, r.ScanRange([]byte("b"), []byte("d")), []itertest.Entry{
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
}

func TestSSTable_ScanRange_NilBounds(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))
	b.Add([]byte("b"), kv.NewValue([]byte("2")))
	b.Add([]byte("c"), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// nil start
	itertest.AssertIterator(t, r.ScanRange(nil, []byte("b")), []itertest.Entry{
		{Key: "a", Value: "1"},
	})

	// nil end
	itertest.AssertIterator(t, r.ScanRange([]byte("b"), nil), []itertest.Entry{
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})

	// both nil = full scan
	itertest.AssertIterator(t, r.ScanRange(nil, nil), []itertest.Entry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
}

func TestSSTable_ScanRange_NoMatch(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))
	b.Add([]byte("z"), kv.NewValue([]byte("26")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	itertest.AssertEmpty(t, r.ScanRange([]byte("m"), []byte("n")))
}

func TestSSTable_ScanRange_StartPastEnd(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))
	b.Add([]byte("b"), kv.NewValue([]byte("2")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	itertest.AssertEmpty(t, r.ScanRange([]byte("z"), nil))
}

// --- Multi-block tests ---

func TestSSTable_MultiBlock(t *testing.T) {
	dir := t.TempDir()
	// Use a tiny block size to force multiple blocks
	b := NewBuilder(dir, 1, 64)

	n := 100
	expected := make([]itertest.Entry, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := b.Add([]byte(key), kv.NewValue([]byte(val))); err != nil {
			t.Fatalf("Add(%q): %v", key, err)
		}
		expected[i] = itertest.Entry{Key: key, Value: val}
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// Should have multiple blocks
	if r.NumBlocks() <= 1 {
		t.Fatalf("expected multiple blocks, got %d", r.NumBlocks())
	}

	// All point lookups should work
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val, found, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", key)
		}
		want := fmt.Sprintf("val-%04d", i)
		if string(val.Data) != want {
			t.Errorf("Get(%q) = %q, want %q", key, val.Data, want)
		}
	}

	// Full scan should return all entries in order
	itertest.AssertIterator(t, r.Scan(), expected)
}

func TestSSTable_MultiBlock_RangeScan(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, 64)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		b.Add([]byte(key), kv.NewValue([]byte(val)))
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// Range scan across block boundaries
	itertest.AssertIterator(t, r.ScanRange([]byte("key-0010"), []byte("key-0015")), []itertest.Entry{
		{Key: "key-0010", Value: "val-0010"},
		{Key: "key-0011", Value: "val-0011"},
		{Key: "key-0012", Value: "val-0012"},
		{Key: "key-0013", Value: "val-0013"},
		{Key: "key-0014", Value: "val-0014"},
	})
}

// --- Bloom filter tests ---

func TestSSTable_BloomFilter(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		b.Add([]byte(key), kv.NewValue([]byte("v")))
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// All present keys should pass the filter
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if !r.MayContain([]byte(key)) {
			t.Fatalf("bloom filter rejected present key %q", key)
		}
	}

	// Check false positive rate on absent keys
	falsePositives := 0
	absent := 10000
	for i := 0; i < absent; i++ {
		key := fmt.Sprintf("absent-%06d", i)
		if r.MayContain([]byte(key)) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(absent)
	t.Logf("bloom filter false positive rate: %.2f%% (%d / %d)", rate*100, falsePositives, absent)

	// With 10 bits/key, expect ~1% — allow up to 3% for statistical variance
	if rate > 0.03 {
		t.Errorf("bloom false positive rate %.2f%% exceeds 3%% threshold", rate*100)
	}
}

// --- Metadata tests ---

func TestSSTable_FirstLastKey(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("alpha"), kv.NewValue([]byte("1")))
	b.Add([]byte("beta"), kv.NewValue([]byte("2")))
	b.Add([]byte("gamma"), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	if string(r.FirstKey()) != "alpha" {
		t.Errorf("FirstKey = %q, want %q", r.FirstKey(), "alpha")
	}
	if string(r.LastKey()) != "gamma" {
		t.Errorf("LastKey = %q, want %q", r.LastKey(), "gamma")
	}
}

// --- Corruption tests ---

func TestSSTable_CorruptBlockChecksum(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("key"), kv.NewValue([]byte("value")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Corrupt a byte in the data block region
	path := SSTPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	data[0] ^= 0xFF // flip bits in first byte
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatalf("write: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// Get should fail with checksum error
	_, _, err = r.Get([]byte("key"))
	if err == nil {
		t.Fatal("expected checksum error on corrupted block")
	}
}

func TestSSTable_CorruptFooter(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("key"), kv.NewValue([]byte("value")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Corrupt the magic number at the very end
	path := SSTPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	data[len(data)-1] ^= 0xFF // corrupt last byte (part of magic)
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err = OpenReader(dir, 1)
	if err == nil {
		t.Fatal("expected error on corrupted footer")
	}
}

func TestSSTable_IteratorCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("a"), kv.NewValue([]byte("1")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	iter := r.Scan()
	if err := iter.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestSSTable_SingleEntry(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add([]byte("only"), kv.NewValue([]byte("one")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	val, found, err := r.Get([]byte("only"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "one" {
		t.Errorf("value = %q, want %q", val.Data, "one")
	}

	itertest.AssertIterator(t, r.Scan(), []itertest.Entry{
		{Key: "only", Value: "one"},
	})
}

// --- Block encoding unit tests ---

func TestBlock_BuildAndDecode(t *testing.T) {
	bb := NewBlockBuilder(4096)

	bb.Add([]byte("a"), kv.NewValue([]byte("1")))
	bb.Add([]byte("b"), kv.NewValue([]byte("2")))
	bb.Add([]byte("c"), kv.NewValue([]byte("3")))

	data := bb.Build()
	block, err := DecodeBlock(data)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}

	if block.NumEntries() != 3 {
		t.Fatalf("NumEntries = %d, want 3", block.NumEntries())
	}

	fk, _ := block.FirstKey()
	if string(fk) != "a" {
		t.Errorf("FirstKey = %q, want %q", fk, "a")
	}

	lk, _ := block.LastKey()
	if string(lk) != "c" {
		t.Errorf("LastKey = %q, want %q", lk, "c")
	}
}

func TestBlock_SizeLimit(t *testing.T) {
	bb := NewBlockBuilder(64)

	// First entry always accepted
	ok, err := bb.Add([]byte("key-0001"), kv.NewValue([]byte("value-0001")))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !ok {
		t.Fatal("first entry should always be accepted")
	}

	// Keep adding until rejected
	rejected := false
	for i := 2; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		ok, err = bb.Add([]byte(key), kv.NewValue([]byte(val)))
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if !ok {
			rejected = true
			break
		}
	}

	if !rejected {
		t.Fatal("expected block builder to reject an entry due to size limit")
	}
}

func TestBlock_GetBinarySearch(t *testing.T) {
	bb := NewBlockBuilder(4096)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		bb.Add([]byte(key), kv.NewValue([]byte(val)))
	}

	data := bb.Build()
	block, err := DecodeBlock(data)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}

	// Find existing key
	val, found, err := block.Get([]byte("key-0025"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "val-0025" {
		t.Errorf("value = %q, want %q", val.Data, "val-0025")
	}

	// Missing key
	_, found, err = block.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Error("expected not found")
	}
}

// --- Bloom filter unit tests ---

func TestBloom_BasicFunctionality(t *testing.T) {
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	hashes := make([]uint32, len(keys))
	for i, k := range keys {
		hashes[i] = BloomHash(k)
	}

	filter := BuildBloomFilter(hashes)

	// All inserted keys must pass
	for _, k := range keys {
		if !BloomMayContain(filter, k) {
			t.Errorf("bloom filter rejected inserted key %q", k)
		}
	}
}

func TestBloom_EmptyKeys(t *testing.T) {
	filter := BuildBloomFilter(nil)
	if filter != nil {
		t.Errorf("expected nil filter for empty hashes, got %d bytes", len(filter))
	}

	// MayContain on nil filter should return true (safe default)
	if !BloomMayContain(nil, []byte("anything")) {
		t.Error("MayContain on nil filter should return true")
	}
}
