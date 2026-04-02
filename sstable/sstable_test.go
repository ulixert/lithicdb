package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ulixert/theseon/kv"
)

// ikey builds an internal key for testing.
func ikey(userKey string, seq uint64) []byte {
	return kv.MakeInternalKey([]byte(userKey), seq)
}

// --- Builder + Reader roundtrip tests ---

func TestSSTable_BuildAndGet(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("apple", 1), kv.NewValue([]byte("red")))
	b.Add(ikey("banana", 2), kv.NewValue([]byte("yellow")))
	b.Add(ikey("cherry", 3), kv.NewValue([]byte("dark red")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	for _, tc := range []struct{ key, val string }{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
	} {
		val, found, err := r.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", tc.key)
		}
		if string(val.Data) != tc.val {
			t.Errorf("Get(%q) = %q, want %q", tc.key, val.Data, tc.val)
		}
	}
}

func TestSSTable_GetNewestVersion(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	// Two versions of "key" — seq 10 (newer) sorts before seq 5
	b.Add(ikey("key", 10), kv.NewValue([]byte("new")))
	b.Add(ikey("key", 5), kv.NewValue([]byte("old")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// Get should return the newest version
	val, found, err := r.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found")
	}
	if string(val.Data) != "new" {
		t.Errorf("value = %q, want %q", val.Data, "new")
	}
}

func TestSSTable_GetMissing(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("b", 1), kv.NewValue([]byte("2")))
	b.Add(ikey("d", 2), kv.NewValue([]byte("4")))

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

	b.Add(ikey("alive", 1), kv.NewValue([]byte("yes")))
	b.Add(ikey("dead", 2), kv.NewTombstone())
	b.Add(ikey("exists", 3), kv.NewValue([]byte("yep")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

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
}

func TestSSTable_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("key", 1), kv.NewValue([]byte{}))

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

	b.Add(ikey("b", 2), kv.NewValue([]byte("2")))
	err := b.Add(ikey("a", 1), kv.NewValue([]byte("1")))
	if err == nil {
		t.Fatal("expected error for unsorted keys")
	}
}

func TestSSTable_DoubleFinish(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("a", 1), kv.NewValue([]byte("1")))

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

	b.Add(ikey("a", 1), kv.NewValue([]byte("1")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	err := b.Add(ikey("b", 2), kv.NewValue([]byte("2")))
	if err == nil {
		t.Fatal("expected error on Add after Finish")
	}
}

func TestSSTable_CleanupTempFiles(t *testing.T) {
	dir := t.TempDir()

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

	if _, err := os.Stat(filepath.Join(dir, "000003.sst")); err != nil {
		t.Errorf("real SSTable was incorrectly removed: %v", err)
	}
}

func TestSSTable_CleanupTempFiles_NonExistentDir(t *testing.T) {
	err := CleanupTempFiles("/nonexistent/path")
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// --- Scan tests ---

func TestSSTable_Scan(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("a", 3), kv.NewValue([]byte("1")))
	b.Add(ikey("b", 2), kv.NewValue([]byte("2")))
	b.Add(ikey("c", 1), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	assertScanUserKeys(t, r.Scan(), []string{"a", "b", "c"})
}

func TestSSTable_ScanRange(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	for i, key := range []string{"a", "b", "c", "d", "e"} {
		b.Add(ikey(key, uint64(i+1)), kv.NewValue([]byte(fmt.Sprintf("%d", i+1))))
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// [b, d) should return b, c
	assertScanUserKeys(t, r.ScanRange([]byte("b"), []byte("d")), []string{"b", "c"})
}

func TestSSTable_ScanRange_NilBounds(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("a", 3), kv.NewValue([]byte("1")))
	b.Add(ikey("b", 2), kv.NewValue([]byte("2")))
	b.Add(ikey("c", 1), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	assertScanUserKeys(t, r.ScanRange(nil, []byte("b")), []string{"a"})
	assertScanUserKeys(t, r.ScanRange([]byte("b"), nil), []string{"b", "c"})
	assertScanUserKeys(t, r.ScanRange(nil, nil), []string{"a", "b", "c"})
}

func TestSSTable_ScanRange_NoMatch(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("a", 2), kv.NewValue([]byte("1")))
	b.Add(ikey("z", 1), kv.NewValue([]byte("26")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	assertScanUserKeys(t, r.ScanRange([]byte("m"), []byte("n")), nil)
}

// --- Multi-block tests ---

func TestSSTable_MultiBlock(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, 64)

	n := 100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := b.Add(ikey(key, uint64(i+1)), kv.NewValue([]byte(val))); err != nil {
			t.Fatalf("Add(%q): %v", key, err)
		}
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	if r.NumBlocks() <= 1 {
		t.Fatalf("expected multiple blocks, got %d", r.NumBlocks())
	}

	// All point lookups by user key
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
}

func TestSSTable_MultiBlock_RangeScan(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, 64)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		b.Add(ikey(key, uint64(i+1)), kv.NewValue([]byte(val)))
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	assertScanUserKeys(t, r.ScanRange([]byte("key-0010"), []byte("key-0015")),
		[]string{"key-0010", "key-0011", "key-0012", "key-0013", "key-0014"})
}

// --- Bloom filter tests ---

func TestSSTable_BloomFilter(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		b.Add(ikey(key, uint64(i+1)), kv.NewValue([]byte("v")))
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// All present keys should pass — check with user keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if !r.MayContain(key) {
			t.Fatalf("bloom filter rejected present key %q", key)
		}
	}

	falsePositives := 0
	absent := 10000
	for i := 0; i < absent; i++ {
		key := []byte(fmt.Sprintf("absent-%06d", i))
		if r.MayContain(key) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(absent)
	t.Logf("bloom filter false positive rate: %.2f%% (%d / %d)", rate*100, falsePositives, absent)

	if rate > 0.03 {
		t.Errorf("bloom false positive rate %.2f%% exceeds 3%% threshold", rate*100)
	}
}

// --- Metadata tests ---

func TestSSTable_FirstLastKey(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("alpha", 3), kv.NewValue([]byte("1")))
	b.Add(ikey("beta", 2), kv.NewValue([]byte("2")))
	b.Add(ikey("gamma", 1), kv.NewValue([]byte("3")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	// FirstKey/LastKey return internal keys
	firstUser := string(kv.UserKey(r.FirstKey()))
	lastUser := string(kv.UserKey(r.LastKey()))

	if firstUser != "alpha" {
		t.Errorf("FirstKey user = %q, want %q", firstUser, "alpha")
	}
	if lastUser != "gamma" {
		t.Errorf("LastKey user = %q, want %q", lastUser, "gamma")
	}
}

// --- Corruption tests ---

func TestSSTable_CorruptBlockChecksum(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("key", 1), kv.NewValue([]byte("value")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	path := SSTPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	data[0] ^= 0xFF
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatalf("write: %v", err)
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	_, _, err = r.Get([]byte("key"))
	if err == nil {
		t.Fatal("expected checksum error on corrupted block")
	}
}

func TestSSTable_CorruptFooter(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(dir, 1, defaultBlockSize)

	b.Add(ikey("key", 1), kv.NewValue([]byte("value")))

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	path := SSTPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	data[len(data)-1] ^= 0xFF
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

	b.Add(ikey("a", 1), kv.NewValue([]byte("1")))

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

	b.Add(ikey("only", 1), kv.NewValue([]byte("one")))

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
}

// --- Block unit tests ---

func TestBlock_BuildAndDecode(t *testing.T) {
	bb := NewBlockBuilder(4096)

	bb.Add(ikey("a", 3), kv.NewValue([]byte("1")))
	bb.Add(ikey("b", 2), kv.NewValue([]byte("2")))
	bb.Add(ikey("c", 1), kv.NewValue([]byte("3")))

	data := bb.Build()
	block, err := DecodeBlock(data)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}

	if block.NumEntries() != 3 {
		t.Fatalf("NumEntries = %d, want 3", block.NumEntries())
	}
}

func TestBlock_SizeLimit(t *testing.T) {
	bb := NewBlockBuilder(64)

	ok, err := bb.Add(ikey("key-0001", 1), kv.NewValue([]byte("value-0001")))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !ok {
		t.Fatal("first entry should always be accepted")
	}

	rejected := false
	for i := 2; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		ok, err = bb.Add(ikey(key, uint64(i)), kv.NewValue([]byte(val)))
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

func TestBlock_GetByUserKey(t *testing.T) {
	bb := NewBlockBuilder(4096)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		bb.Add(ikey(key, uint64(i+1)), kv.NewValue([]byte(val)))
	}

	data := bb.Build()
	block, err := DecodeBlock(data)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}

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

	if !BloomMayContain(nil, []byte("anything")) {
		t.Error("MayContain on nil filter should return true")
	}
}

// --- helpers ---

func assertScanUserKeys(t *testing.T, iter *SSTableIterator, expected []string) {
	t.Helper()
	defer iter.Close()

	var got []string
	for iter.IsValid() {
		got = append(got, string(kv.UserKey(iter.Key())))
		iter.Next()
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("got %d user keys %v, want %d %v", len(got), got, len(expected), expected)
	}

	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("user key %d = %q, want %q", i, got[i], expected[i])
		}
	}
}
