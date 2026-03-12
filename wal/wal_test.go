package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

// ============================================================
// Encoding / Decoding round-trip tests
// ============================================================

func TestEncodeDecodeRoundTrip_SinglePut(t *testing.T) {
	entries := []Entry{
		{Key: []byte("hello"), Value: kv.NewValue([]byte("world"))},
	}

	data := encodeRecord(entries)
	decoded, n, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	if n != len(data) {
		t.Errorf("consumed %d bytes, want %d", n, len(data))
	}

	assertEntriesEqual(t, decoded, entries)
}

func TestEncodeDecodeRoundTrip_SingleTombstone(t *testing.T) {
	entries := []Entry{
		{Key: []byte("deleted-key"), Value: kv.NewTombstone()},
	}

	data := encodeRecord(entries)
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	assertEntriesEqual(t, decoded, entries)
}

func TestEncodeDecodeRoundTrip_Batch(t *testing.T) {
	entries := []Entry{
		{Key: []byte("key1"), Value: kv.NewValue([]byte("val1"))},
		{Key: []byte("key2"), Value: kv.NewValue([]byte("val2"))},
		{Key: []byte("key3"), Value: kv.NewTombstone()},
		{Key: []byte("key4"), Value: kv.NewValue([]byte("val4"))},
	}

	data := encodeRecord(entries)
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	assertEntriesEqual(t, decoded, entries)
}

func TestEncodeDecodeRoundTrip_EmptyValue(t *testing.T) {
	// Empty value is NOT a tombstone
	entries := []Entry{
		{Key: []byte("key"), Value: kv.NewValue([]byte{})},
	}

	data := encodeRecord(entries)
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("got %d entries, want 1", len(decoded))
	}

	if decoded[0].Value.Tombstone {
		t.Error("empty value should not be decoded as tombstone")
	}

	if len(decoded[0].Value.Data) != 0 {
		t.Errorf("expected empty data, got %q", decoded[0].Value.Data)
	}
}

func TestEncodeDecodeRoundTrip_LargeKey(t *testing.T) {
	// Key near the 64KB limit (key_len is uint16)
	bigKey := bytes.Repeat([]byte("k"), 60000)
	entries := []Entry{
		{Key: bigKey, Value: kv.NewValue([]byte("v"))},
	}

	data := encodeRecord(entries)
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	if !bytes.Equal(decoded[0].Key, bigKey) {
		t.Error("large key not preserved through encode/decode")
	}
}

func TestEncodeDecodeRoundTrip_LargeValue(t *testing.T) {
	bigVal := bytes.Repeat([]byte("v"), 1<<20) // 1MB
	entries := []Entry{
		{Key: []byte("key"), Value: kv.NewValue(bigVal)},
	}

	data := encodeRecord(entries)
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decodeRecord error: %v", err)
	}

	if !bytes.Equal(decoded[0].Value.Data, bigVal) {
		t.Error("large value not preserved through encode/decode")
	}
}

// ============================================================
// Decode error tests
// ============================================================

func TestDecode_TooShort(t *testing.T) {
	_, _, err := decodeRecord([]byte{0x01, 0x02})
	if err != ErrShortRecord {
		t.Errorf("expected ErrShortRecord, got %v", err)
	}
}

func TestDecode_Empty(t *testing.T) {
	_, _, err := decodeRecord([]byte{})
	if err != ErrShortRecord {
		t.Errorf("expected ErrShortRecord, got %v", err)
	}
}

func TestDecode_CorruptChecksum(t *testing.T) {
	entries := []Entry{
		{Key: []byte("key"), Value: kv.NewValue([]byte("val"))},
	}

	data := encodeRecord(entries)
	// Flip a bit in the payload (after the checksum)
	data[headerSize+2] ^= 0xFF

	_, _, err := decodeRecord(data)
	if err != ErrCorruptRecord {
		t.Errorf("expected ErrCorruptRecord, got %v", err)
	}
}

func TestDecode_TruncatedPayload(t *testing.T) {
	entries := []Entry{
		{Key: []byte("key"), Value: kv.NewValue([]byte("value"))},
	}

	data := encodeRecord(entries)
	// Truncate half the payload
	truncated := data[:headerSize+3]

	_, _, err := decodeRecord(truncated)
	if err != ErrShortRecord {
		t.Errorf("expected ErrShortRecord, got %v", err)
	}
}

func TestDecode_InvalidFlag(t *testing.T) {
	entries := []Entry{
		{Key: []byte("key"), Value: kv.NewValue([]byte("val"))},
	}

	data := encodeRecord(entries)

	// Find the flag byte and set it to an invalid value
	flagOffset := headerSize + countSize
	data[flagOffset] = 0xFF

	// Recompute checksum to isolate the flag error
	checksum := crc32.ChecksumIEEE(data[checksumSize:])
	binary.LittleEndian.PutUint32(data[:checksumSize], checksum)

	_, _, err := decodeRecord(data)
	if err != ErrInvalidFlag {
		t.Errorf("expected ErrInvalidFlag, got %v", err)
	}
}

// ============================================================
// WAL file write and recovery tests
// ============================================================

func TestWAL_PutAndRecover(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("hello"), []byte("world")); err != nil {
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
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	assertEntriesEqual(t, entries, []Entry{
		{Key: []byte("hello"), Value: kv.NewValue([]byte("world"))},
	})
}

func TestWAL_DeleteAndRecover(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Delete([]byte("gone")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	if !entries[0].Value.Tombstone {
		t.Error("expected tombstone")
	}
}

func TestWAL_MultipleRecords(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write three separate records
	if err := w.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Delete([]byte("c")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}

	// Verify order
	if string(entries[0].Key) != "a" {
		t.Errorf("entry 0 key = %q, want %q", entries[0].Key, "a")
	}
	if string(entries[1].Key) != "b" {
		t.Errorf("entry 1 key = %q, want %q", entries[1].Key, "b")
	}
	if string(entries[2].Key) != "c" {
		t.Errorf("entry 2 key = %q, want %q", entries[2].Key, "c")
	}
	if !entries[2].Value.Tombstone {
		t.Error("entry 2 should be tombstone")
	}
}

func TestWAL_BatchWrite(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	batch := []Entry{
		{Key: []byte("k1"), Value: kv.NewValue([]byte("v1"))},
		{Key: []byte("k2"), Value: kv.NewValue([]byte("v2"))},
		{Key: []byte("k3"), Value: kv.NewTombstone()},
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

	assertEntriesEqual(t, entries, batch)
}

// ============================================================
// Crash recovery / corruption tests
// ============================================================

func TestRecover_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")

	if err := os.WriteFile(path, []byte{}, 0o640); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	entries, err := Recover(path)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("expected 0 entries from empty WAL, got %d", len(entries))
	}
}

func TestRecover_CorruptTail(t *testing.T) {
	dir := t.TempDir()

	// Write two valid records
	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("good1"), []byte("val1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("good2"), []byte("val2")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append garbage to simulate a crash mid-write
	f, err := os.OpenFile(w.Path(), os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("this is garbage from a crash")); err != nil {
		t.Fatalf("Write garbage: %v", err)
	}
	f.Close()

	// Recovery should return the two valid records
	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2 (corrupt tail ignored)", len(entries))
	}

	if string(entries[0].Key) != "good1" {
		t.Errorf("entry 0 key = %q, want %q", entries[0].Key, "good1")
	}
	if string(entries[1].Key) != "good2" {
		t.Errorf("entry 1 key = %q, want %q", entries[1].Key, "good2")
	}
}

func TestRecover_TruncatedRecord(t *testing.T) {
	dir := t.TempDir()

	// Write a valid record, then a truncated one
	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := w.Put([]byte("safe"), []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Encode another record and write only half of it
	partialRecord := encodeRecord([]Entry{
		{Key: []byte("lost"), Value: kv.NewValue([]byte("gone"))},
	})
	f, err := os.OpenFile(w.Path(), os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write(partialRecord[:len(partialRecord)/2]); err != nil {
		t.Fatalf("Write partial: %v", err)
	}
	f.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1 (truncated record ignored)", len(entries))
	}

	if string(entries[0].Key) != "safe" {
		t.Errorf("key = %q, want %q", entries[0].Key, "safe")
	}
}

func TestRecover_FlippedBitInMiddleRecord(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write three records
	if err := w.Put([]byte("first"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("second"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Put([]byte("third"), []byte("3")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read the file, find the second record, and flip a bit in its payload
	data, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Find offset of second record by decoding the first
	_, firstLen, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decode first: %v", err)
	}

	// Corrupt a byte in the second record's payload
	corruptOffset := firstLen + headerSize + 2
	if corruptOffset < len(data) {
		data[corruptOffset] ^= 0xFF
	}

	if err := os.WriteFile(w.Path(), data, 0o640); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Recovery should return only the first record (corruption stops reading)
	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1 (corrupt second record stops recovery)", len(entries))
	}

	if string(entries[0].Key) != "first" {
		t.Errorf("key = %q, want %q", entries[0].Key, "first")
	}
}

func TestRecover_NonExistentFile(t *testing.T) {
	_, err := Recover("/nonexistent/path/000001.wal")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// ============================================================
// FindWALFiles and RecoverDir tests
// ============================================================

func TestFindWALFiles_Empty(t *testing.T) {
	dir := t.TempDir()

	ids, err := FindWALFiles(dir)
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if len(ids) != 0 {
		t.Errorf("expected 0 WAL files, got %d", len(ids))
	}
}

func TestFindWALFiles_NonExistentDir(t *testing.T) {
	ids, err := FindWALFiles("/nonexistent/wal/dir")
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if ids != nil {
		t.Errorf("expected nil for non-existent dir, got %v", ids)
	}
}

func TestFindWALFiles_SortedOrder(t *testing.T) {
	dir := t.TempDir()

	// Create WAL files out of order
	for _, id := range []uint64{5, 1, 3, 2, 4} {
		w, err := Create(dir, id)
		if err != nil {
			t.Fatalf("Create id=%d: %v", id, err)
		}
		w.Close()
	}

	ids, err := FindWALFiles(dir)
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if len(ids) != 5 {
		t.Fatalf("got %d files, want 5", len(ids))
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("IDs not sorted: %v", ids)
			break
		}
	}
}

func TestFindWALFiles_IgnoresNonWALFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a real WAL file
	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w.Close()

	// Create non-WAL files
	os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("hi"), 0o640)
	os.WriteFile(filepath.Join(dir, "data.sst"), []byte("data"), 0o640)
	os.WriteFile(filepath.Join(dir, "notanumber.wal"), []byte("bad"), 0o640)

	ids, err := FindWALFiles(dir)
	if err != nil {
		t.Fatalf("FindWALFiles: %v", err)
	}

	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("expected [1], got %v", ids)
	}
}

func TestRecoverDir_MultipleWALs(t *testing.T) {
	dir := t.TempDir()

	// Create WAL 1 with two entries
	w1, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w1.Put([]byte("a"), []byte("1"))
	w1.Put([]byte("b"), []byte("2"))
	w1.Close()

	// Create WAL 3 with one entry (skip ID 2)
	w3, err := Create(dir, 3)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w3.Put([]byte("c"), []byte("3"))
	w3.Close()

	recovered, err := RecoverDir(dir)
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	if len(recovered) != 2 {
		t.Fatalf("got %d recovered WALs, want 2", len(recovered))
	}

	// Should be in ascending ID order
	if recovered[0].ID != 1 {
		t.Errorf("recovered[0].ID = %d, want 1", recovered[0].ID)
	}
	if recovered[1].ID != 3 {
		t.Errorf("recovered[1].ID = %d, want 3", recovered[1].ID)
	}

	if len(recovered[0].Entries) != 2 {
		t.Errorf("WAL 1: %d entries, want 2", len(recovered[0].Entries))
	}
	if len(recovered[1].Entries) != 1 {
		t.Errorf("WAL 3: %d entries, want 1", len(recovered[1].Entries))
	}
}

func TestRecoverDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	recovered, err := RecoverDir(dir)
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered WALs, got %d", len(recovered))
	}
}

func TestRecoverDir_NonExistentDir(t *testing.T) {
	recovered, err := RecoverDir("/nonexistent/wal/dir")
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	if recovered != nil && len(recovered) != 0 {
		t.Errorf("expected empty result, got %v", recovered)
	}
}

func TestRecoverDir_IncludesEmptyWALs(t *testing.T) {
	dir := t.TempDir()

	// WAL 1: has data
	w1, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w1.Put([]byte("key"), []byte("val"))
	w1.Close()

	// WAL 2: empty file (no records written)
	w2, err := Create(dir, 2)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w2.Close()

	recovered, err := RecoverDir(dir)
	if err != nil {
		t.Fatalf("RecoverDir: %v", err)
	}

	// Both WALs should be present — empty WALs carry ID information
	// needed for correct next-ID allocation on recovery.
	if len(recovered) != 2 {
		t.Fatalf("got %d recovered WALs, want 2", len(recovered))
	}

	if recovered[0].ID != 1 {
		t.Errorf("recovered[0].ID = %d, want 1", recovered[0].ID)
	}
	if recovered[1].ID != 2 {
		t.Errorf("recovered[1].ID = %d, want 2", recovered[1].ID)
	}

	if len(recovered[0].Entries) != 1 {
		t.Errorf("WAL 1: %d entries, want 1", len(recovered[0].Entries))
	}
	if len(recovered[1].Entries) != 0 {
		t.Errorf("WAL 2: %d entries, want 0 (empty WAL)", len(recovered[1].Entries))
	}
}

// ============================================================
// WAL file lifecycle tests
// ============================================================

func TestWAL_Remove(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 42)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	w.Put([]byte("key"), []byte("val"))
	w.Close()

	path := w.Path()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("WAL file should exist before Remove")
	}

	if err := w.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("WAL file should not exist after Remove")
	}
}

func TestWAL_Path(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 7)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer w.Close()

	expected := filepath.Join(dir, "000007.wal")
	if w.Path() != expected {
		t.Errorf("Path() = %q, want %q", w.Path(), expected)
	}
}

func TestWAL_OpenAppend(t *testing.T) {
	dir := t.TempDir()

	// Create and write first entry
	w1, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w1.Put([]byte("first"), []byte("1"))
	w1.Close()

	// Open for append and write second entry
	w2, err := Open(dir, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w2.Put([]byte("second"), []byte("2"))
	w2.Close()

	// Recover should see both entries
	entries, err := Recover(filepath.Join(dir, walFileName(1)))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	if string(entries[0].Key) != "first" {
		t.Errorf("entry 0 key = %q, want %q", entries[0].Key, "first")
	}
	if string(entries[1].Key) != "second" {
		t.Errorf("entry 1 key = %q, want %q", entries[1].Key, "second")
	}
}

// ============================================================
// Edge cases
// ============================================================

func TestWAL_ManySmallWrites(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	n := 500
	for i := 0; i < n; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		val := []byte{byte(i)}
		if err := w.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != n {
		t.Errorf("got %d entries, want %d", len(entries), n)
	}
}

func TestWAL_MixedBatchAndSingle(t *testing.T) {
	dir := t.TempDir()

	w, err := Create(dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Single write
	w.Put([]byte("single1"), []byte("s1"))

	// Batch write
	w.WriteEntries([]Entry{
		{Key: []byte("batch1"), Value: kv.NewValue([]byte("b1"))},
		{Key: []byte("batch2"), Value: kv.NewValue([]byte("b2"))},
	})

	// Another single write
	w.Delete([]byte("del1"))

	w.Close()

	entries, err := Recover(w.Path())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("got %d entries, want 4", len(entries))
	}

	// Verify all present in order
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = string(e.Key)
	}

	expected := []string{"single1", "batch1", "batch2", "del1"}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("entry %d: key = %q, want %q", i, k, expected[i])
		}
	}

	if !entries[3].Value.Tombstone {
		t.Error("last entry should be tombstone")
	}
}

// ============================================================
// Helpers
// ============================================================

func assertEntriesEqual(t *testing.T, got, want []Entry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}

	for i := range got {
		if !bytes.Equal(got[i].Key, want[i].Key) {
			t.Errorf("entry %d: key = %q, want %q", i, got[i].Key, want[i].Key)
		}

		if got[i].Value.Tombstone != want[i].Value.Tombstone {
			t.Errorf("entry %d: tombstone = %v, want %v", i, got[i].Value.Tombstone, want[i].Value.Tombstone)
		}

		if !got[i].Value.Tombstone {
			if !bytes.Equal(got[i].Value.Data, want[i].Value.Data) {
				t.Errorf("entry %d: value = %q, want %q", i, got[i].Value.Data, want[i].Value.Data)
			}
		}
	}
}
