package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManifest_CreateAndReopen(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 5, 100)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if state.NextMemID != 5 {
		t.Errorf("NextMemID = %d, want 5", state.NextMemID)
	}
	if state.NextSeq != 100 {
		t.Errorf("NextSeq = %d, want 100", state.NextSeq)
	}
}

func TestManifest_AddSSTable(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := m.AddSSTable(SSTableInfo{
		ID: 1, Level: 0,
		FirstKey: []byte("a"), LastKey: []byte("m"),
	}); err != nil {
		t.Fatalf("AddSSTable: %v", err)
	}

	if err := m.AddSSTable(SSTableInfo{
		ID: 2, Level: 0,
		FirstKey: []byte("d"), LastKey: []byte("z"),
	}); err != nil {
		t.Fatalf("AddSSTable: %v", err)
	}

	m.Close()

	// Reopen and verify
	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 2 {
		t.Fatalf("L0 count = %d, want 2", len(state.L0))
	}

	// L0 should be newest first (ID 2 before ID 1)
	if state.L0[0].ID != 2 {
		t.Errorf("L0[0].ID = %d, want 2", state.L0[0].ID)
	}
	if state.L0[1].ID != 1 {
		t.Errorf("L0[1].ID = %d, want 1", state.L0[1].ID)
	}
}

func TestManifest_RemoveSSTable(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.AddSSTable(SSTableInfo{ID: 1, Level: 0, FirstKey: []byte("a"), LastKey: []byte("m")})
	m.AddSSTable(SSTableInfo{ID: 2, Level: 0, FirstKey: []byte("d"), LastKey: []byte("z")})
	m.RemoveSSTable(1, 0)
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 1 {
		t.Fatalf("L0 count = %d, want 1", len(state.L0))
	}
	if state.L0[0].ID != 2 {
		t.Errorf("L0[0].ID = %d, want 2", state.L0[0].ID)
	}
}

func TestManifest_MultiLevel(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.AddSSTable(SSTableInfo{ID: 1, Level: 0, FirstKey: []byte("a"), LastKey: []byte("z")})
	m.AddSSTable(SSTableInfo{ID: 2, Level: 1, FirstKey: []byte("a"), LastKey: []byte("m")})
	m.AddSSTable(SSTableInfo{ID: 3, Level: 1, FirstKey: []byte("n"), LastKey: []byte("z")})
	m.AddSSTable(SSTableInfo{ID: 4, Level: 2, FirstKey: []byte("a"), LastKey: []byte("z")})
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 1 {
		t.Fatalf("L0 count = %d, want 1", len(state.L0))
	}

	l1 := state.Levels[1]
	if len(l1) != 2 {
		t.Fatalf("L1 count = %d, want 2", len(l1))
	}
	// L1 sorted by first key
	if string(l1[0].FirstKey) != "a" || string(l1[1].FirstKey) != "n" {
		t.Errorf("L1 first keys = [%q, %q], want [a, n]", l1[0].FirstKey, l1[1].FirstKey)
	}

	l2 := state.Levels[2]
	if len(l2) != 1 {
		t.Fatalf("L2 count = %d, want 1", len(l2))
	}
}

func TestManifest_UpdateNextIDs(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.UpdateNextIDs(10, 500)
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if state.NextMemID != 10 {
		t.Errorf("NextMemID = %d, want 10", state.NextMemID)
	}
	if state.NextSeq != 500 {
		t.Errorf("NextSeq = %d, want 500", state.NextSeq)
	}
}

func TestManifest_CorruptTail(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.AddSSTable(SSTableInfo{ID: 1, Level: 0, FirstKey: []byte("a"), LastKey: []byte("z")})
	m.Close()

	// Append garbage
	path := filepath.Join(dir, manifestFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	f.Write([]byte("corrupt garbage"))
	f.Close()

	// Recovery should still find the valid records
	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 1 {
		t.Fatalf("L0 count = %d, want 1", len(state.L0))
	}
	if state.L0[0].ID != 1 {
		t.Errorf("L0[0].ID = %d, want 1", state.L0[0].ID)
	}
}

func TestManifest_Snapshot(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write enough records to trigger a snapshot
	for i := uint64(1); i <= 100; i++ {
		m.AddSSTable(SSTableInfo{
			ID: i, Level: 0,
			FirstKey: []byte("a"), LastKey: []byte("z"),
		})
	}

	// Remove some
	for i := uint64(1); i <= 50; i++ {
		m.RemoveSSTable(i, 0)
	}

	m.Close()

	// Reopen — should recover correctly from snapshot + subsequent records
	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 50 {
		t.Fatalf("L0 count = %d, want 50", len(state.L0))
	}

	// Should have IDs 51-100
	for _, sst := range state.L0 {
		if sst.ID < 51 || sst.ID > 100 {
			t.Errorf("unexpected SSTable ID %d (expected 51-100)", sst.ID)
		}
	}
}

func TestManifest_SnapshotPreservesNextIDs(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.UpdateNextIDs(50, 999)

	// Trigger a snapshot by writing many records
	for i := uint64(1); i <= 100; i++ {
		m.AddSSTable(SSTableInfo{
			ID: i, Level: 0,
			FirstKey: []byte("a"), LastKey: []byte("z"),
		})
	}

	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	// NextIDs should survive the snapshot
	if state.NextMemID != 50 {
		t.Errorf("NextMemID = %d, want 50", state.NextMemID)
	}
	if state.NextSeq != 999 {
		t.Errorf("NextSeq = %d, want 999", state.NextSeq)
	}
}

func TestManifest_Exists(t *testing.T) {
	dir := t.TempDir()

	if Exists(dir) {
		t.Fatal("manifest should not exist yet")
	}

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m.Close()

	if !Exists(dir) {
		t.Fatal("manifest should exist after Create")
	}
}

func TestManifest_Empty(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if len(state.L0) != 0 {
		t.Errorf("L0 count = %d, want 0", len(state.L0))
	}
	if len(state.Levels) != 0 {
		t.Errorf("Levels count = %d, want 0", len(state.Levels))
	}
}

func TestManifest_KeysPreserved(t *testing.T) {
	dir := t.TempDir()

	m, err := Create(dir, 1, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m.AddSSTable(SSTableInfo{
		ID: 1, Level: 0,
		FirstKey: []byte("alpha"), LastKey: []byte("omega"),
	})
	m.Close()

	m2, state, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m2.Close()

	if string(state.L0[0].FirstKey) != "alpha" {
		t.Errorf("FirstKey = %q, want %q", state.L0[0].FirstKey, "alpha")
	}
	if string(state.L0[0].LastKey) != "omega" {
		t.Errorf("LastKey = %q, want %q", state.L0[0].LastKey, "omega")
	}
}

// --- Encoding round-trip tests ---

func TestEncoding_SSTableAdded(t *testing.T) {
	info := SSTableInfo{ID: 42, Level: 1, FirstKey: []byte("abc"), LastKey: []byte("xyz")}
	rec := Record{Type: typeSSTableAdded, SSTable: info}

	data, err := encodeRecord(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, n, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != len(data) {
		t.Errorf("consumed %d bytes, want %d", n, len(data))
	}
	if decoded.SSTable.ID != 42 || decoded.SSTable.Level != 1 {
		t.Errorf("SSTable = %+v, want ID=42 Level=1", decoded.SSTable)
	}
	if string(decoded.SSTable.FirstKey) != "abc" || string(decoded.SSTable.LastKey) != "xyz" {
		t.Errorf("keys = [%q, %q], want [abc, xyz]", decoded.SSTable.FirstKey, decoded.SSTable.LastKey)
	}
}

func TestEncoding_SSTableRemoved(t *testing.T) {
	rec := Record{Type: typeSSTableRemoved, SSTable: SSTableInfo{ID: 7, Level: 2}}

	data, err := encodeRecord(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.SSTable.ID != 7 || decoded.SSTable.Level != 2 {
		t.Errorf("SSTable = %+v, want ID=7 Level=2", decoded.SSTable)
	}
}

func TestEncoding_Snapshot(t *testing.T) {
	tables := []SSTableInfo{
		{ID: 1, Level: 0, FirstKey: []byte("a"), LastKey: []byte("m")},
		{ID: 2, Level: 1, FirstKey: []byte("n"), LastKey: []byte("z")},
	}
	rec := Record{Type: typeSnapshot, SSTables: tables}

	data, err := encodeRecord(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.SSTables) != 2 {
		t.Fatalf("count = %d, want 2", len(decoded.SSTables))
	}
	if decoded.SSTables[0].ID != 1 || decoded.SSTables[1].ID != 2 {
		t.Errorf("IDs = [%d, %d], want [1, 2]", decoded.SSTables[0].ID, decoded.SSTables[1].ID)
	}
}

func TestEncoding_NextIDs(t *testing.T) {
	rec := Record{Type: typeNextIDs, NextMemID: 99, NextSeq: 12345}

	data, err := encodeRecord(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, _, err := decodeRecord(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.NextMemID != 99 || decoded.NextSeq != 12345 {
		t.Errorf("IDs = (%d, %d), want (99, 12345)", decoded.NextMemID, decoded.NextSeq)
	}
}

func TestEncoding_UnknownType(t *testing.T) {
	_, err := encodeRecord(Record{Type: 255})
	if err == nil {
		t.Fatal("expected error for unknown record type")
	}
}
