package compaction

import (
	"testing"

	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/sstable"
)

// testIDAlloc is a simple ID allocator for tests.
type testIDAlloc struct {
	next uint64
}

func (a *testIDAlloc) NextID() uint64 {
	a.next++
	return a.next
}

// buildSSTable creates an SSTable from the given entries. Each entry
// is an (internalKey, value) pair where value is nil for tombstones.
func buildSSTable(t *testing.T, dir string, id uint64, entries []testEntry) *sstable.TableHandle {
	t.Helper()

	b := sstable.NewBuilder(dir, id, 4096)
	for _, e := range entries {
		if e.tombstone {
			if err := b.Add(e.key, kv.NewTombstone()); err != nil {
				t.Fatalf("add tombstone: %v", err)
			}
		} else {
			if err := b.Add(e.key, kv.NewValue(e.value)); err != nil {
				t.Fatalf("add entry: %v", err)
			}
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("finish SSTable %d: %v", id, err)
	}

	reader, err := sstable.OpenReader(dir, id)
	if err != nil {
		t.Fatalf("open SSTable %d: %v", id, err)
	}

	return sstable.NewTableHandle(reader, dir)
}

type testEntry struct {
	key       []byte
	value     []byte
	tombstone bool
}

func ikey(userKey string, seq uint64) []byte {
	return kv.MakeInternalKey([]byte(userKey), seq)
}

// collectCompactionOutput runs compaction and returns the entries from
// the output SSTables as (userKey, seq, value, tombstone) tuples.
type outputEntry struct {
	userKey   string
	seq       uint64
	value     string
	tombstone bool
}

func collectOutput(t *testing.T, result *CompactionResult) []outputEntry {
	t.Helper()

	var entries []outputEntry
	for _, reader := range result.NewTables {
		iter := reader.Scan()
		for iter.IsValid() {
			uk := string(kv.UserKey(iter.Key()))
			seq := kv.SeqNum(iter.Key())
			val := iter.Value()
			if val == nil {
				entries = append(entries, outputEntry{userKey: uk, seq: seq, tombstone: true})
			} else {
				entries = append(entries, outputEntry{userKey: uk, seq: seq, value: string(val)})
			}
			iter.Next()
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("iterate output: %v", err)
		}
		iter.Close()
	}
	return entries
}

func TestExecute_KeepsNewestPerKey(t *testing.T) {
	dir := t.TempDir()

	// Two versions of "a": seq=10 (newest) and seq=5 (oldest).
	// With watermark=0 (no snapshots), only newest is kept.
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), value: []byte("v10")},
		{key: ikey("a", 5), value: []byte("v5")},
		{key: ikey("b", 8), value: []byte("b8")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	assertOutput(t, entries[0], "a", 10, "v10", false)
	assertOutput(t, entries[1], "b", 8, "b8", false)
}

func TestExecute_KeepsVersionsAboveWatermark(t *testing.T) {
	dir := t.TempDir()

	// "a" has seq=10, 7, 3. Watermark=6.
	// seq=10: newest → keep
	// seq=7: >= watermark → keep
	// seq=3: < watermark, first below → keep (one below watermark)
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), value: []byte("v10")},
		{key: ikey("a", 7), value: []byte("v7")},
		{key: ikey("a", 3), value: []byte("v3")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 6)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 10, "v10", false)
	assertOutput(t, entries[1], "a", 7, "v7", false)
	assertOutput(t, entries[2], "a", 3, "v3", false)
}

func TestExecute_DropsOldVersionsBelowWatermark(t *testing.T) {
	dir := t.TempDir()

	// "a" has seq=10, 7, 3, 1. Watermark=6.
	// seq=10: newest → keep
	// seq=7: >= watermark → keep
	// seq=3: < watermark, first below → keep
	// seq=1: < watermark, already kept one below → DROP
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), value: []byte("v10")},
		{key: ikey("a", 7), value: []byte("v7")},
		{key: ikey("a", 3), value: []byte("v3")},
		{key: ikey("a", 1), value: []byte("v1")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 6)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 10, "v10", false)
	assertOutput(t, entries[1], "a", 7, "v7", false)
	assertOutput(t, entries[2], "a", 3, "v3", false)
}

func TestExecute_DropsTombstoneAtBottommost(t *testing.T) {
	dir := t.TempDir()

	// Tombstone for "a" at seq=5. Watermark=0, bottommost.
	// Tombstone should be dropped (no data below, no snapshots).
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 5), tombstone: true},
		{key: ikey("b", 3), value: []byte("b3")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "b", 3, "b3", false)
}

func TestExecute_KeepsTombstoneAboveWatermark(t *testing.T) {
	dir := t.TempDir()

	// Tombstone for "a" at seq=10. Watermark=6, bottommost.
	// seq=10 >= watermark → keep (a snapshot might need it).
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), tombstone: true},
		{key: ikey("b", 3), value: []byte("b3")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 6)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 10, "", true)
	assertOutput(t, entries[1], "b", 3, "b3", false)
}

func TestExecute_KeepsTombstoneNonBottommost(t *testing.T) {
	dir := t.TempDir()

	// Tombstone for "a" at seq=3. Watermark=0, NOT bottommost.
	// Even with no snapshots, tombstone must be kept because older
	// data might exist in lower levels.
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 3), tombstone: true},
		{key: ikey("b", 2), value: []byte("b2")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: false, // NOT bottommost
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 3, "", true)
	assertOutput(t, entries[1], "b", 2, "b2", false)
}

func TestExecute_ZeroWatermark_DropsAllOldVersions(t *testing.T) {
	dir := t.TempDir()

	// No snapshots (watermark=0). All old versions should be dropped.
	h := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), value: []byte("newest")},
		{key: ikey("a", 5), value: []byte("old")},
		{key: ikey("a", 1), value: []byte("oldest")},
		{key: ikey("b", 8), value: []byte("b-newest")},
		{key: ikey("b", 2), value: []byte("b-old")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h},
		IsBottommost: true,
	}

	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 10, "newest", false)
	assertOutput(t, entries[1], "b", 8, "b-newest", false)
}

func TestExecute_MultipleInputFiles(t *testing.T) {
	dir := t.TempDir()

	// Two input SSTables with overlapping keys.
	h1 := buildSSTable(t, dir, 1, []testEntry{
		{key: ikey("a", 10), value: []byte("new-a")},
		{key: ikey("c", 8), value: []byte("new-c")},
	})
	h2 := buildSSTable(t, dir, 2, []testEntry{
		{key: ikey("a", 3), value: []byte("old-a")},
		{key: ikey("b", 5), value: []byte("b5")},
		{key: ikey("c", 2), value: []byte("old-c")},
	})

	task := &CompactionTask{
		InputLevel:   0,
		OutputLevel:  1,
		Inputs:       []*sstable.TableHandle{h1},
		Overlapping:  []*sstable.TableHandle{h2},
		IsBottommost: true,
	}

	// Watermark=0: only newest per key survives.
	result, err := Execute(task, dir, 4096, 1<<20, &testIDAlloc{next: 100}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectOutput(t, result)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(entries), entries)
	}
	assertOutput(t, entries[0], "a", 10, "new-a", false)
	assertOutput(t, entries[1], "b", 5, "b5", false)
	assertOutput(t, entries[2], "c", 8, "new-c", false)
}

func assertOutput(t *testing.T, got outputEntry, wantKey string, wantSeq uint64, wantValue string, wantTombstone bool) {
	t.Helper()
	if got.userKey != wantKey {
		t.Errorf("userKey = %q, want %q", got.userKey, wantKey)
	}
	if got.seq != wantSeq {
		t.Errorf("seq = %d, want %d (key=%q)", got.seq, wantSeq, wantKey)
	}
	if got.tombstone != wantTombstone {
		t.Errorf("tombstone = %v, want %v (key=%q)", got.tombstone, wantTombstone, wantKey)
	}
	if !wantTombstone && got.value != wantValue {
		t.Errorf("value = %q, want %q (key=%q)", got.value, wantValue, wantKey)
	}
}
