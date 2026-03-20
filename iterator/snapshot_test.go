package iterator

import (
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

func TestSnapshotIterator_FiltersAboveMaxSeq(t *testing.T) {
	// Entries with seq > maxSeq should be invisible.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("v10")},
			{key: ikey("a", 5), value: []byte("v5")},
			{key: ikey("a", 2), value: []byte("v2")},
		}),
	})

	it := NewSnapshotIterator(inner, 5)
	defer it.Close()

	assertSnapshotEntries(t, it, []snapshotExpected{
		{userKey: "a", seq: 5, value: "v5"},
	})
}

func TestSnapshotIterator_DeduplicatesUserKey(t *testing.T) {
	// Multiple visible versions of "a" — only newest visible emitted.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 8), value: []byte("v8")},
			{key: ikey("a", 3), value: []byte("v3")},
			{key: ikey("b", 7), value: []byte("b7")},
			{key: ikey("b", 1), value: []byte("b1")},
		}),
	})

	it := NewSnapshotIterator(inner, 10)
	defer it.Close()

	assertSnapshotEntries(t, it, []snapshotExpected{
		{userKey: "a", seq: 8, value: "v8"},
		{userKey: "b", seq: 7, value: "b7"},
	})
}

func TestSnapshotIterator_EmptyInner(t *testing.T) {
	inner := NewMergeIterator([]Iterator{})
	it := NewSnapshotIterator(inner, 100)
	defer it.Close()

	if it.IsValid() {
		t.Error("expected empty iterator")
	}
}

func TestSnapshotIterator_AllFilteredOut(t *testing.T) {
	// All entries have seq > maxSeq.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("v10")},
			{key: ikey("b", 20), value: []byte("v20")},
		}),
	})

	it := NewSnapshotIterator(inner, 5)
	defer it.Close()

	if it.IsValid() {
		t.Error("expected empty iterator when all entries above maxSeq")
	}
}

func TestSnapshotIterator_TombstonesEmitted(t *testing.T) {
	// Tombstones (nil value) with seq <= maxSeq should be emitted.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 5), value: nil},
			{key: ikey("b", 3), value: []byte("val")},
		}),
	})

	it := NewSnapshotIterator(inner, 10)
	defer it.Close()

	if !it.IsValid() {
		t.Fatal("expected valid")
	}
	if string(kv.UserKey(it.Key())) != "a" {
		t.Errorf("user key = %q, want %q", kv.UserKey(it.Key()), "a")
	}
	if it.Value() != nil {
		t.Errorf("value = %v, want nil (tombstone)", it.Value())
	}

	it.Next()
	if !it.IsValid() {
		t.Fatal("expected valid for b")
	}
	if string(kv.UserKey(it.Key())) != "b" {
		t.Errorf("user key = %q, want %q", kv.UserKey(it.Key()), "b")
	}

	it.Next()
	if it.IsValid() {
		t.Error("expected exhausted")
	}
}

func TestSnapshotIterator_SkipsNewerPicksOlder(t *testing.T) {
	// For key "a": seq=10 is too new, seq=7 is the newest visible.
	// For key "b": seq=6 is visible immediately.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("too-new")},
			{key: ikey("a", 7), value: []byte("visible")},
			{key: ikey("a", 3), value: []byte("older")},
			{key: ikey("b", 6), value: []byte("b-val")},
		}),
	})

	it := NewSnapshotIterator(inner, 8)
	defer it.Close()

	assertSnapshotEntries(t, it, []snapshotExpected{
		{userKey: "a", seq: 7, value: "visible"},
		{userKey: "b", seq: 6, value: "b-val"},
	})
}

func TestSnapshotIterator_MultipleIteratorSources(t *testing.T) {
	// Simulates memtable + SSTable with overlapping keys.
	inner := NewMergeIterator([]Iterator{
		// "memtable" — newer writes
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("mem-a")},
			{key: ikey("c", 8), value: []byte("mem-c")},
		}),
		// "sstable" — older writes
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 3), value: []byte("sst-a")},
			{key: ikey("b", 5), value: []byte("sst-b")},
			{key: ikey("c", 2), value: []byte("sst-c")},
		}),
	})

	// Snapshot at seq=6: sees mem-a(10) is too new, sst-a(3) is visible.
	// sst-b(5) visible. mem-c(8) too new, sst-c(2) visible.
	it := NewSnapshotIterator(inner, 6)
	defer it.Close()

	assertSnapshotEntries(t, it, []snapshotExpected{
		{userKey: "a", seq: 3, value: "sst-a"},
		{userKey: "b", seq: 5, value: "sst-b"},
		{userKey: "c", seq: 2, value: "sst-c"},
	})
}

func TestSnapshotIterator_MaxSeqShowsAll(t *testing.T) {
	// With maxSeq = MaxSeqNum, the snapshot iterator behaves like
	// the old dedup MergeIterator — newest version per user key.
	inner := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("newest-a")},
			{key: ikey("a", 5), value: []byte("old-a")},
			{key: ikey("b", 8), value: []byte("newest-b")},
		}),
	})

	it := NewSnapshotIterator(inner, kv.MaxSeqNum)
	defer it.Close()

	assertSnapshotEntries(t, it, []snapshotExpected{
		{userKey: "a", seq: 10, value: "newest-a"},
		{userKey: "b", seq: 8, value: "newest-b"},
	})
}

// --- helpers ---

type snapshotExpected struct {
	userKey string
	seq     uint64
	value   string
}

func assertSnapshotEntries(t *testing.T, it *SnapshotIterator, expected []snapshotExpected) {
	t.Helper()

	for i, want := range expected {
		if !it.IsValid() {
			t.Fatalf("entry %d: iterator exhausted early", i)
		}

		gotUser := string(kv.UserKey(it.Key()))
		if gotUser != want.userKey {
			t.Errorf("entry %d: user key = %q, want %q", i, gotUser, want.userKey)
		}

		gotSeq := kv.SeqNum(it.Key())
		if gotSeq != want.seq {
			t.Errorf("entry %d: seq = %d, want %d", i, gotSeq, want.seq)
		}

		gotVal := string(it.Value())
		if gotVal != want.value {
			t.Errorf("entry %d: value = %q, want %q", i, gotVal, want.value)
		}

		it.Next()
	}

	if it.IsValid() {
		t.Errorf("iterator not exhausted: extra key %q (seq %d)",
			kv.UserKey(it.Key()), kv.SeqNum(it.Key()))
	}
}
