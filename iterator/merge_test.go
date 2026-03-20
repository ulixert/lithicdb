package iterator

import (
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

// sliceIterator is a simple test iterator over a fixed list of entries
// using internal keys.
type sliceIterator struct {
	entries []sliceEntry
	pos     int
}

type sliceEntry struct {
	key   []byte // internal key
	value []byte
}

func newSliceIterator(entries []sliceEntry) *sliceIterator {
	return &sliceIterator{entries: entries}
}

func (s *sliceIterator) Key() []byte   { return s.entries[s.pos].key }
func (s *sliceIterator) Value() []byte { return s.entries[s.pos].value }
func (s *sliceIterator) IsValid() bool { return s.pos < len(s.entries) }
func (s *sliceIterator) Next()         { s.pos++ }
func (s *sliceIterator) Err() error    { return nil }
func (s *sliceIterator) Close() error  { return nil }

// ikey builds an internal key for testing.
func ikey(userKey string, seq uint64) []byte {
	return kv.MakeInternalKey([]byte(userKey), seq)
}

func TestMergeIterator_SingleIterator(t *testing.T) {
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 1), value: []byte("1")},
			{key: ikey("b", 2), value: []byte("2")},
			{key: ikey("c", 3), value: []byte("3")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 1, value: "1"},
		{userKey: "b", seq: 2, value: "2"},
		{userKey: "c", seq: 3, value: "3"},
	})
}

func TestMergeIterator_TwoIterators_NoOverlap(t *testing.T) {
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 3), value: []byte("1")},
			{key: ikey("c", 4), value: []byte("3")},
		}),
		newSliceIterator([]sliceEntry{
			{key: ikey("b", 1), value: []byte("2")},
			{key: ikey("d", 2), value: []byte("4")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 3, value: "1"},
		{userKey: "b", seq: 1, value: "2"},
		{userKey: "c", seq: 4, value: "3"},
		{userKey: "d", seq: 2, value: "4"},
	})
}

func TestMergeIterator_DuplicateKeys_AllVersionsEmitted(t *testing.T) {
	// Both iterators have "b" — raw merge emits both versions
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("new-a")},
			{key: ikey("b", 9), value: []byte("new-b")},
		}),
		newSliceIterator([]sliceEntry{
			{key: ikey("b", 5), value: []byte("old-b")},
			{key: ikey("c", 3), value: []byte("old-c")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 10, value: "new-a"},
		{userKey: "b", seq: 9, value: "new-b"},
		{userKey: "b", seq: 5, value: "old-b"},
		{userKey: "c", seq: 3, value: "old-c"},
	})
}

func TestMergeIterator_SameUserKey_MultipleVersions(t *testing.T) {
	// Three iterators all have "a" — raw merge emits all three versions
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 30), value: []byte("newest")},
		}),
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 20), value: []byte("middle")},
			{key: ikey("b", 15), value: []byte("middle-b")},
		}),
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 10), value: []byte("oldest")},
			{key: ikey("b", 5), value: []byte("oldest-b")},
			{key: ikey("c", 1), value: []byte("oldest-c")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 30, value: "newest"},
		{userKey: "a", seq: 20, value: "middle"},
		{userKey: "a", seq: 10, value: "oldest"},
		{userKey: "b", seq: 15, value: "middle-b"},
		{userKey: "b", seq: 5, value: "oldest-b"},
		{userKey: "c", seq: 1, value: "oldest-c"},
	})
}

func TestMergeIterator_Empty(t *testing.T) {
	it := NewMergeIterator([]Iterator{})
	defer it.Close()

	if it.IsValid() {
		t.Error("expected empty iterator")
	}
}

func TestMergeIterator_AllEmpty(t *testing.T) {
	it := NewMergeIterator([]Iterator{
		newSliceIterator(nil),
		newSliceIterator(nil),
	})
	defer it.Close()

	if it.IsValid() {
		t.Error("expected empty iterator")
	}
}

func TestMergeIterator_OneEmptyOneNot(t *testing.T) {
	it := NewMergeIterator([]Iterator{
		newSliceIterator(nil),
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 1), value: []byte("1")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 1, value: "1"},
	})
}

func TestMergeIterator_ManyVersionsOfSameKey(t *testing.T) {
	// 10 iterators, each with one version of "same" — all emitted
	iters := make([]Iterator, 10)
	for i := 0; i < 10; i++ {
		iters[i] = newSliceIterator([]sliceEntry{
			{key: ikey("same", uint64(100-i)), value: []byte{byte('0' + i)}},
		})
	}

	it := NewMergeIterator(iters)
	defer it.Close()

	count := 0
	for it.IsValid() {
		gotUser := string(kv.UserKey(it.Key()))
		if gotUser != "same" {
			t.Errorf("entry %d: user key = %q, want %q", count, gotUser, "same")
		}
		count++
		it.Next()
	}
	if count != 10 {
		t.Errorf("got %d entries, want 10", count)
	}
}

func TestMergeIterator_CloseClosesAll(t *testing.T) {
	s1 := newSliceIterator([]sliceEntry{{key: ikey("a", 2), value: []byte("1")}})
	s2 := newSliceIterator([]sliceEntry{{key: ikey("b", 1), value: []byte("2")}})

	it := NewMergeIterator([]Iterator{s1, s2})
	if err := it.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	if it.IsValid() {
		t.Error("expected invalid after Close")
	}
}

func TestMergeIterator_MultipleVersionsInSameIterator(t *testing.T) {
	// A single memtable iterator with multiple versions of the same
	// user key — raw merge emits all of them in order.
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 3), value: []byte("v3")},
			{key: ikey("a", 2), value: []byte("v2")},
			{key: ikey("a", 1), value: []byte("v1")},
			{key: ikey("b", 4), value: []byte("b-val")},
		}),
	})
	defer it.Close()

	assertEntries(t, it, []expectedEntry{
		{userKey: "a", seq: 3, value: "v3"},
		{userKey: "a", seq: 2, value: "v2"},
		{userKey: "a", seq: 1, value: "v1"},
		{userKey: "b", seq: 4, value: "b-val"},
	})
}

// --- helpers ---

type expectedEntry struct {
	userKey string
	seq     uint64
	value   string
}

func assertEntries(t *testing.T, it *MergeIterator, expected []expectedEntry) {
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
