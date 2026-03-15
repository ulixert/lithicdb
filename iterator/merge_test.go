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

	assertUserKeys(t, it, []string{"a", "b", "c"})
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

	assertUserKeys(t, it, []string{"a", "b", "c", "d"})
}

func TestMergeIterator_DuplicateKeys_NewestWins(t *testing.T) {
	// iters[0] has higher priority AND newer seq for "b"
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

	assertMergeEntries(t, it, []mergeExpected{
		{userKey: "a", value: "new-a"},
		{userKey: "b", value: "new-b"},
		{userKey: "c", value: "old-c"},
	})
}

func TestMergeIterator_SameUserKey_MultipleVersions(t *testing.T) {
	// Three iterators all have "a" — only the one from iters[0]
	// (highest priority) should appear
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

	assertMergeEntries(t, it, []mergeExpected{
		{userKey: "a", value: "newest"},
		{userKey: "b", value: "middle-b"},
		{userKey: "c", value: "oldest-c"},
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

	assertUserKeys(t, it, []string{"a"})
}

func TestMergeIterator_ManyDuplicatesOfSameKey(t *testing.T) {
	iters := make([]Iterator, 10)
	for i := 0; i < 10; i++ {
		iters[i] = newSliceIterator([]sliceEntry{
			{key: ikey("same", uint64(100-i)), value: []byte{byte('0' + i)}},
		})
	}

	it := NewMergeIterator(iters)
	defer it.Close()

	if !it.IsValid() {
		t.Fatal("expected valid")
	}

	gotUser := string(kv.UserKey(it.Key()))
	if gotUser != "same" {
		t.Errorf("user key = %q, want %q", gotUser, "same")
	}
	if string(it.Value()) != "0" {
		t.Errorf("value = %q, want %q", it.Value(), "0")
	}

	it.Next()
	if it.IsValid() {
		t.Error("expected exhausted after one user key")
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
	// A single memtable iterator might have multiple versions of the
	// same user key (different seq numbers). The merge iterator should
	// only emit the newest (first in sort order).
	it := NewMergeIterator([]Iterator{
		newSliceIterator([]sliceEntry{
			{key: ikey("a", 3), value: []byte("v3")},
			{key: ikey("a", 2), value: []byte("v2")},
			{key: ikey("a", 1), value: []byte("v1")},
			{key: ikey("b", 4), value: []byte("b-val")},
		}),
	})
	defer it.Close()

	assertMergeEntries(t, it, []mergeExpected{
		{userKey: "a", value: "v3"},
		{userKey: "b", value: "b-val"},
	})
}

// --- helpers ---

type mergeExpected struct {
	userKey string
	value   string
}

func assertMergeEntries(t *testing.T, it *MergeIterator, expected []mergeExpected) {
	t.Helper()

	for i, want := range expected {
		if !it.IsValid() {
			t.Fatalf("entry %d: iterator exhausted early", i)
		}

		gotUser := string(kv.UserKey(it.Key()))
		if gotUser != want.userKey {
			t.Errorf("entry %d: user key = %q, want %q", i, gotUser, want.userKey)
		}

		gotVal := string(it.Value())
		if gotVal != want.value {
			t.Errorf("entry %d: value = %q, want %q", i, gotVal, want.value)
		}

		it.Next()
	}

	if it.IsValid() {
		t.Errorf("iterator not exhausted: extra key %q", kv.UserKey(it.Key()))
	}
}

func assertUserKeys(t *testing.T, it *MergeIterator, expected []string) {
	t.Helper()

	for i, want := range expected {
		if !it.IsValid() {
			t.Fatalf("entry %d: iterator exhausted early", i)
		}

		got := string(kv.UserKey(it.Key()))
		if got != want {
			t.Errorf("entry %d: user key = %q, want %q", i, got, want)
		}

		it.Next()
	}

	if it.IsValid() {
		t.Errorf("iterator not exhausted: extra key %q", kv.UserKey(it.Key()))
	}
}
