package iterator

import (
	"slices"
	"testing"

	"github.com/ulixert/theseon/kv"
)

func sortedEntries(entries []WriteBufferEntry) []WriteBufferEntry {
	slices.SortFunc(entries, func(a, b WriteBufferEntry) int {
		return slices.Compare(a.Key, b.Key)
	})
	return entries
}

func TestWriteBufferIterator_Basic(t *testing.T) {
	entries := sortedEntries([]WriteBufferEntry{
		{Key: kv.MakeInternalKey([]byte("b"), 10), Value: kv.NewValue([]byte("b-val"))},
		{Key: kv.MakeInternalKey([]byte("a"), 10), Value: kv.NewValue([]byte("a-val"))},
		{Key: kv.MakeInternalKey([]byte("c"), 10), Value: kv.NewValue([]byte("c-val"))},
	})

	it := NewWriteBufferIterator(entries)

	// Should iterate in sorted key order: a, b, c.
	expected := []struct {
		userKey string
		value   string
	}{
		{"a", "a-val"},
		{"b", "b-val"},
		{"c", "c-val"},
	}

	for i, exp := range expected {
		if !it.IsValid() {
			t.Fatalf("entry %d: expected valid", i)
		}
		gotKey := string(kv.UserKey(it.Key()))
		if gotKey != exp.userKey {
			t.Errorf("entry %d: key = %q, want %q", i, gotKey, exp.userKey)
		}
		gotVal := string(it.Value())
		if gotVal != exp.value {
			t.Errorf("entry %d: value = %q, want %q", i, gotVal, exp.value)
		}
		it.Next()
	}

	if it.IsValid() {
		t.Error("expected exhausted")
	}
	if err := it.Err(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWriteBufferIterator_Empty(t *testing.T) {
	it := NewWriteBufferIterator(nil)

	if it.IsValid() {
		t.Error("expected invalid for empty iterator")
	}
	if err := it.Err(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWriteBufferIterator_Tombstone(t *testing.T) {
	entries := []WriteBufferEntry{
		{Key: kv.MakeInternalKey([]byte("x"), 5), Value: kv.NewTombstone()},
	}

	it := NewWriteBufferIterator(entries)

	if !it.IsValid() {
		t.Fatal("expected valid")
	}
	if string(kv.UserKey(it.Key())) != "x" {
		t.Errorf("key = %q, want x", kv.UserKey(it.Key()))
	}
	if it.Value() != nil {
		t.Errorf("tombstone value should be nil, got %q", it.Value())
	}
}

func TestWriteBufferIterator_Close(t *testing.T) {
	entries := []WriteBufferEntry{
		{Key: kv.MakeInternalKey([]byte("a"), 1), Value: kv.NewValue([]byte("v"))},
	}

	it := NewWriteBufferIterator(entries)
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if it.IsValid() {
		t.Error("should be invalid after close")
	}
}
