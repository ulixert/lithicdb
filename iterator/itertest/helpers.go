package itertest

import (
	"testing"

	"github.com/ulixert/theseon/iterator"
)

// Entry is a key-value pair used in test expectations.
type Entry struct {
	Key   string
	Value string
}

// AssertIterator walks the iterator and verifies it produces exactly
// the expected entries in order, then confirms it is exhausted with no error.
func AssertIterator(t *testing.T, iter iterator.Iterator, expected []Entry) {
	t.Helper()
	defer func() {
		if err := iter.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	}()

	i := 0
	for iter.IsValid() {
		if i >= len(expected) {
			t.Fatalf("iterator produced more entries than expected (%d >= %d)", i, len(expected))
		}

		gotKey := string(iter.Key())
		gotValue := string(iter.Value())

		if gotKey != expected[i].Key {
			t.Errorf("entry %d: key = %q, want %q", i, gotKey, expected[i].Key)
		}

		if gotValue != expected[i].Value {
			t.Errorf("entry %d: value = %q, want %q", i, gotValue, expected[i].Value)
		}

		iter.Next()
		i++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("iterator produced %d entries, want %d", i, len(expected))
	}
}

// AssertEmpty varifies that the iterator produces no entries and has no error.
func AssertEmpty(t *testing.T, iter iterator.Iterator) {
	t.Helper()
	AssertIterator(t, iter, nil)
}

// CollectKeys walks the iterator and returns all keys as strings.
func CollectKeys(t *testing.T, iter iterator.Iterator) []string {
	t.Helper()
	defer func() {
		if err := iter.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	}()

	var keys []string
	for iter.IsValid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	return keys
}
