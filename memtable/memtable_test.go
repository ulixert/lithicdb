package memtable

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ulixert/lithicdb/itertest"
)

func TestMemtable_PutAndGet(t *testing.T) {
	m := New(1)
	m.Put([]byte("hello"), []byte("world"))

	val, found := m.Get([]byte("hello"))
	if !found {
		t.Fatal("expected key to be found")
	}

	if val.Tombstone {
		t.Fatal("expected non-tombstone value")
	}

	if string(val.Data) != "world" {
		t.Errorf("value = %q, want %q", val.Data, "world")
	}
}

func TestMemtable_GetMissing(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))

	_, found := m.Get([]byte("missing"))
	if found {
		t.Fatal("expected key to not be found")
	}
}

func TestMemtable_Overwrite(t *testing.T) {
	m := New(1)
	m.Put([]byte("key"), []byte("first"))
	m.Put([]byte("key"), []byte("second"))

	val, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected key to be found")
	}

	if string(val.Data) != "second" {
		t.Errorf("value = %q, want %q", val.Data, "second")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := New(1)
	m.Put([]byte("key"), []byte("value"))
	m.Delete([]byte("key"))

	val, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected key to be found (as tombstone)")
	}

	if !val.Tombstone {
		t.Fatal("expected tombstone, got regular value")
	}
}

func TestMemtable_PutAfterDelete(t *testing.T) {
	m := New(1)
	m.Put([]byte("key"), []byte("v1"))
	m.Delete([]byte("key"))
	m.Put([]byte("key"), []byte("v2"))

	val, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected key to be found")
	}

	if val.Tombstone {
		t.Fatal("expected non-tombstone after re-put")
	}

	if string(val.Data) != "v2" {
		t.Errorf("value = %q, want %q", val.Data, "v2")
	}
}

func TestMemtable_Scan_Sorted(t *testing.T) {
	m := New(1)
	// Insert out of order
	m.Put([]byte("cherry"), []byte("3"))
	m.Put([]byte("apple"), []byte("1"))
	m.Put([]byte("banana"), []byte("2"))

	itertest.AssertIterator(t, m.Scan(), []itertest.Entry{
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "2"},
		{Key: "cherry", Value: "3"},
	})
}

func TestMemtable_Scan_SingleEntry(t *testing.T) {
	m := New(1)
	m.Put([]byte("only"), []byte("one"))

	itertest.AssertIterator(t, m.Scan(), []itertest.Entry{
		{Key: "only", Value: "one"},
	})
}

func TestMemtable_Scan_TombstoneValue(t *testing.T) {
	m := New(1)
	m.Put([]byte("alive"), []byte("yes"))
	m.Delete([]byte("dead"))
	m.Put([]byte("exists"), []byte("yep"))

	iter := m.Scan()
	defer func() {
		if err := iter.Close(); err != nil {
			t.Errorf("iter.Close() error = %v", err)
		}
	}()

	// "alive" -> normal value
	if !iter.IsValid() {
		t.Fatal("expected valid entry")
	}

	if string(iter.Key()) != "alive" {
		t.Errorf("key = %q, want %q", iter.Key(), "alive")
	}

	if iter.Value() == nil {
		t.Error("expected non-nil value for alive key")
	}

	iter.Next()

	// "dead" -> tombstone (nil value)
	if !iter.IsValid() {
		t.Fatal("expected valid entry")
	}

	if string(iter.Key()) != "dead" {
		t.Errorf("key = %q, want %q", iter.Key(), "dead")
	}

	if iter.Value() != nil {
		t.Errorf("expected nil value for tombstone, got %q", iter.Value())
	}

	iter.Next()

	// "exists" -> normal value
	if !iter.IsValid() {
		t.Fatal("expected valid entry")
	}

	if string(iter.Key()) != "exists" {
		t.Errorf("key = %q, want %q", iter.Key(), "exists")
	}

	iter.Next()
	if iter.IsValid() {
		t.Error("expected iterator to be exhausted")
	}
}

func TestMemtable_ScanRange(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))
	m.Put([]byte("d"), []byte("4"))

	// [b, d) should return b, c
	itertest.AssertIterator(t, m.ScanRange([]byte("b"), []byte("d")), []itertest.Entry{
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
}

func TestMemtable_ScanRange_NilStart(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))

	// [nil, b) should return a
	itertest.AssertIterator(t, m.ScanRange(nil, []byte("b")), []itertest.Entry{
		{Key: "a", Value: "1"},
	})
}

func TestMemtable_ScanRange_NilEnd(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))

	// [b, nil) should return b, c
	itertest.AssertIterator(t, m.ScanRange([]byte("b"), nil), []itertest.Entry{
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
}

func TestMemtable_ScanRange_NilBoth(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))

	// [nil, nil) is equivalent to Scan
	itertest.AssertIterator(t, m.ScanRange(nil, nil), []itertest.Entry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	})
}

func TestMemtable_ScanRange_NoMatch(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("z"), []byte("26"))

	// [m, n) — no keys in this range
	itertest.AssertEmpty(t, m.ScanRange([]byte("m"), []byte("n")))
}

func TestMemtable_ScanRange_StartAfterAllKeys(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))

	itertest.AssertEmpty(t, m.ScanRange([]byte("z"), nil))
}

func TestMemtable_ApproximateSize(t *testing.T) {
	m := New(1)

	if m.ApproximateSize() != 0 {
		t.Errorf("empty memtable size = %d, want 0", m.ApproximateSize())
	}

	m.Put([]byte("key"), []byte("value"))
	size1 := m.ApproximateSize()
	if size1 <= 0 {
		t.Error("expected positive size after put")
	}

	m.Put([]byte("another"), []byte("entry"))
	size2 := m.ApproximateSize()
	if size2 <= size1 {
		t.Error("expected size to grow after second put")
	}
}

func TestMemtable_ApproximateSize_Overwrite(t *testing.T) {
	m := New(1)
	m.Put([]byte("key"), []byte("short"))
	size1 := m.ApproximateSize()

	m.Put([]byte("key"), []byte("a much longer value"))
	size2 := m.ApproximateSize()

	if size2 <= size1 {
		t.Error("expected size to grow after overwriting with longer value")
	}
}

func TestMemtable_Len(t *testing.T) {
	m := New(1)
	if m.Len() != 0 {
		t.Errorf("empty memtable Len = %d, want 0", m.Len())
	}

	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2", m.Len())
	}

	// Overwrite should not change Len
	m.Put([]byte("a"), []byte("updated"))
	if m.Len() != 2 {
		t.Errorf("Len after overwrite = %d, want 2", m.Len())
	}

	// Delete adds a tombstone entry (if key doesn't exist, still a new entry)
	m.Delete([]byte("c"))
	if m.Len() != 3 {
		t.Errorf("Len after delete of new key = %d, want 3", m.Len())
	}
}

func TestMemtable_Freeze(t *testing.T) {
	m := New(1)
	if m.IsFrozen() {
		t.Error("new memtable should not be frozen")
	}

	m.Freeze()
	if !m.IsFrozen() {
		t.Error("expected memtable to be frozen")
	}
}

func TestMemtable_ID(t *testing.T) {
	m := New(42)
	if m.ID() != 42 {
		t.Errorf("ID = %d, want 42", m.ID())
	}
}

func TestMemtable_ManyKeys(t *testing.T) {
	m := New(1)
	n := 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		m.Put([]byte(key), []byte(val))
	}

	if m.Len() != n {
		t.Errorf("Len = %d, want %d", m.Len(), n)
	}

	// Verify sorted order via scan
	keys := itertest.CollectKeys(t, m.Scan())
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("keys not sorted at index %d: %q <= %q", i, keys[i], keys[i-1])
		}
	}
}

func TestMemtable_ConcurrentReadWrite(t *testing.T) {
	m := New(1)
	var wg sync.WaitGroup
	n := 100

	// Concurrent writers
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("w%d-key-%04d", id, i)
				val := fmt.Sprintf("val-%04d", i)
				m.Put([]byte(key), []byte(val))
			}
		}(g)
	}

	// Concurrent readers
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				m.Get([]byte(fmt.Sprintf("w0-key-%04d", i)))
			}
		}()
	}

	// Concurrent scanners
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				iter := m.Scan()
				for iter.IsValid() {
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
				}

				if err := iter.Close(); err != nil {
					t.Errorf("Close() returned error: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// All 4 writes x 100 keys = 400 unique keys
	if m.Len() != 4*n {
		t.Errorf("Len = %d, want %d", m.Len(), 4*n)
	}
}

func TestMemtable_EmptyValue(t *testing.T) {
	m := New(1)
	// An empty value is NOT a tombstone
	m.Put([]byte("key"), []byte{})

	val, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected key to be found")
	}

	if val.Tombstone {
		t.Fatal("empty value should not be a tombstone")
	}

	if len(val.Data) != 0 {
		t.Errorf("expected empty data, got %q", val.Data)
	}

	// Verify iterator also distinguishes: empty value (non-nil) vs tombstone (nil)
	iter := m.Scan()
	defer func() {
		if err := iter.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	}()

	if !iter.IsValid() {
		t.Fatal("expected valid entry")
	}

	if iter.Value() == nil {
		t.Error("empty value should return non-nil []byte{}, not nil")
	}
}

func TestMemtable_IteratorCloseIsIdempotent(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("1"))

	iter := m.Scan()
	if err := iter.Close(); err != nil {
		t.Errorf("first Close() error: %v", err)
	}

	if err := iter.Close(); err != nil {
		t.Errorf("second Close() error: %v", err)
	}
}
