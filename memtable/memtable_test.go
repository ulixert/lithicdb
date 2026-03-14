package memtable

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ulixert/lithicdb/iterator/itertest"
	"github.com/ulixert/lithicdb/kv"
)

// Helper: put a user key with a given seq number
func put(m *Memtable, key string, val string, seq uint64) {
	ikey := kv.MakeInternalKey([]byte(key), seq)
	m.Put(ikey, kv.NewValue([]byte(val)))
}

// Helper: delete a user key with a given seq number
func del(m *Memtable, key string, seq uint64) {
	ikey := kv.MakeInternalKey([]byte(key), seq)
	m.Put(ikey, kv.NewTombstone())
}

func TestMemtable_PutAndGet(t *testing.T) {
	m := New(1)
	put(m, "hello", "world", 1)

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
	put(m, "a", "1", 1)

	_, found := m.Get([]byte("missing"))
	if found {
		t.Fatal("expected key to not be found")
	}
}

func TestMemtable_Overwrite(t *testing.T) {
	m := New(1)
	put(m, "key", "first", 1)
	put(m, "key", "second", 2) // newer seq

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
	put(m, "key", "value", 1)
	del(m, "key", 2)

	val, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected key to be found (as tombstone)")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone, got regular value")
	}
}

func TestMemtable_DeleteNonExistent(t *testing.T) {
	m := New(1)
	del(m, "ghost", 1)

	val, found := m.Get([]byte("ghost"))
	if !found {
		t.Fatal("expected tombstone to be found")
	}
	if !val.Tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestMemtable_PutAfterDelete(t *testing.T) {
	m := New(1)
	put(m, "key", "v1", 1)
	del(m, "key", 2)
	put(m, "key", "v2", 3)

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
	// Insert out of order — scan returns internal keys sorted by
	// user key ascending, then seq descending
	put(m, "cherry", "3", 3)
	put(m, "apple", "1", 1)
	put(m, "banana", "2", 2)

	iter := m.Scan()
	defer iter.Close()

	// Verify user keys are in sorted order
	var userKeys []string
	for iter.IsValid() {
		userKeys = append(userKeys, string(kv.UserKey(iter.Key())))
		iter.Next()
	}

	if len(userKeys) != 3 {
		t.Fatalf("got %d entries, want 3", len(userKeys))
	}
	if userKeys[0] != "apple" || userKeys[1] != "banana" || userKeys[2] != "cherry" {
		t.Errorf("user keys = %v, want [apple banana cherry]", userKeys)
	}
}

func TestMemtable_Scan_MultipleVersions(t *testing.T) {
	m := New(1)
	put(m, "key", "v1", 1)
	put(m, "key", "v2", 2)
	put(m, "key", "v3", 3)

	iter := m.Scan()
	defer iter.Close()

	// All three versions should appear, newest (seq 3) first
	var seqs []uint64
	for iter.IsValid() {
		seqs = append(seqs, kv.SeqNum(iter.Key()))
		iter.Next()
	}

	if len(seqs) != 3 {
		t.Fatalf("got %d entries, want 3", len(seqs))
	}
	// Newest first for the same user key
	if seqs[0] != 3 || seqs[1] != 2 || seqs[2] != 1 {
		t.Errorf("seqs = %v, want [3 2 1]", seqs)
	}
}

func TestMemtable_Scan_Empty(t *testing.T) {
	m := New(1)
	itertest.AssertEmpty(t, m.Scan())
}

func TestMemtable_ScanRange(t *testing.T) {
	m := New(1)
	put(m, "a", "1", 1)
	put(m, "b", "2", 2)
	put(m, "c", "3", 3)
	put(m, "d", "4", 4)
	put(m, "e", "5", 5)

	// [b, d) should include b, c
	iter := m.ScanRange([]byte("b"), []byte("d"))
	defer iter.Close()

	var userKeys []string
	for iter.IsValid() {
		userKeys = append(userKeys, string(kv.UserKey(iter.Key())))
		iter.Next()
	}

	if len(userKeys) != 2 {
		t.Fatalf("got %d entries, want 2", len(userKeys))
	}
	if userKeys[0] != "b" || userKeys[1] != "c" {
		t.Errorf("user keys = %v, want [b c]", userKeys)
	}
}

func TestMemtable_ScanRange_NilBounds(t *testing.T) {
	m := New(1)
	put(m, "a", "1", 1)
	put(m, "b", "2", 2)
	put(m, "c", "3", 3)

	// nil start
	iter1 := m.ScanRange(nil, []byte("b"))
	defer iter1.Close()
	count1 := 0
	for iter1.IsValid() {
		count1++
		iter1.Next()
	}
	if count1 != 1 { // only "a"
		t.Errorf("nil start: got %d, want 1", count1)
	}

	// nil end
	iter2 := m.ScanRange([]byte("b"), nil)
	defer iter2.Close()
	count2 := 0
	for iter2.IsValid() {
		count2++
		iter2.Next()
	}
	if count2 != 2 { // "b", "c"
		t.Errorf("nil end: got %d, want 2", count2)
	}
}

func TestMemtable_ApproximateSize(t *testing.T) {
	m := New(1)

	if m.ApproximateSize() != 0 {
		t.Errorf("empty memtable size = %d, want 0", m.ApproximateSize())
	}

	put(m, "key", "value", 1)
	size1 := m.ApproximateSize()
	if size1 <= 0 {
		t.Error("expected positive size after put")
	}

	put(m, "another", "entry", 2)
	size2 := m.ApproximateSize()
	if size2 <= size1 {
		t.Error("expected size to grow after second put")
	}
}

func TestMemtable_Len(t *testing.T) {
	m := New(1)
	if m.Len() != 0 {
		t.Errorf("empty memtable Len = %d, want 0", m.Len())
	}

	put(m, "a", "1", 1)
	put(m, "b", "2", 2)
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2", m.Len())
	}

	// "Overwrite" with new seq — this is a NEW entry, not a replacement
	put(m, "a", "updated", 3)
	if m.Len() != 3 {
		t.Errorf("Len after new version = %d, want 3", m.Len())
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
		put(m, key, val, uint64(i+1))
	}

	if m.Len() != n {
		t.Errorf("Len = %d, want %d", m.Len(), n)
	}

	// Verify all keys retrievable
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val, found := m.Get([]byte(key))
		if !found {
			t.Fatalf("key %q not found", key)
		}
		want := fmt.Sprintf("val-%04d", i)
		if string(val.Data) != want {
			t.Errorf("Get(%q) = %q, want %q", key, val.Data, want)
		}
	}
}

func TestMemtable_ConcurrentReadWrite(t *testing.T) {
	m := New(1)
	var wg sync.WaitGroup
	n := 100
	var seq atomic.Uint64

	// Concurrent writers
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("w%d-key-%04d", id, i)
				val := fmt.Sprintf("val-%04d", i)
				s := seq.Add(1)
				ikey := kv.MakeInternalKey([]byte(key), s)
				m.Put(ikey, kv.NewValue([]byte(val)))
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
				iter.Close()
			}
		}()
	}

	wg.Wait()

	// 4 writers × 100 keys = 400 entries
	if m.Len() != 4*n {
		t.Errorf("Len = %d, want %d", m.Len(), 4*n)
	}
}

func TestMemtable_EmptyValue(t *testing.T) {
	m := New(1)
	ikey := kv.MakeInternalKey([]byte("key"), 1)
	m.Put(ikey, kv.NewValue([]byte{}))

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
}

func TestMemtable_IteratorCloseIsIdempotent(t *testing.T) {
	m := New(1)
	put(m, "a", "1", 1)

	iter := m.Scan()
	if err := iter.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
