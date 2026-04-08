package hintedhandoff

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/iterator"
)

func testStoreConfig(dir string) StoreConfig {
	return StoreConfig{
		Dir:      dir,
		MaxBytes: 1024 * 1024, // 1MB for tests
		HintTTL:  time.Hour,
	}
}

func TestStore_AddAndIterate(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	ts := hlc.Timestamp{WallTime: 1000, Logical: 1, NodeID: "n1"}
	envelope := []byte("envelope-data")

	if err := s.Add("target-1", []byte("key-a"), envelope, ts); err != nil {
		t.Fatalf("Add: %v", err)
	}

	iter := s.Iterate("target-1")
	defer iter.Close()

	if !iter.IsValid() {
		t.Fatal("expected at least one hint")
	}

	gotValue := iter.Value()
	if string(gotValue) != string(envelope) {
		t.Errorf("value = %q, want %q", gotValue, envelope)
	}

	iter.Next()
	if iter.IsValid() {
		t.Error("expected exactly one hint")
	}
}

func TestStore_PrefixIsolation(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	ts1 := hlc.Timestamp{WallTime: 1000, Logical: 1}
	ts2 := hlc.Timestamp{WallTime: 2000, Logical: 1}

	s.Add("node-A", []byte("k1"), []byte("env-A1"), ts1)
	s.Add("node-A", []byte("k2"), []byte("env-A2"), ts2)
	s.Add("node-B", []byte("k1"), []byte("env-B1"), ts1)

	countA := iterCount(t, s.Iterate("node-A"))
	countB := iterCount(t, s.Iterate("node-B"))

	if countA != 2 {
		t.Errorf("node-A hints = %d, want 2", countA)
	}
	if countB != 1 {
		t.Errorf("node-B hints = %d, want 1", countB)
	}
}

func TestStore_CapacityEnforcement(t *testing.T) {
	dir := t.TempDir()
	cfg := testStoreConfig(dir)
	cfg.MaxBytes = 100 // very small cap

	s, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	ts := hlc.Timestamp{WallTime: 1000, Logical: 1}
	bigEnv := make([]byte, 60)

	if err := s.Add("t1", []byte("k1"), bigEnv, ts); err != nil {
		t.Fatalf("first Add should succeed: %v", err)
	}

	err = s.Add("t1", []byte("k2"), bigEnv, hlc.Timestamp{WallTime: 2000, Logical: 1})
	if err != ErrCapacityExceeded {
		t.Fatalf("second Add: got %v, want ErrCapacityExceeded", err)
	}
}

func TestStore_ConcurrentCapacity(t *testing.T) {
	dir := t.TempDir()
	cfg := testStoreConfig(dir)
	cfg.MaxBytes = 500

	s, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	env := make([]byte, 50)
	var wg sync.WaitGroup
	var succeeded, rejected int64
	var mu sync.Mutex

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts := hlc.Timestamp{WallTime: int64(i), Logical: 1}
			err := s.Add("target", []byte(fmt.Sprintf("k%d", i)), env, ts)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				succeeded++
			} else {
				rejected++
			}
		}(i)
	}
	wg.Wait()

	// 500 / 50 = 10 max hints
	if succeeded > 10 {
		t.Errorf("succeeded = %d, expected <= 10 (cap = 500, env = 50)", succeeded)
	}
	if succeeded+rejected != 20 {
		t.Errorf("succeeded(%d) + rejected(%d) != 20", succeeded, rejected)
	}

	// Logical size must not exceed cap
	if s.LogicalSize() > cfg.MaxBytes {
		t.Errorf("logical size %d exceeds cap %d", s.LogicalSize(), cfg.MaxBytes)
	}
}

func TestStore_Remove(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	ts := hlc.Timestamp{WallTime: 1000, Logical: 1}
	env := []byte("test-envelope")

	s.Add("target-1", []byte("key-a"), env, ts)

	sizeBefore := s.LogicalSize()
	if sizeBefore != int64(len(env)) {
		t.Fatalf("size before = %d, want %d", sizeBefore, len(env))
	}

	// Iterate to get the hint key
	iter := s.Iterate("target-1")
	if !iter.IsValid() {
		iter.Close()
		t.Fatal("expected hint")
	}
	hintKey := make([]byte, len(iter.Key()))
	copy(hintKey, iter.Key())
	envSize := int64(len(iter.Value()))
	iter.Close()

	if err := s.Remove(hintKey, envSize); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if s.LogicalSize() != 0 {
		t.Errorf("size after remove = %d, want 0", s.LogicalSize())
	}

	// Verify hint is gone
	iter2 := s.Iterate("target-1")
	defer iter2.Close()
	if iter2.IsValid() {
		t.Error("hint should be deleted")
	}
}

func TestStore_TargetIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	ts := hlc.Timestamp{WallTime: 1000, Logical: 1}

	s.Add("node-A", []byte("k1"), []byte("e1"), ts)
	s.Add("node-B", []byte("k2"), []byte("e2"), ts)

	targets := s.Targets()
	if len(targets) != 2 {
		t.Fatalf("targets = %v, want 2 entries", targets)
	}

	has := make(map[string]bool)
	for _, id := range targets {
		has[id] = true
	}
	if !has["node-A"] || !has["node-B"] {
		t.Errorf("targets = %v, want node-A and node-B", targets)
	}

	s.RemoveTarget("node-A")
	targets = s.Targets()
	if len(targets) != 1 || targets[0] != "node-B" {
		t.Errorf("after RemoveTarget: targets = %v", targets)
	}
}

func TestStore_KeyEncoding(t *testing.T) {
	ts := hlc.Timestamp{WallTime: 1234567890, Logical: 42, NodeID: "us-east/1"}
	userKey := []byte("my-key")

	hintKey := encodeHintKey("us-east/1", ts, userKey)

	gotNodeID := extractNodeID(hintKey)
	if gotNodeID != "us-east/1" {
		t.Errorf("extractNodeID = %q, want %q", gotNodeID, "us-east/1")
	}

	gotTS := ExtractTimestamp(hintKey)
	if gotTS.WallTime != ts.WallTime || gotTS.Logical != ts.Logical {
		t.Errorf("ExtractTimestamp = %+v, want walltime=%d logical=%d", gotTS, ts.WallTime, ts.Logical)
	}

	gotUserKey := ExtractUserKey(hintKey)
	if string(gotUserKey) != string(userKey) {
		t.Errorf("ExtractUserKey = %q, want %q", gotUserKey, userKey)
	}
}

func TestStore_StartupRecoversSize(t *testing.T) {
	dir := t.TempDir()
	cfg := testStoreConfig(dir)

	s1, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	ts := hlc.Timestamp{WallTime: 1000, Logical: 1}
	env := []byte("persistent-data")
	s1.Add("target-1", []byte("k1"), env, ts)
	s1.Add("target-1", []byte("k2"), env, hlc.Timestamp{WallTime: 2000, Logical: 1})
	s1.Close()

	// Reopen — should recover size and targets
	s2, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore reopen: %v", err)
	}
	defer s2.Close()

	expectedSize := int64(len(env)) * 2
	if s2.LogicalSize() != expectedSize {
		t.Errorf("recovered size = %d, want %d", s2.LogicalSize(), expectedSize)
	}

	targets := s2.Targets()
	if len(targets) != 1 || targets[0] != "target-1" {
		t.Errorf("recovered targets = %v, want [target-1]", targets)
	}
}

// --- helpers ---

func iterCount(t *testing.T, iter iterator.Iterator) int {
	t.Helper()
	defer iter.Close()
	n := 0
	for iter.IsValid() {
		n++
		iter.Next()
	}
	return n
}
