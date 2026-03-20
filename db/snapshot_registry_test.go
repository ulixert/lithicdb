package db

import "testing"

func TestRegistry_RegisterDeregister(t *testing.T) {
	var r snapshotRegistry

	r.Register(10)
	r.Register(20)
	r.Register(30)
	if r.Len() != 3 {
		t.Fatalf("Len = %d, want 3", r.Len())
	}

	r.Deregister(20)
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}

	oldest, ok := r.OldestSeq()
	if !ok || oldest != 10 {
		t.Errorf("OldestSeq = (%d, %v), want (10, true)", oldest, ok)
	}
}

func TestRegistry_OldestSeq(t *testing.T) {
	var r snapshotRegistry

	r.Register(30)
	r.Register(10)
	r.Register(20)

	oldest, ok := r.OldestSeq()
	if !ok || oldest != 10 {
		t.Errorf("OldestSeq = (%d, %v), want (10, true)", oldest, ok)
	}

	r.Deregister(10)
	oldest, ok = r.OldestSeq()
	if !ok || oldest != 20 {
		t.Errorf("after removing 10: OldestSeq = (%d, %v), want (20, true)", oldest, ok)
	}
}

func TestRegistry_Empty(t *testing.T) {
	var r snapshotRegistry

	_, ok := r.OldestSeq()
	if ok {
		t.Error("expected OldestSeq to return false for empty registry")
	}

	if r.Len() != 0 {
		t.Errorf("Len = %d, want 0", r.Len())
	}
}

func TestRegistry_DeregisterMiddle(t *testing.T) {
	var r snapshotRegistry

	r.Register(5)
	r.Register(15)
	r.Register(25)

	// Removing 15 (middle) should not change oldest.
	r.Deregister(15)
	oldest, ok := r.OldestSeq()
	if !ok || oldest != 5 {
		t.Errorf("OldestSeq = (%d, %v), want (5, true)", oldest, ok)
	}
	if r.Len() != 2 {
		t.Errorf("Len = %d, want 2", r.Len())
	}
}

func TestRegistry_DuplicateSeq(t *testing.T) {
	var r snapshotRegistry

	// Two snapshots at the same seq (concurrent GetSnapshot before any writes).
	r.Register(10)
	r.Register(10)

	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}

	// Deregister one copy — the other should remain.
	r.Deregister(10)
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}

	oldest, ok := r.OldestSeq()
	if !ok || oldest != 10 {
		t.Errorf("OldestSeq = (%d, %v), want (10, true)", oldest, ok)
	}

	r.Deregister(10)
	if r.Len() != 0 {
		t.Fatalf("Len = %d, want 0", r.Len())
	}

	_, ok = r.OldestSeq()
	if ok {
		t.Error("expected OldestSeq to return false after all deregistered")
	}
}

func TestRegistry_DeregisterNonexistent(t *testing.T) {
	var r snapshotRegistry

	r.Register(10)
	r.Deregister(999) // should be a no-op

	if r.Len() != 1 {
		t.Errorf("Len = %d, want 1", r.Len())
	}
}

func TestRegistry_DeregisterAll(t *testing.T) {
	var r snapshotRegistry

	r.Register(1)
	r.Register(2)
	r.Register(3)
	r.Deregister(1)
	r.Deregister(2)
	r.Deregister(3)

	_, ok := r.OldestSeq()
	if ok {
		t.Error("expected empty after deregistering all")
	}
}
