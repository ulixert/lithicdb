package hlc

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

// mockClock returns a physical clock function that advances by 1ns
// each call, starting from the given value.
func mockClock(start int64) func() int64 {
	val := start
	return func() int64 {
		v := val
		val++
		return v
	}
}

// fixedClock returns a physical clock that always returns the same value.
func fixedClock(val int64) func() int64 {
	return func() int64 { return val }
}

func TestNow_Monotonicity(t *testing.T) {
	c := NewClock("node-1", mockClock(1000))

	var prev Timestamp
	for i := range 1000 {
		ts := c.Now()
		if i > 0 && !prev.Less(ts) {
			t.Fatalf("iteration %d: %+v not less than %+v", i, prev, ts)
		}
		prev = ts
	}
}

func TestNow_WallClockAdvanceResetsLogical(t *testing.T) {
	// mockClock advances by 1 each call, so every Now() sees a new
	// wall time and logical should always be 0.
	c := NewClock("node-1", mockClock(1000))

	for range 10 {
		ts := c.Now()
		if ts.Logical != 0 {
			t.Fatalf("expected logical=0 on wall clock advance, got %d", ts.Logical)
		}
	}
}

func TestNow_StaleWallClockIncrementsLogical(t *testing.T) {
	// Fixed clock — physical never advances. Logical must increment.
	c := NewClock("node-1", fixedClock(1000))

	for i := range 10 {
		ts := c.Now()
		if ts.WallTime != 1000 {
			t.Fatalf("iteration %d: expected WallTime=1000, got %d", i, ts.WallTime)
		}
		if ts.Logical != uint32(i) {
			t.Fatalf("iteration %d: expected Logical=%d, got %d", i, i, ts.Logical)
		}
	}
}

func TestNow_NodeIDSet(t *testing.T) {
	c := NewClock("test-node", mockClock(1000))
	ts := c.Now()
	if ts.NodeID != "test-node" {
		t.Fatalf("expected NodeID=%q, got %q", "test-node", ts.NodeID)
	}
}

func TestUpdate_FutureAdvancesClock(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))

	// Local clock is at 1000. Receive a timestamp from the future.
	remote := Timestamp{WallTime: 2000, Logical: 5, NodeID: "node-2"}
	if err := c.Update(remote); err != nil {
		t.Fatalf("Update: %v", err)
	}

	ts := c.Now()
	// The clock should have advanced to at least the remote's wall time.
	if ts.WallTime < 2000 {
		t.Fatalf("expected WallTime >= 2000, got %d", ts.WallTime)
	}
}

func TestUpdate_PastDoesNotRegress(t *testing.T) {
	c := NewClock("node-1", fixedClock(2000))

	// Generate a local timestamp at wall time 2000.
	before := c.Now()

	// Receive a timestamp from the past.
	remote := Timestamp{WallTime: 500, Logical: 0, NodeID: "node-2"}
	if err := c.Update(remote); err != nil {
		t.Fatalf("Update: %v", err)
	}

	after := c.Now()
	if !before.Less(after) {
		t.Fatalf("clock regressed: before=%+v, after=%+v", before, after)
	}
	if after.WallTime < before.WallTime {
		t.Fatalf("WallTime regressed: %d < %d", after.WallTime, before.WallTime)
	}
}

func TestUpdate_EqualWallTimeMergesLogical(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))

	// Advance local logical to 3.
	c.Now() // logical=0
	c.Now() // logical=1
	c.Now() // logical=2
	c.Now() // logical=3

	// Receive with same wall time but higher logical.
	remote := Timestamp{WallTime: 1000, Logical: 10, NodeID: "node-2"}
	if err := c.Update(remote); err != nil {
		t.Fatalf("Update: %v", err)
	}

	ts := c.Now()
	// Should be max(3, 10) + 1 = 11 from Update, then +1 from Now = 12.
	if ts.WallTime != 1000 {
		t.Fatalf("expected WallTime=1000, got %d", ts.WallTime)
	}
	if ts.Logical != 12 {
		t.Fatalf("expected Logical=12, got %d", ts.Logical)
	}
}

func TestConcurrentNow_UniqueAndOrdered(t *testing.T) {
	c := NewClock("node-1", nil) // real clock

	const goroutines = 8
	const perGoroutine = 200
	results := make([][]Timestamp, goroutines)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ts := make([]Timestamp, perGoroutine)
			for i := range perGoroutine {
				ts[i] = c.Now()
			}
			results[id] = ts
		}(g)
	}
	wg.Wait()

	// Collect all timestamps and check uniqueness.
	all := make([]Timestamp, 0, goroutines*perGoroutine)
	for _, r := range results {
		all = append(all, r...)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Less(all[j])
	})

	for i := 1; i < len(all); i++ {
		if all[i].Equal(all[i-1]) {
			t.Fatalf("duplicate timestamp at index %d: %+v", i, all[i])
		}
		if !all[i-1].Less(all[i]) {
			t.Fatalf("not strictly ordered at index %d: %+v vs %+v", i, all[i-1], all[i])
		}
	}
}

func TestEncodeDecode_Roundtrip(t *testing.T) {
	tests := []Timestamp{
		{WallTime: 0, Logical: 0, NodeID: ""},
		{WallTime: 1234567890, Logical: 42, NodeID: "node-1"},
		{WallTime: 1<<62 - 1, Logical: 1<<32 - 1, NodeID: "a-very-long-node-id-string"},
	}

	for _, ts := range tests {
		encoded, err := ts.Encode()
		if err != nil {
			t.Fatalf("Encode(%+v): %v", ts, err)
		}
		decoded, err := DecodeTimestamp(encoded)
		if err != nil {
			t.Fatalf("DecodeTimestamp(%+v): %v", ts, err)
		}
		if !ts.Equal(decoded) {
			t.Fatalf("roundtrip mismatch: %+v != %+v", ts, decoded)
		}
	}
}

func TestDecodeTimestamp_TooShort(t *testing.T) {
	_, err := DecodeTimestamp([]byte{1, 2, 3})
	if !errors.Is(err, ErrCorruptTimestamp) {
		t.Fatalf("expected ErrCorruptTimestamp, got %v", err)
	}
}

func TestDecodeTimestamp_TruncatedNodeID(t *testing.T) {
	ts := Timestamp{WallTime: 1000, Logical: 1, NodeID: "node-1"}
	encoded, err := ts.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Truncate the last byte of the NodeID.
	_, err = DecodeTimestamp(encoded[:len(encoded)-1])
	if !errors.Is(err, ErrCorruptTimestamp) {
		t.Fatalf("expected ErrCorruptTimestamp, got %v", err)
	}
}

func TestDecodeTimestamp_TrailingBytes(t *testing.T) {
	ts := Timestamp{WallTime: 1000, Logical: 1, NodeID: "node-1"}
	encoded, err := ts.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Append extra bytes.
	padded := append(encoded, 0xFF, 0xFF)
	_, err = DecodeTimestamp(padded)
	if !errors.Is(err, ErrCorruptTimestamp) {
		t.Fatalf("expected ErrCorruptTimestamp for trailing bytes, got %v", err)
	}
}

func TestEncode_NodeIDTooLong(t *testing.T) {
	ts := Timestamp{WallTime: 1000, Logical: 1, NodeID: string(make([]byte, 65536))}
	_, err := ts.Encode()
	if !errors.Is(err, ErrNodeIDTooLong) {
		t.Fatalf("expected ErrNodeIDTooLong, got %v", err)
	}
}

func TestLess_Ordering(t *testing.T) {
	tests := []struct {
		name string
		a, b Timestamp
		want bool
	}{
		{
			name: "WallTime dominates",
			a:    Timestamp{WallTime: 100, Logical: 99, NodeID: "z"},
			b:    Timestamp{WallTime: 200, Logical: 0, NodeID: "a"},
			want: true,
		},
		{
			name: "Same WallTime, Logical decides",
			a:    Timestamp{WallTime: 100, Logical: 5, NodeID: "z"},
			b:    Timestamp{WallTime: 100, Logical: 10, NodeID: "a"},
			want: true,
		},
		{
			name: "Same WallTime and Logical, NodeID decides",
			a:    Timestamp{WallTime: 100, Logical: 5, NodeID: "aaa"},
			b:    Timestamp{WallTime: 100, Logical: 5, NodeID: "bbb"},
			want: true,
		},
		{
			name: "Equal timestamps",
			a:    Timestamp{WallTime: 100, Logical: 5, NodeID: "node"},
			b:    Timestamp{WallTime: 100, Logical: 5, NodeID: "node"},
			want: false,
		},
		{
			name: "Reverse",
			a:    Timestamp{WallTime: 200, Logical: 0, NodeID: "a"},
			b:    Timestamp{WallTime: 100, Logical: 99, NodeID: "z"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Fatalf("%+v.Less(%+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestIsZero(t *testing.T) {
	zero := Timestamp{}
	if !zero.IsZero() {
		t.Fatal("zero timestamp: IsZero() = false")
	}

	nonZero := Timestamp{WallTime: 1}
	if nonZero.IsZero() {
		t.Fatal("non-zero timestamp: IsZero() = true")
	}

	// NodeID set with zero time fields is not zero — something created it.
	withNodeOnly := Timestamp{NodeID: "node-1"}
	if withNodeOnly.IsZero() {
		t.Fatal("timestamp with NodeID set: IsZero() should be false")
	}
}

func TestDriftProtection_Rejected(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))
	c.SetMaxDrift(100 * time.Nanosecond)

	// Received timestamp is 200ns ahead — exceeds max drift of 100ns.
	remote := Timestamp{WallTime: 1200, Logical: 0, NodeID: "node-2"}
	err := c.Update(remote)
	if !errors.Is(err, ErrClockDrift) {
		t.Fatalf("expected ErrClockDrift, got %v", err)
	}

	// Clock should not have advanced — the update was rejected.
	ts := c.Now()
	if ts.WallTime >= 1200 {
		t.Fatalf("clock advanced despite drift rejection: %+v", ts)
	}
}

func TestDriftProtection_Accepted(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))
	c.SetMaxDrift(500 * time.Nanosecond)

	// Received timestamp is 200ns ahead — within max drift of 500ns.
	remote := Timestamp{WallTime: 1200, Logical: 0, NodeID: "node-2"}
	err := c.Update(remote)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	ts := c.Now()
	if ts.WallTime < 1200 {
		t.Fatalf("expected WallTime >= 1200, got %d", ts.WallTime)
	}
}

func TestDriftProtection_OldTimestampAccepted(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))
	c.SetMaxDrift(100 * time.Nanosecond)

	// Received timestamp is 500ns BEHIND local — this is fine.
	// Old timestamps don't push the HLC forward, and must be accepted
	// for anti-entropy and partition recovery to work.
	remote := Timestamp{WallTime: 500, Logical: 0, NodeID: "node-2"}
	err := c.Update(remote)
	if err != nil {
		t.Fatalf("Update with old timestamp should succeed: %v", err)
	}
}

func TestDriftProtection_Disabled(t *testing.T) {
	c := NewClock("node-1", fixedClock(1000))
	c.SetMaxDrift(0) // disable

	// Huge skew — should be accepted since drift check is disabled.
	remote := Timestamp{WallTime: 1e18, Logical: 0, NodeID: "node-2"}
	err := c.Update(remote)
	if err != nil {
		t.Fatalf("Update with disabled drift check: %v", err)
	}
}
