package hlc

import (
	"errors"
	"sync"
	"time"
)

// ErrClockDrift is returned by Update when the received timestamp's
// wall clock differs from the local physical clock by more than
// MaxDrift. This prevents a node with a wildly wrong clock from
// dominating LWW conflict resolution.
var ErrClockDrift = errors.New("hlc: clock drift exceeds maximum")

// DefaultMaxDrift is the default maximum allowed clock skew between
// the local physical clock and a received HLC timestamp.
const DefaultMaxDrift = time.Minute

// Clock is a hybrid logical clock. It is safe for concurrent use.
type Clock struct {
	mu       sync.Mutex
	nodeID   string
	now      func() int64 // returns physical time in nanoseconds
	last     Timestamp
	maxDrift int64 // nanoseconds
}

// NewClock creates a new HLC for the given node. If physicalClock is
// nil, time.Now().UnixNano() is used.
func NewClock(nodeID string, physicalClock func() int64) *Clock {
	if physicalClock == nil {
		physicalClock = func() int64 {
			return time.Now().UnixNano()
		}
	}
	return &Clock{
		nodeID:   nodeID,
		now:      physicalClock,
		maxDrift: int64(DefaultMaxDrift),
	}
}

// SetMaxDrift configures the maximum allowed clock skew. A zero or
// negative value disables drift checking.
func (c *Clock) SetMaxDrift(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxDrift = int64(d)
}

// Now generates a new timestamp for a local event or message send.
//
// Algorithm (Kulkarni et al. §3, send/local rule):
//  1. pt = max(physical(), last.WallTime)
//  2. If pt advanced past last.WallTime → logical = 0
//     Else → logical = last.Logical + 1
//  3. Store as last, stamp with NodeID
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.now()
	if pt > c.last.WallTime {
		c.last = Timestamp{
			WallTime: pt,
			Logical:  0,
			NodeID:   c.nodeID,
		}
	} else {
		c.last = Timestamp{
			WallTime: c.last.WallTime,
			Logical:  c.last.Logical + 1,
			NodeID:   c.nodeID,
		}
	}

	return c.last
}

// Update merges a received timestamp into the local clock (receive rule).
//
// Algorithm (Kulkarni et al. §3, receive rule):
//  1. Check drift: if |physical - received.WallTime| > maxDrift, reject
//  2. pt = max(physical(), last.WallTime, received.WallTime)
//  3. Logical counter depends on which component(s) determined pt:
//     - pt == last.WallTime == received.WallTime → max(last.Logical, received.Logical) + 1
//     - pt == last.WallTime only → last.Logical + 1
//     - pt == received.WallTime only → received.Logical + 1
//     - pt from physical (strictly greater) → 0
//  4. Store as last
func (c *Clock) Update(received Timestamp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.now()

	// Drift check: reject if the received timestamp is too far AHEAD of
	// local physical time. A future timestamp is dangerous because it
	// permanently pushes the HLC forward (HLC never goes backward).
	// Timestamps in the past are safe — max(physical, last, received)
	// just ignores them. Allowing old timestamps is essential for
	// anti-entropy and partition recovery, where replicated data
	// legitimately carries old HLC timestamps.
	if c.maxDrift > 0 && received.WallTime-pt > c.maxDrift {
		return ErrClockDrift
	}

	// Determine a new wall time: max of all three.
	newWall := pt
	if c.last.WallTime > newWall {
		newWall = c.last.WallTime
	}
	if received.WallTime > newWall {
		newWall = received.WallTime
	}

	// Determine logical counter.
	var newLogical uint32
	switch {
	case newWall == c.last.WallTime && newWall == received.WallTime:
		// All three equal - take max of the two logical counters + 1.
		newLogical = c.last.Logical
		if received.Logical > newLogical {
			newLogical = received.Logical
		}
		newLogical++
	case newWall == c.last.WallTime:
		// Last was ahead of both physical and received.
		newLogical = c.last.Logical + 1
	case newWall == received.WallTime:
		// Received was ahead of both physical and last.
		newLogical = received.Logical + 1
	default:
		// Physical clock is strictly ahead - reset logical.
		newLogical = 0
	}

	c.last = Timestamp{
		WallTime: newWall,
		Logical:  newLogical,
		NodeID:   c.nodeID,
	}

	return nil
}
