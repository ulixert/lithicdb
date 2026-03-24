// Package hlc implements Hybrid Logical Clocks (Kulkarni et al. 2014).
//
// HLC combines a physical wall clock with a logical counter to provide
// timestamps that are:
//   - Monotonically increasing on a single node (even if the wall clock
//     jumps backward)
//   - Causally ordered across nodes for events connected by message
//     passing (send/receive)
//   - Deterministically totally ordered via NodeID tiebreaking for
//     concurrent events
//
// This is a pure library with no networking or I/O.
package hlc

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// timestampHeaderSize is the fixed portion of an encoded timestamp:
// 8 bytes WallTime + 4 bytes Logical + 2 bytes NodeID length.
const timestampHeaderSize = 8 + 4 + 2

var ErrCorruptTimestamp = errors.New("hlc: corrupt timestamp encoding")

// Timestamp is a hybrid logical clock value. It combines a physical
// wall clock (nanoseconds since epoch), a logical counter for sub-tick
// ordering, and a NodeID for deterministic tiebreaking.
//
// Timestamps are compared by (WallTime, Logical, NodeID) in that order.
type Timestamp struct {
	WallTime int64  // nanoseconds since epoch
	Logical  uint32 // logical counter, reset on wall clock advance
	NodeID   string // tie-breaker for total ordering
}

// Less returns true if t is strictly less than other in the total order:
// WallTime first, then Logical, then NodeID (lexicographic).
func (t Timestamp) Less(other Timestamp) bool {
	if t.WallTime != other.WallTime {
		return t.WallTime < other.WallTime
	}
	if t.Logical != other.Logical {
		return t.Logical < other.Logical
	}
	return t.NodeID < other.NodeID
}

// Equal returns true if all three fields match.
func (t Timestamp) Equal(other Timestamp) bool {
	return t.WallTime == other.WallTime &&
		t.Logical == other.Logical &&
		t.NodeID == other.NodeID
}

// IsZero reports whether the timestamp is the zero value.
func (t Timestamp) IsZero() bool {
	return t.WallTime == 0 && t.Logical == 0
}

// Encode serializes the timestamp to a big-endian byte slice.
// Format: [WallTime:8][Logical:4][NodeIDLen:2][NodeID:*]
// Big-endian so that byte-wise comparison preserves temporal ordering
// for the fixed-width fields.
func (t Timestamp) Encode() []byte {
	buf := make([]byte, timestampHeaderSize+len(t.NodeID))
	binary.BigEndian.PutUint64(buf[0:], uint64(t.WallTime))
	binary.BigEndian.PutUint32(buf[8:], t.Logical)
	binary.BigEndian.PutUint16(buf[12:], uint16(len(t.NodeID)))
	copy(buf[timestampHeaderSize:], t.NodeID)
	return buf
}

// DecodeTimestamp deserializes a timestamp from the format produced by
// Encode. Returns an error if the data is too short or truncated.
func DecodeTimestamp(b []byte) (Timestamp, error) {
	if len(b) < timestampHeaderSize {
		return Timestamp{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrCorruptTimestamp, timestampHeaderSize, len(b))
	}

	wallTime := int64(binary.BigEndian.Uint64(b[0:]))
	logical := binary.BigEndian.Uint32(b[8:])
	nodeIDLen := int(binary.BigEndian.Uint16(b[12:]))

	if timestampHeaderSize+nodeIDLen > len(b) {
		return Timestamp{}, fmt.Errorf("%w: nodeID length %d exceeds data",
			ErrCorruptTimestamp, nodeIDLen)
	}

	nodeID := string(b[timestampHeaderSize : timestampHeaderSize+nodeIDLen])

	return Timestamp{
		WallTime: wallTime,
		Logical:  logical,
		NodeID:   nodeID,
	}, nil
}
