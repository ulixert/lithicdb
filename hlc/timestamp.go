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

const (
	wallTimeSize  = 8 // int64 big-endian
	logicalSize   = 4 // uint32 big-endian
	nodeIDLenSize = 2 // uint16 big-endian

	// TimestampHeaderSize is the fixed portion of an encoded timestamp:
	// [WallTime:8][Logical:4][NodeIDLen:2]. The full encoded size is
	// TimestampHeaderSize + len(NodeID).
	TimestampHeaderSize = wallTimeSize + logicalSize + nodeIDLenSize

	// NodeIDLenOffset is the byte offset of the NodeIDLen field within
	// an encoded timestamp. Used by callers that need to determine the
	// variable-length timestamp size without fully decoding it.
	NodeIDLenOffset = wallTimeSize + logicalSize

	// maxNodeIDLen is the maximum NodeID length encodable in uint16.
	maxNodeIDLen = 1<<16 - 1
)

var (
	ErrCorruptTimestamp = errors.New("hlc: corrupt timestamp encoding")
	ErrNodeIDTooLong    = errors.New("hlc: NodeID exceeds maximum length (65535 bytes)")
)

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
	return t.WallTime == 0 && t.Logical == 0 && t.NodeID == ""
}

// Encode serializes the timestamp to a big-endian byte slice.
// Format: [WallTime:8][Logical:4][NodeIDLen:2][NodeID:*]
// Big-endian so that byte-wise comparison preserves temporal ordering
// for the fixed-width fields.
//
// Returns an error if NodeID exceeds 65535 bytes.
func (t Timestamp) Encode() ([]byte, error) {
	if len(t.NodeID) > maxNodeIDLen {
		return nil, ErrNodeIDTooLong
	}
	buf := make([]byte, TimestampHeaderSize+len(t.NodeID))
	binary.BigEndian.PutUint64(buf[0:], uint64(t.WallTime))
	binary.BigEndian.PutUint32(buf[wallTimeSize:], t.Logical)
	binary.BigEndian.PutUint16(buf[wallTimeSize+logicalSize:], uint16(len(t.NodeID)))
	copy(buf[TimestampHeaderSize:], t.NodeID)
	return buf, nil
}

// DecodeTimestamp deserializes a timestamp from the format produced by
// Encode. Returns an error if the data is too short or truncated.
func DecodeTimestamp(b []byte) (Timestamp, error) {
	if len(b) < TimestampHeaderSize {
		return Timestamp{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrCorruptTimestamp, TimestampHeaderSize, len(b))
	}

	wallTime := int64(binary.BigEndian.Uint64(b[0:]))
	logical := binary.BigEndian.Uint32(b[wallTimeSize:])
	nodeIDLen := int(binary.BigEndian.Uint16(b[wallTimeSize+logicalSize:]))

	expectedLen := TimestampHeaderSize + nodeIDLen
	if len(b) < expectedLen {
		return Timestamp{}, fmt.Errorf("%w: nodeID length %d exceeds data",
			ErrCorruptTimestamp, nodeIDLen)
	}
	if len(b) != expectedLen {
		return Timestamp{}, fmt.Errorf("%w: %d trailing bytes",
			ErrCorruptTimestamp, len(b)-expectedLen)
	}

	nodeID := string(b[TimestampHeaderSize:expectedLen])

	return Timestamp{
		WallTime: wallTime,
		Logical:  logical,
		NodeID:   nodeID,
	}, nil
}
