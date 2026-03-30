package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ulixert/lithicdb/hlc"
)

const (
	// envelopeVersion is the current binary format version.
	envelopeVersion = 1

	// envelopeHeaderSize is the fixed overhead before the timestamp:
	// version(1) + deleted(1) = 2 bytes. The timestamp is variable-length
	// (hlc.TimestampHeaderSize + len(NodeID)). Value bytes follow the
	// timestamp directly - no length prefix needed since the value is
	// the last field.
	envelopeHeaderSize = 1 + 1 // version + deleted flag
)

var (
	ErrUnknownEnvelopeVersion = errors.New("envelope: unknown version")
	ErrCorruptEnvelope        = errors.New("envelope: corrupt encoding")
)

// Envelope wraps a value with an HLC timestamp for LWW conflict
// resolution. Stored as the value bytes in db.DB - the embedded
// storage engine sees only opaque bytes.
type Envelope struct {
	Timestamp hlc.Timestamp
	Deleted   bool   // true = tombstone
	Value     []byte // nil when Deleted
}

// EncodeEnvelope serializes an envelope to binary format:
// [version:1][deleted:1][timestamp:variable][value:*]
//
// Value is the last field - no length prefix needed since the
// remaining bytes after the timestamp are the value. When Deleted
// is true, value bytes are omitted.
func EncodeEnvelope(e Envelope) ([]byte, error) {
	tsBytes, err := e.Timestamp.Encode()
	if err != nil {
		return nil, fmt.Errorf("envelope: encode timestamp: %w", err)
	}

	if e.Deleted {
		buf := make([]byte, envelopeHeaderSize+len(tsBytes))
		buf[0] = envelopeVersion
		buf[1] = 1
		copy(buf[envelopeHeaderSize:], tsBytes)
		return buf, nil
	}

	buf := make([]byte, envelopeHeaderSize+len(tsBytes)+len(e.Value))
	buf[0] = envelopeVersion
	buf[1] = 0
	copy(buf[envelopeHeaderSize:], tsBytes)
	copy(buf[envelopeHeaderSize+len(tsBytes):], e.Value)
	return buf, nil
}

// DecodeEnvelope deserializes an envelope from the format produced
// by EncodeEnvelope.
func DecodeEnvelope(b []byte) (Envelope, error) {
	if len(b) < envelopeHeaderSize {
		return Envelope{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrCorruptEnvelope, envelopeHeaderSize, len(b))
	}

	if b[0] != envelopeVersion {
		return Envelope{}, fmt.Errorf("%w: got %d, expected %d",
			ErrUnknownEnvelopeVersion, b[0], envelopeVersion)
	}

	deleted := b[1] != 0
	tsStart := envelopeHeaderSize

	// Timestamp is variable-length. Read the fixed header to find the
	// NodeID length, then compute the full timestamp size.
	if len(b) < tsStart+hlc.TimestampHeaderSize {
		return Envelope{}, fmt.Errorf("%w: truncated timestamp", ErrCorruptEnvelope)
	}
	nodeIDLen := int(binary.BigEndian.Uint16(b[tsStart+hlc.NodeIDLenOffset:]))
	tsEnd := tsStart + hlc.TimestampHeaderSize + nodeIDLen
	if tsEnd > len(b) {
		return Envelope{}, fmt.Errorf("%w: timestamp nodeID extends past buffer", ErrCorruptEnvelope)
	}

	ts, err := hlc.DecodeTimestamp(b[tsStart:tsEnd])
	if err != nil {
		return Envelope{}, fmt.Errorf("envelope: decode timestamp: %w", err)
	}

	if deleted {
		if len(b) != tsEnd {
			return Envelope{}, fmt.Errorf("%w: %d trailing bytes after tombstone",
				ErrCorruptEnvelope, len(b)-tsEnd)
		}
		return Envelope{Timestamp: ts, Deleted: true}, nil
	}

	// Non-tombstone: remaining bytes after timestamp are the value.
	remaining := b[tsEnd:]
	value := make([]byte, len(remaining))
	copy(value, remaining)

	return Envelope{Timestamp: ts, Value: value}, nil
}

// NewerEnvelope returns whichever envelope has the greater HLC
// timestamp (last-writer-wins). On equal timestamps, returns a.
func NewerEnvelope(a, b Envelope) Envelope {
	if b.Timestamp.Less(a.Timestamp) || a.Timestamp.Equal(b.Timestamp) {
		return a
	}
	return b
}
