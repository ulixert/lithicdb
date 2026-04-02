package cluster

import (
	"testing"

	"github.com/ulixert/theseon/hlc"
)

func TestEncodeDecodeEnvelope(t *testing.T) {
	ts := hlc.Timestamp{WallTime: 1700000000000000000, Logical: 42, NodeID: "node-1"}

	t.Run("round-trip with value", func(t *testing.T) {
		original := Envelope{
			Timestamp: ts,
			Value:     []byte("hello world"),
		}
		encoded, err := EncodeEnvelope(original)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := DecodeEnvelope(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !decoded.Timestamp.Equal(original.Timestamp) {
			t.Errorf("timestamp mismatch: got %+v, want %+v", decoded.Timestamp, original.Timestamp)
		}
		if decoded.Deleted {
			t.Error("expected non-deleted")
		}
		if string(decoded.Value) != string(original.Value) {
			t.Errorf("value mismatch: got %q, want %q", decoded.Value, original.Value)
		}
	})

	t.Run("round-trip tombstone", func(t *testing.T) {
		original := Envelope{
			Timestamp: ts,
			Deleted:   true,
		}
		encoded, err := EncodeEnvelope(original)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := DecodeEnvelope(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !decoded.Timestamp.Equal(original.Timestamp) {
			t.Errorf("timestamp mismatch: got %+v, want %+v", decoded.Timestamp, original.Timestamp)
		}
		if !decoded.Deleted {
			t.Error("expected deleted")
		}
		if decoded.Value != nil {
			t.Errorf("expected nil value for tombstone, got %v", decoded.Value)
		}
	})

	t.Run("round-trip empty value", func(t *testing.T) {
		original := Envelope{
			Timestamp: ts,
			Value:     []byte{},
		}
		encoded, err := EncodeEnvelope(original)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := DecodeEnvelope(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Deleted {
			t.Error("expected non-deleted")
		}
		if decoded.Value == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(decoded.Value) != 0 {
			t.Errorf("expected empty value, got %v", decoded.Value)
		}
	})

	t.Run("round-trip empty nodeID", func(t *testing.T) {
		original := Envelope{
			Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: ""},
			Value:     []byte("data"),
		}
		encoded, err := EncodeEnvelope(original)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := DecodeEnvelope(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !decoded.Timestamp.Equal(original.Timestamp) {
			t.Errorf("timestamp mismatch: got %+v, want %+v", decoded.Timestamp, original.Timestamp)
		}
	})
}

func TestDecodeEnvelopeErrors(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		_, err := DecodeEnvelope(nil)
		if err == nil {
			t.Fatal("expected error for nil input")
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		_, err := DecodeEnvelope([]byte{envelopeVersion})
		if err == nil {
			t.Fatal("expected error for truncated header")
		}
	})

	t.Run("wrong version", func(t *testing.T) {
		buf := []byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		_, err := DecodeEnvelope(buf)
		if err == nil {
			t.Fatal("expected error for wrong version")
		}
	})

	t.Run("truncated timestamp", func(t *testing.T) {
		buf := []byte{envelopeVersion, 0, 0, 0, 0}
		_, err := DecodeEnvelope(buf)
		if err == nil {
			t.Fatal("expected error for truncated timestamp")
		}
	})

	t.Run("trailing bytes after tombstone", func(t *testing.T) {
		e := Envelope{
			Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: "n"},
			Deleted:   true,
		}
		encoded, _ := EncodeEnvelope(e)
		corrupted := append(encoded, 0xFF)
		_, err := DecodeEnvelope(corrupted)
		if err == nil {
			t.Fatal("expected error for trailing bytes after tombstone")
		}
	})

	t.Run("truncated value decodes as shorter value", func(t *testing.T) {
		// Without a length prefix, truncating the end just gives a shorter
		// value — this is not an error. Verify the behavior is correct.
		e := Envelope{
			Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: "n"},
			Value:     []byte("hello"),
		}
		encoded, _ := EncodeEnvelope(e)
		truncated := encoded[:len(encoded)-2]
		decoded, err := DecodeEnvelope(truncated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(decoded.Value) != "hel" {
			t.Fatalf("got %q, want %q", decoded.Value, "hel")
		}
	})

	t.Run("nodeID length extends past buffer", func(t *testing.T) {
		// Craft a buffer where the timestamp's nodeIDLen claims more
		// bytes than available.
		buf := make([]byte, envelopeHeaderSize+hlc.TimestampHeaderSize)
		buf[0] = envelopeVersion
		buf[1] = 0 // not deleted
		// Set nodeIDLen to 100 but provide no nodeID bytes.
		buf[envelopeHeaderSize+hlc.NodeIDLenOffset] = 0
		buf[envelopeHeaderSize+hlc.NodeIDLenOffset+1] = 100
		_, err := DecodeEnvelope(buf)
		if err == nil {
			t.Fatal("expected error for nodeID extending past buffer")
		}
	})
}

func TestNewerEnvelope(t *testing.T) {
	tsOld := hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: "node-1"}
	tsNew := hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: "node-1"}

	a := Envelope{Timestamp: tsNew, Value: []byte("a")}
	b := Envelope{Timestamp: tsOld, Value: []byte("b")}

	t.Run("a newer", func(t *testing.T) {
		result := NewerEnvelope(a, b)
		if string(result.Value) != "a" {
			t.Errorf("expected a, got %q", result.Value)
		}
	})

	t.Run("b newer", func(t *testing.T) {
		result := NewerEnvelope(b, a)
		if string(result.Value) != "a" {
			t.Errorf("expected a (newer), got %q", result.Value)
		}
	})

	t.Run("equal timestamps returns a", func(t *testing.T) {
		x := Envelope{Timestamp: tsOld, Value: []byte("x")}
		y := Envelope{Timestamp: tsOld, Value: []byte("y")}
		result := NewerEnvelope(x, y)
		if string(result.Value) != "x" {
			t.Errorf("expected x (first arg on tie), got %q", result.Value)
		}
	})

	t.Run("same wall time different logical", func(t *testing.T) {
		lo := Envelope{Timestamp: hlc.Timestamp{WallTime: 100, Logical: 1, NodeID: "n"}, Value: []byte("lo")}
		hi := Envelope{Timestamp: hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: "n"}, Value: []byte("hi")}
		result := NewerEnvelope(lo, hi)
		if string(result.Value) != "hi" {
			t.Errorf("expected hi (higher logical), got %q", result.Value)
		}
	})

	t.Run("same wall time and logical different nodeID", func(t *testing.T) {
		ea := Envelope{Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: "aaa"}, Value: []byte("ea")}
		eb := Envelope{Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: "zzz"}, Value: []byte("eb")}
		result := NewerEnvelope(ea, eb)
		if string(result.Value) != "eb" {
			t.Errorf("expected eb (higher nodeID), got %q", result.Value)
		}
	})
}
