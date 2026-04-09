package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

// Metadata holds arbitrary key-value pairs associated with a vector.
// Supported value types: string, int64, float64, bool, []byte.
type Metadata map[string]any

// VectorVersion is a causally ordered version stamp for LWW conflict resolution.
// Comparison: WallTime first, then Logical (same as HLC ordering).
type VectorVersion struct {
	WallTime int64  // nanoseconds since epoch
	Logical  uint32 // HLC logical counter
}

// After reports whether v is strictly after another.
func (v VectorVersion) After(other VectorVersion) bool {
	return v.WallTime > other.WallTime ||
		(v.WallTime == other.WallTime && v.Logical > other.Logical)
}

// IsZero reports whether v is the zero value (legacy v1 records).
func (v VectorVersion) IsZero() bool {
	return v.WallTime == 0 && v.Logical == 0
}

// Encoding constants.
const (
	encodingVersionV1 byte = 1
	encodingVersionV2 byte = 2

	// versionSize is the byte size of VectorVersion in the v2 encoding:
	// walltime(8 big-endian) + logical(4 big-endian).
	versionSize = 8 + 4

	metaTypeString  byte = 1
	metaTypeInt64   byte = 2
	metaTypeFloat64 byte = 3
	metaTypeBool    byte = 4
	metaTypeBytes   byte = 5
)

// EncodeVector serializes a vector, its version, and metadata into a binary format.
// Returns an error if metadata contains an unsupported value type.
//
// v2 binary wire format:
//
//	[encVersion:1=2][walltime:8 BE][logical:4 BE][dim:2 LE][float32s: dim*4 LE][num_fields:2 LE]
//	repeated num_fields times:
//	  [name_len:2 LE][name][type:1][val_len:2 LE][val]
//
// Type tags:
//
//	1 = string
//	2 = int64
//	3 = float64
//	4 = bool
//	5 = []byte
func EncodeVector(vec []float32, metadata Metadata, ver VectorVersion) ([]byte, error) {
	dim := len(vec)
	// encVersion(1) + version(12) + dim(2) + floats(dim*4) + num_fields(2)
	size := 1 + versionSize + 2 + dim*4 + 2

	// Pre-calculate metadata size and validate types.
	keys := make([]string, 0, len(metadata))
	for k := range metadata {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		v := metadata[k]
		// name_len(2) + name + type(1) + val_len(2) + val
		size += 2 + len(k) + 1 + 2
		switch val := v.(type) {
		case string:
			size += len(val)
		case int64:
			size += 8
		case float64:
			size += 8
		case bool:
			size += 1
		case []byte:
			size += len(val)
		default:
			return nil, fmt.Errorf("vector: unsupported metadata type %T for key %q", v, k)
		}
	}

	buf := make([]byte, 0, size)

	// Header: encoding version + VectorVersion (big-endian for lexicographic ordering).
	buf = append(buf, encodingVersionV2)
	buf = binary.BigEndian.AppendUint64(buf, uint64(ver.WallTime))
	buf = binary.BigEndian.AppendUint32(buf, ver.Logical)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(dim))

	// Vector data.
	for _, f := range vec {
		buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(f))
	}

	// Metadata fields.
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(keys)))
	for _, k := range keys {
		v := metadata[k]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)

		switch val := v.(type) {
		case string:
			buf = append(buf, metaTypeString)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(val)))
			buf = append(buf, val...)
		case int64:
			buf = append(buf, metaTypeInt64)
			buf = binary.LittleEndian.AppendUint16(buf, 8)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(val))
		case float64:
			buf = append(buf, metaTypeFloat64)
			buf = binary.LittleEndian.AppendUint16(buf, 8)
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(val))
		case bool:
			buf = append(buf, metaTypeBool)
			buf = binary.LittleEndian.AppendUint16(buf, 1)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case []byte:
			buf = append(buf, metaTypeBytes)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(val)))
			buf = append(buf, val...)
		}
	}

	return buf, nil
}

// DecodeVector deserializes a vector, its version, and metadata from binary data.
// Supports both v1 (no version, returns zero VectorVersion) and v2.
func DecodeVector(data []byte) ([]float32, Metadata, VectorVersion, error) {
	if len(data) < 3 {
		return nil, nil, VectorVersion{}, fmt.Errorf("vector: data too short (%d bytes)", len(data))
	}

	encVer := data[0]
	var ver VectorVersion
	var off int

	switch encVer {
	case encodingVersionV1:
		// v1: [encVersion:1][dim:2][...]
		off = 1
	case encodingVersionV2:
		// v2: [encVersion:1][walltime:8 BE][logical:4 BE][dim:2][...]
		if len(data) < 1+versionSize+2 {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: v2 data too short (%d bytes)", len(data))
		}
		ver.WallTime = int64(binary.BigEndian.Uint64(data[1:9]))
		ver.Logical = binary.BigEndian.Uint32(data[9:13])
		off = 1 + versionSize
	default:
		return nil, nil, VectorVersion{}, fmt.Errorf("vector: unsupported encoding version %d", encVer)
	}

	if off+2 > len(data) {
		return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated dim at offset %d", off)
	}
	dim := int(binary.LittleEndian.Uint16(data[off : off+2]))
	off += 2

	// Vector data.
	needed := dim * 4
	if off+needed > len(data) {
		return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated vector data (need %d bytes at offset %d, have %d)", needed, off, len(data))
	}
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
		off += 4
	}

	// Metadata.
	if off+2 > len(data) {
		return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated metadata header at offset %d", off)
	}
	numFields := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2

	meta := make(Metadata, numFields)
	for i := 0; i < numFields; i++ {
		if off+2 > len(data) {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated field name length at field %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2

		if off+nameLen > len(data) {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated field name at field %d", i)
		}
		name := string(data[off : off+nameLen])
		off += nameLen

		if off+1 > len(data) {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated type tag at field %d", i)
		}
		typeByte := data[off]
		off++

		if off+2 > len(data) {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated value length at field %d", i)
		}
		valLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2

		if off+valLen > len(data) {
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: truncated value at field %d", i)
		}
		valBytes := data[off : off+valLen]
		off += valLen

		switch typeByte {
		case metaTypeString:
			meta[name] = string(valBytes)
		case metaTypeInt64:
			if valLen != 8 {
				return nil, nil, VectorVersion{}, fmt.Errorf("vector: int64 field %q has %d bytes, want 8", name, valLen)
			}
			meta[name] = int64(binary.LittleEndian.Uint64(valBytes))
		case metaTypeFloat64:
			if valLen != 8 {
				return nil, nil, VectorVersion{}, fmt.Errorf("vector: float64 field %q has %d bytes, want 8", name, valLen)
			}
			meta[name] = math.Float64frombits(binary.LittleEndian.Uint64(valBytes))
		case metaTypeBool:
			if valLen != 1 {
				return nil, nil, VectorVersion{}, fmt.Errorf("vector: bool field %q has %d bytes, want 1", name, valLen)
			}
			meta[name] = valBytes[0] != 0
		case metaTypeBytes:
			cp := make([]byte, valLen)
			copy(cp, valBytes)
			meta[name] = cp
		default:
			return nil, nil, VectorVersion{}, fmt.Errorf("vector: unknown type tag %d for field %q", typeByte, name)
		}
	}

	return vec, meta, ver, nil
}

// DecodeVectorVersion extracts just the VectorVersion from encoded data
// without fully decoding the vector and metadata. Useful for LWW comparison.
func DecodeVectorVersion(data []byte) (VectorVersion, error) {
	if len(data) < 1 {
		return VectorVersion{}, fmt.Errorf("vector: data too short")
	}
	switch data[0] {
	case encodingVersionV1:
		return VectorVersion{}, nil
	case encodingVersionV2:
		if len(data) < 1+versionSize {
			return VectorVersion{}, fmt.Errorf("vector: v2 data too short for version")
		}
		return VectorVersion{
			WallTime: int64(binary.BigEndian.Uint64(data[1:9])),
			Logical:  binary.BigEndian.Uint32(data[9:13]),
		}, nil
	default:
		return VectorVersion{}, fmt.Errorf("vector: unsupported encoding version %d", data[0])
	}
}
