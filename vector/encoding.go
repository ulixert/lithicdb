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

// Encoding constants.
const (
	encodingVersion byte = 1

	metaTypeString  byte = 1
	metaTypeInt64   byte = 2
	metaTypeFloat64 byte = 3
	metaTypeBool    byte = 4
	metaTypeBytes   byte = 5
)

// EncodeVector serializes a vector and its metadata into a binary format.
// Returns an error if metadata contains an unsupported value type.
//
// Binary wire format (little-endian):
//
//	[version:1][dim:2][float32s: dim*4][num_fields:2]
//	repeated num_fields times:
//	  [name_len:2][name][type:1][val_len:2][val]
//
// Type tags:
//
//	1 = string
//	2 = int64
//	3 = float64
//	4 = bool
//	5 = []byte
func EncodeVector(vec []float32, metadata Metadata) ([]byte, error) {
	dim := len(vec)
	// version(1) + dim(2) + floats(dim*4) + num_fields(2)
	size := 1 + 2 + dim*4 + 2

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

	// Header.
	buf = append(buf, encodingVersion)
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

// DecodeVector deserializes a vector and its metadata from binary data.
func DecodeVector(data []byte) ([]float32, Metadata, error) {
	if len(data) < 3 {
		return nil, nil, fmt.Errorf("vector: data too short (%d bytes)", len(data))
	}

	version := data[0]
	if version != encodingVersion {
		return nil, nil, fmt.Errorf("vector: unsupported encoding version %d", version)
	}

	dim := int(binary.LittleEndian.Uint16(data[1:3]))
	off := 3

	// Vector data.
	needed := dim * 4
	if off+needed > len(data) {
		return nil, nil, fmt.Errorf("vector: truncated vector data (need %d bytes at offset %d, have %d)", needed, off, len(data))
	}
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
		off += 4
	}

	// Metadata.
	if off+2 > len(data) {
		return nil, nil, fmt.Errorf("vector: truncated metadata header at offset %d", off)
	}
	numFields := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2

	meta := make(Metadata, numFields)
	for i := 0; i < numFields; i++ {
		if off+2 > len(data) {
			return nil, nil, fmt.Errorf("vector: truncated field name length at field %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2

		if off+nameLen > len(data) {
			return nil, nil, fmt.Errorf("vector: truncated field name at field %d", i)
		}
		name := string(data[off : off+nameLen])
		off += nameLen

		if off+1 > len(data) {
			return nil, nil, fmt.Errorf("vector: truncated type tag at field %d", i)
		}
		typeByte := data[off]
		off++

		if off+2 > len(data) {
			return nil, nil, fmt.Errorf("vector: truncated value length at field %d", i)
		}
		valLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2

		if off+valLen > len(data) {
			return nil, nil, fmt.Errorf("vector: truncated value at field %d", i)
		}
		valBytes := data[off : off+valLen]
		off += valLen

		switch typeByte {
		case metaTypeString:
			meta[name] = string(valBytes)
		case metaTypeInt64:
			if valLen != 8 {
				return nil, nil, fmt.Errorf("vector: int64 field %q has %d bytes, want 8", name, valLen)
			}
			meta[name] = int64(binary.LittleEndian.Uint64(valBytes))
		case metaTypeFloat64:
			if valLen != 8 {
				return nil, nil, fmt.Errorf("vector: float64 field %q has %d bytes, want 8", name, valLen)
			}
			meta[name] = math.Float64frombits(binary.LittleEndian.Uint64(valBytes))
		case metaTypeBool:
			if valLen != 1 {
				return nil, nil, fmt.Errorf("vector: bool field %q has %d bytes, want 1", name, valLen)
			}
			meta[name] = valBytes[0] != 0
		case metaTypeBytes:
			cp := make([]byte, valLen)
			copy(cp, valBytes)
			meta[name] = cp
		default:
			return nil, nil, fmt.Errorf("vector: unknown type tag %d for field %q", typeByte, name)
		}
	}

	return vec, meta, nil
}
