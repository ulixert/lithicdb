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
