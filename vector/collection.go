package vector

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/ulixert/theseon/vector/hnsw"
)

// Key prefix and kind bytes for the vector namespace in the KV store.
const (
	keyPrefixVector byte = 0x02
	kindConfig      byte = 0x00 // collection config; sorts before kindVector
	kindVector      byte = 0x01 // vector entry
)

const collectionConfigVersion byte = 1

// Distance metric identifiers stored in collection config.
const (
	MetricL2           uint8 = 1
	MetricCosine       uint8 = 2
	MetricInnerProduct uint8 = 3
)

// CollectionConfig defines the parameters for a vector collection.
type CollectionConfig struct {
	Dim         int
	Metric      uint8
	M           int
	EfConstruct int
	EfSearch    int
	MaxVectors  int64
}

// Config wire format:
// [version:1][dim:2][metric:1][M:2][efConstruct:2][efSearch:2][maxVectors:8]
const collectionConfigSize = 1 + 2 + 1 + 2 + 2 + 2 + 8 // 18 bytes

func encodeCollectionConfig(cfg CollectionConfig) []byte {
	buf := make([]byte, 0, collectionConfigSize)
	buf = append(buf, collectionConfigVersion)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(cfg.Dim))
	buf = append(buf, cfg.Metric)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(cfg.M))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(cfg.EfConstruct))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(cfg.EfSearch))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(cfg.MaxVectors))
	return buf
}

func decodeCollectionConfig(data []byte) (CollectionConfig, error) {
	if len(data) < collectionConfigSize {
		return CollectionConfig{}, fmt.Errorf("vector: collection config too short (%d bytes)", len(data))
	}
	if data[0] != collectionConfigVersion {
		return CollectionConfig{}, fmt.Errorf("vector: unsupported config version %d", data[0])
	}
	return CollectionConfig{
		Dim:         int(binary.LittleEndian.Uint16(data[1:3])),
		Metric:      data[3],
		M:           int(binary.LittleEndian.Uint16(data[4:6])),
		EfConstruct: int(binary.LittleEndian.Uint16(data[6:8])),
		EfSearch:    int(binary.LittleEndian.Uint16(data[8:10])),
		MaxVectors:  int64(binary.LittleEndian.Uint64(data[10:18])),
	}, nil
}

// makeCollectionConfigKey returns the KV key for a collection's config.
// Format: [keyPrefixVector][name_len:2 LE][name][kindConfig]
func makeCollectionConfigKey(collection string) []byte {
	buf := make([]byte, 0, 1+2+len(collection)+1)
	buf = append(buf, keyPrefixVector)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(collection)))
	buf = append(buf, collection...)
	buf = append(buf, kindConfig)
	return buf
}

// makeVectorKey returns the KV key for a vector entry.
// Format: [keyPrefixVector][name_len:2 LE][name][kindVector][uuid:16]
func makeVectorKey(collection string, id [16]byte) []byte {
	buf := make([]byte, 0, 1+2+len(collection)+1+16)
	buf = append(buf, keyPrefixVector)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(collection)))
	buf = append(buf, collection...)
	buf = append(buf, kindVector)
	buf = append(buf, id[:]...)
	return buf
}

// parseVectorKey extracts the collection name, kind byte, and UUID from
// a user key produced by makeCollectionConfigKey or makeVectorKey.
func parseVectorKey(userKey []byte) (collection string, id [16]byte, kind byte, err error) {
	if len(userKey) < 4 { // prefix(1) + name_len(2) + kind(1) minimum
		return "", id, 0, fmt.Errorf("vector: key too short (%d bytes)", len(userKey))
	}
	if userKey[0] != keyPrefixVector {
		return "", id, 0, fmt.Errorf("vector: unexpected key prefix 0x%02x", userKey[0])
	}
	nameLen := int(binary.LittleEndian.Uint16(userKey[1:3]))
	if 3+nameLen+1 > len(userKey) {
		return "", id, 0, fmt.Errorf("vector: key truncated at collection name")
	}
	collection = string(userKey[3 : 3+nameLen])
	kind = userKey[3+nameLen]

	switch kind {
	case kindConfig:
		// No UUID for config keys.
	case kindVector:
		uuidStart := 3 + nameLen + 1
		if uuidStart+16 > len(userKey) {
			return "", id, 0, fmt.Errorf("vector: key truncated at UUID")
		}
		copy(id[:], userKey[uuidStart:uuidStart+16])
	default:
		return "", id, 0, fmt.Errorf("vector: unknown kind byte 0x%02x", kind)
	}
	return collection, id, kind, nil
}

// makeVectorKeyPrefix returns the key prefix for all vector entries in a
// collection (excluding config keys). Used as the start bound for ScanRange.
// Format: [keyPrefixVector][name_len:2 LE][name][kindVector]
func makeVectorKeyPrefix(collection string) []byte {
	buf := make([]byte, 0, 1+2+len(collection)+1)
	buf = append(buf, keyPrefixVector)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(collection)))
	buf = append(buf, collection...)
	buf = append(buf, kindVector)
	return buf
}

// makeVectorKeyPrefixEnd returns the exclusive end bound for scanning
// all vector entries in a collection.
// Format: [keyPrefixVector][name_len:2 LE][name][kindVector+1]
func makeVectorKeyPrefixEnd(collection string) []byte {
	buf := make([]byte, 0, 1+2+len(collection)+1)
	buf = append(buf, keyPrefixVector)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(collection)))
	buf = append(buf, collection...)
	buf = append(buf, kindVector+1)
	return buf
}

// MetricToDistanceFunc maps a stored metric identifier to an HNSW distance function.
func MetricToDistanceFunc(metric uint8) (hnsw.DistanceFunc, error) {
	switch metric {
	case MetricL2:
		return hnsw.DistanceL2Squared, nil
	case MetricCosine:
		return hnsw.DistanceCosine, nil
	case MetricInnerProduct:
		return hnsw.DistanceInnerProduct, nil
	default:
		return nil, fmt.Errorf("vector: unknown metric %d", metric)
	}
}

// ConfigDigest returns a hash of the collection config fields that must match
// across replicas (dim, metric, M, efConstruct). Used for config validation
// in distributed vector RPCs.
func ConfigDigest(cfg CollectionConfig) uint64 {
	h := fnv.New64a()
	var buf [16]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(cfg.Dim))
	buf[4] = cfg.Metric
	binary.LittleEndian.PutUint32(buf[5:9], uint32(cfg.M))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(cfg.EfConstruct))
	h.Write(buf[:13])
	return h.Sum64()
}
