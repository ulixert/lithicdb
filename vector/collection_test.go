package vector

import (
	"bytes"
	"testing"
)

func TestCollectionConfig_RoundTrip(t *testing.T) {
	cfg := CollectionConfig{
		Dim:         768,
		Metric:      MetricCosine,
		M:           32,
		EfConstruct: 200,
		EfSearch:    100,
		MaxVectors:  1_000_000,
	}

	data := encodeCollectionConfig(cfg)
	got, err := decodeCollectionConfig(data)
	if err != nil {
		t.Fatalf("decodeCollectionConfig: %v", err)
	}

	if got != cfg {
		t.Errorf("roundtrip mismatch:\n  got  %+v\n  want %+v", got, cfg)
	}
}

func TestDecodeCollectionConfig_TooShort(t *testing.T) {
	_, err := decodeCollectionConfig([]byte{1, 2})
	if err == nil {
		t.Error("expected error for truncated config data")
	}
}

func TestDecodeCollectionConfig_BadVersion(t *testing.T) {
	data := make([]byte, collectionConfigSize)
	data[0] = 99 // unsupported version
	_, err := decodeCollectionConfig(data)
	if err == nil {
		t.Error("expected error for unsupported config version")
	}
}

func TestKeyOrdering_ConfigBeforeVector(t *testing.T) {
	configKey := makeCollectionConfigKey("myindex")
	vectorKey := makeVectorKey("myindex", [16]byte{0x01})

	if bytes.Compare(configKey, vectorKey) >= 0 {
		t.Errorf("config key should sort before vector key:\n  config: %x\n  vector: %x", configKey, vectorKey)
	}
}

func TestKeyOrdering_SameCollectionDifferentUUIDs(t *testing.T) {
	k1 := makeVectorKey("col", [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	k2 := makeVectorKey("col", [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})
	if bytes.Compare(k1, k2) >= 0 {
		t.Error("expected k1 < k2")
	}
}

func TestKeyOrdering_DifferentCollections(t *testing.T) {
	k1 := makeVectorKey("aaa", [16]byte{0xFF})
	k2 := makeVectorKey("bbb", [16]byte{0x01})
	if bytes.Compare(k1, k2) >= 0 {
		t.Error("expected collection 'aaa' keys to sort before 'bbb' keys")
	}
}

func TestParseVectorKey_ConfigKey(t *testing.T) {
	key := makeCollectionConfigKey("testcol")
	col, id, kind, err := parseVectorKey(key)
	if err != nil {
		t.Fatalf("parseVectorKey: %v", err)
	}
	if col != "testcol" {
		t.Errorf("collection: got %q, want %q", col, "testcol")
	}
	if kind != kindConfig {
		t.Errorf("kind: got 0x%02x, want 0x%02x", kind, kindConfig)
	}
	if id != [16]byte{} {
		t.Errorf("id should be zero for config key, got %x", id)
	}
}

func TestParseVectorKey_VectorKey(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	key := makeVectorKey("testcol", uuid)
	col, id, kind, err := parseVectorKey(key)
	if err != nil {
		t.Fatalf("parseVectorKey: %v", err)
	}
	if col != "testcol" {
		t.Errorf("collection: got %q, want %q", col, "testcol")
	}
	if kind != kindVector {
		t.Errorf("kind: got 0x%02x, want 0x%02x", kind, kindVector)
	}
	if id != uuid {
		t.Errorf("id: got %x, want %x", id, uuid)
	}
}

func TestParseVectorKey_TooShort(t *testing.T) {
	_, _, _, err := parseVectorKey([]byte{0x02})
	if err == nil {
		t.Error("expected error for key that is too short")
	}
}

func TestParseVectorKey_BadPrefix(t *testing.T) {
	_, _, _, err := parseVectorKey([]byte{0x99, 0, 0, 0})
	if err == nil {
		t.Error("expected error for wrong prefix byte")
	}
}

func TestMetricToDistanceFunc(t *testing.T) {
	tests := []struct {
		metric uint8
		ok     bool
	}{
		{MetricL2, true},
		{MetricCosine, true},
		{MetricInnerProduct, true},
		{0, false},
		{99, false},
	}
	for _, tt := range tests {
		_, err := metricToDistanceFunc(tt.metric)
		if (err == nil) != tt.ok {
			t.Errorf("metric %d: err=%v, wantOK=%v", tt.metric, err, tt.ok)
		}
	}
}
