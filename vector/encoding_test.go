package vector

import (
	"bytes"
	"math"
	"testing"
)

func TestEncodeDecodeVector_RoundTrip(t *testing.T) {
	vec := []float32{1.0, 2.5, -3.14, 0.0}
	meta := Metadata{
		"label":   "test",
		"count":   int64(42),
		"score":   3.14,
		"active":  true,
		"payload": []byte{0xDE, 0xAD},
	}

	encoded, err := EncodeVector(vec, meta)
	if err != nil {
		t.Fatalf("EncodeVector: %v", err)
	}

	gotVec, gotMeta, err := DecodeVector(encoded)
	if err != nil {
		t.Fatalf("DecodeVector: %v", err)
	}

	if len(gotVec) != len(vec) {
		t.Fatalf("vector length: got %d, want %d", len(gotVec), len(vec))
	}
	for i := range vec {
		if gotVec[i] != vec[i] {
			t.Errorf("vec[%d]: got %v, want %v", i, gotVec[i], vec[i])
		}
	}

	if gotMeta["label"] != "test" {
		t.Errorf("label: got %v, want %q", gotMeta["label"], "test")
	}
	if gotMeta["count"] != int64(42) {
		t.Errorf("count: got %v, want 42", gotMeta["count"])
	}
	if gotMeta["score"] != 3.14 {
		t.Errorf("score: got %v, want 3.14", gotMeta["score"])
	}
	if gotMeta["active"] != true {
		t.Errorf("active: got %v, want true", gotMeta["active"])
	}
	if !bytes.Equal(gotMeta["payload"].([]byte), []byte{0xDE, 0xAD}) {
		t.Errorf("payload: got %v, want [0xDE, 0xAD]", gotMeta["payload"])
	}
}

func TestEncodeDecodeVector_EmptyMetadata(t *testing.T) {
	vec := []float32{1.0, 2.0}

	tests := []struct {
		name string
		meta Metadata
	}{
		{"nil", nil},
		{"empty map", Metadata{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeVector(vec, tt.meta)
			if err != nil {
				t.Fatalf("EncodeVector: %v", err)
			}
			gotVec, gotMeta, err := DecodeVector(encoded)
			if err != nil {
				t.Fatalf("DecodeVector: %v", err)
			}
			if len(gotVec) != 2 || gotVec[0] != 1.0 || gotVec[1] != 2.0 {
				t.Errorf("vector mismatch: got %v", gotVec)
			}
			if len(gotMeta) != 0 {
				t.Errorf("expected empty metadata, got %v", gotMeta)
			}
		})
	}
}

func TestEncodeDecodeVector_EmptyVector(t *testing.T) {
	encoded, err := EncodeVector([]float32{}, nil)
	if err != nil {
		t.Fatalf("EncodeVector: %v", err)
	}
	gotVec, _, err := DecodeVector(encoded)
	if err != nil {
		t.Fatalf("DecodeVector: %v", err)
	}
	if len(gotVec) != 0 {
		t.Errorf("expected empty vector, got %v", gotVec)
	}
}

func TestEncodeVector_UnsupportedType(t *testing.T) {
	_, err := EncodeVector([]float32{1.0}, Metadata{"bad": []int{1, 2}})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestDecodeVector_TruncatedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"just version", []byte{1}},
		{"truncated dim", []byte{1, 0}},
		{"truncated vector", []byte{1, 2, 0, 0, 0, 0, 0}}, // dim=2, only 4 bytes of vector data
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeVector(tt.data)
			if err == nil {
				t.Error("expected error for truncated data")
			}
		})
	}
}

func TestDecodeVector_BadVersion(t *testing.T) {
	_, _, err := DecodeVector([]byte{99, 0, 0, 0, 0})
	if err == nil {
		t.Error("expected error for unsupported version")
	}
}

func TestEncodeVector_Deterministic(t *testing.T) {
	vec := []float32{1.0, 2.0, 3.0}
	meta := Metadata{
		"z_last":  "zzz",
		"a_first": "aaa",
		"m_mid":   int64(7),
	}

	encoded1, err := EncodeVector(vec, meta)
	if err != nil {
		t.Fatal(err)
	}
	encoded2, err := EncodeVector(vec, meta)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(encoded1, encoded2) {
		t.Error("encoding is non-deterministic: two encodes produced different bytes")
	}
}

func TestEncodeDecodeVector_LargeVector(t *testing.T) {
	dim := 1536
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}

	encoded, err := EncodeVector(vec, nil)
	if err != nil {
		t.Fatalf("EncodeVector: %v", err)
	}

	gotVec, _, err := DecodeVector(encoded)
	if err != nil {
		t.Fatalf("DecodeVector: %v", err)
	}
	if len(gotVec) != dim {
		t.Fatalf("vector length: got %d, want %d", len(gotVec), dim)
	}
	for i := range vec {
		if gotVec[i] != vec[i] {
			t.Errorf("vec[%d]: got %v, want %v", i, gotVec[i], vec[i])
		}
	}
}

func TestEncodeDecodeVector_SpecialFloats(t *testing.T) {
	vec := []float32{
		float32(math.Inf(1)),
		float32(math.Inf(-1)),
		float32(math.NaN()),
		0,
		math.SmallestNonzeroFloat32,
		math.MaxFloat32,
	}

	encoded, err := EncodeVector(vec, nil)
	if err != nil {
		t.Fatalf("EncodeVector: %v", err)
	}

	gotVec, _, err := DecodeVector(encoded)
	if err != nil {
		t.Fatalf("DecodeVector: %v", err)
	}

	if len(gotVec) != len(vec) {
		t.Fatalf("vector length: got %d, want %d", len(gotVec), len(vec))
	}
	// NaN != NaN, so check with math.IsNaN.
	if !math.IsInf(float64(gotVec[0]), 1) {
		t.Errorf("vec[0]: got %v, want +Inf", gotVec[0])
	}
	if !math.IsInf(float64(gotVec[1]), -1) {
		t.Errorf("vec[1]: got %v, want -Inf", gotVec[1])
	}
	if !math.IsNaN(float64(gotVec[2])) {
		t.Errorf("vec[2]: got %v, want NaN", gotVec[2])
	}
}
