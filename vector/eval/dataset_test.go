package eval

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFvecs_Roundtrip(t *testing.T) {
	vecs := &Vectors{
		Data: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
		N:    2,
		Dim:  3,
	}
	path := filepath.Join(t.TempDir(), "test.fvecs")
	if err := WriteFvecs(path, vecs); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadFvecs(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.N != 2 || loaded.Dim != 3 {
		t.Fatalf("N=%d Dim=%d, want N=2 Dim=3", loaded.N, loaded.Dim)
	}
	for i := 0; i < loaded.N; i++ {
		for j := 0; j < loaded.Dim; j++ {
			if loaded.Vec(i)[j] != vecs.Vec(i)[j] {
				t.Errorf("Vec(%d)[%d]: got %v, want %v", i, j, loaded.Vec(i)[j], vecs.Vec(i)[j])
			}
		}
	}
}

func TestIvecs_Roundtrip(t *testing.T) {
	gt := &GroundTruth{
		IDs: []int32{10, 20, 30, 40, 50, 60},
		N:   3,
		K:   2,
	}
	path := filepath.Join(t.TempDir(), "test.ivecs")
	if err := WriteIvecs(path, gt); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadIvecs(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.N != 3 || loaded.K != 2 {
		t.Fatalf("N=%d K=%d, want N=3 K=2", loaded.N, loaded.K)
	}
	for i := 0; i < loaded.N; i++ {
		nbs := loaded.Neighbors(i)
		want := gt.Neighbors(i)
		for j := range nbs {
			if nbs[j] != want[j] {
				t.Errorf("Neighbors(%d)[%d]: got %d, want %d", i, j, nbs[j], want[j])
			}
		}
	}
}

func TestLoadFvecs_TruncatedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.fvecs")
	// Write a valid header (dim=3) but truncate the data.
	os.WriteFile(path, []byte{3, 0, 0, 0, 1, 0, 0, 0}, 0o644)
	_, err := LoadFvecs(path)
	if err == nil {
		t.Error("expected error for truncated fvecs file")
	}
}

func TestLoadFvecs_EmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.fvecs")
	os.WriteFile(path, []byte{}, 0o644)
	_, err := LoadFvecs(path)
	if err == nil {
		t.Error("expected error for empty fvecs file")
	}
}

func TestVec_SubSlice(t *testing.T) {
	vecs := &Vectors{
		Data: []float32{1, 2, 3, 4, 5, 6},
		N:    2,
		Dim:  3,
	}
	v0 := vecs.Vec(0)
	v1 := vecs.Vec(1)
	if v0[0] != 1 || v0[2] != 3 {
		t.Errorf("Vec(0) = %v, want [1,2,3]", v0)
	}
	if v1[0] != 4 || v1[2] != 6 {
		t.Errorf("Vec(1) = %v, want [4,5,6]", v1)
	}
}
