package eval

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

// Vectors hold a collection of float32 vectors in row-major layout.
type Vectors struct {
	Data []float32 // row-major: Data[i*Dim ... (i+1)*Dim] is vector i
	N    int       // number of vectors
	Dim  int       // dimensionality
}

// Vec returns vector i as a sub-slice of Data. No copy is made.
func (v *Vectors) Vec(i int) []float32 {
	return v.Data[i*v.Dim : (i+1)*v.Dim]
}

// GroundTruth holds k-nearest-neighbor IDs in row-major layout.
type GroundTruth struct {
	IDs []int32 // row-major: IDs[i*K ... (i+1)*K] is the neighbors of query i
	N   int     // number of queries
	K   int     // neighbors per query
}

// Neighbors return the ground-truth neighbor IDs for query i.
func (gt *GroundTruth) Neighbors(i int) []int32 {
	return gt.IDs[i*gt.K : (i+1)*gt.K]
}

// LoadFvecs loads vectors from a file in the fvecs binary format.
// Format: repeated records of [dim:int32_LE][v0:float32_LE]...[v_{dim-1}:float32_LE].
func LoadFvecs(path string) (*Vectors, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("fvecs: file too small (%d bytes)", len(data))
	}

	dim := int(binary.LittleEndian.Uint32(data[:4]))
	if dim <= 0 {
		return nil, fmt.Errorf("fvecs: invalid dimension %d", dim)
	}
	recordSize := 4 + dim*4
	if len(data)%recordSize != 0 {
		return nil, fmt.Errorf("fvecs: file size %d not divisible by record size %d", len(data), recordSize)
	}

	n := len(data) / recordSize
	vecs := make([]float32, n*dim)
	for i := 0; i < n; i++ {
		offset := i*recordSize + 4 // skip dim prefix
		for j := 0; j < dim; j++ {
			bits := binary.LittleEndian.Uint32(data[offset+j*4:])
			vecs[i*dim+j] = math.Float32frombits(bits)
		}
	}

	return &Vectors{Data: vecs, N: n, Dim: dim}, nil
}

// LoadIvecs load ground-truth neighbor IDs from a file in the ivecs format.
// Format: repeated records of [k:int32_LE][id0:int32_LE]...[id_{k-1}:int32_LE].
func LoadIvecs(path string) (*GroundTruth, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("ivecs: file too small (%d bytes)", len(data))
	}

	k := int(binary.LittleEndian.Uint32(data[:4]))
	if k <= 0 {
		return nil, fmt.Errorf("ivecs: invalid k %d", k)
	}
	recordSize := 4 + k*4
	if len(data)%recordSize != 0 {
		return nil, fmt.Errorf("ivecs: file size %d not divisible by record size %d", len(data), recordSize)
	}

	n := len(data) / recordSize
	ids := make([]int32, n*k)
	for i := 0; i < n; i++ {
		offset := i*recordSize + 4 // skip k prefix
		for j := 0; j < k; j++ {
			ids[i*k+j] = int32(binary.LittleEndian.Uint32(data[offset+j*4:]))
		}
	}

	return &GroundTruth{IDs: ids, N: n, K: k}, nil
}

// WriteFvecs writes vectors to a file in fvecs format. Used for testing.
func WriteFvecs(path string, vecs *Vectors) error {
	recordSize := 4 + vecs.Dim*4
	data := make([]byte, vecs.N*recordSize)
	for i := 0; i < vecs.N; i++ {
		offset := i * recordSize
		binary.LittleEndian.PutUint32(data[offset:], uint32(vecs.Dim))
		for j := 0; j < vecs.Dim; j++ {
			binary.LittleEndian.PutUint32(data[offset+4+j*4:], math.Float32bits(vecs.Vec(i)[j]))
		}
	}
	return os.WriteFile(path, data, 0o644)
}

// WriteIvecs writes ground truth to a file in ivecs format. Used for testing.
func WriteIvecs(path string, gt *GroundTruth) error {
	recordSize := 4 + gt.K*4
	data := make([]byte, gt.N*recordSize)
	for i := 0; i < gt.N; i++ {
		offset := i * recordSize
		binary.LittleEndian.PutUint32(data[offset:], uint32(gt.K))
		for j := 0; j < gt.K; j++ {
			binary.LittleEndian.PutUint32(data[offset+4+j*4:], uint32(gt.Neighbors(i)[j]))
		}
	}
	return os.WriteFile(path, data, 0o644)
}
