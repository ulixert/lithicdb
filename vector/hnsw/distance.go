package hnsw

import (
	"math"

	"github.com/ulixert/theseon/internal/simd"
)

// DistanceFunc computes the distance between two vectors of equal length.
// Smaller values indicate more similar vectors. Callers must ensure both
// slices have the same length; no bounds checking is performed here
// because these functions sit in the innermost loop of search and insert.
type DistanceFunc func(a, b []float32) float32

// DistanceL2Squared returns the squared Euclidean distance between a and b.
// The implementation delegates to internal/simd, which picks the best
// kernel at init() time (NEON on arm64, AVX2 on amd64, generic Go
// otherwise or under -tags=purego).
//
// DefaultOptions wires Options.Dist directly to simd.L2SquaredFloat32 to
// skip this wrapper in the search hot path; direct callers of this
// function pay one extra indirect call.
func DistanceL2Squared(a, b []float32) float32 {
	return simd.L2SquaredFloat32(a, b)
}

// DistanceCosine returns 1 - cos(a, b), ranging from 0 (identical direction)
// to 2 (opposite direction). Uses float64 intermediates for sqrt precision.
func DistanceCosine(a, b []float32) float32 {
	var dot, normA, normB float64
	for i := range a {
		ai, bi := float64(a[i]), float64(b[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return float32(1.0 - dot/denom)
}

// DistanceInnerProduct returns the negated dot product of a and b.
// Negation converts maximum inner product search into a minimization
// problem so that the min-heap in beam search works correctly.
func DistanceInnerProduct(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}
