package hnsw

import "math"

// DistanceFunc computes the distance between two vectors of equal length.
// Smaller values indicate more similar vectors. Callers must ensure both
// slices have the same length; no bounds checking is performed here
// because these functions sit in the innermost loop of search and insert.
type DistanceFunc func(a, b []float32) float32

// DistanceL2Squared returns the squared Euclidean distance between a and b.
// Using squared distance avoids an expensive sqrt while preserving ordering.
func DistanceL2Squared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
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
