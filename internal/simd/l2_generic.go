package simd

// l2SquaredGeneric is the pure-Go reference implementation of L2-squared
// distance. It is always compiled (no build tag) so it can serve as:
//
//   - the default value of L2SquaredFloat32 before any dispatch runs;
//   - the fallback when the running CPU lacks the required SIMD features;
//   - the path activated by -tags=purego;
//   - the implementation tests compare scalar tail logic against.
//
// SIMD kernels are compared against l2SquaredReference (in distance_test.go),
// which accumulates in float64 to minimize the rounding noise the tolerance
// harness has to absorb.
func l2SquaredGeneric(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}
