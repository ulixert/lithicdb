package simd

// init wires the architecture-specific kernel into L2SquaredFloat32 if one
// is available. Now leaves it empty by design — no SIMD kernels exist
// yet, so L2SquaredFloat32 retains its default value (l2SquaredGeneric)
// from distance.go.
//
// Later we will add the ARM64 NEON branch, and the AMD64 AVX2
// branch. The shape becomes:
//
//	if hasAVX2() {
//	    L2SquaredFloat32 = l2SquaredAVX2
//	    return
//	}
//	if hasNEON() {
//	    L2SquaredFloat32 = l2SquaredNEON
//	    return
//	}
//
// The cpu_*.go helpers (hasAVX2, hasNEON) are already in place so adding
// the branches is purely additive — no test or scaffolding changes needed.
func init() {
	// Intentionally empty for now.
}
