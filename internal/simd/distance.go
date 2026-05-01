// Package simd provides architecture-specific SIMD distance kernels for
// HNSW vector search, with a runtime CPU dispatch and a pure-Go fallback.
//
// The package exposes function-valued variables (e.g. L2SquaredFloat32)
// that are reassigned at init() time to the best available implementation
// for the running CPU. The default value is the pure-Go reference, so
// callers always have a working implementation even on architectures with
// no SIMD support.
//
// Build with -tags=purego to force the generic path everywhere, useful
// for verifying the fallback compiles cleanly even when the SIMD assembly
// files are excluded.
package simd

// L2SquaredFloat32 returns the squared Euclidean distance between a and b.
//
// The caller must ensure len(a) == len(b). The function performs no length
// check because it is called from the HNSW search hot path.
//
// At init() time this variable may be reassigned to an architecture-specific
// SIMD kernel; until a kernel is wired in, it is the pure-Go reference
// implementation.
var L2SquaredFloat32 = l2SquaredGeneric
