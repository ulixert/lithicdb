//go:build purego || (!amd64 && !arm64)

package simd

// hasAVX2 returns false on architectures without an AVX2 path, and on any
// arch when -tags=purego forces the generic fallback.
func hasAVX2() bool { return false }

// hasNEON returns false on architectures without a NEON path, and on any
// arch when -tags=purego forces the generic fallback.
func hasNEON() bool { return false }
