//go:build arm64 && !purego

package simd

func init() {
	if hasNEON() {
		L2SquaredFloat32 = l2SquaredNEON
	}
}
