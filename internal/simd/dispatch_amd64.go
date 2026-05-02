//go:build amd64 && !purego

package simd

func init() {
	if hasAVX2() {
		L2SquaredFloat32 = l2SquaredAVX2
	}
}
