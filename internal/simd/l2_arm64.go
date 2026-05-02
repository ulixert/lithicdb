//go:build arm64 && !purego

package simd

import "unsafe"

// l2SquaredNEON computes the squared Euclidean distance between a and b
// using ARM64 NEON. The 8-float-aligned body is dispatched to assembly
// (l2_arm64.s); the 0–7-element scalar tail is handled here in Go so the
// assembly stays short and easy to audit.
func l2SquaredNEON(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}
	body := n &^ 7 // round down to multiple of 8
	var sum float32
	if body > 0 {
		sum = l2SquaredNEONBody(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), body)
	}
	for i := body; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// l2SquaredNEONBody is implemented in l2_arm64.s. n must be a positive
// multiple of 8; a and b must each point to at least n float32 elements.
//
//go:noescape
func l2SquaredNEONBody(a, b unsafe.Pointer, n int) float32
