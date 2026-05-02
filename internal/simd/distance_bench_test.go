package simd

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// sinkFloat32 prevents dead-code elimination from hoisting the kernel call
// out of benchmark loops. Without it, some Go versions can fold the
// repeated computation into a single iteration and report misleading
// throughput numbers.
var sinkFloat32 float32

// benchDims covers SIFT-1M (128) plus the embedding sizes that production
// vector workloads commonly hit (384/768/1536). Used for both the kernel
// chart in blog #15 and as a smoke benchmark in CI.
var benchDims = []int{128, 384, 768, 1536}

func BenchmarkL2Squared(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, dim := range benchDims {
		x := makeRandomVector(rng, dim, 1)
		y := makeRandomVector(rng, dim, 1)
		b.Run(fmt.Sprintf("dim=%d", dim), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(dim * 2 * 4)) // two float32 vectors loaded per call
			b.ResetTimer()
			var sum float32
			for b.Loop() {
				sum += L2SquaredFloat32(x, y)
			}
			sinkFloat32 = sum
		})
	}
}
