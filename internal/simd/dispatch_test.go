package simd

import (
	"math"
	"math/rand/v2"
	"testing"
)

// l2SquaredReference is the float64-accumulating truth oracle. SIMD kernels
// (and l2SquaredGeneric itself) are compared against this. Using float64
// internally minimizes accumulation noise so the tolerance check has a
// stable target to validate against.
func l2SquaredReference(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(sum)
}

// closeEnough reports whether got is within the SIMD-tolerant numerical
// envelope of want. The bound is scale-aware: 1e-5 absolute floor, scaled
// up with |want| to absorb the rounding differences SIMD reordered
// summation and FMA produce on large dimensions and large values.
func closeEnough(got, want float32) bool {
	diff := math.Abs(float64(got - want))
	scale := math.Max(1, math.Abs(float64(want)))
	return diff <= 1e-5*scale
}

// boundaryLengths covers the off-by-one cluster zones around chunk
// boundaries for both AVX2 (16 floats/iter) and NEON (8 floats/iter),
// plus the production embedding sizes the kernels are expected to handle.
var boundaryLengths = []int{
	0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33,
	127, 128, 129, 384, 768, 1536,
}

func makeRandomVector(rng *rand.Rand, n int, scale float32) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = (rng.Float32()*2 - 1) * scale
	}
	return v
}

func TestL2SquaredAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xC0FFEE, 0xDEADBEEF))
	const trialsPerLen = 100
	for _, n := range boundaryLengths {
		for trial := range trialsPerLen {
			a := makeRandomVector(rng, n, 100)
			b := makeRandomVector(rng, n, 100)
			got := L2SquaredFloat32(a, b)
			want := l2SquaredReference(a, b)
			if !closeEnough(got, want) {
				t.Fatalf("len=%d trial=%d: got=%g want=%g (diff=%g)",
					n, trial, got, want, got-want)
			}
		}
	}
}

func TestL2SquaredZeroVectors(t *testing.T) {
	for _, n := range []int{0, 1, 8, 128, 1536} {
		a := make([]float32, n)
		b := make([]float32, n)
		if got := L2SquaredFloat32(a, b); got != 0 {
			t.Errorf("len=%d zero vectors: got=%g, want 0", n, got)
		}
	}
}

func TestL2SquaredIdenticalVectors(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	for _, n := range []int{1, 8, 16, 128, 1536} {
		a := makeRandomVector(rng, n, 10)
		if got := L2SquaredFloat32(a, a); got != 0 {
			t.Errorf("len=%d a=a: got=%g, want 0", n, got)
		}
	}
}

func TestL2SquaredNoAllocs(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	a := makeRandomVector(rng, 128, 1)
	b := makeRandomVector(rng, 128, 1)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = L2SquaredFloat32(a, b)
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocations, got %v", allocs)
	}
}

// FuzzL2Squared compares L2SquaredFloat32 against l2SquaredReference on
// arbitrary float32 byte payloads. Catches tail-loop and length-edge bugs
// that the explicit boundaryLengths list might miss.
//
// NaN / Inf inputs are skipped: any (got != want) check on those would
// trigger on every fuzz iteration regardless of kernel correctness.
func FuzzL2Squared(f *testing.F) {
	seed := func(n int) []byte {
		b := make([]byte, n*4)
		for i := range b {
			b[i] = byte(i*131 + 7)
		}
		return b
	}
	f.Add(seed(0))
	f.Add(seed(1))
	f.Add(seed(8))
	f.Add(seed(15))
	f.Add(seed(16))
	f.Add(seed(17))
	f.Add(seed(128))
	f.Add(seed(129))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Use the same byte stream for both vectors with a fixed offset shuffle,
		// so the fuzzer effectively explores (a, b) pairs of equal length.
		n := len(data) / 8
		if n == 0 {
			return
		}
		a := bytesToFloats(data[:n*4])
		b := bytesToFloats(data[n*4 : n*8])
		if hasNonFinite(a) || hasNonFinite(b) {
			return
		}
		want := l2SquaredReference(a, b)
		// Skip overflow cases: if the truth oracle itself overflowed to Inf
		// or produced NaN, the tolerance check is meaningless. SIMD kernels
		// are never expected to produce meaningful results in that regime.
		wantF := float64(want)
		if math.IsNaN(wantF) || math.IsInf(wantF, 0) {
			return
		}
		got := L2SquaredFloat32(a, b)
		if !closeEnough(got, want) {
			t.Fatalf("len=%d got=%g want=%g (diff=%g)", n, got, want, got-want)
		}
	})
}

func bytesToFloats(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	for i := range n {
		bits := uint32(b[i*4]) |
			uint32(b[i*4+1])<<8 |
			uint32(b[i*4+2])<<16 |
			uint32(b[i*4+3])<<24
		out[i] = math.Float32frombits(bits)
	}
	return out
}

func hasNonFinite(v []float32) bool {
	for _, x := range v {
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return true
		}
	}
	return false
}
