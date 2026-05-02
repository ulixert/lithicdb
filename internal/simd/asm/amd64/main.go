//go:build ignore

// Code generator for the AMD64 AVX2+FMA L2-squared distance kernel.
// Run with `go run ./internal/simd/asm/amd64 -out internal/simd/l2_amd64.s
// -stubs internal/simd/l2_amd64.go -pkg simd` from the module root.
//
// Algorithm: 16 float32s per body iteration (2× YMM accumulators to break
// the FMA dependency chain), an 8-float fast-tail loop for one extra YMM
// chunk, and a scalar 0-7 element loop at the end. Loads are unaligned
// (VMOVUPS) because Go does not guarantee 32-byte alignment of slice
// backing arrays. Reduction uses a shuffle tree (NOT VHADDPS, which is
// microcoded on most Intel µarchs).
package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	ConstraintExpr("amd64,!purego")

	TEXT("l2SquaredAVX2", NOSPLIT, "func(a, b []float32) float32")
	Doc("l2SquaredAVX2 returns the squared Euclidean distance between a and b.",
		"It assumes len(a) == len(b) and AVX2+FMA support; the caller (dispatch.go)",
		"verifies hasAVX2() before wiring it in.")

	aPtr := Load(Param("a").Base(), GP64())
	bPtr := Load(Param("b").Base(), GP64())
	n := Load(Param("a").Len(), GP64())

	// Two YMM accumulators (8×float32 each) to break the FMA dep chain.
	acc0 := YMM()
	acc1 := YMM()
	VXORPS(acc0, acc0, acc0)
	VXORPS(acc1, acc1, acc1)

	// === 16-float body loop ===
	Label("loop16")
	CMPQ(n, U32(16))
	JL(LabelRef("after16"))

	a0 := YMM()
	a1 := YMM()
	b0 := YMM()
	b1 := YMM()
	VMOVUPS(Mem{Base: aPtr}, a0)
	VMOVUPS(Mem{Base: aPtr, Disp: 32}, a1)
	VMOVUPS(Mem{Base: bPtr}, b0)
	VMOVUPS(Mem{Base: bPtr, Disp: 32}, b1)

	// diff = a - b (in-place into a0/a1, no longer needed)
	VSUBPS(b0, a0, a0)
	VSUBPS(b1, a1, a1)

	// acc += diff * diff
	VFMADD231PS(a0, a0, acc0)
	VFMADD231PS(a1, a1, acc1)

	ADDQ(U32(64), aPtr)
	ADDQ(U32(64), bPtr)
	SUBQ(U32(16), n)
	JMP(LabelRef("loop16"))

	// === 8-float fast-tail iteration ===
	Label("after16")
	CMPQ(n, U32(8))
	JL(LabelRef("reduce"))

	a8 := YMM()
	b8 := YMM()
	VMOVUPS(Mem{Base: aPtr}, a8)
	VMOVUPS(Mem{Base: bPtr}, b8)
	VSUBPS(b8, a8, a8)
	VFMADD231PS(a8, a8, acc0)
	ADDQ(U32(32), aPtr)
	ADDQ(U32(32), bPtr)
	SUBQ(U32(8), n)

	// === Reduce two YMM accumulators to one scalar in xmmAcc[0] ===
	Label("reduce")
	VADDPS(acc1, acc0, acc0) // 8 partial sums in acc0

	// Fold the high 128 bits into the low.
	hi := XMM()
	VEXTRACTF128(U8(1), acc0, hi)
	low := acc0.AsX()    // low 128 of acc0 alias
	VADDPS(hi, low, low) // 4 partial sums in low

	// Pairwise: shuffle [a,b,c,d] to [c,d,_,_] and add → [a+c, b+d, _, _]
	tmp := XMM()
	VPERMILPS(U8(0xee), low, tmp)
	VADDPS(tmp, low, low)

	// Pairwise again: shuffle [s0,s1,_,_] to [s1,_,_,_] and add → low[0] = sum
	VPERMILPS(U8(0x55), low, tmp)
	VADDPS(tmp, low, low) // low[0] now holds the total of the vector body

	// === Scalar tail: 0-7 floats remaining, accumulate into low[0] ===
	Label("tail")
	TESTQ(n, n)
	JZ(LabelRef("done"))

	asx := XMM()
	bsx := XMM()
	Label("tail_loop")
	VMOVSS(Mem{Base: aPtr}, asx)
	VMOVSS(Mem{Base: bPtr}, bsx)
	VSUBSS(bsx, asx, asx)
	VFMADD231SS(asx, asx, low) // low[0] += diff*diff
	ADDQ(U32(4), aPtr)
	ADDQ(U32(4), bPtr)
	DECQ(n)
	JNZ(LabelRef("tail_loop"))

	Label("done")
	Store(low, ReturnIndex(0))
	RET()

	Generate()
}
