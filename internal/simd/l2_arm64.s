// L2-squared distance kernel for ARM64 NEON.
// Validated against l2SquaredGeneric in distance_test.go.
//
// Algorithm: 8 float32s per body iteration, two accumulators to break
// the FMA dependency chain.
//
//   for i in 0..n step 8:
//     V2,V3 = a[i:i+8]            ; VLD1.P
//     V4,V5 = b[i:i+8]            ; VLD1.P
//     V2 = V2 - V4                ; VFSUB (encoded via WORD)
//     V3 = V3 - V5                ; VFSUB (encoded via WORD)
//     V0 += V2 * V2               ; VFMLA
//     V1 += V3 * V3               ; VFMLA
//   V0 = V0 + V1                  ; VFADD (encoded via WORD)
//   V0 = pairwise(V0,V0)          ; VFADDP S4 (encoded via WORD)
//   F0 = V0[0] + V0[1]            ; FADDP scalar S (encoded via WORD)
//   return F0
//
// Plan 9 assembler limitation: VFADD, VFSUB, and VFADDP for the .S4/.2S
// forms are not implemented (they are listed as TODO in
// cmd/asm/internal/asm/testdata/arm64enc.s). They are emitted via WORD
// directives. The encoding for each is computed inline; verify against
// the AArch64 ARM (DDI 0487) before changing register allocations.

#include "textflag.h"

// func l2SquaredNEONBody(a, b unsafe.Pointer, n int) float32
TEXT ·l2SquaredNEONBody(SB), NOSPLIT, $0-28
	MOVD	a+0(FP), R0
	MOVD	b+8(FP), R1
	MOVD	n+16(FP), R2

	// Zero the two accumulator vectors (4×float32 each).
	VEOR	V0.B16, V0.B16, V0.B16
	VEOR	V1.B16, V1.B16, V1.B16

loop:
	// Load 8 floats from a and 8 from b, post-incrementing both pointers.
	VLD1.P	32(R0), [V2.S4, V3.S4]
	VLD1.P	32(R1), [V4.S4, V5.S4]

	// VFSUB V4.S4, V2.S4, V2.S4 ; V2 = V2 - V4
	// Encoding: 0x4EA0_D400 | (Rm<<16) | (Rn<<5) | Rd
	//         = 0x4EA0_D400 | (4<<16) | (2<<5) | 2 = 0x4EA4_D442
	WORD	$0x4EA4D442
	// VFSUB V5.S4, V3.S4, V3.S4 ; V3 = V3 - V5
	//         = 0x4EA0_D400 | (5<<16) | (3<<5) | 3 = 0x4EA5_D463
	WORD	$0x4EA5D463

	// V0 += V2 * V2 ; V1 += V3 * V3 (FMA, fully inlinable in Plan 9 asm)
	VFMLA	V2.S4, V2.S4, V0.S4
	VFMLA	V3.S4, V3.S4, V1.S4

	SUB	$8, R2, R2
	CBNZ	R2, loop

	// Reduction: V0 (4 partial sums) + V1 (4 partial sums) → scalar.
	// Step 1: V0 = V0 + V1
	// VFADD V1.S4, V0.S4, V0.S4
	// Encoding: 0x4E20_D400 | (Rm<<16) | (Rn<<5) | Rd
	//         = 0x4E20_D400 | (1<<16) | 0 | 0 = 0x4E21_D400
	WORD	$0x4E21D400
	// Step 2: pairwise add adjacent lanes of V0 with itself.
	// VFADDP V0.S4, V0.S4, V0.S4
	// Result: V0[0] = orig[0]+orig[1], V0[1] = orig[2]+orig[3] (and the
	// upper half holds duplicates we don't read).
	// Encoding: 0x6E20_D400 | (Rm<<16) | (Rn<<5) | Rd = 0x6E20_D400
	WORD	$0x6E20D400
	// Step 3: scalar pairwise add of the bottom 2 lanes.
	// FADDP S0, V0.2S
	// Result: S0 = V0[0] + V0[1] = total sum.
	// Encoding: 0x7E30_D800 | (Rn<<5) | Rd = 0x7E30_D800
	WORD	$0x7E30D800

	FMOVS	F0, ret+24(FP)
	RET
