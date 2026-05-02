//go:build arm64 && !purego

package simd

import "golang.org/x/sys/cpu"

// hasAVX2 exists for the dispatch ladder; on arm64 it is always false.
func hasAVX2() bool { return false }

// hasNEON reports whether the running CPU supports NEON (Advanced SIMD).
//
// AArch64 mandates ASIMD support architecturally, so this is effectively
// always true on arm64; the check is a defensive formality and a hook for
// hardware that disables it via CPU feature gating.
func hasNEON() bool { return cpu.ARM64.HasASIMD }
