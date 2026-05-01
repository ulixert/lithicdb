//go:build amd64 && !purego

package simd

import "golang.org/x/sys/cpu"

// hasAVX2 reports whether the running CPU supports both AVX2 and FMA3.
//
// AVX2 implies SSE/AVX support, but FMA3 is technically a separate CPUID
// feature. Both have been universal on consumer x86 hardware since Haswell
// (2013), but the conjunction guards against weird VMs and older Xeons that
// report AVX2 without FMA — where VFMADD231PS would raise SIGILL.
//
// cpu.X86.HasAVX2 is set only when CPUID reports the feature AND the OS
// has enabled YMM register state, so this also gates on OS support.
func hasAVX2() bool { return cpu.X86.HasAVX2 && cpu.X86.HasFMA }

// hasNEON exists for the dispatch ladder; on amd64 it is always false.
func hasNEON() bool { return false }
