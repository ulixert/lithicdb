//go:build tools

// Package tools anchors build-time tooling dependencies in go.mod so
// `go mod tidy` does not remove them. None of these imports are used in
// production builds (the `tools` build tag is never set), but they
// ensure `github.com/mmcloughlin/avo` and its transitive deps stay in
// go.sum for reproducible asm regeneration.
package tools

import (
	_ "github.com/mmcloughlin/avo/build"
	_ "github.com/mmcloughlin/avo/operand"
)
