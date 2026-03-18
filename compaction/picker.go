package compaction

import (
	"bytes"

	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/sstable"
)

// Config controls compaction behavior.
type Config struct {
	// L0CompactionTrigger is the number of L0 files that triggers
	// an L0 → L1 compaction.
	L0CompactionTrigger int

	// LevelSizeBase is the target size for L1 in bytes.
	// Each subsequent level is LevelSizeMultiplier × the previous level.
	LevelSizeBase int64

	// LevelSizeMultiplier is the size ratio between adjacent levels.
	// Default is 10 (L1=256MB, L2=2.5GB, L3=25GB, ...).
	LevelSizeMultiplier int

	// MaxLevels is the maximum number of levels (including L0).
	MaxLevels int
}

// DefaultConfig returns reasonable compaction defaults.
func DefaultConfig() Config {
	return Config{
		L0CompactionTrigger: 4,
		LevelSizeBase:       256 * 1024 * 1024, // 256MB
		LevelSizeMultiplier: 10,
		MaxLevels:           7,
	}
}

// CompactionTask describes a single compaction operation: merge
// files from an input level into an output level.
type CompactionTask struct {
	// InputLevel is the level being compacted from (0 for L0→L1).
	InputLevel int

	// OutputLevel is the level being compacted into (InputLevel + 1).
	OutputLevel int

	// Inputs are the SSTable handles from InputLevel to compact.
	Inputs []*sstable.TableHandle

	// Overlapping are the SSTable handles from OutputLevel that
	// overlap with the input key range and must be included in the merge.
	Overlapping []*sstable.TableHandle

	// KeyRange is the union key range of all inputs.
	MinKey []byte
	MaxKey []byte
}

// AllInputs returns all handles involved in this compaction
// (both input level and overlapping output level files).
func (t *CompactionTask) AllInputs() []*sstable.TableHandle {
	result := make([]*sstable.TableHandle, 0, len(t.Inputs)+len(t.Overlapping))
	result = append(result, t.Inputs...)
	result = append(result, t.Overlapping...)
	return result
}

// LevelState holds the SSTable handles for each level.
// L0 is stored separately because it has overlapping key ranges.
type LevelState struct {
	L0     []*sstable.TableHandle   // newest first
	Levels [][]*sstable.TableHandle // index 0 = L1, sorted by first key
}

// LevelSize returns the total file size for a level (by summing
// data in each reader). Level 0 is index 0 in the Levels array.
// For L0, call L0Size().
func (ls *LevelState) LevelSize(level int) int64 {
	if level <= 0 || level-1 >= len(ls.Levels) {
		return 0
	}
	var total int64
	for _, h := range ls.Levels[level-1] {
		total += int64(h.Reader.FileSize())
	}
	return total
}

// L0Size returns the total file size of all L0 SSTables.
func (ls *LevelState) L0Size() int64 {
	var total int64
	for _, h := range ls.L0 {
		total += int64(h.Reader.FileSize())
	}
	return total
}

// PickCompaction examines the current LSM state and returns a
// compaction task, or nil if no compaction is needed.
//
// Priority:
// 1. L0 → L1 if L0 file count exceeds the trigger
// 2. Ln → Ln+1 if Ln size exceeds its target
func PickCompaction(state *LevelState, cfg Config) *CompactionTask {
	// Priority 1: L0 → L1 compaction
	if len(state.L0) >= cfg.L0CompactionTrigger {
		return pickL0Compaction(state, cfg)
	}

	// Priority 2: level size overflow
	levelTarget := cfg.LevelSizeBase
	for level := 1; level < cfg.MaxLevels-1; level++ {
		if state.LevelSize(level) > levelTarget {
			return pickLevelCompaction(state, level, cfg)
		}
		levelTarget *= int64(cfg.LevelSizeMultiplier)
	}

	return nil
}

// pickL0Compaction selects all L0 files and all overlapping L1 files.
func pickL0Compaction(state *LevelState, cfg Config) *CompactionTask {
	if len(state.L0) == 0 {
		return nil
	}

	// All L0 files are inputs (they may overlap each other)
	inputs := make([]*sstable.TableHandle, len(state.L0))
	copy(inputs, state.L0)

	// Compute the union key range of all L0 files
	minKey, maxKey := keyRange(inputs)

	// Find all overlapping L1 files
	var overlapping []*sstable.TableHandle
	if len(state.Levels) > 0 {
		overlapping = findOverlapping(state.Levels[0], minKey, maxKey)
	}

	return &CompactionTask{
		InputLevel:  0,
		OutputLevel: 1,
		Inputs:      inputs,
		Overlapping: overlapping,
		MinKey:      minKey,
		MaxKey:      maxKey,
	}
}

// pickLevelCompaction selects one file from the given level and all
// overlapping files from the next level.
func pickLevelCompaction(state *LevelState, level int, cfg Config) *CompactionTask {
	if level-1 >= len(state.Levels) || len(state.Levels[level-1]) == 0 {
		return nil
	}

	outputLevel := level + 1

	// Pick the first file in the level (oldest by position).
	// A smarter picker would choose the file with the most overlap
	// into the next level, but this is correct and simple.
	input := state.Levels[level-1][0]
	inputs := []*sstable.TableHandle{input}

	minKey := input.Reader.FirstKey()
	maxKey := input.Reader.LastKey()

	// Find overlapping files in the output level
	var overlapping []*sstable.TableHandle
	if outputLevel-1 < len(state.Levels) {
		overlapping = findOverlapping(state.Levels[outputLevel-1], minKey, maxKey)
	}

	return &CompactionTask{
		InputLevel:  level,
		OutputLevel: outputLevel,
		Inputs:      inputs,
		Overlapping: overlapping,
		MinKey:      minKey,
		MaxKey:      maxKey,
	}
}

// keyRange computes the union key range across all handles.
func keyRange(handles []*sstable.TableHandle) (minKey, maxKey []byte) {
	for _, h := range handles {
		fk := h.Reader.FirstKey()
		lk := h.Reader.LastKey()

		if minKey == nil || bytes.Compare(fk, minKey) < 0 {
			minKey = fk
		}
		if maxKey == nil || bytes.Compare(lk, maxKey) > 0 {
			maxKey = lk
		}
	}
	return
}

// findOverlapping returns all handles in a sorted level whose key
// range overlaps with [minKey, maxKey]. Handles in L1+ are sorted
// by the first key and have non-overlapping ranges, so we can use
// binary-search-like logic.
func findOverlapping(level []*sstable.TableHandle, minKey, maxKey []byte) []*sstable.TableHandle {
	var result []*sstable.TableHandle

	for _, h := range level {
		hFirst := h.Reader.FirstKey()
		hLast := h.Reader.LastKey()

		// Two ranges [minKey, maxKey] and [hFirst, hLast] overlap iff:
		//   hFirst <= maxKey AND hLast >= minKey
		//
		// Compare user keys for overlap detection, since internal key
		// ordering within the same user key shouldn't affect whether
		// two SSTables' ranges overlap.
		if bytes.Compare(kv.UserKey(hFirst), kv.UserKey(maxKey)) <= 0 &&
			bytes.Compare(kv.UserKey(hLast), kv.UserKey(minKey)) >= 0 {
			result = append(result, h)
		}
	}

	return result
}
