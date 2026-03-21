package compaction

import (
	"bytes"
	"fmt"

	"github.com/ulixert/lithicdb/iterator"
	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/sstable"
)

// CompactionResult describes the output of a compaction.
type CompactionResult struct {
	// Task is the original compaction task.
	Task *CompactionTask

	// NewTables are the SSTable readers produced by compaction.
	// They should be added to the output level.
	NewTables []*sstable.Reader

	// NewTableIDs are the IDs of the new SSTables (for manifest).
	NewTableIDs []uint64
}

// IDAllocator provides monotonically increasing SSTable IDs.
// The DB engine implements this by using its nextMemID counter
// (SSTable IDs and memtable IDs share the same namespace to
// avoid collisions).
type IDAllocator interface {
	NextID() uint64
}

// Execute runs a compaction task: merges all input files into new
// SSTables at the output level.
//
// The executor:
// 1. Creates iterators over all input files
// 2. Merges them with MergeIterator (which emits ALL versions)
// 3. Applies MVCC version GC based on the watermark
// 4. Writes output SSTables, splitting at targetFileSize boundaries
// 5. Returns the new readers (caller handles manifest + state updates)
//
// Version GC rules (per user key):
//   - Always keep the newest version
//   - Keep all versions with seq >= watermark (active snapshots need them)
//   - Keep one version just below watermark (the "last visible" for the oldest snapshot)
//   - Drop everything else
//   - Tombstones at the bottommost level with seq < watermark are dropped
//     (no older data below to shadow)
//
// When watermark is 0 (no active snapshots), all old versions and
// bottommost tombstones are dropped aggressively.
//
// The executor does NOT modify any shared state - it only reads input
// files and writes new files. The caller is responsible for atomically
// updating the manifest and LSM state.
func Execute(
	task *CompactionTask,
	dir string,
	blockSize int,
	targetFileSize int64,
	alloc IDAllocator,
	cache *sstable.BlockCache,
	watermark uint64,
) (*CompactionResult, error) {
	// Build iterators over all inputs.
	// Input level files come first (higher priority = newer data).
	iters := make([]iterator.Iterator, 0, len(task.Inputs)+len(task.Overlapping))

	for _, h := range task.Inputs {
		iters = append(iters, h.Reader.Scan())
	}
	for _, h := range task.Overlapping {
		iters = append(iters, h.Reader.Scan())
	}

	mergeIter := iterator.NewMergeIterator(iters)
	defer mergeIter.Close()

	var (
		newTables []*sstable.Reader
		newIDs    []uint64
		builder   *sstable.Builder
		currentID uint64
	)

	// finishCurrentBuilder flushes the current SSTable builder.
	finishCurrentBuilder := func() error {
		if builder == nil {
			return nil
		}

		if err := builder.Finish(); err != nil {
			return fmt.Errorf("compaction: finish SSTable %d: %w", currentID, err)
		}

		reader, err := sstable.OpenReader(dir, currentID, cache)
		if err != nil {
			return fmt.Errorf("compaction: open new SSTable %d: %w", currentID, err)
		}

		newTables = append(newTables, reader)
		newIDs = append(newIDs, currentID)
		builder = nil

		return nil
	}

	// startNewBuilder creates a new SSTable builder with a fresh ID.
	startNewBuilder := func() {
		currentID = alloc.NextID()
		builder = sstable.NewBuilder(dir, currentID, blockSize)
	}

	// Version GC state tracked per user key.
	var (
		lastUserKey        []byte
		keptBelowWatermark bool
	)

	for mergeIter.IsValid() {
		key := mergeIter.Key()
		val := mergeIter.Value()

		userKey := kv.UserKey(key)
		seq := kv.SeqNum(key)
		isTombstone := val == nil

		// Detect user key boundary.
		isFirstForKey := !bytes.Equal(userKey, lastUserKey)
		if isFirstForKey {
			lastUserKey = append(lastUserKey[:0], userKey...)
			keptBelowWatermark = false
		}

		// Decide whether to keep this version.
		keep := shouldKeep(isFirstForKey, seq, watermark, keptBelowWatermark)

		if keep && seq < watermark {
			keptBelowWatermark = true
		}

		if keep {
			// Drop tombstones at the bottommost level when no snapshot needs them.
			// watermark=0 means no active snapshots, so all tombstones are safe to drop.
			if isTombstone && task.IsBottommost && (watermark == 0 || seq < watermark) {
				mergeIter.Next()
				continue
			}

			var value kv.Value
			if isTombstone {
				value = kv.NewTombstone()
			} else {
				value = kv.NewValue(val)
			}

			if builder == nil {
				startNewBuilder()
			}

			if err := builder.Add(key, value); err != nil {
				return nil, fmt.Errorf("compaction: add key: %w", err)
			}

			// Split into a new SSTable if the current one is large enough
			if builder.EstimatedSize() >= uint64(targetFileSize) {
				if err := finishCurrentBuilder(); err != nil {
					return nil, err
				}
			}
		}

		mergeIter.Next()
	}

	if err := mergeIter.Err(); err != nil {
		return nil, fmt.Errorf("compaction: merge iterate: %w", err)
	}

	// Finish the last SSTable
	if builder != nil {
		if err := finishCurrentBuilder(); err != nil {
			return nil, err
		}
	}

	return &CompactionResult{
		Task:        task,
		NewTables:   newTables,
		NewTableIDs: newIDs,
	}, nil
}

// shouldKeep determines whether a version should be retained during
// compaction.
//
// Rules:
//  1. Always keep the newest version of each user key.
//  2. Keep versions with seq >= watermark (an active snapshot may read them).
//  3. Keep one version just below the watermark (the "last visible" version
//     for the oldest active snapshot).
//  4. Drop everything else.
func shouldKeep(isFirstForKey bool, seq uint64, watermark uint64, keptBelowWatermark bool) bool {
	if isFirstForKey {
		return true
	}
	// watermark=0 means no active snapshots - only keep the newest version.
	if watermark == 0 {
		return false
	}
	if seq >= watermark {
		return true
	}
	if !keptBelowWatermark {
		return true
	}
	return false
}
