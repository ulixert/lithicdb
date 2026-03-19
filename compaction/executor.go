package compaction

import (
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
// 2. Merges them with the standard MergeIterator (the newest version wins)
// 3. Writes output SSTables, splitting at targetFileSize boundaries
// 4. Returns the new readers (caller handles manifest + state updates)
//
// The executor does NOT modify any shared state - it only reads input
// files and writes new files. The caller is responsible for atomically
// updating the manifest and LSM state.
func Execute(task *CompactionTask, dir string, blockSize int, targetFileSize int64, alloc IDAllocator, cache *sstable.BlockCache) (*CompactionResult, error) {
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

	for mergeIter.IsValid() {
		key := mergeIter.Key()
		val := mergeIter.Value()

		var value kv.Value
		if val == nil {
			// Tombstone: in L0→L1 compaction we must keep tombstones
			// because older data may exist in lower levels. In the
			// deepest level compaction, tombstones can be dropped -
			// but that optimization is deferred until MVCC-aware
			// compaction (which needs to check active snapshots).
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
