package db

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ulixert/lithicdb/compaction"
	"github.com/ulixert/lithicdb/manifest"
	"github.com/ulixert/lithicdb/sstable"
)

// idAllocator adapts DB's nextMemID counter to the
// compaction.IDAllocator interface.
type idAllocator struct {
	db *DB
}

func (a *idAllocator) NextID() uint64 {
	return a.db.nextMemID.Add(1)
}

func (db *DB) compactionLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.compactCh:
			db.runCompaction()
		case <-db.closeCh:
			return
		}
	}
}

func (db *DB) triggerCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

// runCompaction checks if compaction is needed and executes it.
// Loops until no more compactions are needed.
func (db *DB) runCompaction() {
	for {
		db.mu.RLock()
		state := db.buildLevelState()
		db.mu.RUnlock()

		task := compaction.PickCompaction(state, db.opts.Compaction)
		if task == nil {
			return
		}

		// Ref all input handles so they aren't deleted during compaction
		for _, h := range task.AllInputs() {
			h.Ref()
		}

		result, err := compaction.Execute(
			task,
			db.opts.Dir,
			db.opts.BlockSize,
			int64(db.opts.Compaction.LevelSizeBase)/10,
			&idAllocator{db: db},
		)

		// Unref inputs regardless of success/failure
		for _, h := range task.AllInputs() {
			h.Unref()
		}

		if err != nil {
			// TODO: log error
			return
		}

		if err := db.applyCompactionResult(result); err != nil {
			// TODO: log error
			return
		}
	}
}

// buildLevelState creates a snapshot of the current LSM levels for
// the compaction picker. Must be called with mu.RLock held.
func (db *DB) buildLevelState() *compaction.LevelState {
	state := &compaction.LevelState{
		L0:     make([]*sstable.TableHandle, len(db.l0)),
		Levels: make([][]*sstable.TableHandle, len(db.levels)),
	}
	copy(state.L0, db.l0)
	for i, level := range db.levels {
		state.Levels[i] = make([]*sstable.TableHandle, len(level))
		copy(state.Levels[i], level)
	}
	return state
}

// applyCompactionResult atomically updates the manifest and in-memory
// state after a successful compaction.
func (db *DB) applyCompactionResult(result *compaction.CompactionResult) error {
	task := result.Task

	// 1. Add new SSTables to the manifest
	for _, reader := range result.NewTables {
		if err := db.manifest.AddSSTable(manifest.SSTableInfo{
			ID:       reader.ID(),
			Level:    uint8(task.OutputLevel),
			FirstKey: reader.FirstKey(),
			LastKey:  reader.LastKey(),
		}); err != nil {
			return fmt.Errorf("db: manifest add compaction output: %w", err)
		}
	}

	// 2. Remove old SSTables from the manifest
	for _, h := range task.Inputs {
		if err := db.manifest.RemoveSSTable(h.Reader.ID(), uint8(task.InputLevel)); err != nil {
			return fmt.Errorf("db: manifest remove input: %w", err)
		}
	}
	for _, h := range task.Overlapping {
		if err := db.manifest.RemoveSSTable(h.Reader.ID(), uint8(task.OutputLevel)); err != nil {
			return fmt.Errorf("db: manifest remove overlapping: %w", err)
		}
	}

	// 3. Update IDs
	if err := db.manifest.UpdateNextIDs(
		db.nextMemID.Load(),
		db.nextSeq.Load(),
	); err != nil {
		return fmt.Errorf("db: manifest update IDs after compaction: %w", err)
	}

	// 4. Create handles for new SSTables
	newHandles := make([]*sstable.TableHandle, len(result.NewTables))
	for i, reader := range result.NewTables {
		newHandles[i] = sstable.NewTableHandle(reader, db.opts.Dir)
	}

	// 5. Build a set of obsolete SSTable IDs for fast lookup
	obsoleteIDs := make(map[uint64]bool)
	for _, h := range task.AllInputs() {
		obsoleteIDs[h.Reader.ID()] = true
	}

	// 6. Atomically swap in-memory state
	db.mu.Lock()

	if task.InputLevel == 0 {
		newL0 := make([]*sstable.TableHandle, 0, len(db.l0))
		for _, h := range db.l0 {
			if !obsoleteIDs[h.Reader.ID()] {
				newL0 = append(newL0, h)
			}
		}
		db.l0 = newL0
	} else {
		if task.InputLevel < len(db.levels) {
			newLevel := make([]*sstable.TableHandle, 0, len(db.levels[task.InputLevel]))
			for _, h := range db.levels[task.InputLevel] {
				if !obsoleteIDs[h.Reader.ID()] {
					newLevel = append(newLevel, h)
				}
			}
			db.levels[task.InputLevel] = newLevel
		}
	}

	if task.OutputLevel >= 0 && task.OutputLevel < len(db.levels) {
		newLevel := make([]*sstable.TableHandle, 0, len(db.levels[task.OutputLevel]))
		for _, h := range db.levels[task.OutputLevel] {
			if !obsoleteIDs[h.Reader.ID()] {
				newLevel = append(newLevel, h)
			}
		}
		newLevel = append(newLevel, newHandles...)
		sortHandlesByFirstKey(newLevel)
		db.levels[task.OutputLevel] = newLevel
	}

	db.mu.Unlock()

	// 7. Release the DB's ownership reference and mark obsolete.
	// If an active iterator still holds a Ref, file deletion is
	// deferred until the iterator calls Unref.
	for _, h := range task.AllInputs() {
		h.MarkObsolete()
		h.Unref()
	}

	return nil
}

func sortHandlesByFirstKey(handles []*sstable.TableHandle) {
	sort.Slice(handles, func(i, j int) bool {
		return bytes.Compare(
			handles[i].Reader.FirstKey(),
			handles[j].Reader.FirstKey(),
		) < 0
	})
}
