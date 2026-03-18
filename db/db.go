package db

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ulixert/lithicdb/compaction"
	"github.com/ulixert/lithicdb/iterator"
	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/manifest"
	"github.com/ulixert/lithicdb/memtable"
	"github.com/ulixert/lithicdb/sstable"
	"github.com/ulixert/lithicdb/wal"
)

// Options configure the storage engine.
type Options struct {
	Dir          string
	MemtableSize int64
	BlockSize    int
	Compaction   compaction.Config
}

// DefaultOptions returns reasonable defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:          dir,
		MemtableSize: 64 * 1024 * 1024, // 64MB
		BlockSize:    4096,             // 4KB
		Compaction:   compaction.DefaultConfig(),
	}
}

// DB is the core storage engine.
//
// Concurrency model:
//   - Writes acquire mu (exclusive) briefly to write WAL + memtable
//   - Reads acquire mu (shared) to snapshot state, then release it
//   - Flush runs in the background, acquire mu briefly to swap state
//   - Compaction runs in the background and acquires mu briefly to swap old tables for new ones.
type DB struct {
	opts Options

	mu         sync.RWMutex
	active     *memtable.Memtable
	activeWAL  *wal.WAL
	immutables []*memtable.Memtable     // frozen, newest first
	l0         []*sstable.TableHandle   // L0 SSTables, newest first
	levels     [][]*sstable.TableHandle // levels[0] = L1, sorted by the first key

	manifest  *manifest.Manifest
	nextMemID atomic.Uint64
	nextSeq   atomic.Uint64 // global monotonic sequence number

	flushCh   chan struct{}
	compactCh chan struct{}
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// idAllocator adapts DB's nextMemID counter to the compaction.IDAllocator interface.
type idAllocator struct {
	db *DB
}

func (a *idAllocator) NextID() uint64 {
	return a.db.nextMemID.Add(1)
}

// Open creates or recovers a DB at the given directory.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("db: directory is required")
	}

	if err := sstable.CleanupTempFiles(opts.Dir); err != nil {
		return nil, fmt.Errorf("db: cleanup temp files: %w", err)
	}

	db := &DB{
		opts:      opts,
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
	}

	// levels[0] = nil (L0 stored separately), levels[1] = L1, etc.
	db.levels = make([][]*sstable.TableHandle, opts.Compaction.MaxLevels)

	if err := db.recover(); err != nil {
		return nil, fmt.Errorf("db: recovery: %w", err)
	}

	if db.active == nil {
		if err := db.newMemtable(); err != nil {
			return nil, err
		}
	}

	db.wg.Add(2)
	go db.flushLoop()
	go db.compactionLoop()

	return db, nil
}

// recover restores the DB state from the manifest and WAL files.
//
// Recovery order:
// 1. Open manifest (or create if first startup) -> get SSTable list + IDs
// 2. Open all SSTables listed in the manifest
// 3. Replay WAL files -> rebuild unflushed memtable state
// 4. Reconcile: max(manifest IDs, WAL IDs) -> set nextMemID, nextSeq
func (db *DB) recover() error {
	var state *manifest.State

	if manifest.Exists(db.opts.Dir) {
		m, s, err := manifest.Open(db.opts.Dir)
		if err != nil {
			return fmt.Errorf("open manifest: %w", err)
		}
		db.manifest = m
		state = s
	} else {
		m, err := manifest.Create(db.opts.Dir, 0, 0)
		if err != nil {
			return fmt.Errorf("create manifest: %w", err)
		}
		db.manifest = m
		state = &manifest.State{}
	}

	// Restore ID counters from manifest
	db.nextMemID.Store(state.NextMemID)
	db.nextSeq.Store(state.NextSeq)

	// Open all L0 SSTables listed in the manifest
	for _, info := range state.L0 {
		reader, err := sstable.OpenReader(db.opts.Dir, info.ID)
		if err != nil {
			return fmt.Errorf("open L0 SSTable %d: %w", info.ID, err)
		}
		db.l0 = append(db.l0, sstable.NewTableHandle(reader, db.opts.Dir))
	}

	// Open L1+ SSTables
	// state.Levels and db.levels use the same indexing: index = level number
	for level, tables := range state.Levels {
		if level == 0 || level >= len(db.levels) {
			continue
		}
		for _, info := range tables {
			reader, err := sstable.OpenReader(db.opts.Dir, info.ID)
			if err != nil {
				return fmt.Errorf("open L%d SSTable %d: %w", level, info.ID, err)
			}
			db.levels[level] = append(db.levels[level], sstable.NewTableHandle(reader, db.opts.Dir))
		}
	}

	// Replay WAL files for unflushed memtable data
	recovered, err := wal.RecoverDir(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("recover WALs: %w", err)
	}

	for _, rw := range recovered {
		mt := memtable.New(rw.ID)
		for _, e := range rw.Entries {
			// Reconstruct internal key and insert into memtable
			ikey := kv.MakeInternalKey(e.Key, e.Seq)
			mt.Put(ikey, e.Value)

			// Track max seq seen in WAL (may exceed manifest's nextSeq
			// if a write happened after the last manifest update)
			if e.Seq >= db.nextSeq.Load() {
				db.nextSeq.Store(e.Seq)
			}
		}

		// Track max memtable ID from WAL
		if rw.ID >= db.nextMemID.Load() {
			db.nextMemID.Store(rw.ID)
		}

		if db.active != nil {
			db.active.Freeze()
			db.immutables = append([]*memtable.Memtable{db.active}, db.immutables...)
		}
		db.active = mt
	}

	// Reopen the active WAL for appending
	if db.active != nil {
		w, err := wal.Open(db.opts.Dir, db.active.ID())
		if err != nil {
			return fmt.Errorf("db: reopen active WAL: %w", err)
		}
		db.activeWAL = w
	}

	return nil
}

func (db *DB) newMemtable() error {
	id := db.nextMemID.Add(1)

	w, err := wal.Create(db.opts.Dir, id)
	if err != nil {
		return fmt.Errorf("db: create WAL: %w", err)
	}

	db.active = memtable.New(id)
	db.activeWAL = w
	return nil
}

// Put inserts or updates a key-value pair.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.nextSeq.Add(1)

	if err := db.activeWAL.Put(key, value, seq); err != nil {
		return fmt.Errorf("db: WAL put: %w", err)
	}

	ikey := kv.MakeInternalKey(key, seq)
	db.active.Put(ikey, kv.NewValue(value))

	if db.active.ApproximateSize() >= db.opts.MemtableSize {
		if err := db.rotateMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Delete marks a key as deleted.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.nextSeq.Add(1)

	if err := db.activeWAL.Delete(key, seq); err != nil {
		return fmt.Errorf("db: WAL delete: %w", err)
	}

	ikey := kv.MakeInternalKey(key, seq)
	db.active.Put(ikey, kv.NewTombstone())

	if db.active.ApproximateSize() >= db.opts.MemtableSize {
		if err := db.rotateMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves the newest visible value for a user key.
// Returns the value and true if found. If the key has been
// deleted, returns a tombstone value with found=true.
func (db *DB) Get(key []byte) (kv.Value, bool) {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.levels
	db.mu.RUnlock()

	// Active memtable
	if val, found := active.Get(key); found {
		return val, true
	}

	// Immutable memtables (newest first)
	for _, mt := range immutables {
		if val, found := mt.Get(key); found {
			return val, true
		}
	}

	// L0 SSTables (newest first, may overlap)
	for _, h := range l0 {
		if !h.Reader.MayContain(key) {
			continue
		}
		val, found, err := h.Reader.Get(key)
		if err != nil {
			// Skip this SSTable rather than failing the entire read.
			// This means corruption in one file doesn't block reads
			// that could be served from other sources.
			// TODO: add error logging and metrics for monitoring
			continue
		}
		if found {
			return val, true
		}
	}

	// L1+ are non-overlapping within each level, so for a given user
	// key there is at most one candidate SSTable per level.
	for _, level := range levels {
		for _, h := range level {
			if !h.Reader.MayContain(key) {
				continue
			}
			val, found, err := h.Reader.Get(key)
			if err != nil {
				continue
			}
			if found {
				return val, true
			}
		}
	}

	return kv.Value{}, false
}

// Scan returns an iterator over all entries in sorted key order.
//
// The iterator reads from in-memory data (Readers load the full file
// into memory), so it does not need to hold TableHandle references.
// If the Reader switches to mmap, iterators would need to Ref/Unref.
//
// Returns internal keys. The caller must call Close().
func (db *DB) Scan() iterator.Iterator {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.levels
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0)+len(levels))

	iters = append(iters, active.Scan())
	for _, mt := range immutables {
		iters = append(iters, mt.Scan())
	}
	for _, h := range l0 {
		iters = append(iters, h.Reader.Scan())
	}
	for _, level := range levels {
		for _, h := range level {
			iters = append(iters, h.Reader.Scan())
		}
	}

	return iterator.NewMergeIterator(iters)
}

// ScanRange returns an iterator over entries whose user key is in
// [start, end). The caller must call Close().
func (db *DB) ScanRange(start, end []byte) iterator.Iterator {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.levels
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0)+len(levels))

	iters = append(iters, active.ScanRange(start, end))
	for _, mt := range immutables {
		iters = append(iters, mt.ScanRange(start, end))
	}
	for _, h := range l0 {
		iters = append(iters, h.Reader.ScanRange(start, end))
	}
	for _, level := range levels {
		for _, h := range level {
			iters = append(iters, h.Reader.ScanRange(start, end))
		}
	}

	return iterator.NewMergeIterator(iters)
}

func (db *DB) rotateMemtable() error {
	db.active.Freeze()
	_ = db.activeWAL.Close()

	db.immutables = append([]*memtable.Memtable{db.active}, db.immutables...)

	if err := db.newMemtable(); err != nil {
		return err
	}

	// Signal flush goroutine (non-blocking)
	select {
	case db.flushCh <- struct{}{}:
	default:
	}

	return nil
}

// --- Flush goroutine ---

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.flushCh:
			db.flushImmutables()
		case <-db.closeCh:
			db.flushImmutables()
			return
		}
	}
}

// flushImmutables flushes all immutable memtables to SSTables,
//
//	the oldest first. Loops until the immutables list is empty, so
//
// a single signal on flushCh is enough to drain the entire backlog.
func (db *DB) flushImmutables() {
	for {
		db.mu.RLock()
		n := len(db.immutables)
		if n == 0 {
			db.mu.RUnlock()
			return
		}
		mt := db.immutables[n-1] // oldest
		db.mu.RUnlock()

		if err := db.flushMemtable(mt); err != nil {
			// TODO: log error, retry with backoff, or shut down
			return
		}
	}
}

func (db *DB) flushMemtable(mt *memtable.Memtable) error {
	id := mt.ID()

	builder := sstable.NewBuilder(db.opts.Dir, id, db.opts.BlockSize)

	iter := mt.Scan()
	defer iter.Close()

	for iter.IsValid() {
		// Iterator returns internal keys and raw values.
		// Reconstruct kv.Value from the iterator output.
		key := iter.Key()
		val := iter.Value()

		var value kv.Value
		if val == nil {
			value = kv.NewTombstone()
		} else {
			value = kv.NewValue(val)
		}

		if err := builder.Add(key, value); err != nil {
			return fmt.Errorf("db: flush add: %w", err)
		}

		iter.Next()
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("db: flush iterate: %w", err)
	}

	if err := builder.Finish(); err != nil {
		return fmt.Errorf("db: flush finish: %w", err)
	}

	reader, err := sstable.OpenReader(db.opts.Dir, id)
	if err != nil {
		return fmt.Errorf("db: open flushed SSTable: %w", err)
	}

	// Record the new SSTable in the manifest BEFORE updating the in-memory
	// state. If we crash after the manifest write but before updating
	// memory, recovery will see the SSTable and load it.
	if err := db.manifest.AddSSTable(manifest.SSTableInfo{
		ID:       id,
		Level:    0,
		FirstKey: reader.FirstKey(),
		LastKey:  reader.LastKey(),
	}); err != nil {
		return fmt.Errorf("db: manifest add: %w", err)
	}

	// Update next IDs in the manifest so they survive WAL deletion
	if err := db.manifest.UpdateNextIDs(
		db.nextMemID.Load(),
		db.nextSeq.Load(),
	); err != nil {
		return fmt.Errorf("db: manifest update IDs: %w", err)
	}

	// Now update in-memory state
	handle := sstable.NewTableHandle(reader, db.opts.Dir)

	db.mu.Lock()
	db.l0 = append([]*sstable.TableHandle{handle}, db.l0...)
	db.immutables = db.immutables[:len(db.immutables)-1]
	db.mu.Unlock()

	// Delete WAL - data is now durable in the SSTable and manifest
	wal.RemoveByID(db.opts.Dir, id)

	// Single compaction check after adding to L0
	db.triggerCompaction()

	return nil
}

// --- Compaction goroutine ---

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
			int64(db.opts.Compaction.LevelSizeBase)/10, // target file size ~1/10 of level size
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
		// Remove compacted L0 files
		newL0 := make([]*sstable.TableHandle, 0, len(db.l0))
		for _, h := range db.l0 {
			if !obsoleteIDs[h.Reader.ID()] {
				newL0 = append(newL0, h)
			}
		}
		db.l0 = newL0
	} else {
		// Remove compacted files from the input level
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

	// Remove compacted files from the output level
	if task.OutputLevel >= 0 && task.OutputLevel < len(db.levels) {
		newLevel := make([]*sstable.TableHandle, 0, len(db.levels[task.OutputLevel]))
		for _, h := range db.levels[task.OutputLevel] {
			if !obsoleteIDs[h.Reader.ID()] {
				newLevel = append(newLevel, h)
			}
		}
		// Insert new handles (sorted by the first key for L1+)
		newLevel = append(newLevel, newHandles...)
		sortHandlesByFirstKey(newLevel)
		db.levels[task.OutputLevel] = newLevel
	}

	db.mu.Unlock()

	// 7. Release the DB's ownership reference and mark obsolete.
	// If an active iterator still holds a Ref, the file deletion
	// is deferred until the iterator calls Unref. If no iterator
	// holds a reference, the file is deleted immediately.
	for _, h := range task.AllInputs() {
		h.MarkObsolete()
		h.Unref() // release DB's initial reference
	}

	return nil
}

func sortHandlesByFirstKey(handles []*sstable.TableHandle) {
	sort.Slice(handles, func(i, j int) bool {
		return bytes.Compare(
			handles[i].Reader.FirstKey(),
			handles[j].Reader.FirstKey()) < 0
	})
}

// Close gracefully shuts down the engine.
func (db *DB) Close() error {
	close(db.closeCh)
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeWAL != nil {
		_ = db.activeWAL.Close()
	}

	if db.manifest != nil {
		// Write final IDs before closing
		_ = db.manifest.UpdateNextIDs(
			db.nextMemID.Load(),
			db.nextSeq.Load(),
		)
		_ = db.manifest.Close()
	}

	return nil
}
