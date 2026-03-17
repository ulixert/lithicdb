package db

import (
	"fmt"
	"sync"
	"sync/atomic"

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
}

// DefaultOptions returns reasonable defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:          dir,
		MemtableSize: 64 * 1024 * 1024, // 64MB
		BlockSize:    4096,             // 4KB
	}
}

// DB is the core storage engine.
//
// Concurrency model:
//   - Writes acquire mu (exclusive) briefly to write WAL + memtable
//   - Reads acquire mu (shared) to snapshot state, then release it
//   - Flush runs in the background, acquire mu briefly to swap state
type DB struct {
	opts Options

	mu         sync.RWMutex
	active     *memtable.Memtable
	activeWAL  *wal.WAL
	immutables []*memtable.Memtable // frozen, newest first
	l0         []*sstable.Reader    // L0 SSTables, newest first

	manifest  *manifest.Manifest
	nextMemID atomic.Uint64
	nextSeq   atomic.Uint64 // global monotonic sequence number

	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
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
		opts:    opts,
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}

	if err := db.recover(); err != nil {
		return nil, fmt.Errorf("db: recovery: %w", err)
	}

	if db.active == nil {
		if err := db.newMemtable(); err != nil {
			return nil, err
		}
	}

	db.wg.Add(1)
	go db.flushLoop()

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
			return fmt.Errorf("open SSTable %d: %w", info.ID, err)
		}
		db.l0 = append(db.l0, reader)
	}

	// TODO: open L1+ SSTables when leveled compaction is implemented

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
	db.mu.RUnlock()

	// Check the active memtable
	if val, found := active.Get(key); found {
		return val, true
	}

	// Check immutable memtables (newest first)
	for _, mt := range immutables {
		if val, found := mt.Get(key); found {
			return val, true
		}
	}

	// Check L0 SSTables (newest first)
	for _, sst := range l0 {
		if !sst.MayContain(key) {
			continue
		}
		val, found, err := sst.Get(key)
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

	return kv.Value{}, false
}

// Scan returns an iterator over all entries in sorted key order.
// Returns internal keys. The caller must call Close().
func (db *DB) Scan() iterator.Iterator {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0))
	iters = append(iters, active.Scan())
	for _, mt := range immutables {
		iters = append(iters, mt.Scan())
	}
	for _, sst := range l0 {
		iters = append(iters, sst.Scan())
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
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0))
	iters = append(iters, active.ScanRange(start, end))
	for _, mt := range immutables {
		iters = append(iters, mt.ScanRange(start, end))
	}
	for _, sst := range l0 {
		iters = append(iters, sst.ScanRange(start, end))
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
	db.mu.Lock()
	db.l0 = append([]*sstable.Reader{reader}, db.l0...)
	db.immutables = db.immutables[:len(db.immutables)-1]
	db.mu.Unlock()

	// Delete WAL - data is now durable in the SSTable and manifest
	wal.RemoveByID(db.opts.Dir, id)

	return nil
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
