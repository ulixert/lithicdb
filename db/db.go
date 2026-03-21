package db

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ulixert/lithicdb/compaction"
	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/manifest"
	"github.com/ulixert/lithicdb/memtable"
	"github.com/ulixert/lithicdb/sstable"
	"github.com/ulixert/lithicdb/wal"
)

// Options configure the storage engine.
type Options struct {
	Dir               string
	MemtableSize      int64
	BlockSize         int
	BlockCacheSize    int64 // total block cache capacity in bytes (0 = 8MB default)
	MaxImmutableCount int   // max unflushed memtables before writes block (0 = 5)
	Compaction        compaction.Config
}

// DefaultOptions returns reasonable defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:               dir,
		MemtableSize:      64 * 1024 * 1024,
		BlockSize:         4096,
		MaxImmutableCount: 5,
		Compaction:        compaction.DefaultConfig(),
	}
}

// DB is the core storage engine.
//
// Concurrency model:
//   - Writes acquire mu (exclusive) briefly to write WAL + memtable
//   - Reads acquire mu (shared) to snapshot state, then release it
//   - Flush runs in the background, acquires mu briefly to swap state
//   - Compaction runs in the background, acquires mu briefly to swap old tables for new ones
type DB struct {
	opts Options

	mu         sync.RWMutex
	active     *memtable.Memtable
	activeWAL  *wal.WAL
	immutables []*memtable.Memtable     // frozen, newest first
	l0         []*sstable.TableHandle   // L0 SSTables, newest first
	levels     [][]*sstable.TableHandle // levels[0] = nil (L0 separate), levels[1] = L1, etc.

	manifest  *manifest.Manifest
	cache     *sstable.BlockCache
	nextMemID atomic.Uint64
	nextSeq   atomic.Uint64
	snapshots snapshotRegistry

	flushCh   chan struct{}
	flushDone *sync.Cond // signaled after each flush, wakes blocked writers
	compactCh chan struct{}
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// Open creates or recovers a DB at the given directory.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("db: directory is required")
	}

	if err := sstable.CleanupTempFiles(opts.Dir); err != nil {
		return nil, fmt.Errorf("db: cleanup temp files: %w", err)
	}

	if opts.MaxImmutableCount <= 0 {
		opts.MaxImmutableCount = 5
	}

	db := &DB{
		opts:      opts,
		cache:     sstable.NewBlockCache(opts.BlockCacheSize),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		levels:    make([][]*sstable.TableHandle, opts.Compaction.MaxLevels),
	}
	db.flushDone = sync.NewCond(&db.mu)

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
// 1. Open manifest (or create if first startup) → get SSTable list + IDs
// 2. Open all SSTables listed in the manifest
// 3. Replay WAL files → rebuild the unflushed memtable state
// 4. Reconcile: max(manifest IDs, WAL IDs) → set nextMemID, nextSeq
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

	db.nextMemID.Store(state.NextMemID)
	db.nextSeq.Store(state.NextSeq)

	// Open L0 SSTables
	for _, info := range state.L0 {
		reader, err := sstable.OpenReader(db.opts.Dir, info.ID, db.cache)
		if err != nil {
			return fmt.Errorf("open L0 SSTable %d: %w", info.ID, err)
		}
		db.l0 = append(db.l0, sstable.NewTableHandle(reader, db.opts.Dir))
	}

	// Open L1+ SSTables (index = level number)
	for level, tables := range state.Levels {
		if level == 0 || level >= len(db.levels) {
			continue
		}
		for _, info := range tables {
			reader, err := sstable.OpenReader(db.opts.Dir, info.ID, db.cache)
			if err != nil {
				return fmt.Errorf("open L%d SSTable %d: %w", level, info.ID, err)
			}
			db.levels[level] = append(db.levels[level], sstable.NewTableHandle(reader, db.opts.Dir))
		}
	}

	// Replay WAL files
	recovered, err := wal.RecoverDir(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("recover WALs: %w", err)
	}

	for _, rw := range recovered {
		mt := memtable.New(rw.ID)
		for _, e := range rw.Entries {
			ikey := kv.MakeInternalKey(e.Key, e.Seq)
			mt.Put(ikey, e.Value)

			if e.Seq >= db.nextSeq.Load() {
				db.nextSeq.Store(e.Seq)
			}
		}

		if rw.ID >= db.nextMemID.Load() {
			db.nextMemID.Store(rw.ID)
		}

		if db.active != nil {
			db.active.Freeze()
			db.immutables = append([]*memtable.Memtable{db.active}, db.immutables...)
		}
		db.active = mt
	}

	if db.active != nil {
		w, err := wal.Open(db.opts.Dir, db.active.ID())
		if err != nil {
			return fmt.Errorf("reopen active WAL: %w", err)
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

// currentSeq returns the latest sequence number that has been assigned.
func (db *DB) currentSeq() uint64 {
	return db.nextSeq.Load()
}

// GetSnapshot returns a point-in-time read view of the database.
// The snapshot sees all writes that were completed before this call
// and none that happen after. Callers must call Snapshot.Close() when
// done to allow compaction to reclaim old versions.
func (db *DB) GetSnapshot() *Snapshot {
	seq := db.currentSeq()
	db.snapshots.Register(seq)
	return &Snapshot{db: db, seq: seq}
}

// BeginTransaction starts an optimistic transaction. The transaction
// reads from a snapshot taken at this point in time. Writes are
// buffered locally until Commit, which checks for write-write
// conflicts and applies them atomically.
func (db *DB) BeginTransaction() *Transaction {
	return &Transaction{
		db:       db,
		snapshot: db.GetSnapshot(),
		writes:   make(map[string]txWrite),
	}
}

// Close gracefully shuts down the engine.
func (db *DB) Close() error {
	close(db.closeCh)

	// Wake any writers blocked on backpressure so they can
	// observe the shutdown and return.
	db.flushDone.Broadcast()

	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeWAL != nil {
		_ = db.activeWAL.Close()
	}

	if db.manifest != nil {
		_ = db.manifest.UpdateNextIDs(
			db.nextMemID.Load(),
			db.nextSeq.Load(),
		)
		_ = db.manifest.Close()
	}

	return nil
}
