package db

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ulixert/lithicdb/kv"
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

// recover replays WAL files to rebuild the memtable state.
func (db *DB) recover() error {
	recovered, err := wal.RecoverDir(db.opts.Dir)
	if err != nil {
		return err
	}

	var maxMemID uint64
	var maxSeq uint64

	for _, rw := range recovered {
		mt := memtable.New(rw.ID)
		for _, e := range rw.Entries {
			// Reconstruct internal key and insert into memtable
			ikey := kv.MakeInternalKey(e.Key, e.Seq)
			mt.Put(ikey, e.Value)

			if e.Seq > maxSeq {
				maxSeq = e.Seq
			}
		}

		if rw.ID > maxMemID {
			maxMemID = rw.ID
		}

		if db.active != nil {
			db.active.Freeze()
			db.immutables = append([]*memtable.Memtable{db.active}, db.immutables...)
		}
		db.active = mt
	}

	db.nextMemID.Store(maxMemID)
	db.nextSeq.Store(maxSeq)

	// Re-open the active WAL for appending
	if db.active != nil {
		w, err := wal.Open(db.opts.Dir, db.active.ID())
		if err != nil {
			return fmt.Errorf("db: reopen active WAL: %w", err)
		}
		db.activeWAL = w
	}

	return nil
}
