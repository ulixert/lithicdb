package db

import (
	"github.com/ulixert/lithicdb/iterator"
	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/sstable"
)

// Get retrieves the newest visible value for a user key.
// Returns the value and true if found. If the key has been
// deleted, returns a tombstone value with found=true.
func (db *DB) Get(key []byte) (kv.Value, bool) {
	return db.getAt(key, db.currentSeq())
}

// Scan returns an iterator over all entries in sorted key order.
//
// The iterator reads from in-memory data (Readers load the full file
// into memory), so it does not need to hold TableHandle references.
// If the Reader switches to mmap, iterators would need to Ref/Unref.
//
// Returns internal keys. The caller must call Close().
func (db *DB) Scan() iterator.Iterator {
	return iterator.NewSnapshotIterator(db.rawScan(), db.currentSeq())
}

// ScanRange returns an iterator over entries whose user key is in
// [start, end). The caller must call Close().
func (db *DB) ScanRange(start, end []byte) iterator.Iterator {
	return iterator.NewSnapshotIterator(db.rawScanRange(start, end), db.currentSeq())
}

// getAt performs a point lookup for the newest version of a user key
// with seq <= maxSeq. Searches: active → immutables → L0 → L1+.
func (db *DB) getAt(key []byte, maxSeq uint64) (kv.Value, bool) {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.snapshotLevels()
	db.mu.RUnlock()

	// Active memtable
	if val, found := active.GetAt(key, maxSeq); found {
		return val, true
	}

	// Immutable memtables (newest first)
	for _, mt := range immutables {
		if val, found := mt.GetAt(key, maxSeq); found {
			return val, true
		}
	}

	// L0 SSTables (newest first, may overlap)
	for _, h := range l0 {
		if !h.Reader.MayContain(key) {
			continue
		}
		val, found, err := h.Reader.GetAt(key, maxSeq)
		if err != nil {
			// Skip this SSTable rather than failing the entire read.
			// TODO: add error logging and metrics
			continue
		}
		if found {
			return val, true
		}
	}

	// L1+ (non-overlapping within each level)
	if len(levels) > 1 {
		for _, level := range levels[1:] {
			for _, h := range level {
				if !h.Reader.MayContain(key) {
					continue
				}
				val, found, err := h.Reader.GetAt(key, maxSeq)
				if err != nil {
					continue
				}
				if found {
					return val, true
				}
			}
		}
	}

	return kv.Value{}, false
}

// rawScan collects all-version iterators from every level and returns
// a raw MergeIterator that emits ALL versions in sorted order.
// The caller wraps this with a SnapshotIterator for version selection.
func (db *DB) rawScan() iterator.Iterator {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.snapshotLevels()
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0)+len(levels))

	iters = append(iters, active.Scan())
	for _, mt := range immutables {
		iters = append(iters, mt.Scan())
	}
	for _, h := range l0 {
		iters = append(iters, h.Reader.Scan())
	}
	if len(levels) > 1 {
		for _, level := range levels[1:] {
			for _, h := range level {
				iters = append(iters, h.Reader.Scan())
			}
		}
	}

	return iterator.NewMergeIterator(iters)
}

// rawScanRange collects all-version iterators with key bounds and
// returns a raw MergeIterator.
func (db *DB) rawScanRange(start, end []byte) iterator.Iterator {
	db.mu.RLock()
	active := db.active
	immutables := db.immutables
	l0 := db.l0
	levels := db.snapshotLevels()
	db.mu.RUnlock()

	iters := make([]iterator.Iterator, 0, 1+len(immutables)+len(l0)+len(levels))

	iters = append(iters, active.ScanRange(start, end))
	for _, mt := range immutables {
		iters = append(iters, mt.ScanRange(start, end))
	}
	for _, h := range l0 {
		iters = append(iters, h.Reader.ScanRange(start, end))
	}
	for _, level := range levels[1:] {
		for _, h := range level {
			iters = append(iters, h.Reader.ScanRange(start, end))
		}
	}

	return iterator.NewMergeIterator(iters)
}

// snapshotLevels returns a shallow copy of the levels slice where
// each inner slice header is independent. This prevents a race where
// compaction replaces db.levels[n] while a reader iterates the old copy.
// Must be called with mu.RLock held.
func (db *DB) snapshotLevels() [][]*sstable.TableHandle {
	cp := make([][]*sstable.TableHandle, len(db.levels))
	for i, level := range db.levels {
		cp[i] = level
	}
	return cp
}
