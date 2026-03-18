package db

import (
	"fmt"

	"github.com/ulixert/lithicdb/kv"
	"github.com/ulixert/lithicdb/manifest"
	"github.com/ulixert/lithicdb/memtable"
	"github.com/ulixert/lithicdb/sstable"
	"github.com/ulixert/lithicdb/wal"
)

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
// oldest first. Loops until the immutables list is empty, so a
// single signal on flushCh is enough to drain the entire backlog.
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

	// Manifest write before in-memory state update
	if err := db.manifest.AddSSTable(manifest.SSTableInfo{
		ID:       id,
		Level:    0,
		FirstKey: reader.FirstKey(),
		LastKey:  reader.LastKey(),
	}); err != nil {
		return fmt.Errorf("db: manifest add: %w", err)
	}

	if err := db.manifest.UpdateNextIDs(
		db.nextMemID.Load(),
		db.nextSeq.Load(),
	); err != nil {
		return fmt.Errorf("db: manifest update IDs: %w", err)
	}

	handle := sstable.NewTableHandle(reader, db.opts.Dir)

	db.mu.Lock()
	db.l0 = append([]*sstable.TableHandle{handle}, db.l0...)
	db.immutables = db.immutables[:len(db.immutables)-1]
	db.mu.Unlock()

	wal.RemoveByID(db.opts.Dir, id)

	db.triggerCompaction()

	return nil
}
