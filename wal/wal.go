package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ulixert/lithicdb/kv"
)

// WAL (Write-Ahead Log) persists every write operation before it is
// applied to the memtable. On crash recovery, the WAL is replayed
// to rebuild the memtable state.
//
// Each WAL file is associated with exactly one memtable via its ID.
// When a memtable is flushed to an SSTable, its WAL file is deleted.
type WAL struct {
	file *os.File
	path string
}

// walFileName returns the WAL file name for a given memtable ID.
func walFileName(id uint64) string {
	return fmt.Sprintf("%06d.wal", id)
}

// Create creates a new WAL file for the given memtable ID in dir.
// Durability is ensured by explicit Sync calls after each write,
// rather than O_SYNC, so batch writes pay the sync cost once per record.
func Create(dir string, id uint64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("wal: create directory: %w", err)
	}

	path := filepath.Join(dir, walFileName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return nil, fmt.Errorf("wal: create file: %w", err)
	}

	return &WAL{file: f, path: path}, nil
}

// Open opens an existing WAL file for appending.
func Open(dir string, id uint64) (*WAL, error) {
	path := filepath.Join(dir, walFileName(id))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("wal: open file: %w", err)
	}

	return &WAL{file: f, path: path}, nil
}

// Put writes a single key-value entry to the WAL and syncs to disk.
func (w *WAL) Put(key, value []byte) error {
	return w.WriteEntries([]Entry{
		{Key: key, Value: kv.NewValue(value)},
	})
}

// Delete writes a tombstone entry to the WAL and syncs to disk.
func (w *WAL) Delete(key []byte) error {
	return w.WriteEntries([]Entry{
		{Key: key, Value: kv.NewTombstone()},
	})
}

// WriteEntries writes a batch of entries as a single WAL record
// and syncs it to disk.
//
// If WriteEntries returns nil, the full record was written and synced.
// If it returns an error, the WAL file may contain a partially written
// or unsynced tail record. The caller must stop writing to this WAL.
// Recovery will detect and ignore the corrupt or incomplete tail.
func (w *WAL) WriteEntries(entries []Entry) error {
	record := encodeRecord(entries)

	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	return w.file.Close()
}

// Remove deletes the WAL file from disk.
// Called after the associated memtable has been successfully
// flushed to an SSTable.
func (w *WAL) Remove() error {
	return os.Remove(w.path)
}

// Path returns the file path of the WAL.
func (w *WAL) Path() string {
	return w.path
}
