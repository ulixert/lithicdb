package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Recover reads a WAL file and returns all valid entries.
// If the file ends with a partially written or corrupt record
// (e.g., due to a crash), the corrupt tail is silently ignored
// and all valid preceding records are returned.
func Recover(path string) ([]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("wal: read file: %w", err)
	}

	var allEntries []Entry
	offset := 0

	for offset < len(data) {
		entries, n, err := decodeRecord(data[offset:])
		if err != nil {
			if errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrShortRecord) {
				// Corrupt tail from a crash - stop here.
				// Everything before this point was fully synced.
				break
			}
			return nil, fmt.Errorf("wal: decode at offset %d: %w", offset, err)
		}

		allEntries = append(allEntries, entries...)
		offset += n
	}

	return allEntries, nil
}

// FindWALFiles scans dir for WAL files and returns their memtable IDs
// in ascending order. This is used during startup to discover which
// WALs need to be replayed.
func FindWALFiles(dir string) ([]uint64, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		// If the directory does not exist, that means:
		// no WAL directory or no WAL files, nothing to recover
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: read directory: %w", err)
	}

	var ids []uint64
	for _, e := range dirEntries {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}

		idStr := strings.TrimSuffix(name, ".wal")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue // skip files that don't match the naming convention
		}

		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// RecoverDir finds all WAL files in dir and recovers entries from
// each one, returning them grouped by memtable ID in ascending order.
// This is the main entry point for crash recovery.
func RecoverDir(dir string) (map[uint64][]Entry, error) {
	ids, err := FindWALFiles(dir)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	result := make(map[uint64][]Entry, len(ids))
	for _, id := range ids {
		path := filepath.Join(dir, walFileName(id))
		entries, err := Recover(path)
		if err != nil {
			return nil, fmt.Errorf("wal: recover id %d: %w", id, err)
		}

		if len(entries) > 0 {
			result[id] = entries
		}
	}

	return result, nil
}
