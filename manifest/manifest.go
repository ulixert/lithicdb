package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const manifestFileName = "MANIFEST"

// Manifest manages the persistent state of the LSM tree: which
// SSTables exist, what level they're in, and what the next IDs are.
//
// The manifest is an append-only log of records. On startup, the
// log is replayed to reconstruct the LSM state. Periodic snapshots
// bound replay time.
//
// The manifest is the source of truth. If an SSTable file exists on
// disk but is not in the manifest, it's garbage from a failed
// operation and should be deleted.
type Manifest struct {
	mu   sync.Mutex
	file *os.File
	dir  string

	// Current state, rebuilt during recovery and maintained on writes.
	tables    map[uint64]SSTableInfo // id → info
	nextMemID uint64
	nextSeq   uint64

	// HNSW snapshot tracking: collection name → latest snapshot info.
	hnswSnapshots map[string]HNSWSnapshotInfo

	// Track how many records since the last snapshot to decide
	// when to write a new snapshot.
	recordsSinceSnapshot int
}

// HNSWSnapshotInfo describes a persisted HNSW graph snapshot file.
type HNSWSnapshotInfo struct {
	Collection string
	Seq        uint64
	Filename   string
}

const snapshotInterval = 64 // write a snapshot every N records

// State is a read-only snapshot of the manifest's current state,
// returned to the DB engine after recovery.
type State struct {
	// L0 contains SSTables at level 0, sorted newest (the highest ID) first.
	L0 []SSTableInfo

	// Levels contain SSTables at level 1+, indexed by level.
	// Levels[0] is nil (L0 is stored separately in the L0 field).
	// Levels[1] = L1, Levels[2] = L2, etc. Index matches level number.
	// Within each level, SSTables are sorted by the first key.
	Levels [][]SSTableInfo

	NextMemID uint64
	NextSeq   uint64

	// HNSWSnapshots maps collection name → latest HNSW snapshot info.
	HNSWSnapshots map[string]HNSWSnapshotInfo
}

// Create creates a new manifest file in dir. Used on the first startup
// when no manifest exists.
func Create(dir string, nextMemID, nextSeq uint64) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("manifest: create dir: %w", err)
	}

	path := filepath.Join(dir, manifestFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return nil, fmt.Errorf("manifest: create file: %w", err)
	}

	m := &Manifest{
		file:          f,
		dir:           dir,
		tables:        make(map[uint64]SSTableInfo),
		hnswSnapshots: make(map[string]HNSWSnapshotInfo),
		nextMemID:     nextMemID,
		nextSeq:       nextSeq,
	}

	// Write initial NextIDs record
	if err := m.writeRecord(Record{
		Type:      typeNextIDs,
		NextMemID: nextMemID,
		NextSeq:   nextSeq,
	}); err != nil {
		_ = f.Close()
		return nil, err
	}

	return m, nil
}

// Open opens an existing manifest file and replays it to recover state.
// Returns an error if the manifest file does not exist.
func Open(dir string) (*Manifest, *State, error) {
	path := filepath.Join(dir, manifestFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("manifest: read file: %w", err)
	}

	m := &Manifest{
		dir:           dir,
		tables:        make(map[uint64]SSTableInfo),
		hnswSnapshots: make(map[string]HNSWSnapshotInfo),
	}

	// Replay all records
	offset := 0
	for offset < len(data) {
		rec, n, err := decodeRecord(data[offset:])
		if err != nil {
			if errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrShortRecord) {
				// Corrupt tail - stop replay, everything before is valid
				break
			}
			return nil, nil, fmt.Errorf("manifest: decode at offset %d: %w", offset, err)
		}
		m.applyRecord(rec)
		offset += n
	}

	// Reopen for appending
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, nil, fmt.Errorf("manifest: reopen for append: %w", err)
	}
	m.file = f

	state := m.buildState()
	return m, state, nil
}

// Exists checks whether a manifest file exists in dir.
func Exists(dir string) bool {
	path := filepath.Join(dir, manifestFileName)
	_, err := os.Stat(path)
	return err == nil
}

// applyRecord updates the in-memory state from a single record.
// Only called with records that passed decodeRecord, which rejects
// unknown types - so the switch is exhaustive.
func (m *Manifest) applyRecord(r Record) {
	switch r.Type {
	case typeSSTableAdded:
		m.tables[r.SSTable.ID] = r.SSTable
	case typeSSTableRemoved:
		delete(m.tables, r.SSTable.ID)
	case typeSnapshot:
		// Snapshot replaces the entire state.
		m.tables = make(map[uint64]SSTableInfo, len(r.SSTables))
		for _, t := range r.SSTables {
			m.tables[t.ID] = t
		}
		if r.HNSWSnapshots != nil {
			m.hnswSnapshots = make(map[string]HNSWSnapshotInfo, len(r.HNSWSnapshots))
			for k, v := range r.HNSWSnapshots {
				m.hnswSnapshots[k] = v
			}
		}
	case typeNextIDs:
		m.nextMemID = r.NextMemID
		m.nextSeq = r.NextSeq
	case typeHNSWSnapshot:
		m.hnswSnapshots[r.HNSWCollection] = HNSWSnapshotInfo{
			Collection: r.HNSWCollection,
			Seq:        r.HNSWSeq,
			Filename:   r.HNSWFilename,
		}
	}
}

// buildState constructs a State from the current in-memory tables.
func (m *Manifest) buildState() *State {
	s := &State{
		NextMemID:     m.nextMemID,
		NextSeq:       m.nextSeq,
		HNSWSnapshots: make(map[string]HNSWSnapshotInfo, len(m.hnswSnapshots)),
	}
	for k, v := range m.hnswSnapshots {
		s.HNSWSnapshots[k] = v
	}

	// Find the maximum level to size the slice
	maxLevel := uint8(0)
	for _, t := range m.tables {
		if t.Level > maxLevel {
			maxLevel = t.Level
		}
	}

	if maxLevel > 0 {
		// index = level number, s.Levels[0] = nil
		s.Levels = make([][]SSTableInfo, maxLevel+1)
	}

	for _, t := range m.tables {
		if t.Level == 0 {
			s.L0 = append(s.L0, t)
		} else {
			s.Levels[t.Level] = append(s.Levels[t.Level], t)
		}
	}

	// L0: sort by ID descending (newest first)
	sort.Slice(s.L0, func(i, j int) bool {
		return s.L0[i].ID > s.L0[j].ID
	})

	// L1+: sort by the first key ascending within each level
	for level, tables := range s.Levels {
		if len(tables) == 0 {
			continue
		}
		sort.Slice(tables, func(i, j int) bool {
			return bytes.Compare(tables[i].FirstKey, tables[j].FirstKey) < 0
		})
		s.Levels[level] = tables
	}

	return s
}

// AddSSTable records that a new SSTable has been added.
func (m *Manifest) AddSSTable(info SSTableInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeRecord(Record{
		Type:    typeSSTableAdded,
		SSTable: info,
	}); err != nil {
		return err
	}
	m.tables[info.ID] = info
	return m.maybeSnapshot()
}

// RemoveSSTable records that an SSTable has been removed.
func (m *Manifest) RemoveSSTable(id uint64, level uint8) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeRecord(Record{
		Type:    typeSSTableRemoved,
		SSTable: SSTableInfo{ID: id, Level: level},
	}); err != nil {
		return err
	}
	delete(m.tables, id)
	return m.maybeSnapshot()
}

// UpdateNextIDs records the latest ID counters.
// Called after a flush or any operation that advances the counters.
func (m *Manifest) UpdateNextIDs(nextMemID, nextSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextMemID = nextMemID
	m.nextSeq = nextSeq
	return m.writeRecord(Record{
		Type:      typeNextIDs,
		NextMemID: nextMemID,
		NextSeq:   nextSeq,
	})
}

// AddHNSWSnapshot records that an HNSW graph snapshot has been written.
func (m *Manifest) AddHNSWSnapshot(collection string, seq uint64, filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeRecord(Record{
		Type:           typeHNSWSnapshot,
		HNSWCollection: collection,
		HNSWSeq:        seq,
		HNSWFilename:   filename,
	}); err != nil {
		return err
	}
	m.hnswSnapshots[collection] = HNSWSnapshotInfo{
		Collection: collection,
		Seq:        seq,
		Filename:   filename,
	}
	return nil
}

// writeRecord encodes and appends a record, then fsyncs.
func (m *Manifest) writeRecord(r Record) error {
	data, err := encodeRecord(r)
	if err != nil {
		return err
	}

	if _, err := m.file.Write(data); err != nil {
		return fmt.Errorf("manifest: write: %w", err)
	}

	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("manifest: sync: %w", err)
	}

	m.recordsSinceSnapshot++
	return nil
}

// maybeSnapshot writes a full snapshot if enough records have
// accumulated since the last one. This bounds replay time on recovery.
func (m *Manifest) maybeSnapshot() error {
	if m.recordsSinceSnapshot < snapshotInterval {
		return nil
	}

	tables := make([]SSTableInfo, 0, len(m.tables))
	for _, t := range m.tables {
		tables = append(tables, t)
	}

	hnswSnaps := make(map[string]HNSWSnapshotInfo, len(m.hnswSnapshots))
	for k, v := range m.hnswSnapshots {
		hnswSnaps[k] = v
	}

	if err := m.writeRecord(Record{
		Type:          typeSnapshot,
		SSTables:      tables,
		HNSWSnapshots: hnswSnaps,
	}); err != nil {
		return err
	}

	// Also write NextIDs after snapshot so they're together
	if err := m.writeRecord(Record{
		Type:      typeNextIDs,
		NextMemID: m.nextMemID,
		NextSeq:   m.nextSeq,
	}); err != nil {
		return err
	}

	m.recordsSinceSnapshot = 0
	return nil
}

// Close closes the manifest file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}
