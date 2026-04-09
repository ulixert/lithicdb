package hintedhandoff

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/iterator"
	"github.com/ulixert/theseon/kv"
)

var ErrCapacityExceeded = errors.New("hinted handoff: capacity exceeded")

const (
	DefaultMaxBytes = 256 * 1024 * 1024 // 256MB
	DefaultHintTTL  = 24 * time.Hour
)

// StoreConfig configures the hint store.
type StoreConfig struct {
	Dir      string
	MaxBytes int64         // default 256MB
	HintTTL  time.Duration // default 24h
	Logger   *slog.Logger
}

func (c *StoreConfig) defaults() {
	if c.MaxBytes <= 0 {
		c.MaxBytes = DefaultMaxBytes
	}
	if c.HintTTL <= 0 {
		c.HintTTL = DefaultHintTTL
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Store persists hints for dead replicas in a separate db.DB instance.
//
// Key format: [nodeID_len:2BE][nodeID][walltime:8BE][logical:4BE][user_key]
// Value: raw encoded envelope bytes (EncodeEnvelope output).
//
// Capacity accounting uses a reserve-then-write pattern: the mu
// mutex only protects the fast capacity check + size increment. The
// actual db.Put happens outside the lock so that WAL I/O does not
// block other hint writers.
type Store struct {
	db      *db.DB
	cfg     StoreConfig
	size    int64               // logical size of all hint values, protected by mu
	targets map[string]struct{} // node IDs with hints, protected by mu
	mu      sync.Mutex
	logger  *slog.Logger
}

// NewStore opens a hint store backed by a separate database directory.
// The hint DB uses DisableWALSync since hints are ephemeral and
// crash-loss is acceptable (anti-entropy is the safety net).
func NewStore(cfg StoreConfig) (*Store, error) {
	cfg.defaults()

	opts := db.DefaultOptions(cfg.Dir)
	opts.DisableWALSync = true
	opts.MemtableSize = 16 * 1024 * 1024 // 16MB - smaller than main DB
	opts.Logger = cfg.Logger

	database, err := db.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &Store{
		db:      database,
		cfg:     cfg,
		targets: make(map[string]struct{}),
		logger:  cfg.Logger,
	}

	s.computeLogicalSize()
	return s, nil
}

// computeLogicalSize iterates all hints on startup to sum value sizes
// and seed the target index.
func (s *Store) computeLogicalSize() {
	iter := s.db.Scan()
	defer iter.Close()

	var total int64
	for iter.IsValid() {
		val := iter.Value()
		if val != nil { // skip tombstones
			total += int64(len(val))
			userKey := kv.UserKey(iter.Key())
			nodeID := extractNodeID(userKey)
			if nodeID != "" {
				s.targets[nodeID] = struct{}{}
			}
		}
		iter.Next()
	}
	s.size = total
}

// Add stores a hint for a dead target node. The envelope bytes should
// be the raw output of EncodeEnvelope - the drainer replays them
// as-is to preserve the original HLC timestamp and delete bit.
//
// Returns ErrCapacityExceeded if the store is over its MaxBytes cap.
func (s *Store) Add(targetNodeID string, key []byte, envelope []byte, ts hlc.Timestamp) error {
	hintKey := encodeHintKey(targetNodeID, ts, key)
	entrySize := int64(len(envelope))

	// Reserve capacity under lock (fast path - no I/O).
	s.mu.Lock()
	if s.size+entrySize > s.cfg.MaxBytes {
		s.mu.Unlock()
		return ErrCapacityExceeded
	}
	s.size += entrySize
	s.mu.Unlock()

	// Write outside lock.
	if err := s.db.Put(hintKey, envelope); err != nil {
		s.mu.Lock()
		s.size -= entrySize
		s.mu.Unlock()
		return err
	}

	// Only update target index after successful persist.
	s.mu.Lock()
	s.targets[targetNodeID] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Iterate returns an iterator over all live hints for the given target
// node. Hints are ordered by timestamp then user key. Tombstoned
// entries (from prior Remove calls) are skipped automatically.
// The caller must close the iterator when done.
func (s *Store) Iterate(targetNodeID string) iterator.Iterator {
	prefix := targetPrefix(targetNodeID)
	f := &tombstoneFilter{inner: s.db.ScanPrefix(prefix)}
	f.skipTombstones()
	return f
}

// tombstoneFilter wraps an iterator and skips entries with nil values
// (tombstones left by db.Delete before compaction cleans them up).
type tombstoneFilter struct {
	inner iterator.Iterator
}

// Key returns the user key (hint key without the internal seq suffix).
func (f *tombstoneFilter) Key() []byte   { return kv.UserKey(f.inner.Key()) }
func (f *tombstoneFilter) Value() []byte { return f.inner.Value() }
func (f *tombstoneFilter) IsValid() bool { return f.inner.IsValid() }
func (f *tombstoneFilter) Err() error    { return f.inner.Err() }
func (f *tombstoneFilter) Close() error  { return f.inner.Close() }

func (f *tombstoneFilter) Next() {
	f.inner.Next()
	f.skipTombstones()
}

func (f *tombstoneFilter) skipTombstones() {
	for f.inner.IsValid() && f.inner.Value() == nil {
		f.inner.Next()
	}
}

// Remove deletes a single hint by its raw key and decrements the size
// tracker by envSize. The delete happens first; size is only decremented
// on success to prevent accounting drift.
func (s *Store) Remove(hintKey []byte, envSize int64) error {
	if err := s.db.Delete(hintKey); err != nil {
		return err
	}
	s.mu.Lock()
	s.size -= envSize
	s.mu.Unlock()
	return nil
}

// Targets return the node IDs that currently have hints.
func (s *Store) Targets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, 0, len(s.targets))
	for id := range s.targets {
		result = append(result, id)
	}
	return result
}

// RemoveTarget removes a node ID from the in-memory target index.
// Called when drain finds no more hints for a target.
func (s *Store) RemoveTarget(nodeID string) {
	s.mu.Lock()
	delete(s.targets, nodeID)
	s.mu.Unlock()
}

// LogicalSize returns the current tracked size of all hint values.
func (s *Store) LogicalSize() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// --- key encoding ---

// encodeHintKey builds: [nodeID_len:2BE][nodeID][walltime:8BE][logical:4BE][user_key]
func encodeHintKey(nodeID string, ts hlc.Timestamp, userKey []byte) []byte {
	nidLen := len(nodeID)
	// 2 (nodeID_len) + nodeID + 8 (walltime) + 4 (logical) + userKey
	buf := make([]byte, 2+nidLen+8+4+len(userKey))
	binary.BigEndian.PutUint16(buf[0:2], uint16(nidLen))
	copy(buf[2:2+nidLen], nodeID)
	off := 2 + nidLen
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(ts.WallTime))
	binary.BigEndian.PutUint32(buf[off+8:off+12], ts.Logical)
	copy(buf[off+12:], userKey)
	return buf
}

// targetPrefix returns the prefix for scanning all hints for a target node.
func targetPrefix(nodeID string) []byte {
	buf := make([]byte, 2+len(nodeID))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(nodeID)))
	copy(buf[2:], nodeID)
	return buf
}

// extractNodeID reads the nodeID from a hint key.
func extractNodeID(hintKey []byte) string {
	if len(hintKey) < 2 {
		return ""
	}
	nidLen := int(binary.BigEndian.Uint16(hintKey[0:2]))
	if len(hintKey) < 2+nidLen {
		return ""
	}
	return string(hintKey[2 : 2+nidLen])
}

// ExtractTimestamp reads the HLC timestamp from a hint key.
func ExtractTimestamp(hintKey []byte) hlc.Timestamp {
	if len(hintKey) < 2 {
		return hlc.Timestamp{}
	}
	nidLen := int(binary.BigEndian.Uint16(hintKey[0:2]))
	off := 2 + nidLen
	if len(hintKey) < off+12 {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(hintKey[off : off+8])),
		Logical:  binary.BigEndian.Uint32(hintKey[off+8 : off+12]),
	}
}

// ExtractUserKey reads the user key suffix from a hint key.
func ExtractUserKey(hintKey []byte) []byte {
	if len(hintKey) < 2 {
		return nil
	}
	nidLen := int(binary.BigEndian.Uint16(hintKey[0:2]))
	off := 2 + nidLen + 12 // skip nodeID_len + nodeID + walltime + logical
	if len(hintKey) < off {
		return nil
	}
	return hintKey[off:]
}
