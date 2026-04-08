package hintedhandoff

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ulixert/theseon/db"
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
// Capacity accounting uses a reserve-then-write pattern: the writeMu
// mutex only protects the fast capacity check + size increment. The
// actual db.Put happens outside the lock so that WAL I/O does not
// block other hint writers.
type Store struct {
	db      *db.DB
	cfg     StoreConfig
	size    int64               // logical size of all hint values, protected by writeMu
	targets map[string]struct{} // node IDs with hints, protected by writeMu
	writeMu sync.Mutex
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
