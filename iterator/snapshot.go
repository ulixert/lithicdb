package iterator

import (
	"bytes"

	"github.com/ulixert/lithicdb/kv"
)

// SnapshotIterator wraps an iterator and filters it to a consistent
// point-in-time view. It:
//   - Skips entries with seq > maxSeq
//   - Deduplicates: for each user key, yields only the newest visible version
//   - Optionally skips tombstones (deleted keys invisible to the caller)
//
// The underlying iterator must yield internal keys in the standard order:
// sorted by user key ascending, then by seq descending within each user key.
type SnapshotIterator struct {
	inner          Iterator
	maxSeq         uint64
	skipTombstones bool

	// Track the last emitted user key for deduplication.
	lastUserKey []byte
}

// NewSnapshotIterator creates a filtered iterator that only sees versions
// with seq <= maxSeq. If skipTombstones is true, deleted keys are skipped
// entirely (used for Scan). If false, tombstones are visible (used for Get).
func NewSnapshotIterator(inner Iterator, maxSeq uint64, skipTombstones bool) *SnapshotIterator {
	s := &SnapshotIterator{
		inner:          inner,
		maxSeq:         maxSeq,
		skipTombstones: skipTombstones,
	}
	s.advance()
	return s
}

// advance moves to the next entry that passes all filters.
func (s *SnapshotIterator) advance() {
	for s.inner.IsValid() {
		key := s.inner.Key()
		seq := kv.SeqNum(key)
		userKey := kv.UserKey(key)

		// Skip entries newer than our snapshot.
		if seq > s.maxSeq {
			s.inner.Next()
			continue
		}

		// Skip duplicate user keys - we already emitted the newest
		// visible version of this key.
		if s.lastUserKey != nil && bytes.Equal(userKey, s.lastUserKey) {
			s.inner.Next()
			continue
		}

		// If the newest visible version is a tombstone and tombstones
		// should be hidden, mark this key as handled and continue.
		if s.skipTombstones && s.inner.Value() == nil {
			s.lastUserKey = append(s.lastUserKey[:0], userKey...)
			s.inner.Next()
			continue
		}

		// Found a valid entry. Stop here.
		return
	}
}

func (s *SnapshotIterator) Key() []byte   { return s.inner.Key() }
func (s *SnapshotIterator) Value() []byte { return s.inner.Value() }
func (s *SnapshotIterator) IsValid() bool { return s.inner.IsValid() }
func (s *SnapshotIterator) Err() error    { return s.inner.Err() }

func (s *SnapshotIterator) Next() {
	if !s.inner.IsValid() {
		return
	}
	// Record the current user key so we skip older versions of it.
	userKey := kv.UserKey(s.inner.Key())
	s.lastUserKey = append(s.lastUserKey[:0], userKey...)
	s.inner.Next()
	s.advance()
}

func (s *SnapshotIterator) Close() error {
	return s.inner.Close()
}
