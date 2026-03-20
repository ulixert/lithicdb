package iterator

import (
	"bytes"

	"github.com/ulixert/lithicdb/kv"
)

// SnapshotIterator wraps a raw (all-versions) iterator and filters it
// to produce the newest visible version of each user key, where
// "visible" means seq <= maxSeq.
//
// Given a raw merge iterator that emits entries in sorted order
// (ascending user key, descending seq within the same user key),
// this iterator:
//  1. Skips entries with seq > maxSeq (invisible to this snapshot).
//  2. Emits the first entry for each user key with seq <= maxSeq
//     (the newest visible version).
//  3. Skips remaining older visible versions of the same user key.
//
// Tombstones are emitted as-is - the caller decides how to handle them.
type SnapshotIterator struct {
	inner       Iterator
	maxSeq      uint64
	lastUserKey []byte // user key of the last emitted entry
	valid       bool
	err         error
}

// NewSnapshotIterator creates a snapshot iterator that filters the
// inner iterator to show only entries visible at the given sequence
// number. The inner iterator must emit entries in global sorted order
// (e.g., from a MergeIterator).
func NewSnapshotIterator(inner Iterator, maxSeq uint64) *SnapshotIterator {
	s := &SnapshotIterator{
		inner:  inner,
		maxSeq: maxSeq,
	}
	s.advance()
	return s
}

// advance scans the inner iterator to find the next entry to emit.
func (s *SnapshotIterator) advance() {
	s.valid = false

	for s.inner.IsValid() {
		key := s.inner.Key()
		userKey := kv.UserKey(key)
		seq := kv.SeqNum(key)

		// Skip entries that are newer than this snapshot.
		if seq > s.maxSeq {
			s.inner.Next()
			continue
		}

		// Skip older visible versions of a key we already emitted.
		if s.lastUserKey != nil && bytes.Equal(userKey, s.lastUserKey) {
			s.inner.Next()
			continue
		}

		// Found the newest visible version of a new user key.
		s.lastUserKey = append(s.lastUserKey[:0], userKey...)
		s.valid = true
		return
	}

	if err := s.inner.Err(); err != nil {
		s.err = err
	}
}

func (s *SnapshotIterator) Key() []byte {
	return s.inner.Key()
}

func (s *SnapshotIterator) Value() []byte {
	return s.inner.Value()
}

func (s *SnapshotIterator) IsValid() bool {
	return s.valid && s.err == nil
}

func (s *SnapshotIterator) Next() {
	if !s.IsValid() {
		return
	}
	s.inner.Next()
	s.advance()
}

func (s *SnapshotIterator) Err() error {
	return s.err
}

func (s *SnapshotIterator) Close() error {
	s.valid = false
	s.lastUserKey = nil
	return s.inner.Close()
}
