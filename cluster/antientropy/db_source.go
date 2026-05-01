package antientropy

import (
	"fmt"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/iterator"
	"github.com/ulixert/theseon/kv"
)

// DBSource is a Source backed by the local key-value store. It iterates
// snapshot-filtered (one entry per latest user key) via db.Scan, decodes
// each envelope, and yields only entries where both selfID and peerID
// are in the top-N replica set for the user key. The value bytes are
// dropped - we hash only (key, hlc, deleted).
type DBSource struct {
	iter   iterator.Iterator
	ring   *hashring.Ring
	selfID string
	peerID string
	n      int

	scannedCounter *uint64 // optional, may be nil
}

// NewDBSource creates a Source over the entire key space, filtered to
// keys co-replicated by selfID and peerID under replication factor n.
func NewDBSource(database *db.DB, ring *hashring.Ring, selfID, peerID string, n int) *DBSource {
	return &DBSource{
		iter:   database.Scan(),
		ring:   ring,
		selfID: selfID,
		peerID: peerID,
		n:      n,
	}
}

// SetScannedCounter wires a counter incremented for every key visited
// (before the ownership filter). Used for metrics and pacing.
func (s *DBSource) SetScannedCounter(c *uint64) { s.scannedCounter = c }

// Next advances the iterator past keys this source skips (not co-owned
// by self+peer) and yields the next reportable Entry.
func (s *DBSource) Next() (Entry, bool, error) {
	for s.iter.IsValid() {
		// CRITICAL: iterator slices are only valid until the next call
		// to Next(). Copy out everything we need BEFORE advancing.
		userKey := kv.UserKey(s.iter.Key())
		keyCopy := make([]byte, len(userKey))
		copy(keyCopy, userKey)

		val := s.iter.Value()
		valCopy := make([]byte, len(val))
		copy(valCopy, val)

		s.iter.Next()

		if s.scannedCounter != nil {
			*s.scannedCounter++
		}

		if !ShouldReconcile(s.ring, keyCopy, s.selfID, s.peerID, s.n) {
			continue
		}

		// LSM-level tombstone (val == nil) shouldn't normally occur in
		// cluster mode - the cluster always writes envelope bytes via
		// Put. Skip defensively rather than crashing the reconcile.
		if len(valCopy) == 0 {
			continue
		}

		env, err := cluster.DecodeEnvelope(valCopy)
		if err != nil {
			return Entry{}, false, fmt.Errorf("anti entropy: decode envelope at user key len=%d: %w",
				len(keyCopy), err)
		}

		return Entry{
			Key:       keyCopy,
			Timestamp: env.Timestamp,
			Deleted:   env.Deleted,
		}, true, nil
	}

	if err := s.iter.Err(); err != nil {
		return Entry{}, false, err
	}
	return Entry{}, false, nil
}

// Close releases the underlying iterator. Safe to call multiple times.
func (s *DBSource) Close() error {
	if s.iter == nil {
		return nil
	}
	err := s.iter.Close()
	s.iter = nil
	return err
}

// BucketSource yields only entries belonging to a specific bucket of a
// (fanout, depth) tree configuration co-owned by selfID and peerID.
// Used by the leaf-listing RPC handler - it scans the local DB and
// filters both by ring ownership and by bucket index.
type BucketSource struct {
	dbs    *DBSource
	tree   *Tree
	bucket int
}

// NewBucketSource creates a BucketSource for the given bucket index.
// graceCutoffWall mirrors BuildTree's filter.
func NewBucketSource(database *db.DB, ring *hashring.Ring, selfID, peerID string,
	n, fanout, depth, bucket int,
) (*BucketSource, error) {
	t, err := NewTree(fanout, depth)
	if err != nil {
		return nil, err
	}
	if bucket < 0 || bucket >= t.NumLeaves() {
		return nil, fmt.Errorf("anti entropy: bucket %d out of range [0,%d)", bucket, t.NumLeaves())
	}
	return &BucketSource{
		dbs:    NewDBSource(database, ring, selfID, peerID, n),
		tree:   t,
		bucket: bucket,
	}, nil
}

// Next yields the next entry in the target bucket.
func (b *BucketSource) Next() (Entry, bool, error) {
	for {
		e, ok, err := b.dbs.Next()
		if err != nil || !ok {
			return e, ok, err
		}
		if b.tree.BucketFor(e.Key) != b.bucket {
			continue
		}
		return e, true, nil
	}
}

// Close releases the underlying DBSource.
func (b *BucketSource) Close() error { return b.dbs.Close() }
