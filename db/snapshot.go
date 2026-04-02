package db

import (
	"sync/atomic"

	"github.com/ulixert/theseon/iterator"
	"github.com/ulixert/theseon/kv"
)

// Snapshot provides a consistent, point-in-time read view of the
// database. All reads through a Snapshot see exactly the state that
// existed when the snapshot was created — subsequent writes, deletes,
// flushes, and compactions do not affect it.
//
// Snapshots are cheap to create (no data is copied). They work by
// recording the current sequence number and filtering out any entries
// with a higher sequence number during reads.
//
// Callers MUST call Close() when done with a snapshot. An open snapshot
// prevents compaction from garbage-collecting old versions of keys,
// so long-lived snapshots can increase space amplification.
type Snapshot struct {
	db     *DB
	seq    uint64
	closed atomic.Bool
}

// Get retrieves the newest version of a user key that was visible
// when this snapshot was created.
func (s *Snapshot) Get(key []byte) (kv.Value, bool) {
	return s.db.getAt(key, s.seq)
}

// Scan returns an iterator over all entries visible at the snapshot's
// point in time, in sorted key order. The caller must call Close()
// on the returned iterator.
func (s *Snapshot) Scan() iterator.Iterator {
	return iterator.NewSnapshotIterator(s.db.rawScan(), s.seq)
}

// ScanRange returns an iterator over entries whose user key is in
// [start, end), visible at the snapshot's point in time.
// The caller must call Close() on the returned iterator.
func (s *Snapshot) ScanRange(start, end []byte) iterator.Iterator {
	return iterator.NewSnapshotIterator(s.db.rawScanRange(start, end), s.seq)
}

// Close releases the snapshot. After Close, the snapshot's sequence
// number is deregistered, allowing compaction to garbage-collect
// versions that are no longer needed by any snapshot.
//
// Close is idempotent - calling it multiple times is safe.
func (s *Snapshot) Close() {
	if s.closed.CompareAndSwap(false, true) {
		s.db.snapshots.Deregister(s.seq)
	}
}
