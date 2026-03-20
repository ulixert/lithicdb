package db

import (
	"slices"
	"sort"
	"sync"
)

// snapshotRegistry tracks the sequence numbers of all active snapshots.
//
// It maintains a sorted slice of sequence numbers so that OldestSeq()
// is O(1). Register and Deregister are O(n) but n is expected to be
// small (< 100 concurrent snapshots in typical workloads).
//
// The oldest active snapshot determines the compaction watermark:
// versions with seq >= OldestSeq must be retained because some
// snapshots might need them.
type snapshotRegistry struct {
	mu   sync.Mutex
	seqs []uint64 // sorted ascending, may contain duplicates
}

// Register adds a snapshot sequence number to the registry.
// Multiple snapshots may share the same seq (e.g., two concurrent
// GetSnapshot calls before any writes occur).
func (r *snapshotRegistry) Register(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := sort.Search(len(r.seqs), func(i int) bool {
		return r.seqs[i] >= seq
	})

	r.seqs = slices.Insert(r.seqs, i, seq)
}

// Deregister removes one instance of a snapshot sequence number.
// If the seq appears multiple times (duplicate snapshots), only
// one copy is removed. No-op if seq is not found.
func (r *snapshotRegistry) Deregister(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := sort.Search(len(r.seqs), func(i int) bool {
		return r.seqs[i] >= seq
	})

	if i < len(r.seqs) && r.seqs[i] == seq {
		r.seqs = slices.Delete(r.seqs, i, i+1)
	}
}

// OldestSeq returns the smallest active snapshot sequence number.
// The second return value is false if there are no active snapshots.
func (r *snapshotRegistry) OldestSeq() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.seqs) == 0 {
		return 0, false
	}
	return r.seqs[0], true
}

// Len returns the number of active snapshots.
func (r *snapshotRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seqs)
}
