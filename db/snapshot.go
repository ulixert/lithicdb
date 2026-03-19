package db

import (
	"slices"
	"sort"
	"sync"
)

// --------------------------------------------------------------------
// Snapshot registry
// --------------------------------------------------------------------

// snapshotRegistry tracks active snapshots so compaction knows its
// watermark: the oldest seq still in use. Versions at or above the
// watermark must be preserved.
type snapshotRegistry struct {
	mu     sync.Mutex
	active []uint64 // sorted ascending
}

func (r *snapshotRegistry) add(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := sort.Search(len(r.active), func(i int) bool {
		return r.active[i] >= seq
	})
	r.active = slices.Insert(r.active, i, seq)
}

func (r *snapshotRegistry) remove(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, s := range r.active {
		if s == seq {
			r.active = slices.Delete(r.active, i, i+1)
			return
		}
	}
}
