package sstable

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// TableHandle wraps a Reader with reference counting. When the
// refcount drops to zero AND the handle is marked obsolete,
// the underlying file is deleted.
//
// Active iterators hold a reference. The compaction worker marks
// old files obsolete after writing replacements. The file is only
// deleted when the last iterator releases its reference.
type TableHandle struct {
	Reader   *Reader
	refcount atomic.Int32
	obsolete atomic.Bool
	dir      string

	// Protects the actual file deletion to ensure it happens at most once.
	deleteOnce sync.Once
}

// NewTableHandle creates a handle with an initial refcount of 1.
// The caller owns this initial reference and must call Unref when done.
func NewTableHandle(reader *Reader, dir string) *TableHandle {
	h := &TableHandle{
		Reader: reader,
		dir:    dir,
	}
	h.refcount.Store(1)
	return h
}

// Ref increments the reference count. Panics if called on a
// handle with zero references (use-after-free).
func (h *TableHandle) Ref() {
	refs := h.refcount.Add(1)
	if refs <= 1 {
		panic(fmt.Sprintf("sstable: Ref called on handle with refcount %d (was 0)", refs-1))
	}
}

// Unref decrements the reference count. If the count reaches zero
// and the handle is marked obsolete, the SSTable file is deleted.
func (h *TableHandle) Unref() {
	remaining := h.refcount.Add(-1)
	if remaining < 0 {
		panic("sstable: Unref called too many times")
	}
	if remaining == 0 && h.obsolete.Load() {
		h.tryDelete()
	}
}

// MarkObsolete marks this SSTable as no longer needed by the LSM
// state. If the refcount is already zero, the file is deleted
// immediately. Otherwise, deletion is deferred until the last
// reference is released.
func (h *TableHandle) MarkObsolete() {
	h.obsolete.Store(true)
	if h.refcount.Load() == 0 {
		h.tryDelete()
	}
}

// IsObsolete returns whether the handle has been marked for deletion.
func (h *TableHandle) IsObsolete() bool {
	return h.obsolete.Load()
}

func (h *TableHandle) tryDelete() {
	h.deleteOnce.Do(func() {
		path := SSTPath(h.dir, h.Reader.ID())
		_ = os.Remove(path)
	})
}
