package memtable

import "bytes"

// SkipListIterator walks the skip list at level 0 in sorted key order.
// It implements iterator.Iterator.
//
// The iterator holds direct pointers to skip list nodes. Callers must
// ensure not concurrent writes occur during iteration or use the
// Memtable wrapper which handles synchronization.
type SkipListIterator struct {
	current *skipListNode
	endKey  []byte
}

func newSkipListIterator(start *skipListNode, endKey []byte) *SkipListIterator {
	it := &SkipListIterator{
		current: start,
		endKey:  endKey,
	}

	it.checkBound()
	return it
}

func (it *SkipListIterator) checkBound() {
	if it.current != nil && it.endKey != nil {
		if bytes.Compare(it.current.key, it.endKey) >= 0 {
			it.current = nil
		}
	}
}
