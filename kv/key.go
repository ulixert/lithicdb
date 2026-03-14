package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Internal key format:
//
//	[user_key][inverted_seq: 8 bytes, big-endian]
//
// The sequence number is inverted (math.MaxUint64 - seq) so that
// bytes.Compare on internal keys gives the correct ordering:
//   - Ascending by user key
//   - Descending by sequence number (the newest version first)
//
// This means the skip list and SSTable blocks need no custom
// comparator - standard byte comparison works.

const SeqSize = 8

// MaxSeqNum is the largest possible sequence number.
const MaxSeqNum = math.MaxUint64

// MakeInternalKey constructs an internal key from a user key and
// sequence number.
func MakeInternalKey(userKey []byte, seq uint64) []byte {
	ikey := make([]byte, len(userKey)+SeqSize)
	copy(ikey, userKey)
	binary.BigEndian.PutUint64(ikey[len(userKey):], math.MaxUint64-seq)
	return ikey
}

// ParseInternalKey extracts the user key and sequence number from
// an internal key. Returns an error if the key is too short.
func ParseInternalKey(ikey []byte) (userKey []byte, seq uint64, err error) {
	if len(ikey) < SeqSize {
		return nil, 0, fmt.Errorf("kv: internal key too short (%d bytes)", len(ikey))
	}
	boundary := len(ikey) - SeqSize
	userKey = ikey[:boundary]
	inverted := binary.BigEndian.Uint64(ikey[boundary:])
	seq = math.MaxUint64 - inverted
	return userKey, seq, nil
}

// UserKey extracts the user key portion from an internal key.
// Does not allocate - returns a slice into the original key.
func UserKey(ikey []byte) []byte {
	if len(ikey) <= SeqSize {
		return nil
	}
	return ikey[:len(ikey)-SeqSize]
}

// SeqNum extracts the sequence number from an internal key.
func SeqNum(ikey []byte) uint64 {
	if len(ikey) < SeqSize {
		return 0
	}
	inverted := binary.BigEndian.Uint64(ikey[len(ikey)-SeqSize:])
	return math.MaxUint64 - inverted
}

// SameUserKey returns true if two internal keys have the same user key.
func SameUserKey(a, b []byte) bool {
	return bytes.Equal(UserKey(a), UserKey(b))
}

// MakeSearchKey constructs an internal key for seeking to the newest
// version of a user key. Uses MaxSeqNum, so the inverted bytes are
// all zeros, which sorts before any real version.
func MakeSearchKey(userKey []byte) []byte {
	return MakeInternalKey(userKey, MaxSeqNum)
}
