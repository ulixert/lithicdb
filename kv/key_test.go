package kv

import (
	"bytes"
	"testing"
)

func TestInternalKey_RoundTrip(t *testing.T) {
	userKey := []byte("hello")
	seq := uint64(42)

	ikey := MakeInternalKey(userKey, seq)

	gotUser, gotSeq, err := ParseInternalKey(ikey)
	if err != nil {
		t.Fatalf("ParseInternalKey: %v", err)
	}
	if string(gotUser) != "hello" {
		t.Errorf("user key = %q, want %q", gotUser, "hello")
	}
	if gotSeq != 42 {
		t.Errorf("seq = %d, want 42", gotSeq)
	}
}

func TestInternalKey_Ordering_SameUserKey(t *testing.T) {
	// For the same user key, higher seq should sort FIRST (smaller bytes)
	a := MakeInternalKey([]byte("key"), 10)
	b := MakeInternalKey([]byte("key"), 5)

	if bytes.Compare(a, b) >= 0 {
		t.Error("seq 10 should sort before seq 5 (smaller inverted bytes)")
	}
}

func TestInternalKey_Ordering_DifferentUserKey(t *testing.T) {
	// Different user keys should sort by user key ascending
	a := MakeInternalKey([]byte("apple"), 100)
	b := MakeInternalKey([]byte("banana"), 1)

	if bytes.Compare(a, b) >= 0 {
		t.Error("apple should sort before banana regardless of seq")
	}
}

func TestInternalKey_SearchKey(t *testing.T) {
	// Search key should sort before any real version of the same user key
	search := MakeSearchKey([]byte("key"))
	real := MakeInternalKey([]byte("key"), 999999)

	if bytes.Compare(search, real) >= 0 {
		t.Error("search key should sort before real versions")
	}
}

func TestInternalKey_SearchKey_SortsAfterPreviousUserKey(t *testing.T) {
	// Search key for "b" should sort after all versions of "a"
	aOld := MakeInternalKey([]byte("a"), 1)
	bSearch := MakeSearchKey([]byte("b"))

	if bytes.Compare(aOld, bSearch) >= 0 {
		t.Error("search for 'b' should sort after all versions of 'a'")
	}
}

func TestUserKey(t *testing.T) {
	ikey := MakeInternalKey([]byte("hello"), 42)
	if string(UserKey(ikey)) != "hello" {
		t.Errorf("UserKey = %q, want %q", UserKey(ikey), "hello")
	}
}

func TestSeqNum(t *testing.T) {
	ikey := MakeInternalKey([]byte("hello"), 42)
	if SeqNum(ikey) != 42 {
		t.Errorf("SeqNum = %d, want 42", SeqNum(ikey))
	}
}

func TestSameUserKey(t *testing.T) {
	a := MakeInternalKey([]byte("key"), 10)
	b := MakeInternalKey([]byte("key"), 20)
	c := MakeInternalKey([]byte("other"), 10)

	if !SameUserKey(a, b) {
		t.Error("same user key should return true")
	}
	if SameUserKey(a, c) {
		t.Error("different user key should return false")
	}
}

func TestInternalKey_SeqZero(t *testing.T) {
	ikey := MakeInternalKey([]byte("key"), 0)
	_, seq, err := ParseInternalKey(ikey)
	if err != nil {
		t.Fatalf("ParseInternalKey: %v", err)
	}
	if seq != 0 {
		t.Errorf("seq = %d, want 0", seq)
	}
}

func TestInternalKey_SeqMax(t *testing.T) {
	ikey := MakeInternalKey([]byte("key"), MaxSeqNum)
	_, seq, err := ParseInternalKey(ikey)
	if err != nil {
		t.Fatalf("ParseInternalKey: %v", err)
	}
	if seq != MaxSeqNum {
		t.Errorf("seq = %d, want MaxSeqNum", seq)
	}
}

func TestParseInternalKey_TooShort(t *testing.T) {
	_, _, err := ParseInternalKey([]byte("short"))
	if err == nil {
		t.Fatal("expected error for short key")
	}
}
