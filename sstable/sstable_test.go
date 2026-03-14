package sstable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ulixert/lithicdb/kv"
)

func TestBlock_RoundTrip(t *testing.T) {
	b := NewBlockBuilder(4096)

	if !b.Add([]byte("a"), kv.NewValue([]byte("one"))) {
		t.Fatal("failed to add a")
	}
	if !b.Add([]byte("b"), kv.NewTombstone()) {
		t.Fatal("failed to add b tombstone")
	}
	if !b.Add([]byte("c"), kv.NewValue([]byte{})) {
		t.Fatal("failed to add c empty value")
	}

	raw := b.Build()
	block, err := DecodeBlock(raw)
	if err != nil {
		t.Fatalf("DecodeBlock() error = %v", err)
	}

	tests := []struct {
		key       string
		wantFound bool
		wantTomb  bool
		wantValue []byte
	}{
		{"a", true, false, []byte("one")},
		{"b", true, true, nil},
		{"c", true, false, []byte{}},
		{"d", false, false, nil},
	}

	for _, tt := range tests {
		got, found, err := block.Get([]byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%q) error = %v", tt.key, err)
		}
		if found != tt.wantFound {
			t.Fatalf("Get(%q) found = %v, want %v", tt.key, found, tt.wantFound)
		}
		if !found {
			continue
		}
		if got.Tombstone != tt.wantTomb {
			t.Fatalf("Get(%q) tombstone = %v, want %v", tt.key, got.Tombstone, tt.wantTomb)
		}
		if string(got.Data) != string(tt.wantValue) {
			t.Fatalf("Get(%q) value = %q, want %q", tt.key, got.Data, tt.wantValue)
		}
	}
}

func TestBlockBuilder_Add_RespectsSizeLimit(t *testing.T) {
	b := NewBlockBuilder(32)

	if !b.Add([]byte("a"), kv.NewValue([]byte("1234567890"))) {
		t.Fatal("first entry should always be accepted")
	}

	if b.Add([]byte("b"), kv.NewValue([]byte("1234567890"))) {
		t.Fatal("second entry should not fit")
	}
}

func TestBlock_FirstAndLastKey(t *testing.T) {
	b := NewBlockBuilder(4096)
	if !b.Add([]byte("apple"), kv.NewValue([]byte("1"))) {
		t.Fatal("failed to add apple")
	}
	if !b.Add([]byte("banana"), kv.NewValue([]byte("2"))) {
		t.Fatal("failed to add banana")
	}
	if !b.Add([]byte("carrot"), kv.NewValue([]byte("3"))) {
		t.Fatal("failed to add carrot")
	}

	raw := b.Build()
	block, err := DecodeBlock(raw)
	if err != nil {
		t.Fatalf("DecodeBlock() error = %v", err)
	}

	first, err := block.FirstKey()
	if err != nil {
		t.Fatalf("FirstKey() error = %v", err)
	}
	last, err := block.LastKey()
	if err != nil {
		t.Fatalf("LastKey() error = %v", err)
	}

	if string(first) != "apple" {
		t.Fatalf("FirstKey = %q, want %q", first, "apple")
	}
	if string(last) != "carrot" {
		t.Fatalf("LastKey = %q, want %q", last, "carrot")
	}
}

func TestDecodeBlock_TooShort(t *testing.T) {
	_, err := DecodeBlock([]byte{1})
	if err == nil {
		t.Fatal("expected error for short block")
	}
}

func TestDecodeBlock_ZeroEntries(t *testing.T) {
	raw := make([]byte, 2) // num_entries = 0
	_, err := DecodeBlock(raw)
	if err == nil {
		t.Fatal("expected error for zero-entry block")
	}
}

func TestDecodeBlock_InvalidOffset(t *testing.T) {
	// fake block:
	// no real entry data
	// one offset = 100
	// num_entries = 1
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint16(raw[0:], 100) // offset table
	binary.LittleEndian.PutUint16(raw[2:], 1)   // num_entries

	block, err := DecodeBlock(raw)
	if err != nil {
		return // also acceptable if DecodeBlock rejects it directly
	}

	_, _, err = block.readEntry(0)
	if err == nil {
		t.Fatal("expected error for invalid offset")
	}
}

func TestBlock_TombstoneWithValueLenFails(t *testing.T) {
	// entry: key_len=1, value_len=1, flag=tombstone, key="a", stray byte="x"
	raw := make([]byte, 0, 16)
	raw = binary.LittleEndian.AppendUint16(raw, 1)
	raw = binary.LittleEndian.AppendUint16(raw, 1)
	raw = append(raw, blockFlagTombstone)
	raw = append(raw, 'a')
	raw = append(raw, 'x') // invalid extra tombstone payload

	// offset table: one entry at offset 0
	raw = binary.LittleEndian.AppendUint16(raw, 0)
	raw = binary.LittleEndian.AppendUint16(raw, 1)

	block, err := DecodeBlock(raw)
	if err != nil {
		t.Fatalf("DecodeBlock() unexpected error = %v", err)
	}

	_, _, err = block.readEntry(0)
	if err == nil {
		t.Fatal("expected error for tombstone with nonzero value length")
	}
}

func TestFooter_RoundTrip(t *testing.T) {
	in := footer{
		bloomOffset: 100,
		bloomLen:    20,
		indexOffset: 120,
		indexLen:    30,
		version:     version1,
	}

	encoded := encodeFooter(in)
	out, err := decodeFooter(encoded)
	if err != nil {
		t.Fatalf("decodeFooter() error = %v", err)
	}

	if out != in {
		t.Fatalf("decoded footer = %+v, want %+v", out, in)
	}
}

func TestFooter_InvalidMagic(t *testing.T) {
	f := footer{
		bloomOffset: 1,
		bloomLen:    2,
		indexOffset: 3,
		indexLen:    4,
		version:     version1,
	}
	encoded := encodeFooter(f)
	encoded[len(encoded)-1] ^= 0xff

	_, err := decodeFooter(encoded)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("decodeFooter() error = %v, want ErrInvalidMagic", err)
	}
}

func TestFooter_InvalidChecksum(t *testing.T) {
	f := footer{
		bloomOffset: 1,
		bloomLen:    2,
		indexOffset: 3,
		indexLen:    4,
		version:     version1,
	}
	encoded := encodeFooter(f)
	encoded[0] ^= 0xff

	_, err := decodeFooter(encoded)
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("decodeFooter() error = %v, want ErrInvalidChecksum", err)
	}
}

func TestIndex_RoundTrip(t *testing.T) {
	first := []byte("apple")
	metas := []blockMeta{
		{offset: 0, size: 100, lastKey: []byte("banana")},
		{offset: 100, size: 120, lastKey: []byte("carrot")},
	}

	encoded, err := encodeIndex(first, metas)
	if err != nil {
		t.Fatalf("decodeIndex() error = %v", err)
	}

	gotFirst, gotMetas, err := decodeIndex(encoded)
	if err != nil {
		t.Fatalf("decodeIndex() error = %v", err)
	}

	if string(gotFirst) != string(first) {
		t.Fatalf("firstKey = %q, want %q", gotFirst, first)
	}

	if len(gotMetas) != len(metas) {
		t.Fatalf("len(metas) = %d, want %d", len(gotMetas), len(metas))
	}

	for i := range metas {
		if gotMetas[i].offset != metas[i].offset ||
			gotMetas[i].size != metas[i].size ||
			string(gotMetas[i].lastKey) != string(metas[i].lastKey) {
			t.Fatalf("meta[%d] = %+v, want %+v", i, gotMetas[i], metas[i])
		}
	}
}
