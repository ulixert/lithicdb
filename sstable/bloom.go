package sstable

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/fnv"
	"math"
)

// Bloom filter implementation using the "double hashing" technique
// from Kirsch & Mitzenmacker (2006): given two independent hash
// functions h1 and h2, simulate k hash functions as:
//
//	g_i(x) = h1(x) + i * h2(x)   for i = 0, 1, ..., k-1
//
// We derive h1 and h2 from a single FNV-1a 64-bit hash by splitting
// the upper and lower 32 bits.
//
// Parameters:
//   - bitsPerKey: bits allocated per key (10 gives ~1% FPR)
//   - k: number of hash probes (optimal = bitsPerKey * ln2 ≈ 0.693)

const (
	defaultBitsPerKey = 10
	bloomMetaLenSize  = 4
)

var (
	ErrShortBloomMeta = errors.New("sstable: bloom meta too short")
)

// BloomFilterBuilder accumulates keys and produces a serialized bloom filter.
type BloomFilterBuilder struct {
	keys       []uint64 // collected FNV-1a hashes
	bitsPerKey int
}

// NewBloomFilterBuilder creates a builder with the default 10 bits/key (~1% FPR)
func NewBloomFilterBuilder() *BloomFilterBuilder {
	return &BloomFilterBuilder{bitsPerKey: defaultBitsPerKey}
}

// Add inserts a key into the filter.
func (b *BloomFilterBuilder) Add(key []byte) {
	b.keys = append(b.keys, bloomHash(key))
}

// Len returns the number of keys added.
func (b *BloomFilterBuilder) Len() int {
	return len(b.keys)
}

// Finish builds the bloom filter and returns its serialized form.
//
// Serialized format:
//
//	[filter_bits: variable]
//	[k: 1 byte]            number of hash probes
//
// The filter is a bit array of ceil(n * bitsPerKey / 8) bytes,
// followed by one byte storing the number of probes.
func (b *BloomFilterBuilder) Finish() []byte {
	n := len(b.keys)
	if n == 0 {
		// Empty filter: just the probe count byte
		return []byte{0}
	}

	// Calculate the optimal number of probes
	k := int(math.Round(float64(b.bitsPerKey) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	// Calculate bit array size
	nBits := n * b.bitsPerKey

	// Round up to 8 bits (1 byte)
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8 // actual number of bits

	filter := make([]byte, nBytes+1) // +1 for probe count

	// Set bits for each key
	for _, h := range b.keys {
		h1 := uint32(h)
		h2 := uint32(h >> 32)

		for i := 0; i < k; i++ {
			bit := (h1 + uint32(i)*h2) % uint32(nBits)
			filter[bit/8] |= 1 << (bit % 8)
		}
	}

	// Store probe count as last byte
	filter[nBytes] = byte(k)

	return filter
}

// BloomFilter is a read-only bloom filter decoded from serialized bytes.
type BloomFilter struct {
	data  []byte // bit array
	k     int    // number of probes
	nBits uint32 // total bits in the filter
}

// DecodeBloomFilter decodes a serialized bloom filter.
// Returns nil if the data is empty of invalid.
func DecodeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 1 {
		return nil
	}

	k := int(data[len(data)-1])
	if k == 0 {
		return nil
	}

	bits := data[:len(data)-1]
	return &BloomFilter{
		data:  bits,
		k:     k,
		nBits: uint32(len(bits)) * 8,
	}
}

// MayContain returns true if the key might be in the set,
// false if it is definitely not in the set.
func (f *BloomFilter) MayContain(key []byte) bool {
	if f == nil || f.nBits == 0 {
		return true // empty filter => assume present
	}

	h := bloomHash(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)

	for i := 0; i < f.k; i++ {
		bit := (h1 + uint32(i)*h2) % f.nBits
		if f.data[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}

	return true
}

// bloomHash computes a 64-bit FNV-1a hash of the key.
func bloomHash(key []byte) uint64 {
	var h hash.Hash64 = fnv.New64a()
	_, _ = h.Write(key)
	return h.Sum64()
}

// EstimateBloomSize returns the approximate byte size of a bloom
// filter for n keys at the default bits-per-key rate.
func EstimateBloomSize(n int) int {
	return (n*defaultBitsPerKey+7)/8 + 1
}

// encodeBloomMeta serializes bloom filter metadata into the meta block.
// Currently, the meta block is just the bloom filter bytes, but this
// allows future extension with additional metadata.
func encodeBloomMeta(bloomData []byte) []byte {
	// Format: [bloom_len: 4 bytes][bloom_data]
	buf := make([]byte, bloomMetaLenSize+len(bloomData))
	binary.LittleEndian.PutUint32(buf, uint32(len(bloomData)))
	copy(buf[4:], bloomData)
	return buf
}

// decodeBloomMeta extracts the bloom filter from a meta block.
func decodeBloomMeta(data []byte) ([]byte, error) {
	if len(data) < bloomMetaLenSize {
		return nil, ErrShortBloomMeta
	}

	bloomLen := int(binary.LittleEndian.Uint32(data))
	if len(data) < bloomMetaLenSize+bloomLen {
		return nil, ErrShortBloomMeta
	}

	return data[bloomMetaLenSize : bloomMetaLenSize+bloomLen], nil
}
