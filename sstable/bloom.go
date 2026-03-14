package sstable

// Bloom filter using the LevelDB approach: a single hash per key with
// k probe positions derived by rotating the hash. The number of hash
// functions k is stored as the last byte of the filter, so the filter
// is self-describing.
//
// With bitsPerKey=10, k≈7, the false positive rate is approximately 1%.

const bitsPerKey = 10

// bloomK computes the optimal number of hash functions.
// k = bitsPerKey * ln(2) ≈ 6.93, clamped to [1, 30].
func bloomK(bitsPerKey int) int {
	// Avoid constant float-to-int conversion that the Go compiler rejects.
	// bitsPerKey * 0.69 ≈ 6.9 → 6
	k := bitsPerKey * 69 / 100
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// BuildBloomFilter creates a bloom filter from pre-computed key hashes.
// Returns nil if hashes are empty. A nil filter is safe to pass to
// BloomMayContain, which will return true (assume key may be present).
//
// In practice, this is never called with empty hashes because the
// SSTable builder rejects empty tables (ErrEmptySSTable).
func BuildBloomFilter(hashes []uint32) []byte {
	n := len(hashes)
	if n == 0 {
		return nil
	}

	numBits := n * bitsPerKey
	if numBits < 64 {
		numBits = 64
	}

	// Round up to whole bytes
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	k := bloomK(bitsPerKey)

	// Last byte stores k
	filter := make([]byte, numBytes+1)
	filter[numBytes] = byte(k)

	for _, h := range hashes {
		delta := (h >> 17) | (h << 15) // rotate right by 17
		for j := 0; j < k; j++ {
			bitPos := h % uint32(numBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	return filter
}

// BloomMayContain checks whether a key might be in the set.
// Returns true if the key is possibly present, false if definitely absent.
//
// Returns true (safe default, never a false rejection) when:
//   - filter is nil (no bloom filter was built)
//   - filter is too short to be valid (<2 bytes)
//   - k > 30 (reserved for future filter formats)
func BloomMayContain(filter, key []byte) bool {
	if len(filter) < 2 {
		return true
	}

	k := int(filter[len(filter)-1])
	if k < 1 || k > 30 {
		// Reserved for future filter formats; assume present
		return true
	}

	numBits := uint32((len(filter) - 1) * 8)
	h := BloomHash(key)
	delta := (h >> 17) | (h << 15)

	for j := 0; j < k; j++ {
		bitPos := h % numBits
		if filter[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}

	return true
}

// BloomHash computes the hash of a key for the bloom filter.
// Exported so the SSTable builder can store hashes instead of full keys.
// Adapted from LevelDB's bloom filter implementation.
func BloomHash(data []byte) uint32 {
	const (
		seed = uint32(0xbc9f1d34)
		m    = uint32(0xc6a4a793)
	)

	h := seed ^ (uint32(len(data)) * m)

	for len(data) >= 4 {
		h += uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		h *= m
		h ^= h >> 16
		data = data[4:]
	}

	switch len(data) {
	case 3:
		h += uint32(data[2]) << 16
		fallthrough
	case 2:
		h += uint32(data[1]) << 8
		fallthrough
	case 1:
		h += uint32(data[0])
		h *= m
		h ^= h >> 24
	}

	return h
}
