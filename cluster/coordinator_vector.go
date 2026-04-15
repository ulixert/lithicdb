package cluster

import (
	"context"
	"errors"

	"github.com/ulixert/theseon/hlc"
)

var (
	ErrNoVectorStore      = errors.New("vector store not configured")
	ErrVectorSearchFailed = errors.New("vector search quorum not met")
)

// Metadata mirrors vector.Metadata to avoid importing the vector package.
type Metadata map[string]any

// VectorVersion is a causally ordered version stamp for LWW conflict resolution.
type VectorVersion struct {
	WallTime int64
	Logical  uint32
}

// After reports whether v is strictly after other.
func (v VectorVersion) After(other VectorVersion) bool {
	return v.WallTime > other.WallTime ||
		(v.WallTime == other.WallTime && v.Logical > other.Logical)
}

// VectorSearchResult is a single result from a local or remote vector search.
type VectorSearchResult struct {
	ID       [16]byte
	Vector   []float32
	Distance float32
	Version  VectorVersion
}

// LatestEntry holds the current state of a vector for post-merge validation.
type LatestEntry struct {
	Version VectorVersion
	Vector  []float32
	Found   bool
	Deleted bool
}

// LocalVectorSearcher abstracts the local vector store for the coordinator.
// The server layer adapts *vector.VectorStore to this interface.
type LocalVectorSearcher interface {
	Put(collection string, id [16]byte, vec []float32, meta Metadata, ver VectorVersion) error
	Delete(collection string, id [16]byte, ver VectorVersion) error
	Search(ctx context.Context, collection string, query []float32, k, efSearch int) ([]VectorSearchResult, error)
	DistanceFunc(collection string) (func(a, b []float32) float32, error)
	CollectionReady(collection string) bool
	ConfigDigest(collection string) (uint64, error)
	GetLatest(collection string, id [16]byte) (LatestEntry, error)
}

// SetVectorStore attaches a local vector store for distributed vector operations.
func (c *Coordinator) SetVectorStore(vs LocalVectorSearcher) {
	c.vectorStore = vs
}

// hlcToVersion converts an HLC timestamp to a VectorVersion.
func hlcToVersion(ts hlc.Timestamp) VectorVersion {
	return VectorVersion{WallTime: ts.WallTime, Logical: ts.Logical}
}

const defaultOversample = 4
