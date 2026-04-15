package server

import (
	"context"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/vector"
)

// vectorStoreAdapter adapts *vector.VectorStore to the cluster.LocalVectorSearcher
// interface, bridging the gap between the vector and cluster packages.
type vectorStoreAdapter struct {
	vs *vector.VectorStore
}

func newVectorStoreAdapter(vs *vector.VectorStore) *vectorStoreAdapter {
	return &vectorStoreAdapter{vs: vs}
}

func (a *vectorStoreAdapter) Put(collection string, id [16]byte, vec []float32, meta cluster.Metadata, ver cluster.VectorVersion) error {
	return a.vs.Put(collection, id, vec, vector.Metadata(meta), vector.VectorVersion{
		WallTime: ver.WallTime,
		Logical:  ver.Logical,
	})
}

func (a *vectorStoreAdapter) Delete(collection string, id [16]byte, ver cluster.VectorVersion) error {
	return a.vs.Delete(collection, id, vector.VectorVersion{
		WallTime: ver.WallTime,
		Logical:  ver.Logical,
	})
}

func (a *vectorStoreAdapter) Search(ctx context.Context, collection string, query []float32, k, efSearch int) ([]cluster.VectorSearchResult, error) {
	var opts *vector.SearchOptions
	if efSearch > 0 {
		opts = &vector.SearchOptions{EfSearch: efSearch}
	}
	results, err := a.vs.Search(collection, query, k, opts)
	if err != nil {
		return nil, err
	}
	out := make([]cluster.VectorSearchResult, len(results))
	for i, r := range results {
		out[i] = cluster.VectorSearchResult{
			ID:       r.ID,
			Vector:   r.Vector,
			Distance: r.Distance,
			Version: cluster.VectorVersion{
				WallTime: r.Version.WallTime,
				Logical:  r.Version.Logical,
			},
		}
	}
	return out, nil
}

func (a *vectorStoreAdapter) DistanceFunc(collection string) (func(a, b []float32) float32, error) {
	cfg, err := a.vs.GetCollectionConfig(collection)
	if err != nil {
		return nil, err
	}
	fn, err := vector.MetricToDistanceFunc(cfg.Metric)
	if err != nil {
		return nil, err
	}
	return fn, nil
}

func (a *vectorStoreAdapter) CollectionReady(collection string) bool {
	return a.vs.CollectionReady(collection)
}

func (a *vectorStoreAdapter) ConfigDigest(collection string) (uint64, error) {
	cfg, err := a.vs.GetCollectionConfig(collection)
	if err != nil {
		return 0, err
	}
	return vector.ConfigDigest(cfg), nil
}

func (a *vectorStoreAdapter) GetLatest(collection string, id [16]byte) (cluster.LatestEntry, error) {
	entry, err := a.vs.GetLatest(collection, id)
	if err != nil {
		return cluster.LatestEntry{}, err
	}
	return cluster.LatestEntry{
		Version: cluster.VectorVersion{
			WallTime: entry.Version.WallTime,
			Logical:  entry.Version.Logical,
		},
		Vector:  entry.Vector,
		Found:   entry.Found,
		Deleted: entry.Deleted,
	}, nil
}
