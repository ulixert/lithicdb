package vector

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/kv"
	"github.com/ulixert/theseon/vector/hnsw"
)

var (
	ErrCollectionNotFound = errors.New("vector: collection not found")
	ErrCollectionExists   = errors.New("vector: collection already exists")
	ErrIndexingFailed     = errors.New("vector: KV write succeeded but HNSW indexing failed")
)

// collectionState holds the in-memory state for a single vector collection.
type collectionState struct {
	graph  *hnsw.Graph
	config CollectionConfig
	ids    map[[16]byte]uint64 // forward map: UUID → HNSW uint64 ID
	nextID uint64
	mu     sync.Mutex // serializes writes within this collection
}

// VectorStoreConfig configures the vector store.
type VectorStoreConfig struct {
	MaxMemoryBytes int64 // hard limit across all collections; 0 = no limit
}

// VectorStoreOption is a functional option for NewVectorStore.
type VectorStoreOption func(*VectorStore)

// WithMetrics sets a Metrics implementation for the vector store.
func WithMetrics(m Metrics) VectorStoreOption {
	return func(vs *VectorStore) {
		vs.metrics = m
	}
}

// WithLogger sets a logger for the vector store.
func WithLogger(l *slog.Logger) VectorStoreOption {
	return func(vs *VectorStore) {
		vs.logger = l
	}
}

// SearchOptions configures a single search query.
type SearchOptions struct {
	EfSearch int // 0 => use 2*k
}

// Result is a single search result.
type Result struct {
	ID       [16]byte
	Vector   []float32
	Metadata Metadata
	Distance float32
}

// VectorStore wraps a db.DB and per-collection HNSW graphs.
// KV is the source of truth; HNSW is a secondary in-memory index.
type VectorStore struct {
	db          *db.DB
	collections map[string]*collectionState
	config      VectorStoreConfig
	metrics     Metrics
	mu          sync.RWMutex // protects the collections map
	logger      *slog.Logger
}

// NewVectorStore creates a VectorStore backed by the given db.DB.
// It rebuilds all HNSW graphs from durable KV data.
func NewVectorStore(database *db.DB, cfg VectorStoreConfig, opts ...VectorStoreOption) (*VectorStore, error) {
	vs := &VectorStore{
		db:          database,
		collections: make(map[string]*collectionState),
		config:      cfg,
		metrics:     noopMetrics{},
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(vs)
	}
	if err := vs.loadCollections(); err != nil {
		return nil, fmt.Errorf("vector: load collections: %w", err)
	}
	return vs, nil
}

// CreateCollection registers a new vector collection.
func (vs *VectorStore) CreateCollection(name string, cfg CollectionConfig) error {
	distFn, err := metricToDistanceFunc(cfg.Metric)
	if err != nil {
		return err
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if _, ok := vs.collections[name]; ok {
		return ErrCollectionExists
	}

	graph, err := hnsw.New(hnsw.Options{
		M:           cfg.M,
		EfConstruct: cfg.EfConstruct,
		EfSearch:    cfg.EfSearch,
		Dim:         cfg.Dim,
		Dist:        distFn,
		Logger:      vs.logger,
	})
	if err != nil {
		return fmt.Errorf("vector: create graph: %w", err)
	}

	// Persist config to KV.
	configKey := makeCollectionConfigKey(name)
	configVal := encodeCollectionConfig(cfg)
	if err := vs.db.Put(configKey, configVal); err != nil {
		return fmt.Errorf("vector: persist config: %w", err)
	}

	vs.collections[name] = &collectionState{
		graph:  graph,
		config: cfg,
		ids:    make(map[[16]byte]uint64),
	}

	vs.metrics.Counter("vector.collection.created", 1, map[string]string{"collection": name})
	return nil
}

// Put inserts or updates a vector in the given collection.
func (vs *VectorStore) Put(collection string, id [16]byte, vec []float32, meta Metadata) error {
	vs.mu.RLock()
	col, ok := vs.collections[collection]
	vs.mu.RUnlock()
	if !ok {
		return ErrCollectionNotFound
	}

	if len(vec) != col.config.Dim {
		return hnsw.ErrDimensionMismatch
	}

	encoded, err := EncodeVector(vec, meta)
	if err != nil {
		return err
	}

	vectorKey := makeVectorKey(collection, id)

	col.mu.Lock()
	defer col.mu.Unlock()

	// Check MaxVectors limit (only for new inserts, not updates).
	_, isUpdate := col.ids[id]
	if !isUpdate && col.config.MaxVectors > 0 && int64(len(col.ids)) >= col.config.MaxVectors {
		return fmt.Errorf("vector: collection %q reached max vectors limit (%d)", collection, col.config.MaxVectors)
	}

	// Persist to KV first.
	if err := vs.db.Put(vectorKey, encoded); err != nil {
		return fmt.Errorf("vector: KV put: %w", err)
	}

	// Update HNSW index.
	// Insert the new node BEFORE tombstoning the old one. This ensures the
	// new node can connect to the old (still-live) node during insertion.
	// If we tombstoned first, the insert's neighbor search would find no
	// live candidates and the new node would be isolated.
	newID := col.nextID
	col.nextID++

	if err := col.graph.Insert(newID, id, vec); err != nil {
		// KV has the vector, but HNSW doesn't. On restart, loadCollections
		// will rebuild the graph and index this vector. If this was an update,
		// the old node is still live in HNSW (we haven't tombstoned it yet),
		// so Search will find the old node but KV verification will return
		// the new vector.
		vs.logger.Error("HNSW insert failed after KV write",
			"collection", collection,
			"error", err,
		)
		return fmt.Errorf("%w: %v", ErrIndexingFailed, err)
	}

	if isUpdate {
		col.graph.MarkDeleted(col.ids[id])
	}

	col.ids[id] = newID

	vs.metrics.Counter("vector.put", 1, map[string]string{"collection": collection})
	return nil
}

// Delete removes a vector from the given collection. Idempotent.
func (vs *VectorStore) Delete(collection string, id [16]byte) error {
	vs.mu.RLock()
	col, ok := vs.collections[collection]
	vs.mu.RUnlock()
	if !ok {
		return ErrCollectionNotFound
	}

	col.mu.Lock()
	defer col.mu.Unlock()

	hnswID, exists := col.ids[id]
	if !exists {
		return nil // idempotent
	}

	// KV delete first. If this fails, HNSW is untouched.
	vectorKey := makeVectorKey(collection, id)
	if err := vs.db.Delete(vectorKey); err != nil {
		return fmt.Errorf("vector: KV delete: %w", err)
	}

	col.graph.MarkDeleted(hnswID)
	delete(col.ids, id)

	vs.metrics.Counter("vector.delete", 1, map[string]string{"collection": collection})
	return nil
}

// Search finds the k nearest neighbors of the query vector.
func (vs *VectorStore) Search(collection string, query []float32, k int, opts *SearchOptions) ([]Result, error) {
	vs.mu.RLock()
	col, ok := vs.collections[collection]
	vs.mu.RUnlock()
	if !ok {
		return nil, ErrCollectionNotFound
	}

	// 2x oversample to account for stale candidates.
	ef := k * 2
	if opts != nil && opts.EfSearch > 0 {
		ef = opts.EfSearch
	}

	candidates, err := col.graph.Search(query, ef, &hnsw.SearchOptions{EfSearch: ef})
	if err != nil {
		return nil, err
	}

	// Verify each candidate against KV (source of truth).
	results := make([]Result, 0, k)
	for _, c := range candidates {
		vectorKey := makeVectorKey(collection, c.ExternalID)
		val, found := vs.db.Get(vectorKey)
		if !found || val.Tombstone {
			continue // stale candidate
		}

		vec, meta, err := DecodeVector(val.Data)
		if err != nil {
			vs.logger.Error("failed to decode vector from KV",
				"collection", collection,
				"id", c.ExternalID,
				"error", err,
			)
			continue
		}

		results = append(results, Result{
			ID:       c.ExternalID,
			Vector:   vec,
			Metadata: meta,
			Distance: c.Distance,
		})
	}

	slices.SortFunc(results, func(a, b Result) int {
		return cmp.Compare(a.Distance, b.Distance)
	})

	if len(results) > k {
		results = results[:k]
	}

	vs.metrics.Histogram("vector.search.results", float64(len(results)), map[string]string{"collection": collection})
	return results, nil
}

// Close releases in-memory resources. It does NOT close the underlying db.DB.
// The caller owns the DB lifecycle.
func (vs *VectorStore) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.collections = nil
	return nil
}

// loadCollections rebuilds all HNSW graphs from durable KV data.
// Called once during NewVectorStore. Config keys (kindConfig) sort before
// vector keys (kindVector) for the same collection, so a collection's
// config is always seen before its vectors.
func (vs *VectorStore) loadCollections() error {
	start := []byte{keyPrefixVector}
	end := []byte{keyPrefixVector + 1}
	iter := vs.db.ScanRange(start, end)
	defer iter.Close()

	for iter.IsValid() {
		userKey := kv.UserKey(iter.Key())
		value := iter.Value()

		// Tombstoned entries: Value() returns nil.
		if value == nil {
			iter.Next()
			continue
		}

		colName, uuid, kind, err := parseVectorKey(userKey)
		if err != nil {
			vs.logger.Warn("skipping unparseable vector key", "error", err)
			iter.Next()
			continue
		}

		switch kind {
		case kindConfig:
			cfg, err := decodeCollectionConfig(value)
			if err != nil {
				vs.logger.Error("failed to decode collection config", "collection", colName, "error", err)
				iter.Next()
				continue
			}

			distFn, err := metricToDistanceFunc(cfg.Metric)
			if err != nil {
				vs.logger.Error("unknown metric in collection config", "collection", colName, "metric", cfg.Metric)
				iter.Next()
				continue
			}

			graph, err := hnsw.New(hnsw.Options{
				M:           cfg.M,
				EfConstruct: cfg.EfConstruct,
				EfSearch:    cfg.EfSearch,
				Dim:         cfg.Dim,
				Dist:        distFn,
				Logger:      vs.logger,
			})
			if err != nil {
				return fmt.Errorf("vector: create graph for %q: %w", colName, err)
			}

			vs.collections[colName] = &collectionState{
				graph:  graph,
				config: cfg,
				ids:    make(map[[16]byte]uint64),
			}

		case kindVector:
			col, ok := vs.collections[colName]
			if !ok {
				vs.logger.Warn("vector key for unknown collection, skipping",
					"collection", colName)
				iter.Next()
				continue
			}

			vec, _, err := DecodeVector(value)
			if err != nil {
				vs.logger.Error("failed to decode vector during recovery",
					"collection", colName,
					"error", err,
				)
				iter.Next()
				continue
			}

			id := col.nextID
			col.nextID++
			if err := col.graph.Insert(id, uuid, vec); err != nil {
				vs.logger.Error("failed to insert vector during recovery",
					"collection", colName,
					"error", err,
				)
				iter.Next()
				continue
			}
			col.ids[uuid] = id
		}

		iter.Next()
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("vector: scan error: %w", err)
	}

	for name, col := range vs.collections {
		vs.logger.Info("loaded collection",
			"collection", name,
			"vectors", len(col.ids),
			"dim", col.config.Dim,
		)
	}

	return nil
}
