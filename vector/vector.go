package vector

import (
	"errors"
	"fmt"
	"log/slog"
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
