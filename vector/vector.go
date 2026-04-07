package vector

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// WithSnapshots provides HNSW snapshot info for snapshot-based recovery.
// If a collection has a snapshot, it will be loaded from the disk instead
// of doing a full graph rebuild from KV.
func WithSnapshots(snapshots map[string]SnapshotInfo) VectorStoreOption {
	return func(vs *VectorStore) {
		vs.snapshots = snapshots
	}
}

// SnapshotInfo describes a persisted HNSW graph snapshot file.
type SnapshotInfo struct {
	Collection string
	Seq        uint64
	Filename   string
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
	snapshots   map[string]SnapshotInfo // optional, for snapshot-based recovery
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

		if len(results) == k {
			break // candidates are pre-sorted by distance; remaining are farther
		}
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

// loadCollections recovers all HNSW graphs from a durable state.
// For collections with a snapshot, it restores from the snapshot then
// reconciles with KV. For others, it does a full rebuild from KV.
func (vs *VectorStore) loadCollections() error {
	if err := vs.loadCollectionConfigs(); err != nil {
		return err
	}

	for name, col := range vs.collections {
		snap, hasSnap := vs.snapshots[name]
		if hasSnap {
			if err := vs.loadFromSnapshot(name, col, snap); err != nil {
				vs.logger.Warn("snapshot recovery failed, falling back to full rebuild",
					"collection", name,
					"error", err,
				)
				// Reset state and fall back to full rebuild.
				col.ids = make(map[[16]byte]uint64)
				col.nextID = 0
				graph, err := hnsw.New(hnsw.Options{
					M:           col.config.M,
					EfConstruct: col.config.EfConstruct,
					EfSearch:    col.config.EfSearch,
					Dim:         col.config.Dim,
					Dist:        mustDistanceFunc(col.config.Metric),
					Logger:      vs.logger,
				})
				if err != nil {
					return fmt.Errorf("vector: recreate graph for %q: %w", name, err)
				}
				col.graph = graph
				if err := vs.fullRebuild(name, col); err != nil {
					return err
				}
			}
		} else {
			if err := vs.fullRebuild(name, col); err != nil {
				return err
			}
		}

		vs.logger.Info("loaded collection",
			"collection", name,
			"vectors", len(col.ids),
			"dim", col.config.Dim,
		)
	}

	return nil
}

// loadCollectionConfigs scans the KV vector namespace for config keys
// and creates an empty collectionState for each.
func (vs *VectorStore) loadCollectionConfigs() error {
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

		colName, _, kind, err := parseVectorKey(userKey)
		if err != nil {
			vs.logger.Warn("skipping unparseable vector key", "error", err)
			iter.Next()
			continue
		}

		if kind == kindConfig {
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
		}

		iter.Next()
	}

	return iter.Err()
}

// fullRebuild rebuilds one collection's HNSW graph from KV vector entries.
func (vs *VectorStore) fullRebuild(name string, col *collectionState) error {
	start := makeVectorKeyPrefix(name)
	end := makeVectorKeyPrefixEnd(name)
	iter := vs.db.ScanRange(start, end)
	defer iter.Close()

	for iter.IsValid() {
		userKey := kv.UserKey(iter.Key())
		value := iter.Value()

		if value == nil {
			iter.Next()
			continue
		}

		_, uuid, _, err := parseVectorKey(userKey)
		if err != nil {
			vs.logger.Warn("skipping unparseable vector key", "error", err)
			iter.Next()
			continue
		}

		vec, _, err := DecodeVector(value)
		if err != nil {
			vs.logger.Error("failed to decode vector during recovery",
				"collection", name, "error", err)
			iter.Next()
			continue
		}

		id := col.nextID
		col.nextID++
		if err := col.graph.Insert(id, uuid, vec); err != nil {
			vs.logger.Error("failed to insert vector during recovery",
				"collection", name, "error", err)
			iter.Next()
			continue
		}
		col.ids[uuid] = id

		iter.Next()
	}

	return iter.Err()
}

// loadFromSnapshot recovers a collection using a snapshot file, then
// reconciles with KV for any changes that happened after the snapshot.
func (vs *VectorStore) loadFromSnapshot(name string, col *collectionState, snap SnapshotInfo) error {
	snapPath := filepath.Join(vs.db.Dir(), snap.Filename)
	f, err := os.Open(snapPath)
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat snapshot: %w", err)
	}

	data, err := hnsw.ReadSnapshot(f, fi.Size())
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	if err := col.graph.RestoreFromSnapshot(data); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	// Rebuild ids map from restored nodes.
	for _, node := range data.Nodes {
		col.ids[node.ExternalID] = node.ID
		if node.ID >= col.nextID {
			col.nextID = node.ID + 1
		}
	}

	// Reconcile with KV: scan all vector keys for this collection.
	// SnapshotIterator emits exactly one entry per user key (newest visible).
	unseen := make(map[[16]byte]struct{}, len(col.ids))
	for uuid := range col.ids {
		unseen[uuid] = struct{}{}
	}

	var inserted, updated, removed int
	start := makeVectorKeyPrefix(name)
	end := makeVectorKeyPrefixEnd(name)
	iter := vs.db.ScanRange(start, end)
	defer iter.Close()

	for iter.IsValid() {
		ikey := iter.Key()
		userKey := kv.UserKey(ikey)
		seq := kv.SeqNum(ikey)
		value := iter.Value()

		_, uuid, _, err := parseVectorKey(userKey)
		if err != nil {
			vs.logger.Warn("skipping unparseable vector key during reconcile", "error", err)
			iter.Next()
			continue
		}

		delete(unseen, uuid)

		if value == nil {
			// Tombstone: vector was deleted after snapshot.
			if oldID, exists := col.ids[uuid]; exists {
				col.graph.MarkDeleted(oldID)
				delete(col.ids, uuid)
				removed++
			}
			iter.Next()
			continue
		}

		if seq <= snap.Seq {
			// Entry unchanged since snapshot, skip.
			iter.Next()
			continue
		}

		// Entry is newer than snapshot: either a new insert or an update.
		vec, _, err := DecodeVector(value)
		if err != nil {
			vs.logger.Error("failed to decode vector during reconcile",
				"collection", name, "error", err)
			iter.Next()
			continue
		}

		if oldID, exists := col.ids[uuid]; exists {
			// Update: tombstone old node, insert new.
			col.graph.MarkDeleted(oldID)
			updated++
		} else {
			inserted++
		}

		newID := col.nextID
		col.nextID++
		if err := col.graph.Insert(newID, uuid, vec); err != nil {
			vs.logger.Error("failed to insert vector during reconcile",
				"collection", name, "error", err)
			iter.Next()
			continue
		}
		col.ids[uuid] = newID

		iter.Next()
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("reconcile scan error: %w", err)
	}

	// Any UUID still in unseen was deleted, AND its tombstone was
	// compacted away (it doesn't appear in KV at all).
	for uuid := range unseen {
		if oldID, exists := col.ids[uuid]; exists {
			col.graph.MarkDeleted(oldID)
			delete(col.ids, uuid)
			removed++
		}
	}

	vs.logger.Info("snapshot recovery complete",
		"collection", name,
		"restored", len(data.Nodes),
		"inserted", inserted,
		"updated", updated,
		"removed", removed,
	)

	return nil
}

// SnapshotAll writes HNSW snapshots for all non-empty collections.
// Each collection's snapshot seq is captured under that collection's
// write lock, ensuring it exactly matches the graph state.
func (vs *VectorStore) SnapshotAll(dir string) ([]SnapshotInfo, error) {
	vs.mu.RLock()
	names := make([]string, 0, len(vs.collections))
	for name := range vs.collections {
		names = append(names, name)
	}
	vs.mu.RUnlock()

	var results []SnapshotInfo
	for _, name := range names {
		vs.mu.RLock()
		col := vs.collections[name]
		vs.mu.RUnlock()
		if col == nil || col.graph.Len() == 0 {
			continue
		}

		info, err := vs.snapshotCollection(dir, name, col)
		if err != nil {
			vs.logger.Error("failed to snapshot collection",
				"collection", name, "error", err)
			continue
		}
		results = append(results, info)
	}

	return results, nil
}

// snapshotCollection writes a single collection's HNSW snapshot to disk.
func (vs *VectorStore) snapshotCollection(dir, name string, col *collectionState) (SnapshotInfo, error) {
	// Capture seq under the collection's write lock to ensure it matches
	// the graph state exactly.
	col.mu.Lock()
	seq := vs.db.CurrentSeq()
	metric := col.config.Metric

	filename := fmt.Sprintf("%s.hnsw.%d.snap", name, seq)
	tmpPath := filepath.Join(dir, filename+".tmp")
	finalPath := filepath.Join(dir, filename)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		col.mu.Unlock()
		return SnapshotInfo{}, fmt.Errorf("create temp file: %w", err)
	}

	bw := bufio.NewWriterSize(f, 4*1024*1024) // 4MB buffer
	err = col.graph.WriteSnapshot(bw, seq, metric)
	col.mu.Unlock() // release collection lock; writes to this collection can resume

	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return SnapshotInfo{}, fmt.Errorf("write snapshot: %w", err)
	}

	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return SnapshotInfo{}, fmt.Errorf("flush: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return SnapshotInfo{}, fmt.Errorf("fsync: %w", err)
	}
	_ = f.Close()

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return SnapshotInfo{}, fmt.Errorf("rename: %w", err)
	}

	if err := syncDir(dir); err != nil {
		return SnapshotInfo{}, fmt.Errorf("sync dir: %w", err)
	}

	return SnapshotInfo{
		Collection: name,
		Seq:        seq,
		Filename:   filename,
	}, nil
}

// CleanupSnapshotFiles removes stale snapshot files not in validFiles,
// and any leftover .snap.tmp files.
func CleanupSnapshotFiles(dir string, validFiles map[string]bool) error {
	// Remove temp files.
	tmps, _ := filepath.Glob(filepath.Join(dir, "*.hnsw.*.snap.tmp"))
	for _, p := range tmps {
		_ = os.Remove(p)
	}

	// Remove stale snapshots.
	snaps, _ := filepath.Glob(filepath.Join(dir, "*.hnsw.*.snap"))
	for _, p := range snaps {
		name := filepath.Base(p)
		if !validFiles[name] {
			_ = os.Remove(p)
		}
	}

	return nil
}

// syncDir fsyncs a directory to ensure rename visibility.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// mustDistanceFunc returns the distance function for a metric, panicking
// on unknown metrics (should only be called with validated configs).
func mustDistanceFunc(metric uint8) hnsw.DistanceFunc {
	fn, err := metricToDistanceFunc(metric)
	if err != nil {
		panic(err)
	}
	return fn
}
