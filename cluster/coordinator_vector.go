package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/protobuf/proto"
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

// VectorWrite replicates a vector put to N replicas using the collection name
// as the ring key (all vectors in a collection land on the same N replicas).
func (c *Coordinator) VectorWrite(ctx context.Context, collection string, id [16]byte, vec []float32, meta Metadata) error {
	if c.vectorStore == nil {
		return ErrNoVectorStore
	}

	ts := c.clock.Now()
	ver := hlcToVersion(ts)

	digest, err := c.vectorStore.ConfigDigest(collection)
	if err != nil {
		return fmt.Errorf("config digest: %w", err)
	}

	replicas := c.ring.GetNodes([]byte(collection), c.cfg.ReplicationFactor)
	if len(replicas) < c.cfg.WriteQuorum {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughReplicas, len(replicas), c.cfg.WriteQuorum)
	}

	type ack struct {
		nodeID string
		err    error
	}
	acks := make(chan ack, len(replicas))

	for _, replica := range replicas {
		go func(node hashring.Node) {
			var writeErr error
			if node.ID == c.selfID {
				writeErr = c.vectorStore.Put(collection, id, vec, meta, ver)
			} else if c.membership.IsRoutable(node.ID) {
				writeErr = c.remoteVectorWrite(ctx, node.Addr, collection, id, vec, meta, ts, digest)
			} else {
				// Dead node - store hint for later replay.
				if c.hintStore != nil {
					hintErr := c.storeVectorWriteHint(node.ID, collection, id, vec, meta, ts, digest)
					if hintErr != nil {
						c.logger.Warn("failed to store vector write hint",
							"node", node.ID, "err", hintErr)
					}
				}
				writeErr = fmt.Errorf("node %s is not routable", node.ID)
				c.logger.Warn("replica not routable, skipping vector write",
					"node", node.ID, "collection", collection)
			}
			acks <- ack{node.ID, writeErr}
		}(replica)
	}

	// Quorum counting identical to writeInternal.
	maxFailures := len(replicas) - c.cfg.WriteQuorum + 1
	successes := 0
	failures := 0
	var errs []error
	for successes < c.cfg.WriteQuorum && failures < maxFailures {
		a := <-acks
		if a.err == nil {
			successes++
		} else {
			failures++
			errs = append(errs, fmt.Errorf("node %s: %w", a.nodeID, a.err))
			c.logger.Warn("replica vector write failed", "node", a.nodeID, "err", a.err)
		}
	}

	if successes >= c.cfg.WriteQuorum {
		return nil
	}
	return fmt.Errorf("%w: got %d/%d acks: %w",
		ErrWriteQuorumNotMet, successes, c.cfg.WriteQuorum, errors.Join(errs...))
}

// VectorDelete replicates a vector delete to N replicas using collection-name ring routing.
func (c *Coordinator) VectorDelete(ctx context.Context, collection string, id [16]byte) error {
	if c.vectorStore == nil {
		return ErrNoVectorStore
	}

	ts := c.clock.Now()
	ver := hlcToVersion(ts)

	digest, err := c.vectorStore.ConfigDigest(collection)
	if err != nil {
		return fmt.Errorf("config digest: %w", err)
	}

	replicas := c.ring.GetNodes([]byte(collection), c.cfg.ReplicationFactor)
	if len(replicas) < c.cfg.WriteQuorum {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughReplicas, len(replicas), c.cfg.WriteQuorum)
	}

	type ack struct {
		nodeID string
		err    error
	}
	acks := make(chan ack, len(replicas))

	for _, replica := range replicas {
		go func(node hashring.Node) {
			var writeErr error
			if node.ID == c.selfID {
				writeErr = c.vectorStore.Delete(collection, id, ver)
			} else if c.membership.IsRoutable(node.ID) {
				writeErr = c.remoteVectorDelete(ctx, node.Addr, collection, id, ts, digest)
			} else {
				if c.hintStore != nil {
					hintErr := c.storeVectorDeleteHint(node.ID, collection, id, ts, digest)
					if hintErr != nil {
						c.logger.Warn("failed to store vector delete hint",
							"node", node.ID, "err", hintErr)
					}
				}
				writeErr = fmt.Errorf("node %s is not routable", node.ID)
			}
			acks <- ack{node.ID, writeErr}
		}(replica)
	}

	maxFailures := len(replicas) - c.cfg.WriteQuorum + 1
	successes := 0
	failures := 0
	var errs []error
	for successes < c.cfg.WriteQuorum && failures < maxFailures {
		a := <-acks
		if a.err == nil {
			successes++
		} else {
			failures++
			errs = append(errs, fmt.Errorf("node %s: %w", a.nodeID, a.err))
		}
	}

	if successes >= c.cfg.WriteQuorum {
		return nil
	}
	return fmt.Errorf("%w: got %d/%d acks: %w",
		ErrWriteQuorumNotMet, successes, c.cfg.WriteQuorum, errors.Join(errs...))
}

// VectorSearch fans out to all readable replicas, requires R successes,
// merges all responses, deduplicates, exact-reranks, and validates results.
func (c *Coordinator) VectorSearch(
	ctx context.Context,
	collection string,
	query []float32,
	k int,
	oversample int,
	efSearch int,
) ([]VectorSearchResult, error) {
	if c.vectorStore == nil {
		return nil, ErrNoVectorStore
	}

	if oversample <= 0 {
		oversample = defaultOversample
	}
	perReplicaK := k * oversample

	replicas := c.ring.GetNodes([]byte(collection), c.cfg.ReplicationFactor)

	// Filter to readable replicas.
	var readable []hashring.Node
	for _, r := range replicas {
		if c.membership.IsRoutable(r.ID) && c.membership.RingStateOf(r.ID) != RingJoining {
			readable = append(readable, r)
		}
	}
	if len(readable) < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: have %d readable, need %d",
			ErrVectorSearchFailed, len(readable), c.cfg.ReadQuorum)
	}

	digest, _ := c.vectorStore.ConfigDigest(collection)

	// Fan out to ALL readable replicas.
	type replicaResult struct {
		nodeID  string
		results []VectorSearchResult
		err     error
	}
	ch := make(chan replicaResult, len(readable))

	for _, replica := range readable {
		go func(node hashring.Node) {
			var res []VectorSearchResult
			var searchErr error
			if node.ID == c.selfID {
				res, searchErr = c.vectorStore.Search(ctx, collection, query, perReplicaK, efSearch)
			} else {
				res, searchErr = c.remoteVectorSearch(ctx, node.Addr, collection, query, perReplicaK, efSearch, digest)
			}
			ch <- replicaResult{node.ID, res, searchErr}
		}(replica)
	}

	// Require R successes, but merge ALL responses received before deadline.
	totalCap := len(readable) * perReplicaK
	allResults := make([]VectorSearchResult, 0, totalCap)
	successes := 0
	failures := 0
	maxFailures := len(readable) - c.cfg.ReadQuorum + 1
	var errs []error

	// Phase 1: wait for quorum.
	for successes < c.cfg.ReadQuorum && failures < maxFailures {
		r := <-ch
		if r.err != nil {
			failures++
			errs = append(errs, fmt.Errorf("node %s: %w", r.nodeID, r.err))
		} else {
			successes++
			allResults = append(allResults, r.results...)
		}
	}

	if successes < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: got %d/%d: %v",
			ErrVectorSearchFailed, successes, c.cfg.ReadQuorum, errors.Join(errs...))
	}

	// Phase 2: drain remaining results (non-blocking).
	remaining := len(readable) - successes - failures
	for range remaining {
		select {
		case r := <-ch:
			if r.err == nil {
				allResults = append(allResults, r.results...)
			}
		default:
			// no more results ready
		}
	}

	// Deduplicate by UUID - keep highest version.
	seen := make(map[[16]byte]int, totalCap)
	deduped := make([]VectorSearchResult, 0, len(allResults))
	for _, r := range allResults {
		if idx, ok := seen[r.ID]; ok {
			if r.Version.After(deduped[idx].Version) {
				deduped[idx] = r
			}
		} else {
			seen[r.ID] = len(deduped)
			deduped = append(deduped, r)
		}
	}

	// Exact rerank with the collection's distance function.
	distFn, err := c.vectorStore.DistanceFunc(collection)
	if err != nil {
		return nil, fmt.Errorf("get distance func: %w", err)
	}
	for i := range deduped {
		deduped[i].Distance = distFn(query, deduped[i].Vector)
	}

	// Sort by distance ascending.
	slices.SortFunc(deduped, func(a, b VectorSearchResult) int {
		return cmp.Compare(a.Distance, b.Distance)
	})

	// Post-merge validation: validate ALL candidates against local state.
	// Must not early-exit at k - substituted vectors may have worse distances
	// than candidates further down the list.
	validated := make([]VectorSearchResult, 0, len(deduped))

	for i := range deduped {
		r := &deduped[i]
		latest, err := c.vectorStore.GetLatest(collection, r.ID)
		if err != nil {
			// Can't validate - include as-is.
			validated = append(validated, *r)
			continue
		}
		if !latest.Found {
			// Not in local KV - include (might be on other replicas only).
			validated = append(validated, *r)
			continue
		}
		if latest.Version.After(r.Version) {
			if latest.Deleted {
				continue // locally known to be deleted, skip
			}
			// Locally has a newer live version - substitute.
			r.Vector = latest.Vector
			r.Distance = distFn(query, latest.Vector)
			r.Version = latest.Version
		}
		validated = append(validated, *r)
	}

	// Re-sort after substitutions (some distances may have changed).
	slices.SortFunc(validated, func(a, b VectorSearchResult) int {
		return cmp.Compare(a.Distance, b.Distance)
	})

	if len(validated) > k {
		validated = validated[:k]
	}

	return validated, nil
}

// remoteVectorWrite sends a ReplicateVectorWrite RPC to a single replica.
func (c *Coordinator) remoteVectorWrite(
	ctx context.Context, addr string,
	collection string, id [16]byte, vec []float32, meta Metadata,
	ts hlc.Timestamp, digest uint64,
) error {
	client, err := c.dialer.GetClient(addr)
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, c.cfg.PerReplicaTimeout)
	defer cancel()

	_, err = client.ReplicateVectorWrite(rctx, &pb.ReplicateVectorWriteRequest{
		Collection:   collection,
		Id:           id[:],
		Vector:       vec,
		Metadata:     metadataToProto(meta),
		Timestamp:    hlcToProto(ts),
		ConfigDigest: digest,
	})
	return err
}

// remoteVectorDelete sends a ReplicateVectorDelete RPC to a single replica.
func (c *Coordinator) remoteVectorDelete(
	ctx context.Context, addr string,
	collection string, id [16]byte,
	ts hlc.Timestamp, digest uint64,
) error {
	client, err := c.dialer.GetClient(addr)
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, c.cfg.PerReplicaTimeout)
	defer cancel()

	_, err = client.ReplicateVectorDelete(rctx, &pb.ReplicateVectorDeleteRequest{
		Collection:   collection,
		Id:           id[:],
		Timestamp:    hlcToProto(ts),
		ConfigDigest: digest,
	})
	return err
}

// remoteVectorSearch sends a ReplicateVectorSearch RPC to a single replica.
func (c *Coordinator) remoteVectorSearch(
	ctx context.Context, addr string,
	collection string, query []float32, k, efSearch int, digest uint64,
) ([]VectorSearchResult, error) {
	client, err := c.dialer.GetClient(addr)
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, c.cfg.PerReplicaTimeout)
	defer cancel()

	resp, err := client.ReplicateVectorSearch(rctx, &pb.ReplicateVectorSearchRequest{
		Collection:   collection,
		Query:        query,
		K:            int32(k),
		EfSearch:     int32(efSearch),
		ConfigDigest: digest,
	})
	if err != nil {
		return nil, err
	}

	results := make([]VectorSearchResult, len(resp.Results))
	for i, r := range resp.Results {
		var id [16]byte
		copy(id[:], r.Id)
		results[i] = VectorSearchResult{
			ID:       id,
			Vector:   r.Vector,
			Distance: r.Distance,
			Version: VectorVersion{
				WallTime: r.VersionWallTime,
				Logical:  r.VersionLogical,
			},
		}
	}
	return results, nil
}

// storeVectorWriteHint stores a vector write hint for a dead replica.
func (c *Coordinator) storeVectorWriteHint(
	nodeID string, collection string, id [16]byte, vec []float32, meta Metadata,
	ts hlc.Timestamp, digest uint64,
) error {
	if c.hintStore == nil {
		return nil
	}
	req := &pb.ReplicateVectorWriteRequest{
		Collection:   collection,
		Id:           id[:],
		Vector:       vec,
		Metadata:     metadataToProto(meta),
		Timestamp:    hlcToProto(ts),
		ConfigDigest: digest,
	}
	payload, err := marshalProto(req)
	if err != nil {
		return err
	}
	// Prepend hint type byte.
	hintValue := make([]byte, 1+len(payload))
	hintValue[0] = HintVectorWrite
	copy(hintValue[1:], payload)

	// Use collection name as the hint key (for grouping).
	return c.hintStore.Add(nodeID, []byte(collection), hintValue, ts)
}

// storeVectorDeleteHint stores a vector delete hint for a dead replica.
func (c *Coordinator) storeVectorDeleteHint(
	nodeID string, collection string, id [16]byte,
	ts hlc.Timestamp, digest uint64,
) error {
	if c.hintStore == nil {
		return nil
	}
	req := &pb.ReplicateVectorDeleteRequest{
		Collection:   collection,
		Id:           id[:],
		Timestamp:    hlcToProto(ts),
		ConfigDigest: digest,
	}
	payload, err := marshalProto(req)
	if err != nil {
		return err
	}
	hintValue := make([]byte, 1+len(payload))
	hintValue[0] = HintVectorDelete
	copy(hintValue[1:], payload)

	return c.hintStore.Add(nodeID, []byte(collection), hintValue, ts)
}

// metadataToProto converts Metadata to the proto map<string, bytes> representation.
func metadataToProto(meta Metadata) map[string][]byte {
	if meta == nil {
		return nil
	}
	result := make(map[string][]byte, len(meta))
	for k, v := range meta {
		switch val := v.(type) {
		case string:
			result[k] = []byte(val)
		case []byte:
			result[k] = val
		default:
			// For other types, skip for now - the server adapter
			// should handle full type-aware serialization.
			c := fmt.Sprintf("%v", val)
			result[k] = []byte(c)
		}
	}
	return result
}

// hlcToProto converts an HLC timestamp to its proto representation.
func hlcToProto(ts hlc.Timestamp) *pb.HLCTimestamp {
	return &pb.HLCTimestamp{
		WallTime: ts.WallTime,
		Logical:  ts.Logical,
		NodeId:   ts.NodeID,
	}
}

// marshalProto marshals a proto message to bytes.
func marshalProto(msg protoMessage) ([]byte, error) {
	return proto.Marshal(msg)
}

// protoMessage is satisfied by all generated proto message types.
type protoMessage = proto.Message

// Hint type constants for type-tagged hint values.
// Must match the constants in hintedhandoff/store.go.
const (
	HintKV           byte = 0x00
	HintVectorWrite  byte = 0xF1
	HintVectorDelete byte = 0xF2
)

// logVectorSearch is a helper for debug logging.
func logVectorSearch(logger *slog.Logger, collection string, k int, readable int, results int) {
	logger.Debug("vector search completed",
		"collection", collection,
		"k", k,
		"readable_replicas", readable,
		"results", results,
	)
}
