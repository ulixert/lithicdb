package antientropy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/metrics"
	pb "github.com/ulixert/theseon/proto/theseonpb"
)

// Dialer abstracts the gRPC client pool. Same shape as
// cluster.ReplicaDialer / hintedhandoff.ReplicaDialer.
type Dialer interface {
	GetClient(addr string) (pb.InternalServiceClient, error)
}

// Repairer applies a (key, value, ts, deleted) repair to a target
// replica preserving the source HLC bit-for-bit. Implemented by
// *cluster.Coordinator.ApplyRepair.
type Repairer interface {
	ApplyRepair(
		ctx context.Context,
		targetID, targetAddr string,
		key, value []byte,
		ts hlc.Timestamp,
		deleted bool,
	) error
}

// ReconcileStats summarizes one reconcile round.
type ReconcileStats struct {
	PeerID          string
	KeysScanned     uint64
	DivergentLeaves uint64
	KeysRepaired    uint64
	Duration        time.Duration
	Err             error
}

// ErrRingVersionMismatch signals that the peer's ring descriptor is
// at a different version than ours; the round should abort and retry.
var ErrRingVersionMismatch = errors.New("antientropy: ring version mismatch")

const (
	// pushBatchSize caps the size of a single ReplicateWriteBatch during
	// repair. Bounds peak memory and RPC payload independent of how
	// large a divergent bucket is. Must be > 0.
	pushBatchSize = 256
)

// reconciler executes one reconcile round between selfID and peerID.
// One-shot: instantiate, Run(ctx), inspect stats.
type reconciler struct {
	selfID            string
	peerID            string
	peerAddr          string
	ring              *hashring.Ring
	database          *db.DB
	dialer            Dialer
	repairer          Repairer
	cfg               cluster.AntiEntropyConfig
	ringVersion       uint64
	replicationFactor int
	logger            *slog.Logger
}

// newReconciler builds a one-shot reconciler. Caller is responsible for
// only invoking it once.
func newReconciler(
	selfID, peerID, peerAddr string,
	ring *hashring.Ring,
	database *db.DB,
	dialer Dialer,
	repairer Repairer,
	cfg cluster.AntiEntropyConfig,
	ringVersion uint64,
	replicationFactor int,
	logger *slog.Logger,
) *reconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &reconciler{
		selfID:            selfID,
		peerID:            peerID,
		peerAddr:          peerAddr,
		ring:              ring,
		database:          database,
		dialer:            dialer,
		repairer:          repairer,
		cfg:               cfg,
		ringVersion:       ringVersion,
		replicationFactor: replicationFactor,
		logger:            logger,
	}
}

// Run executes the reconcile state machine: build local tree, exchange
// roots with peer, descend on mismatch, stream-compare divergent leaves,
// and apply LWW repairs.
func (r *reconciler) Run(ctx context.Context) ReconcileStats {
	start := time.Now()
	stats := ReconcileStats{PeerID: r.peerID}

	defer func() {
		stats.Duration = time.Since(start)
		metrics.AEReconcileDuration.Observe(stats.Duration.Seconds())
	}()

	digest := r.digest()

	// Phase 1: build the local tree. Snapshot-filtered (db.Scan wraps SnapshotIterator).
	srcCounter := uint64(0)
	src := NewDBSource(r.database, r.ring, r.selfID, r.peerID, r.replicationFactor)
	src.SetScannedCounter(&srcCounter)

	localTree, err := BuildTree(src, digest.GraceCutoffWall, r.cfg.Fanout, r.cfg.Depth)
	stats.KeysScanned = srcCounter
	metrics.AEKeysScanned.Add(float64(srcCounter))
	if err != nil {
		stats.Err = fmt.Errorf("build local tree: %w", err)
		return stats
	}

	client, err := r.dialer.GetClient(r.peerAddr)
	if err != nil {
		stats.Err = fmt.Errorf("dial peer %s: %w", r.peerID, err)
		return stats
	}

	// Phase 2: compare roots.
	rootResp, err := client.ComputeAERoot(ctx, &pb.AERootRequest{Digest: digest.toProto()})
	if err != nil {
		stats.Err = fmt.Errorf("ComputeAERoot: %w", err)
		return stats
	}
	if rootResp.RingVersionMismatch {
		stats.Err = ErrRingVersionMismatch
		return stats
	}
	if rootResp.RootHash == localTree.Root() {
		return stats // in sync
	}

	// Phase 3: BFS descend to find divergent leaf buckets.
	divergentBuckets, err := r.findDivergentBuckets(ctx, client, localTree, digest)
	if err != nil {
		stats.Err = err
		return stats
	}
	stats.DivergentLeaves = uint64(len(divergentBuckets))
	metrics.AEDivergentLeaves.Add(float64(len(divergentBuckets)))

	// Phase 4: reconcile each divergent bucket.
	for _, bucket := range divergentBuckets {
		if stats.KeysRepaired >= uint64(r.cfg.MaxRepairPerRound) {
			r.logger.Info("anti-entropy hit per-round repair cap, deferring rest",
				"peer", r.peerID, "cap", r.cfg.MaxRepairPerRound)
			break
		}
		repaired, err := r.reconcileBucket(ctx, client, digest, bucket, r.cfg.MaxRepairPerRound-int(stats.KeysRepaired))
		stats.KeysRepaired += uint64(repaired)
		if err != nil {
			stats.Err = fmt.Errorf("reconcile bucket %d: %w", bucket, err)
			return stats
		}
	}

	return stats
}

// findDivergentBuckets walks the tree level-by-level via GetAESubtree
// RPCs, descending only into mismatched subtrees. Returns the bucket
// indices whose leaves differ.
func (r *reconciler) findDivergentBuckets(
	ctx context.Context,
	client pb.InternalServiceClient,
	localTree *Tree,
	digest keyspaceDigest,
) ([]int, error) {
	type frame struct {
		path []int
	}
	queue := []frame{{path: nil}}
	var divergent []int

	for len(queue) > 0 {
		f := queue[0]
		queue[0] = frame{}
		queue = queue[1:]

		// Local children on this path.
		localChildren, err := localTree.ChildrenAt(f.path)
		if err != nil {
			return nil, fmt.Errorf("local children at %v: %w", f.path, err)
		}

		// Peer children at the same path.
		req := &pb.AESubtreeRequest{
			Digest: digest.toProto(),
			Path:   uint32Path(f.path),
		}
		resp, err := client.GetAESubtree(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("GetAESubtree path=%v: %w", f.path, err)
		}
		if resp.RingVersionMismatch {
			return nil, ErrRingVersionMismatch
		}
		if len(resp.ChildHashes) != len(localChildren) {
			return nil, fmt.Errorf("peer returned %d children, expected %d",
				len(resp.ChildHashes), len(localChildren))
		}

		nextLevel := len(f.path) + 1
		for i, peerHash := range resp.ChildHashes {
			if peerHash == localChildren[i] {
				continue
			}
			childPath := append(append([]int(nil), f.path...), i)
			if nextLevel == localTree.Depth() {
				// Leaf level - record the bucket index.
				bucketIdx := 0
				for _, p := range childPath {
					bucketIdx = bucketIdx*localTree.Fanout() + p
				}
				divergent = append(divergent, bucketIdx)
			} else {
				queue = append(queue, frame{path: childPath})
			}
		}
	}
	return divergent, nil
}

// reconcileBucket stream-compares the local bucket against the peer's
// bucket entries via GetAELeafKeys, applying LWW repairs in both
// directions.
//
// CRITICAL ordering: the local bucket is fully drained and the iterator
// closed BEFORE any repair Puts run. Memtable iterators hold a memtable
// RLock for their lifetime; calling db.Put on the same memtable while
// an iterator is open deadlocks (RWMutex is not reentrant). After
// draining, peer leaves are streamed and merged against the snapshotted
// local bucket; repair Puts may then run freely. Memory is bounded by
// the local bucket size - for fanout 16 / depth 4 that's ~1/65k of total
// keys, which is acceptable for v1. The peer side remains fully
// streaming.
//
// Returns the number of keys repaired (at most repairBudget).
func (r *reconciler) reconcileBucket(
	ctx context.Context,
	client pb.InternalServiceClient,
	digest keyspaceDigest,
	bucket int,
	repairBudget int,
) (int, error) {
	if repairBudget <= 0 {
		return 0, nil
	}

	// Drain the local bucket into memory, then close the iterator. After
	// this the memtable is unlocked and Puts can run.
	localEntries, err := r.drainLocalBucket(bucket)
	if err != nil {
		return 0, err
	}

	// Peer leaf stream.
	stream, err := client.GetAELeafKeys(ctx, &pb.AELeafRequest{
		Digest:      digest.toProto(),
		BucketIndex: uint32(bucket),
	})
	if err != nil {
		return 0, fmt.Errorf("GetAELeafKeys bucket=%d: %w", bucket, err)
	}

	pushBatch := make([]*pb.ReplicateWriteRequest, 0, pushBatchSize)
	repaired := 0

	flushPush := func() error {
		if len(pushBatch) == 0 {
			return nil
		}
		_, err := client.ReplicateWriteBatch(ctx, &pb.ReplicateWriteBatchRequest{Entries: pushBatch})
		if err != nil {
			return fmt.Errorf("ReplicateWriteBatch (push, %d entries): %w", len(pushBatch), err)
		}
		metrics.AEKeysRepaired.WithLabelValues("push").Add(float64(len(pushBatch)))
		pushBatch = pushBatch[:0]
		return nil
	}

	pushLocal := func(e Entry) error {
		// Fetch the live value bytes from local DB (the source only carries ts/deleted).
		val, found := r.database.Get(e.Key)
		if !found {
			// Key was deleted between scan and now; skip - peer will
			// learn about the tombstone via a future round.
			return nil
		}
		env, err := cluster.DecodeEnvelope(val.Data)
		if err != nil {
			return fmt.Errorf("decode envelope (push key len=%d): %w", len(e.Key), err)
		}
		// Use the freshly-read envelope's ts/deleted to avoid races where
		// the LSM has a newer version than what the source captured.
		pushBatch = append(pushBatch, &pb.ReplicateWriteRequest{
			Key:   append([]byte(nil), e.Key...),
			Value: env.Value,
			Timestamp: &pb.HLCTimestamp{
				WallTime: env.Timestamp.WallTime,
				Logical:  env.Timestamp.Logical,
				NodeId:   env.Timestamp.NodeID,
			},
			Deleted: env.Deleted,
		})
		repaired++
		if len(pushBatch) >= pushBatchSize {
			return flushPush()
		}
		return nil
	}

	pullFromPeer := func(key []byte) error {
		// Fetch authoritative value from peer.
		readResp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{Key: key})
		if err != nil {
			return fmt.Errorf("ReplicateRead (pull, key len=%d): %w", len(key), err)
		}
		if !readResp.Found {
			// Peer no longer has it - skip.
			return nil
		}
		ts := hlc.Timestamp{
			WallTime: readResp.Timestamp.WallTime,
			Logical:  readResp.Timestamp.Logical,
			NodeID:   readResp.Timestamp.NodeId,
		}
		err = r.repairer.ApplyRepair(ctx, r.selfID, "" /* unused for self */, key,
			readResp.Value, ts, readResp.Deleted)
		if err != nil {
			return fmt.Errorf("ApplyRepair (pull, key len=%d): %w", len(key), err)
		}
		metrics.AEKeysRepaired.WithLabelValues("pull").Inc()
		repaired++
		return nil
	}

	// Stream-merge: localEntries is sorted by user key (drained in scan
	// order); peer emits in user-key sorted order from its bucket source.
	var (
		localIdx    int
		peerEntry   *pb.AELeafEntry
		peerErr     error
		havePeerEnd bool
	)
	hasLocal := func() bool { return localIdx < len(localEntries) }
	currentLocal := func() Entry { return localEntries[localIdx] }
	advanceLocal := func() { localIdx++ }
	advancePeer := func() {
		if havePeerEnd {
			return
		}
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			peerEntry = nil
			havePeerEnd = true
			return
		}
		if err != nil {
			peerEntry = nil
			havePeerEnd = true
			peerErr = err
			return
		}
		peerEntry = msg
	}
	advancePeer()

	for repaired < repairBudget {
		if peerErr != nil {
			return repaired, fmt.Errorf("peer leaf stream: %w", peerErr)
		}
		if !hasLocal() && peerEntry == nil {
			break
		}

		switch {
		case !hasLocal():
			// Only peer has more entries → pull each.
			if err := pullFromPeer(peerEntry.Key); err != nil {
				return repaired, err
			}
			advancePeer()

		case peerEntry == nil:
			// Only local has more → push each.
			if err := pushLocal(currentLocal()); err != nil {
				return repaired, err
			}
			advanceLocal()

		default:
			localEntry := currentLocal()
			cmp := bytes.Compare(localEntry.Key, peerEntry.Key)
			switch {
			case cmp == 0:
				// Both sides have key - compare HLC, repair the loser.
				peerTS := hlc.Timestamp{
					WallTime: peerEntry.Timestamp.WallTime,
					Logical:  peerEntry.Timestamp.Logical,
					NodeID:   peerEntry.Timestamp.NodeId,
				}
				if localEntry.Timestamp.Less(peerTS) {
					if err := pullFromPeer(peerEntry.Key); err != nil {
						return repaired, err
					}
				} else if peerTS.Less(localEntry.Timestamp) {
					if err := pushLocal(localEntry); err != nil {
						return repaired, err
					}
				}
				if localEntry.Timestamp.Equal(peerTS) && localEntry.Deleted != peerEntry.Deleted {
					r.logger.Warn("anti-entropy: equal HLC but different deleted bits",
						"key_len", len(localEntry.Key),
						"local_deleted", localEntry.Deleted,
						"peer_deleted", peerEntry.Deleted)
				}
				advanceLocal()
				advancePeer()

			case cmp < 0:
				// Local has key, peer doesn't → push.
				if err := pushLocal(localEntry); err != nil {
					return repaired, err
				}
				advanceLocal()

			default: // cmp > 0
				// Peer has key, local doesn't → pull.
				if err := pullFromPeer(peerEntry.Key); err != nil {
					return repaired, err
				}
				advancePeer()
			}
		}
	}

	if err := flushPush(); err != nil {
		return repaired, err
	}
	return repaired, nil
}

// drainLocalBucket reads the entire local bucket into a sorted slice
// and closes the iterator before returning. This is required because
// applying repair Puts to the local DB would deadlock against the
// memtable RLock held by an open iterator (RWMutex is not reentrant).
//
// Memory bound: O(local bucket size). With fanout 16 / depth 4, that's
// ~1/65k of total keys per bucket on average. Acceptable for v1.
func (r *reconciler) drainLocalBucket(bucket int) ([]Entry, error) {
	bs, err := NewBucketSource(r.database, r.ring, r.selfID, r.peerID,
		r.replicationFactor, r.cfg.Fanout, r.cfg.Depth, bucket)
	if err != nil {
		return nil, err
	}
	defer bs.Close()

	graceCutoff := time.Now().Add(-r.cfg.GracePeriod).UnixNano()
	var entries []Entry
	for {
		e, ok, err := bs.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		// Symmetric grace filter, mirroring BuildTree and the server.
		if e.Timestamp.WallTime >= graceCutoff {
			continue
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// keyspaceDigest is the Go-side mirror of pb.AEKeyspaceDigest. It
// identifies both endpoints so each side can compute the "other" peer
// from its own perspective and apply the symmetric ShouldReconcile
// filter.
type keyspaceDigest struct {
	RingVersion       uint64
	InitiatorID       string
	TargetID          string
	Depth             uint32
	Fanout            uint32
	GraceCutoffWall   int64
	ReplicationFactor int32
}

// OtherFrom returns the endpoint ID that ISN'T selfID. If selfID
// matches neither side, returns "" (caller should reject the request).
func (d keyspaceDigest) OtherFrom(selfID string) string {
	switch selfID {
	case d.InitiatorID:
		return d.TargetID
	case d.TargetID:
		return d.InitiatorID
	default:
		return ""
	}
}

func (d keyspaceDigest) toProto() *pb.AEKeyspaceDigest {
	return &pb.AEKeyspaceDigest{
		RingVersion:       d.RingVersion,
		InitiatorId:       d.InitiatorID,
		TargetId:          d.TargetID,
		Depth:             d.Depth,
		Fanout:            d.Fanout,
		GraceCutoffWall:   d.GraceCutoffWall,
		ReplicationFactor: d.ReplicationFactor,
	}
}

func digestFromProto(p *pb.AEKeyspaceDigest) keyspaceDigest {
	if p == nil {
		return keyspaceDigest{}
	}
	return keyspaceDigest{
		RingVersion:       p.RingVersion,
		InitiatorID:       p.InitiatorId,
		TargetID:          p.TargetId,
		Depth:             p.Depth,
		Fanout:            p.Fanout,
		GraceCutoffWall:   p.GraceCutoffWall,
		ReplicationFactor: p.ReplicationFactor,
	}
}

func (r *reconciler) digest() keyspaceDigest {
	return keyspaceDigest{
		RingVersion:       r.ringVersion,
		InitiatorID:       r.selfID,
		TargetID:          r.peerID,
		Depth:             uint32(r.cfg.Depth),
		Fanout:            uint32(r.cfg.Fanout),
		GraceCutoffWall:   time.Now().Add(-r.cfg.GracePeriod).UnixNano(),
		ReplicationFactor: int32(r.replicationFactor),
	}
}

func uint32Path(p []int) []uint32 {
	out := make([]uint32, len(p))
	for i, v := range p {
		out[i] = uint32(v)
	}
	return out
}

func intPath(p []uint32) []int {
	out := make([]int, len(p))
	for i, v := range p {
		out[i] = int(v)
	}
	return out
}
