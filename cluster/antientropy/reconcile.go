package antientropy

import (
	"context"
	"errors"
	"fmt"
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
