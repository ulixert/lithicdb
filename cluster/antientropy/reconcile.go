package antientropy

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
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
