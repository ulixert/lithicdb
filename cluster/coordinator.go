package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ulixert/lithicdb/db"
	"github.com/ulixert/lithicdb/hashring"
	"github.com/ulixert/lithicdb/hlc"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
)

var (
	ErrNotEnoughReplicas = errors.New("not enough replicas in ring")
	ErrWriteQuorumNotMet = errors.New("write quorum not met")
	ErrReadQuorumNotMet  = errors.New("read quorum not met")
	ErrEmptyKey          = errors.New("key must not be empty")
)

// CoordinatorConfig tunes quorum behavior.
type CoordinatorConfig struct {
	ReplicationFactor int           // N - total replicas per key
	WriteQuorum       int           // W - acks needed for a successful write
	ReadQuorum        int           // R - responses needed for a successful read
	PerReplicaTimeout time.Duration // per-RPC deadline for replica communication
}

// DefaultCoordinatorConfig returns N=3, W=2, R=2, 5s timeout.
func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		PerReplicaTimeout: 5 * time.Second,
	}
}

// Coordinator routes client reads and writes through the hash ring,
// fanning out to N replicas and collecting quorum responses. It
// performs async read repair when stale replicas are detected.
type Coordinator struct {
	cfg        CoordinatorConfig
	selfID     string
	ring       *hashring.Ring
	membership *Membership
	clock      *hlc.Clock
	localDB    *db.DB
	dialer     ReplicaDialer
	logger     *slog.Logger
}

// NewCoordinator creates a coordinator. All dependencies are required
// except logger (defaults to slog.Default).
func NewCoordinator(
	cfg CoordinatorConfig,
	selfID string,
	ring *hashring.Ring,
	membership *Membership,
	clock *hlc.Clock,
	localDB *db.DB,
	dialer ReplicaDialer,
	logger *slog.Logger,
) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		cfg:        cfg,
		selfID:     selfID,
		ring:       ring,
		membership: membership,
		clock:      clock,
		localDB:    localDB,
		dialer:     dialer,
		logger:     logger,
	}
}

// Write replicates a key-value pair to N replicas and waits for W acks.
func (c *Coordinator) Write(ctx context.Context, key, value []byte) error {
	return c.writeInternal(ctx, key, value, false)
}

// Delete replicates a tombstone to N replicas and waits for W acks.
func (c *Coordinator) Delete(ctx context.Context, key []byte) error {
	return c.writeInternal(ctx, key, nil, true)
}

func (c *Coordinator) writeInternal(ctx context.Context, key, value []byte, deleted bool) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	ts := c.clock.Now()
	replicas := c.ring.GetNodes(key, c.cfg.ReplicationFactor)

	// Operations succeed as long as enough replicas exist to meet quorum.
	// A 1-node cluster with W=1 works; a 2-node cluster with W=2 works.
	// GetNodes caps at the number of physical nodes, so len(replicas) <= N.
	if len(replicas) < c.cfg.WriteQuorum {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughReplicas, len(replicas), c.cfg.WriteQuorum)
	}

	// Encode envelope for local writes.
	encoded, err := EncodeEnvelope(Envelope{
		Timestamp: ts,
		Deleted:   deleted,
		Value:     value,
	})
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	type ack struct {
		nodeID string
		err    error
	}
	// Buffered so all goroutines can complete without blocking,
	// even if we return early after quorum is met.
	acks := make(chan ack, len(replicas))

	for _, replica := range replicas {
		go func(node hashring.Node) {
			var writeErr error
			if node.ID == c.selfID {
				writeErr = c.localDB.Put(key, encoded)
			} else if c.membership.IsRoutable(node.ID) {
				writeErr = c.remoteWrite(ctx, node.Addr, key, value, ts, deleted)
			} else {
				// Dead node - hinted handoff will catch this.
				writeErr = fmt.Errorf("node %s is not routable", node.ID)
				c.logger.Warn("replica not routable, skipping write",
					"node", node.ID, "key_len", len(key))
			}
			acks <- ack{node.ID, writeErr}
		}(replica)
	}

	// Return as soon as W acks are collected or quorum becomes impossible.
	// maxFailures: once these many replicas fail, W acks can never be reached.
	maxFailures := len(replicas) - c.cfg.WriteQuorum + 1
	successes := 0
	failures := 0
	var lastErr error
	for successes < c.cfg.WriteQuorum && failures < maxFailures {
		a := <-acks
		if a.err == nil {
			successes++
		} else {
			failures++
			lastErr = a.err
			c.logger.Warn("replica write failed", "node", a.nodeID, "err", a.err)
		}
	}

	if successes >= c.cfg.WriteQuorum {
		return nil
	}
	return fmt.Errorf("%w: got %d/%d acks: %w",
		ErrWriteQuorumNotMet, successes, c.cfg.WriteQuorum, lastErr)
}

// remoteWrite sends a ReplicateWrite RPC to a single replica.
func (c *Coordinator) remoteWrite(ctx context.Context, addr string, key, value []byte, ts hlc.Timestamp, deleted bool) error {
	client, err := c.dialer.GetClient(addr)
	if err != nil {
		return err
	}

	rctx, cancel := context.WithTimeout(ctx, c.cfg.PerReplicaTimeout)
	defer cancel()

	_, err = client.ReplicateWrite(rctx, &pb.ReplicateWriteRequest{
		Key:   key,
		Value: value,
		Timestamp: &pb.HLCTimestamp{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   ts.NodeID,
		},
		Deleted: deleted,
	})
	return err
}
