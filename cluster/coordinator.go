package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ulixert/theseon/cluster/hintedhandoff"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
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
	cfg         CoordinatorConfig
	selfID      string
	ring        *hashring.Ring
	membership  *Membership
	clock       *hlc.Clock
	localDB     *db.DB
	dialer      ReplicaDialer
	hintStore   *hintedhandoff.Store
	vectorStore LocalVectorSearcher
	logger      *slog.Logger
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

// SetHintStore attaches a hint store for hinted handoff. When set,
// writes to dead replicas are buffered for later replay.
func (c *Coordinator) SetHintStore(hs *hintedhandoff.Store) {
	c.hintStore = hs
}

// splitReplicas partitions ring replicas into voters (Active) and learners
// (Joining). Only voter acks count toward write quorum; learners receive
// fire-and-forget writes for data seeding during onboarding.
func (c *Coordinator) splitReplicas(replicas []hashring.Node) (voters, learners []hashring.Node) {
	for _, r := range replicas {
		if c.membership.RingStateOf(r.ID) == RingJoining {
			learners = append(learners, r)
		} else {
			voters = append(voters, r)
		}
	}
	return voters, learners
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
	//
	// Split into voters (Active) and learners (Joining).
	// Only voter acks count toward write quorum.
	voters, learners := c.splitReplicas(replicas)
	if len(voters) < c.cfg.WriteQuorum {
		return fmt.Errorf("%w: have %d active replicas, need %d", ErrNotEnoughReplicas, len(voters), c.cfg.WriteQuorum)
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

	// Fire-and-forget writes to learners (data seeding, no quorum impact).
	for _, node := range learners {
		go func(n hashring.Node) {
			if !c.membership.IsRoutable(n.ID) {
				return
			}
			if err := c.remoteWrite(ctx, n.Addr, key, value, ts, deleted); err != nil {
				c.logger.Warn("learner write failed", "node", n.ID, "err", err)
			}
		}(node)
	}

	type ack struct {
		nodeID string
		err    error
	}
	// Buffered so all goroutines can complete without blocking,
	// even if we return early after quorum is met.
	acks := make(chan ack, len(voters))

	for _, replica := range voters {
		go func(node hashring.Node) {
			var writeErr error
			if node.ID == c.selfID {
				writeErr = c.localDB.Put(key, encoded)
			} else if c.membership.IsRoutable(node.ID) {
				writeErr = c.remoteWrite(ctx, node.Addr, key, value, ts, deleted)
			} else {
				// Dead voter - store hint for later replay.
				if c.hintStore != nil {
					if hintErr := c.hintStore.Add(node.ID, key, encoded, ts); hintErr != nil {
						c.logger.Warn("failed to store hint",
							"node", node.ID, "err", hintErr)
					}
				}
				writeErr = fmt.Errorf("node %s is not routable", node.ID)
				c.logger.Warn("replica not routable, skipping write",
					"node", node.ID, "key_len", len(key))
			}
			acks <- ack{node.ID, writeErr}
		}(replica)
	}

	// Return as soon as W acks are collected or quorum becomes impossible.
	// maxFailures: once these many voters fail, W acks can never be reached.
	maxFailures := len(voters) - c.cfg.WriteQuorum + 1
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
			c.logger.Warn("replica write failed", "node", a.nodeID, "err", a.err)
		}
	}

	if successes >= c.cfg.WriteQuorum {
		return nil
	}
	return fmt.Errorf("%w: got %d/%d acks: %w",
		ErrWriteQuorumNotMet, successes, c.cfg.WriteQuorum, errors.Join(errs...))
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

// ReadResult is the coordinator's response for a quorum read.
type ReadResult struct {
	Value   []byte
	Found   bool
	Deleted bool
}

// replicaReadResult holds a single replica's response during a quorum read.
type replicaReadResult struct {
	nodeID string
	addr   string
	resp   *pb.ReplicateReadResponse
	err    error
}

// Read fans out to N replicas, waits for R responses, returns the
// newest value, and asynchronously repairs stale replicas.
func (c *Coordinator) Read(ctx context.Context, key []byte) (ReadResult, error) {
	if len(key) == 0 {
		return ReadResult{}, ErrEmptyKey
	}

	replicas := c.ring.GetNodes(key, c.cfg.ReplicationFactor)

	// Filter to eligible replicas by ring state only: JOINING nodes receive
	// writes but don't serve reads (they may not have all data yet). We do
	// NOT filter by IsRoutable here: that liveness bit is flap-prone under
	// load (SWIM pings can queue up and transiently mark alive replicas as
	// suspect) and using it as a pre-dispatch gate caused R=N reads to
	// fast-fail as soon as one replica hiccupped. Instead, dispatch to all
	// eligible replicas and let the collection loop count per-RPC failures
	// — mirroring the write path's behavior.
	var readable []hashring.Node
	for _, r := range replicas {
		if c.membership.RingStateOf(r.ID) != RingJoining {
			readable = append(readable, r)
		}
	}

	// Same intentional semantics as writes: succeed with fewer than N
	// active replicas as long as R can still be met.
	if len(readable) < c.cfg.ReadQuorum {
		return ReadResult{}, fmt.Errorf("%w: have %d active replicas, need %d",
			ErrReadQuorumNotMet, len(readable), c.cfg.ReadQuorum)
	}

	// Buffered so all goroutines can complete without blocking even after
	// we've met quorum and are preparing to return.
	results := make(chan replicaReadResult, len(readable))

	for _, replica := range readable {
		go func(node hashring.Node) {
			start := time.Now()
			var resp *pb.ReplicateReadResponse
			var readErr error
			if node.ID == c.selfID {
				resp, readErr = c.localRead(key)
			} else {
				resp, readErr = c.remoteRead(ctx, node.Addr, key)
			}
			if readErr != nil {
				c.logger.Warn("coord.Read per-replica failure",
					"target", node.ID,
					"self", c.selfID,
					"local", node.ID == c.selfID,
					"elapsed", time.Since(start),
					"err_type", fmt.Sprintf("%T", readErr),
					"err", readErr.Error(),
				)
			}
			results <- replicaReadResult{node.ID, node.Addr, resp, readErr}
		}(replica)
	}

	// Phase 1: collect until R responses arrive or quorum becomes impossible.
	// Return after R - slow replicas are handled in phase 2 below.
	maxFailures := len(readable) - c.cfg.ReadQuorum + 1
	var quorumResponses []replicaReadResult
	var errs []error
	failures := 0
	for len(quorumResponses) < c.cfg.ReadQuorum && failures < maxFailures {
		r := <-results
		if r.err != nil {
			failures++
			errs = append(errs, fmt.Errorf("node %s: %w", r.nodeID, r.err))
		} else {
			quorumResponses = append(quorumResponses, r)
		}
	}

	if len(quorumResponses) < c.cfg.ReadQuorum {
		c.logger.Warn("coord.Read quorum-not-met",
			"self", c.selfID,
			"R", c.cfg.ReadQuorum,
			"readable", len(readable),
			"responses", len(quorumResponses),
			"failures", failures,
			"maxFailures", maxFailures,
		)
		return ReadResult{}, fmt.Errorf("%w: got %d/%d responses: %v",
			ErrReadQuorumNotMet, len(quorumResponses), c.cfg.ReadQuorum, errors.Join(errs...))
	}

	responses := quorumResponses

	// Find the newest value across all responses.
	newestIdx := -1
	for i, r := range responses {
		if !r.resp.Found {
			continue
		}
		if newestIdx == -1 {
			newestIdx = i
			continue
		}
		cur := protoToHLC(responses[newestIdx].resp.Timestamp)
		candidate := protoToHLC(r.resp.Timestamp)
		if cur.Less(candidate) {
			newestIdx = i
		}
	}

	// No replica has the key.
	if newestIdx == -1 {
		return ReadResult{Found: false}, nil
	}

	newest := responses[newestIdx]

	// Phase 2 (background): collect remaining in-flight responses and repair
	// all stale replicas - not just the R we waited for. The results channel
	// is buffered to len(readable), so remaining goroutines never block.
	remaining := len(readable) - len(quorumResponses) - failures
	go c.collectAndRepair(key, newest, quorumResponses, results, remaining)

	return ReadResult{
		Value:   newest.resp.Value,
		Found:   true,
		Deleted: newest.resp.Deleted,
	}, nil
}

// localRead reads from the local db and returns a ReplicateReadResponse
// to keep the code path uniform with remote reads.
func (c *Coordinator) localRead(key []byte) (*pb.ReplicateReadResponse, error) {
	val, found := c.localDB.Get(key)
	if !found {
		return &pb.ReplicateReadResponse{Found: false}, nil
	}

	env, err := DecodeEnvelope(val.Data)
	if err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}

	return &pb.ReplicateReadResponse{
		Value: env.Value,
		Timestamp: &pb.HLCTimestamp{
			WallTime: env.Timestamp.WallTime,
			Logical:  env.Timestamp.Logical,
			NodeId:   env.Timestamp.NodeID,
		},
		Found:   true,
		Deleted: env.Deleted,
	}, nil
}

// remoteRead sends a ReplicateRead RPC to a single replica.
func (c *Coordinator) remoteRead(ctx context.Context, addr string, key []byte) (*pb.ReplicateReadResponse, error) {
	client, err := c.dialer.GetClient(addr)
	if err != nil {
		return nil, err
	}

	rctx, cancel := context.WithTimeout(ctx, c.cfg.PerReplicaTimeout)
	defer cancel()

	return client.ReplicateRead(rctx, &pb.ReplicateReadRequest{Key: key})
}

// collectAndRepair drains remaining results from ch, merges them with
// already-collected responses, updates newest if a later response is
// fresher, then repairs all stale replicas. Meant to run in a goroutine.
func (c *Coordinator) collectAndRepair(
	key []byte,
	newest replicaReadResult,
	alreadyCollected []replicaReadResult,
	ch <-chan replicaReadResult,
	remaining int,
) {
	all := make([]replicaReadResult, len(alreadyCollected), len(alreadyCollected)+remaining)
	copy(all, alreadyCollected)

	for i := 0; i < remaining; i++ {
		r := <-ch
		if r.err != nil {
			continue
		}
		all = append(all, r)
		// A late response might have a newer value - update newest.
		if r.resp.Found {
			rTS := protoToHLC(r.resp.Timestamp)
			newestTS := protoToHLC(newest.resp.Timestamp)
			if !newest.resp.Found || newestTS.Less(rTS) {
				newest = r
			}
		}
	}

	c.readRepair(key, newest, all)
}

// readRepair asynchronously sends the newest value to replicas that
// responded with stale or missing data. Fire-and-forget: failures are
// logged but don't affect the client response.
func (c *Coordinator) readRepair(key []byte, newest replicaReadResult, responses []replicaReadResult) {
	newestTS := protoToHLC(newest.resp.Timestamp)

	type staleNode struct {
		nodeID string
		addr   string
	}
	var stale []staleNode

	for _, r := range responses {
		if r.nodeID == newest.nodeID {
			continue
		}
		if !r.resp.Found {
			stale = append(stale, staleNode{r.nodeID, r.addr})
			continue
		}
		rTS := protoToHLC(r.resp.Timestamp)
		if rTS.Less(newestTS) {
			stale = append(stale, staleNode{r.nodeID, r.addr})
		}
	}

	if len(stale) == 0 {
		return
	}

	// Fire-and-forget repairs. This method is always called from a
	// background goroutine (collectAndRepair), so there's no caller
	// to block. Each repair runs in its own goroutine with a timeout.
	for _, s := range stale {
		go func(nodeID, addr string) {
			rctx, cancel := context.WithTimeout(context.Background(), c.cfg.PerReplicaTimeout)
			defer cancel()

			var repairErr error
			if nodeID == c.selfID {
				repairErr = c.localRepair(key, newest.resp)
			} else {
				repairErr = c.remoteWrite(rctx, addr, key,
					newest.resp.Value, newestTS, newest.resp.Deleted)
			}
			if repairErr != nil {
				c.logger.Warn("read repair failed",
					"node", nodeID, "key_len", len(key), "err", repairErr)
			} else {
				c.logger.Debug("read repair sent",
					"node", nodeID, "key_len", len(key))
			}
		}(s.nodeID, s.addr)
	}
}

// localRepair writes the repair envelope to the local database.
func (c *Coordinator) localRepair(key []byte, resp *pb.ReplicateReadResponse) error {
	ts := protoToHLC(resp.Timestamp)
	encoded, err := EncodeEnvelope(Envelope{
		Timestamp: ts,
		Deleted:   resp.Deleted,
		Value:     resp.Value,
	})
	if err != nil {
		return err
	}
	return c.localDB.Put(key, encoded)
}

func protoToHLC(p *pb.HLCTimestamp) hlc.Timestamp {
	if p == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{
		WallTime: p.WallTime,
		Logical:  p.Logical,
		NodeID:   p.NodeId,
	}
}
