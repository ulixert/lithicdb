package hintedhandoff

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
)

const (
	DefaultMaxBatchBytes = 512 * 1024 // 512KB
	DefaultMaxBatchItems = 500
	DefaultSweepInterval = 60 * time.Second
	DefaultMaxRetries    = 3
	DefaultRetryDelay    = 30 * time.Second
)

// ReplicaDialer obtains gRPC clients for data-plane RPCs.
type ReplicaDialer interface {
	GetClient(addr string) (pb.InternalServiceClient, error)
	Close()
}

// MemberInfo holds the subset of member state needed by the drainer.
type MemberInfo struct {
	NodeID string
	Addr   string
}

// MembershipQuerier is the subset of cluster.Membership needed by the drainer.
type MembershipQuerier interface {
	IsAlive(nodeID string) bool
	GetMemberInfos() []MemberInfo
}

// DecodedEnvelope holds the fields extracted from an encoded envelope.
type DecodedEnvelope struct {
	Timestamp hlc.Timestamp
	Deleted   bool
	Value     []byte
}

// EnvelopeDecoder decodes raw envelope bytes into structured fields.
type EnvelopeDecoder func([]byte) (DecodedEnvelope, error)

// DrainerConfig configures the hint drainer.
type DrainerConfig struct {
	Store          *Store
	Dialer         ReplicaDialer
	Membership     MembershipQuerier
	DecodeEnvelope EnvelopeDecoder
	MaxBatchBytes  int64         // default 512KB - primary bound
	MaxBatchItems  int           // default 500 - secondary bound for tiny hints
	SweepInterval  time.Duration // default 60s
	MaxRetries     int           // default 3
	RetryDelay     time.Duration // default 30s
	Logger         *slog.Logger
}

func (c *DrainerConfig) defaults() {
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if c.MaxBatchItems <= 0 {
		c.MaxBatchItems = DefaultMaxBatchItems
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = DefaultSweepInterval
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = DefaultRetryDelay
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Drainer replays buffered hints to recovered nodes.
//
// Replay preserves the original envelope exactly - no timestamp
// reconstruction, no re-encoding. Duplicates are harmless because
// LWW with HLC timestamps makes them idempotent.
type Drainer struct {
	cfg DrainerConfig

	mu       sync.Mutex
	draining map[string]struct{} // in-progress drain targets

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewDrainer creates a new hint drainer.
func NewDrainer(cfg DrainerConfig) *Drainer {
	cfg.defaults()
	return &Drainer{
		cfg:      cfg,
		draining: make(map[string]struct{}),
		stopCh:   make(chan struct{}),
	}
}

// Start begins the periodic sweep ticker.
func (d *Drainer) Start() {
	d.wg.Add(1)
	go d.sweepLoop()
}

// TriggerDrain initiates a non-blocking drain for the given target node.
// If a drain is already in progress for this target, the call is a no-op.
func (d *Drainer) TriggerDrain(nodeID string) {
	d.mu.Lock()
	if _, ok := d.draining[nodeID]; ok {
		d.mu.Unlock()
		return
	}
	d.draining[nodeID] = struct{}{}
	d.mu.Unlock()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			d.mu.Lock()
			delete(d.draining, nodeID)
			d.mu.Unlock()
		}()
		d.drainTarget(nodeID)
	}()
}

// Stop stops the sweep and waits for in-flight drains to complete.
func (d *Drainer) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

// sweepLoop runs periodically to purge expired hints and retrigger
// drains for targets that are now alive.
func (d *Drainer) sweepLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.sweep()
		}
	}
}

func (d *Drainer) sweep() {
	targets := d.cfg.Store.Targets()

	for _, nodeID := range targets {
		// TTL purge: delete expired hints regardless of liveness.
		d.purgeExpired(nodeID)

		// Membership gate: only trigger drain if target is Alive.
		if d.cfg.Membership.IsAlive(nodeID) {
			d.TriggerDrain(nodeID)
		}
	}
}

// purgeExpired deletes hints that have exceeded the TTL.
func (d *Drainer) purgeExpired(nodeID string) {
	type entry struct {
		key     []byte
		envSize int64
	}

	now := time.Now()
	ttl := d.cfg.Store.cfg.HintTTL

	// Collect expired keys under iterator (read lock).
	var expired []entry
	iter := d.cfg.Store.Iterate(nodeID)
	for iter.IsValid() {
		hintKey := make([]byte, len(iter.Key()))
		copy(hintKey, iter.Key())
		envSize := int64(len(iter.Value()))
		ts := ExtractTimestamp(hintKey)

		hintAge := now.Sub(time.Unix(0, ts.WallTime))
		if hintAge > ttl {
			expired = append(expired, entry{key: hintKey, envSize: envSize})
		}
		iter.Next()
	}
	_ = iter.Close()

	// Delete outside iterator (needs write lock).
	for _, e := range expired {
		if err := d.cfg.Store.Remove(e.key, e.envSize); err != nil {
			d.cfg.Logger.Warn("failed to purge expired hint", "node", nodeID, "err", err)
		}
	}
}

// drainTarget replays all hints for a single target node.
func (d *Drainer) drainTarget(nodeID string) {
	addr := d.resolveAddr(nodeID)
	if addr == "" {
		d.cfg.Logger.Warn("cannot resolve address for drain target", "node", nodeID)
		return
	}

	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		batch := d.collectBatch(nodeID)
		if len(batch) == 0 {
			// No more hints for this target - clean up index.
			d.cfg.Store.RemoveTarget(nodeID)
			return
		}

		if err := d.replayBatch(addr, batch); err != nil {
			d.cfg.Logger.Warn("hint replay failed", "node", nodeID, "err", err)
			// Retry with backoff
			if !d.retryReplay(addr, batch) {
				d.cfg.Logger.Warn("giving up hint replay after retries", "node", nodeID)
				return
			}
		}

		// Delete replayed hints from the store.
		for _, h := range batch {
			if err := d.cfg.Store.Remove(h.hintKey, h.envSize); err != nil {
				d.cfg.Logger.Warn("failed to remove replayed hint", "err", err)
			}
		}
	}
}

type hintEntry struct {
	hintKey []byte
	userKey []byte
	value   []byte // raw encoded envelope
	envSize int64
	ts      hlc.Timestamp
	deleted bool
}

// collectBatch reads up to MaxBatchBytes / MaxBatchItems hints for a target.
// Expired and corrupt hints are collected separately and deleted after
// the iterator is closed (to avoid deadlock with the DB write lock).
func (d *Drainer) collectBatch(nodeID string) []hintEntry {
	type expiredEntry struct {
		key     []byte
		envSize int64
	}

	var batch []hintEntry
	var expired []expiredEntry
	var batchBytes int64
	now := time.Now()
	ttl := d.cfg.Store.cfg.HintTTL

	// Phase 1: scan under iterator (holds DB read lock).
	iter := d.cfg.Store.Iterate(nodeID)
	for iter.IsValid() && batchBytes < d.cfg.MaxBatchBytes && len(batch) < d.cfg.MaxBatchItems {
		hintKey := make([]byte, len(iter.Key()))
		copy(hintKey, iter.Key())
		envData := make([]byte, len(iter.Value()))
		copy(envData, iter.Value())
		envSize := int64(len(envData))

		ts := ExtractTimestamp(hintKey)

		// Check TTL - expired hints deferred for deletion.
		hintAge := now.Sub(time.Unix(0, ts.WallTime))
		if hintAge > ttl {
			expired = append(expired, expiredEntry{key: hintKey, envSize: envSize})
			iter.Next()
			continue
		}

		// Decode the envelope to extract the deleted flag for the RPC.
		envelope, err := d.cfg.DecodeEnvelope(envData)
		if err != nil {
			d.cfg.Logger.Warn("corrupt hint envelope, will delete", "err", err)
			expired = append(expired, expiredEntry{key: hintKey, envSize: envSize})
			iter.Next()
			continue
		}

		userKey := ExtractUserKey(hintKey)
		batch = append(batch, hintEntry{
			hintKey: hintKey,
			userKey: userKey,
			value:   envelope.Value,
			envSize: envSize,
			ts:      envelope.Timestamp,
			deleted: envelope.Deleted,
		})
		batchBytes += envSize
		iter.Next()
	}
	_ = iter.Close()

	// Phase 2: delete expired/corrupt hints (needs DB write lock).
	for _, e := range expired {
		if err := d.cfg.Store.Remove(e.key, e.envSize); err != nil {
			d.cfg.Logger.Warn("failed to purge expired hint", "err", err)
		}
	}

	return batch
}

// replayBatch sends a batch of hints to the target node via ReplicateWriteBatch.
func (d *Drainer) replayBatch(addr string, batch []hintEntry) error {
	client, err := d.cfg.Dialer.GetClient(addr)
	if err != nil {
		return err
	}

	entries := make([]*pb.ReplicateWriteRequest, len(batch))
	for i, h := range batch {
		entries[i] = &pb.ReplicateWriteRequest{
			Key:   h.userKey,
			Value: h.value,
			Timestamp: &pb.HLCTimestamp{
				WallTime: h.ts.WallTime,
				Logical:  h.ts.Logical,
				NodeId:   h.ts.NodeID,
			},
			Deleted: h.deleted,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.ReplicateWriteBatch(ctx, &pb.ReplicateWriteBatchRequest{
		Entries: entries,
	})
	return err
}

// retryReplay retries the batch replay with backoff.
// Returns true if successful, false if all retries are exhausted.
func (d *Drainer) retryReplay(addr string, batch []hintEntry) bool {
	for attempt := 0; attempt < d.cfg.MaxRetries; attempt++ {
		select {
		case <-d.stopCh:
			return false
		case <-time.After(d.cfg.RetryDelay):
		}

		if err := d.replayBatch(addr, batch); err == nil {
			return true
		}
	}
	return false
}

// resolveAddr finds the network address for a node ID.
func (d *Drainer) resolveAddr(nodeID string) string {
	for _, m := range d.cfg.Membership.GetMemberInfos() {
		if m.NodeID == nodeID {
			return m.Addr
		}
	}
	return ""
}
