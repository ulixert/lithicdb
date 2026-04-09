package hintedhandoff

import (
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
func (d *Drainer) collectBatch(nodeID string) []hintEntry {

}

func (d *Drainer) resolveAddr(nodeID string) string {
	for _, m := range d.cfg.Membership.GetMemberInfos() {
		if m.NodeID == nodeID {
			return m.Addr
		}
	}
	return ""
}
