package antientropy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/metrics"
)

// MemberInfo captures the subset of node identity needed to resolve a
// peer's network address. Provided by an adapter over cluster.Membership.
type MemberInfo struct {
	NodeID string
	Addr   string
}

// MembershipQuerier is the membership subset Manager needs. Defined here
// (rather than imported from cluster) to keep this package independent
// of cluster.Membership concretely - and, importantly, to avoid an
// import cycle, since the cluster package depends on this package via
// the AntiEntropyTrigger interface.
type MembershipQuerier interface {
	IsAlive(nodeID string) bool
	Members() []MemberInfo
	RingVersion() uint64
}

// Trigger labels the source that initiated a reconcile. Used for
// metrics labels.
type Trigger string

const (
	TriggerTimer    Trigger = "timer"
	TriggerRecovery Trigger = "recovery"
	TriggerAdmin    Trigger = "admin"
)

// Manager orchestrates background and on-demand reconciles. One manager
// per node. Reconciles are deduplicated per-peer (matching the drainer's
// in-flight pattern) and bounded by MaxConcurrent across all peers.
type Manager struct {
	cfg               cluster.AntiEntropyConfig
	selfID            string
	ring              *hashring.Ring
	membership        MembershipQuerier
	database          *db.DB
	dialer            Dialer
	repairer          Repairer
	replicationFactor int
	logger            *slog.Logger

	mu       sync.Mutex
	inflight map[string]struct{}
	sema     chan struct{}

	stopCh chan struct{}
	wg     sync.WaitGroup
	rng    func() uint64 // peer-rotation index source
	tick   uint64
}

// Config is the constructor argument bundle for Manager.
type Config struct {
	Cfg               cluster.AntiEntropyConfig
	SelfID            string
	Ring              *hashring.Ring
	Membership        MembershipQuerier
	DB                *db.DB
	Dialer            Dialer
	Repairer          Repairer
	ReplicationFactor int
	Logger            *slog.Logger
}

// NewManager constructs a Manager. Start must be called to begin the
// periodic ticker. Trigger / TriggerSync work even before/without Start.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.SelfID == "" {
		return nil, errors.New("antientropy: SelfID is required")
	}
	if cfg.Ring == nil || cfg.Membership == nil || cfg.DB == nil ||
		cfg.Dialer == nil || cfg.Repairer == nil {
		return nil, errors.New("antientropy: Ring/Membership/DB/Dialer/Repairer are required")
	}
	if cfg.ReplicationFactor <= 0 {
		return nil, errors.New("antientropy: ReplicationFactor must be > 0")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ae := cfg.Cfg
	defaults := cluster.DefaultAntiEntropyConfig()
	if ae.Interval <= 0 {
		ae.Interval = defaults.Interval
	}
	if ae.Depth <= 0 {
		ae.Depth = defaults.Depth
	}
	if ae.Fanout <= 0 {
		ae.Fanout = defaults.Fanout
	}
	if ae.GracePeriod <= 0 {
		ae.GracePeriod = defaults.GracePeriod
	}
	if ae.MaxConcurrent <= 0 {
		ae.MaxConcurrent = defaults.MaxConcurrent
	}
	if ae.MaxRepairPerRound <= 0 {
		ae.MaxRepairPerRound = defaults.MaxRepairPerRound
	}
	if ae.ScanKeysPerTick <= 0 {
		ae.ScanKeysPerTick = defaults.ScanKeysPerTick
	}

	return &Manager{
		cfg:               ae,
		selfID:            cfg.SelfID,
		ring:              cfg.Ring,
		membership:        cfg.Membership,
		database:          cfg.DB,
		dialer:            cfg.Dialer,
		repairer:          cfg.Repairer,
		replicationFactor: cfg.ReplicationFactor,
		logger:            logger,
		inflight:          make(map[string]struct{}),
		sema:              make(chan struct{}, ae.MaxConcurrent),
		stopCh:            make(chan struct{}),
		rng:               func() uint64 { return uint64(time.Now().UnixNano()) },
	}, nil
}

// Start launches the periodic ticker if Enabled. No-op when disabled -
// in that case Trigger / TriggerSync still work for admin paths.
func (m *Manager) Start() {
	if !m.cfg.Enabled {
		m.logger.Info("anti-entropy disabled; admin path remains available")
		return
	}
	m.wg.Add(1)
	go m.tickLoop()
}

// Stop signals the ticker to exit and waits for in-flight reconciles to
// finish.
func (m *Manager) Stop() {
	select {
	case <-m.stopCh:
		// already stopped
	default:
		close(m.stopCh)
	}
	m.wg.Wait()
}

// Trigger is the cluster.AntiEntropyTrigger entry point - non-blocking
// reconcile against peerID, labeled "admin" for metrics.
func (m *Manager) Trigger(peerID string) {
	m.triggerInternal(peerID, TriggerAdmin)
}

// TriggerWith fires a non-blocking reconcile labeled with the caller-
// supplied trigger source. Used internally by the timer loop and the
// recovery callback.
func (m *Manager) TriggerWith(peerID string, trigger Trigger) {
	m.triggerInternal(peerID, trigger)
}

func (m *Manager) triggerInternal(peerID string, trigger Trigger) {
	if peerID == "" || peerID == m.selfID {
		return
	}
	if m.tryAcquire(peerID) {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer m.release(peerID)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				select {
				case <-m.stopCh:
					cancel()
				case <-ctx.Done():
				}
			}()
			defer cancel()
			_ = m.runOne(ctx, peerID, trigger)
		}()
	}
}

// tryAcquire reserves a slot + per-peer dedup. Non-blocking. Returns
// true if the caller now holds the reservation and should proceed.
func (m *Manager) tryAcquire(peerID string) bool {
	m.mu.Lock()
	if _, dup := m.inflight[peerID]; dup {
		m.mu.Unlock()
		return false
	}
	// Try sema non-blockingly.
	select {
	case m.sema <- struct{}{}:
	default:
		m.mu.Unlock()
		return false
	}
	m.inflight[peerID] = struct{}{}
	m.mu.Unlock()
	return true
}

func (m *Manager) release(peerID string) {
	m.mu.Lock()
	delete(m.inflight, peerID)
	m.mu.Unlock()
	<-m.sema
}

// runOne executes a single reconcile and emits metrics. The semaphore
// and inflight map must be held by the caller.
func (m *Manager) runOne(ctx context.Context, peerID string, trigger Trigger) ReconcileStats {
	addr := m.peerAddr(peerID)
	if addr == "" {
		stats := ReconcileStats{PeerID: peerID, Err: fmt.Errorf("peer %s not found in membership", peerID)}
		m.logger.Warn("anti-entropy skipping peer with unresolved address", "peer", peerID)
		metrics.AEReconcilesCompleted.WithLabelValues("skipped").Inc()
		return stats
	}
	if !m.membership.IsAlive(peerID) {
		stats := ReconcileStats{PeerID: peerID, Err: fmt.Errorf("peer %s is not alive", peerID)}
		metrics.AEReconcilesCompleted.WithLabelValues("skipped").Inc()
		return stats
	}

	metrics.AEReconcilesStarted.WithLabelValues(string(trigger)).Inc()
	metrics.AEInFlight.Inc()
	defer metrics.AEInFlight.Dec()

	rec := newReconciler(
		m.selfID, peerID, addr,
		m.ring, m.database, m.dialer, m.repairer,
		m.cfg, m.membership.RingVersion(), m.replicationFactor,
		m.logger,
	)
	stats := rec.Run(ctx)

	switch {
	case stats.Err == nil:
		metrics.AEReconcilesCompleted.WithLabelValues("success").Inc()
		m.logger.Debug("anti-entropy reconcile complete",
			"peer", peerID,
			"trigger", trigger,
			"keys_scanned", stats.KeysScanned,
			"divergent_leaves", stats.DivergentLeaves,
			"keys_repaired", stats.KeysRepaired,
			"duration_ms", stats.Duration.Milliseconds())
	case errors.Is(stats.Err, ErrRingVersionMismatch):
		metrics.AEReconcilesCompleted.WithLabelValues("ring_version_mismatch").Inc()
		m.logger.Info("anti-entropy aborted: ring version mismatch", "peer", peerID)
	default:
		metrics.AEReconcilesCompleted.WithLabelValues("failure").Inc()
		m.logger.Warn("anti-entropy reconcile failed",
			"peer", peerID, "trigger", trigger, "err", stats.Err)
	}
	return stats
}

func (m *Manager) peerAddr(peerID string) string {
	for _, mi := range m.membership.Members() {
		if mi.NodeID == peerID {
			return mi.Addr
		}
	}
	return ""
}

// tickLoop runs the periodic reconcile schedule. Each tick advances
// through the owned-peer list one peer at a time.
func (m *Manager) tickLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			peers := OwnedPeers(m.ring, m.selfID, m.replicationFactor)
			if len(peers) == 0 {
				continue
			}
			// Round-robin: pick the next peer based on monotonic tick.
			idx := int(m.tick % uint64(len(peers)))
			m.tick++
			m.triggerInternal(peers[idx], TriggerTimer)
		}
	}
}
