package antientropy

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
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
