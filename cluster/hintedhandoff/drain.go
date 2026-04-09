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
