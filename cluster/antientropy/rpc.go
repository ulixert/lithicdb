package antientropy

import (
	"log/slog"

	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MembershipRingVersioner exposes the current ring descriptor version
// for ring-version mismatch detection. *cluster.Membership satisfies it
// via GetRingDescriptor().Version.
type MembershipRingVersioner interface {
	RingVersion() uint64
}

// Service implements the server-side anti-entropy RPCs:
// ComputeAERoot, GetAESubtree, GetAELeafKeys.
//
// The service is stateless across calls; each request is a fresh tree
// build over the local DB filtered to keys co-owned with the requester.
// Caching trees across calls would speed up follow-up subtree calls but
// breaks if writes land between calls — keep it simple, rebuild per RPC.
type Service struct {
	selfID    string
	ring      *hashring.Ring
	database  *db.DB
	ringVer   MembershipRingVersioner
	defaultRF int
	logger    *slog.Logger
}

// NewService constructs the server-side anti-entropy service.
func NewService(
	selfID string,
	ring *hashring.Ring,
	database *db.DB,
	ringVer MembershipRingVersioner,
	defaultReplicationFactor int,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		selfID:    selfID,
		ring:      ring,
		database:  database,
		ringVer:   ringVer,
		defaultRF: defaultReplicationFactor,
		logger:    logger,
	}
}

// validateDigest checks the digest is well-formed, that this node is
// one of the two named endpoints, and that ring versions agree.
// Returns (digest, otherEndpointID, ringMismatch, error).
func (s *Service) validateDigest(p *pb.AEKeyspaceDigest) (keyspaceDigest, string, bool, error) {
	if p == nil {
		return keyspaceDigest{}, "", false, status.Error(codes.InvalidArgument, "digest is required")
	}
	d := digestFromProto(p)
	if d.InitiatorID == "" || d.TargetID == "" {
		return d, "", false, status.Error(codes.InvalidArgument, "digest.initiator_id and target_id are required")
	}
	if d.Fanout < 2 || d.Depth < 1 {
		return d, "", false, status.Error(codes.InvalidArgument, "digest.fanout/depth invalid")
	}
	if d.ReplicationFactor <= 0 {
		d.ReplicationFactor = int32(s.defaultRF)
	}
	other := d.OtherFrom(s.selfID)
	if other == "" {
		return d, "", false, status.Errorf(codes.InvalidArgument,
			"this node (%s) is not one of the digest endpoints (initiator=%s, target=%s)",
			s.selfID, d.InitiatorID, d.TargetID)
	}
	if s.ringVer != nil && d.RingVersion != s.ringVer.RingVersion() {
		return d, other, true, nil
	}
	return d, other, false, nil
}
