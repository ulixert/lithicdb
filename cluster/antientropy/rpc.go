package antientropy

import (
	"context"
	"errors"
	"fmt"
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

// buildTreeFor constructs a tree filtered to keys co-owned with the
// "other" endpoint (computed per receiver perspective).
func (s *Service) buildTreeFor(d keyspaceDigest, otherID string) (*Tree, error) {
	scanned := uint64(0)
	src := NewDBSource(s.database, s.ring, s.selfID, otherID, int(d.ReplicationFactor))
	src.SetScannedCounter(&scanned)
	t, err := BuildTree(src, d.GraceCutoffWall, int(d.Fanout), int(d.Depth))
	s.logger.Debug("ae service buildTreeFor",
		"self", s.selfID, "other", otherID,
		"scanned", scanned, "rf", d.ReplicationFactor)
	return t, err
}

// ComputeAERoot returns the root hash of the local tree built for the
// (selfID, otherEndpoint) range with the digest's parameters.
func (s *Service) ComputeAERoot(_ context.Context, req *pb.AERootRequest) (*pb.AERootResponse, error) {
	d, other, mismatch, err := s.validateDigest(req.GetDigest())
	if err != nil {
		return nil, err
	}
	if mismatch {
		return &pb.AERootResponse{RingVersionMismatch: true}, nil
	}
	tree, err := s.buildTreeFor(d, other)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build tree: %v", err)
	}
	return &pb.AERootResponse{RootHash: tree.Root()}, nil
}

// GetAESubtree returns the fanout child hashes of the internal node at
// the requested path.
func (s *Service) GetAESubtree(_ context.Context, req *pb.AESubtreeRequest) (*pb.AESubtreeResponse, error) {
	d, other, mismatch, err := s.validateDigest(req.GetDigest())
	if err != nil {
		return nil, err
	}
	if mismatch {
		return &pb.AESubtreeResponse{RingVersionMismatch: true}, nil
	}
	tree, err := s.buildTreeFor(d, other)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build tree: %v", err)
	}
	path := intPath(req.GetPath())
	if len(path) >= tree.Depth() {
		return nil, status.Error(codes.InvalidArgument, "path length must be < depth")
	}
	children, err := tree.ChildrenAt(path)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "children: %v", err)
	}
	out := make([]uint64, len(children))
	copy(out, children)
	return &pb.AESubtreeResponse{ChildHashes: out}, nil
}

// GetAELeafKeys server-streams the (key, ts, deleted) triples in the
// requested bucket, in user-key sorted order. Memory is bounded - one
// entry at a time - independent of bucket size.
func (s *Service) GetAELeafKeys(req *pb.AELeafRequest, stream pb.InternalService_GetAELeafKeysServer) error {
	d, other, mismatch, err := s.validateDigest(req.GetDigest())
	if err != nil {
		return err
	}
	if mismatch {
		// No way to signal mismatch on a server-stream cleanly; fail
		// the stream so the initiator notices and aborts.
		return status.Error(codes.FailedPrecondition, "ring version mismatch")
	}

	bs, err := NewBucketSource(s.database, s.ring, s.selfID, other,
		int(d.ReplicationFactor), int(d.Fanout), int(d.Depth), int(req.GetBucketIndex()))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bucket source: %v", err)
	}
	defer bs.Close()

	graceCutoff := d.GraceCutoffWall
	for {
		e, ok, err := bs.Next()
		if err != nil {
			return status.Errorf(codes.Internal, "iterate bucket: %v", err)
		}
		if !ok {
			return nil
		}
		if e.Timestamp.WallTime >= graceCutoff {
			continue // symmetric grace filter
		}

		err = stream.Send(&pb.AELeafEntry{
			Key: e.Key,
			Timestamp: &pb.HLCTimestamp{
				WallTime: e.Timestamp.WallTime,
				Logical:  e.Timestamp.Logical,
				NodeId:   e.Timestamp.NodeID,
			},
			Deleted: e.Deleted,
		})
		if err != nil {
			if errors.Is(stream.Context().Err(), context.Canceled) {
				return status.Error(codes.Canceled, "stream canceled")
			}
			return fmt.Errorf("send leaf entry: %w", err)
		}
	}
}
