package cluster

import (
	"context"
	"slices"

	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// adminServer implements the AdminService gRPC interface for cluster
// topology management. All ring-mutating operations use CAS via
// expected_version to prevent conflicting concurrent changes.
type adminServer struct {
	pb.UnimplementedAdminServiceServer
	membership *Membership
	selfID     string
	selfAddr   string
}

// NewAdminServer creates an AdminService handler backed by the given
// Membership instance. selfID and selfAddr identify this node.
func NewAdminServer(membership *Membership, selfID, selfAddr string) pb.AdminServiceServer {
	return &adminServer{
		membership: membership,
		selfID:     selfID,
		selfAddr:   selfAddr,
	}
}

func (a *adminServer) GetNodeInfo(_ context.Context, _ *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	return &pb.GetNodeInfoResponse{
		NodeId:    a.selfID,
		Addr:      a.selfAddr,
		Liveness:  int32(a.membership.livenessOf(a.selfID)),
		RingState: int32(a.membership.RingStateOf(a.selfID)),
	}, nil
}

func (a *adminServer) GetClusterStatus(_ context.Context, _ *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	members := a.membership.GetMembers()
	rd := a.membership.GetRingDescriptor()

	protoMembers := make([]*pb.MemberUpdateProto, len(members))
	for i, m := range members {
		protoMembers[i] = &pb.MemberUpdateProto{
			NodeId:      m.NodeID,
			Addr:        m.Addr,
			Liveness:    int32(m.Liveness),
			RingState:   int32(m.Ring),
			Incarnation: m.Incarnation,
		}
	}

	protoRD := adminRingDescToProto(&rd)
	return &pb.GetClusterStatusResponse{
		Members:        protoMembers,
		RingDescriptor: protoRD,
	}, nil
}

func (a *adminServer) JoinRing(_ context.Context, req *pb.JoinRingRequest) (*pb.JoinRingResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.Addr == "" {
		return nil, status.Error(codes.InvalidArgument, "addr is required")
	}

	// Verify the node has been discovered via SWIM.
	if addr := a.membership.AddrOf(req.NodeId); addr == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %q not yet discovered via SWIM - ensure it is running with --seeds pointing to this cluster",
			req.NodeId)
	}

	rd := a.membership.GetRingDescriptor()
	if req.ExpectedVersion != rd.Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d", req.ExpectedVersion, rd.Version)
	}

	// Check node isn't already in the ring.
	for _, rm := range rd.Members {
		if rm.NodeID == req.NodeId {
			return nil, status.Errorf(codes.AlreadyExists,
				"node %q already in ring (state: %s)", req.NodeId, rm.State)
		}
	}

	newRD := RingDescriptor{
		Version: rd.Version + 1,
		Members: append(slices.Clone(rd.Members), RingMember{
			NodeID: req.NodeId,
			Addr:   req.Addr,
			State:  RingJoining,
		}),
	}

	if err := a.membership.ApplyRingDescriptor(newRD); err != nil {
		return nil, status.Errorf(codes.Internal, "apply ring descriptor: %v", err)
	}
	return &pb.JoinRingResponse{NodeId: req.NodeId}, nil
}

func (a *adminServer) ActivateNode(_ context.Context, req *pb.ActivateNodeRequest) (*pb.ActivateNodeResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	rd := a.membership.GetRingDescriptor()
	if req.ExpectedVersion != rd.Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d", req.ExpectedVersion, rd.Version)
	}

	idx := -1
	for i, rm := range rd.Members {
		if rm.NodeID == req.NodeId {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, status.Errorf(codes.NotFound, "node %q not in ring", req.NodeId)
	}
	if rd.Members[idx].State != RingJoining {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %q is %s, not joining", req.NodeId, rd.Members[idx].State)
	}

	newMembers := slices.Clone(rd.Members)
	newMembers[idx].State = RingActive
	newRD := RingDescriptor{
		Version: rd.Version + 1,
		Members: newMembers,
	}

	if err := a.membership.ApplyRingDescriptor(newRD); err != nil {
		return nil, status.Errorf(codes.Internal, "apply ring descriptor: %v", err)
	}
	return &pb.ActivateNodeResponse{}, nil
}

func (a *adminServer) RemoveNode(_ context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	rd := a.membership.GetRingDescriptor()
	if req.ExpectedVersion != rd.Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d", req.ExpectedVersion, rd.Version)
	}

	found := false
	newMembers := make([]RingMember, 0, len(rd.Members))
	for _, rm := range rd.Members {
		if rm.NodeID == req.NodeId {
			found = true
			continue
		}
		newMembers = append(newMembers, rm)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q not in ring", req.NodeId)
	}

	newRD := RingDescriptor{
		Version: rd.Version + 1,
		Members: newMembers,
	}

	if err := a.membership.ApplyRingDescriptor(newRD); err != nil {
		return nil, status.Errorf(codes.Internal, "apply ring descriptor: %v", err)
	}
	return &pb.RemoveNodeResponse{}, nil
}

func adminRingDescToProto(rd *RingDescriptor) *pb.RingDescriptorProto {
	members := make([]*pb.RingMemberProto, len(rd.Members))
	for i, rm := range rd.Members {
		members[i] = &pb.RingMemberProto{
			NodeId:    rm.NodeID,
			Addr:      rm.Addr,
			RingState: int32(rm.State),
		}
	}
	return &pb.RingDescriptorProto{
		Version: rd.Version,
		Members: members,
	}
}

// livenessOf returns the liveness state of the given node.
// This is unexported to avoid polluting the Membership API.
func (m *Membership) livenessOf(nodeID string) LivenessState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ms, ok := m.members[nodeID]; ok {
		return ms.Liveness
	}
	return Dead
}
