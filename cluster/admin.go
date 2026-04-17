package cluster

import (
	"context"

	pb "github.com/ulixert/theseon/proto/theseonpb"
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
