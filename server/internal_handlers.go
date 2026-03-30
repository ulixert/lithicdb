package server

import (
	"context"

	"github.com/ulixert/lithicdb/cluster"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
)

// internalServer implements the InternalService gRPC service,
// delegating SWIM protocol handling to the Membership.
type internalServer struct {
	pb.UnimplementedInternalServiceServer
	membership *cluster.Membership
}

func (s *internalServer) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	incoming := &cluster.PingMessage{
		SenderID:   req.SenderId,
		SenderAddr: req.SenderAddr,
		Updates:    protoToMemberStates(req.Updates),
		RingDesc:   protoToRingDesc(req.RingDescriptor),
	}

	resp := s.membership.HandlePing(incoming)

	return &pb.PingResponse{
		SenderId:       resp.SenderID,
		SenderAddr:     resp.SenderAddr,
		Updates:        memberStatesToProto(resp.Updates),
		RingDescriptor: ringDescToProto(resp.RingDesc),
	}, nil
}

func (s *internalServer) PingReq(_ context.Context, req *pb.PingReqRequest) (*pb.PingReqResponse, error) {
	ack := s.membership.HandlePingReq(req.TargetId, req.TargetAddr)
	return &pb.PingReqResponse{Ack: ack}, nil
}

func (s *internalServer) GossipSync(_ context.Context, req *pb.GossipSyncRequest) (*pb.GossipSyncResponse, error) {
	remote := protoToMemberStates(req.Members)
	remoteRD := protoToRingDesc(req.RingDescriptor)
	local, localRD := s.membership.HandleGossipSync(remote, remoteRD)
	return &pb.GossipSyncResponse{
		Members:        memberStatesToProto(local),
		RingDescriptor: ringDescToProto(localRD),
	}, nil
}

// --- Proto ↔ domain conversion (server-side) ---

func memberStatesToProto(states []cluster.MemberState) []*pb.MemberUpdateProto {
	if states == nil {
		return nil
	}
	out := make([]*pb.MemberUpdateProto, len(states))
	for i, s := range states {
		out[i] = &pb.MemberUpdateProto{
			NodeId:      s.NodeID,
			Addr:        s.Addr,
			Liveness:    int32(s.Liveness),
			RingState:   int32(s.Ring),
			Incarnation: s.Incarnation,
		}
	}
	return out
}

func protoToMemberStates(protos []*pb.MemberUpdateProto) []cluster.MemberState {
	if protos == nil {
		return nil
	}
	out := make([]cluster.MemberState, len(protos))
	for i, p := range protos {
		out[i] = cluster.MemberState{
			NodeID:      p.NodeId,
			Addr:        p.Addr,
			Liveness:    cluster.LivenessState(p.Liveness),
			Ring:        cluster.RingState(p.RingState),
			Incarnation: p.Incarnation,
		}
	}
	return out
}

func ringDescToProto(rd *cluster.RingDescriptor) *pb.RingDescriptorProto {
	if rd == nil {
		return nil
	}
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

func protoToRingDesc(p *pb.RingDescriptorProto) *cluster.RingDescriptor {
	if p == nil {
		return nil
	}
	members := make([]cluster.RingMember, len(p.Members))
	for i, rm := range p.Members {
		members[i] = cluster.RingMember{
			NodeID: rm.NodeId,
			Addr:   rm.Addr,
			State:  cluster.RingState(rm.RingState),
		}
	}
	return &cluster.RingDescriptor{
		Version: p.Version,
		Members: members,
	}
}
