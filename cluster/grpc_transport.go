package cluster

import (
	"context"
	"sync"

	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements Transport using gRPC RPCs to the
// InternalService on remote nodes.
type GRPCTransport struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // addr → cached connection
}

// NewGRPCTransport creates a gRPC-based transport.
func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) Ping(ctx context.Context, addr string, msg *PingMessage) (*PingMessage, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return nil, err
	}

	resp, err := client.Ping(ctx, &pb.PingRequest{
		SenderId:       msg.SenderID,
		SenderAddr:     msg.SenderAddr,
		Updates:        memberStatesToProto(msg.Updates),
		RingDescriptor: ringDescToProto(msg.RingDesc),
	})
	if err != nil {
		return nil, err
	}

	return &PingMessage{
		SenderID:   resp.SenderId,
		SenderAddr: resp.SenderAddr,
		Updates:    protoToMemberStates(resp.Updates),
		RingDesc:   protoToRingDesc(resp.RingDescriptor),
	}, nil
}

func (t *GRPCTransport) PingReq(ctx context.Context, addr, targetID, targetAddr string) (bool, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return false, err
	}

	resp, err := client.PingReq(ctx, &pb.PingReqRequest{
		TargetId:   targetID,
		TargetAddr: targetAddr,
	})
	if err != nil {
		return false, err
	}

	return resp.Ack, nil
}

func (t *GRPCTransport) GossipSync(ctx context.Context, addr string, members []MemberState, ringDesc *RingDescriptor) ([]MemberState, *RingDescriptor, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return nil, nil, err
	}

	resp, err := client.GossipSync(ctx, &pb.GossipSyncRequest{
		Members:        memberStatesToProto(members),
		RingDescriptor: ringDescToProto(ringDesc),
	})
	if err != nil {
		return nil, nil, err
	}

	return protoToMemberStates(resp.Members), protoToRingDesc(resp.RingDescriptor), nil
}

// Close tears down all cached connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, addr)
	}
}

func (t *GRPCTransport) getClient(addr string) (pb.InternalServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[addr]; ok {
		return pb.NewInternalServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	t.conns[addr] = conn
	return pb.NewInternalServiceClient(conn), nil
}

// --- Proto ↔ domain conversion ---

func memberStatesToProto(states []MemberState) []*pb.MemberUpdateProto {
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

func protoToMemberStates(protos []*pb.MemberUpdateProto) []MemberState {
	if protos == nil {
		return nil
	}
	out := make([]MemberState, len(protos))
	for i, p := range protos {
		out[i] = MemberState{
			NodeID:      p.NodeId,
			Addr:        p.Addr,
			Liveness:    LivenessState(p.Liveness),
			Ring:        RingState(p.RingState),
			Incarnation: p.Incarnation,
		}
	}
	return out
}

func ringDescToProto(rd *RingDescriptor) *pb.RingDescriptorProto {
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

func protoToRingDesc(p *pb.RingDescriptorProto) *RingDescriptor {
	if p == nil {
		return nil
	}
	members := make([]RingMember, len(p.Members))
	for i, rm := range p.Members {
		members[i] = RingMember{
			NodeID: rm.NodeId,
			Addr:   rm.Addr,
			State:  RingState(rm.RingState),
		}
	}
	return &RingDescriptor{
		Version: p.Version,
		Members: members,
	}
}
