package cluster

import (
	"context"
	"testing"

	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupAdmin(t *testing.T) (*adminServer, *Membership) {
	t.Helper()
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Add node-2 and node-3 as discovered SWIM members.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "localhost:50052", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "localhost:50053", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	admin := &adminServer{
		membership: m,
		selfID:     "node-1",
		selfAddr:   "localhost:50051",
	}
	return admin, m
}

func TestGetNodeInfo(t *testing.T) {
	admin, _ := setupAdmin(t)
	resp, err := admin.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeId != "node-1" {
		t.Errorf("got node_id=%q, want node-1", resp.NodeId)
	}
	if resp.Addr != "localhost:50051" {
		t.Errorf("got addr=%q, want localhost:50051", resp.Addr)
	}
}

func TestGetClusterStatus(t *testing.T) {
	admin, _ := setupAdmin(t)
	resp, err := admin.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Members) != 3 {
		t.Fatalf("got %d members, want 3", len(resp.Members))
	}
	if resp.RingDescriptor == nil {
		t.Fatal("ring descriptor is nil")
	}
}

func TestJoinRing_Success(t *testing.T) {
	admin, m := setupAdmin(t)

	resp, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "node-2",
		Addr:            "localhost:50052",
		ExpectedVersion: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeId != "node-2" {
		t.Errorf("got node_id=%q, want node-2", resp.NodeId)
	}

	rd := m.GetRingDescriptor()
	if rd.Version != 1 {
		t.Errorf("ring version=%d, want 1", rd.Version)
	}
	if len(rd.Members) != 1 {
		t.Fatalf("ring members=%d, want 1", len(rd.Members))
	}
	if rd.Members[0].State != RingJoining {
		t.Errorf("ring state=%v, want joining", rd.Members[0].State)
	}
}

func TestJoinRing_CASMismatch(t *testing.T) {
	admin, _ := setupAdmin(t)

	_, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "node-2",
		Addr:            "localhost:50052",
		ExpectedVersion: 99, // wrong version
	})
	if err == nil {
		t.Fatal("expected error for CAS mismatch")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.FailedPrecondition {
		t.Errorf("got code=%v, want FailedPrecondition", status.Code(err))
	}
}

func TestJoinRing_UnknownNode(t *testing.T) {
	admin, _ := setupAdmin(t)

	_, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "unknown-node",
		Addr:            "localhost:60000",
		ExpectedVersion: 0,
	})
	if err == nil {
		t.Fatal("expected error for unknown node")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.FailedPrecondition {
		t.Errorf("got code=%v, want FailedPrecondition", status.Code(err))
	}
}

func TestJoinRing_AlreadyInRing(t *testing.T) {
	admin, _ := setupAdmin(t)

	// First join succeeds.
	_, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "node-2",
		Addr:            "localhost:50052",
		ExpectedVersion: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second join with correct version fails — already exists.
	_, err = admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "node-2",
		Addr:            "localhost:50052",
		ExpectedVersion: 1,
	})
	if err == nil {
		t.Fatal("expected error for duplicate join")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
		t.Errorf("got code=%v, want AlreadyExists", status.Code(err))
	}
}

func TestActivateNode_Success(t *testing.T) {
	admin, m := setupAdmin(t)

	// Join first.
	_, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          "node-2",
		Addr:            "localhost:50052",
		ExpectedVersion: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Activate.
	_, err = admin.ActivateNode(context.Background(), &pb.ActivateNodeRequest{
		NodeId:          "node-2",
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	rd := m.GetRingDescriptor()
	if rd.Version != 2 {
		t.Errorf("ring version=%d, want 2", rd.Version)
	}
	if rd.Members[0].State != RingActive {
		t.Errorf("ring state=%v, want active", rd.Members[0].State)
	}
}

func TestActivateNode_NotJoining(t *testing.T) {
	admin, _ := setupAdmin(t)

	// Join + activate.
	_, _ = admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId: "node-2", Addr: "localhost:50052", ExpectedVersion: 0,
	})
	_, _ = admin.ActivateNode(context.Background(), &pb.ActivateNodeRequest{
		NodeId: "node-2", ExpectedVersion: 1,
	})

	// Activate again — already active.
	_, err := admin.ActivateNode(context.Background(), &pb.ActivateNodeRequest{
		NodeId:          "node-2",
		ExpectedVersion: 2,
	})
	if err == nil {
		t.Fatal("expected error for non-joining node")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.FailedPrecondition {
		t.Errorf("got code=%v, want FailedPrecondition", status.Code(err))
	}
}

func TestRemoveNode_Success(t *testing.T) {
	admin, m := setupAdmin(t)

	// Join + remove.
	_, _ = admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId: "node-2", Addr: "localhost:50052", ExpectedVersion: 0,
	})

	_, err := admin.RemoveNode(context.Background(), &pb.RemoveNodeRequest{
		NodeId:          "node-2",
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	rd := m.GetRingDescriptor()
	if rd.Version != 2 {
		t.Errorf("ring version=%d, want 2", rd.Version)
	}
	if len(rd.Members) != 0 {
		t.Errorf("ring members=%d, want 0", len(rd.Members))
	}
}

func TestRemoveNode_NotFound(t *testing.T) {
	admin, _ := setupAdmin(t)

	_, err := admin.RemoveNode(context.Background(), &pb.RemoveNodeRequest{
		NodeId:          "nonexistent",
		ExpectedVersion: 0,
	})
	if err == nil {
		t.Fatal("expected error for missing node")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("got code=%v, want NotFound", status.Code(err))
	}
}

func TestJoinRing_RequiresNodeId(t *testing.T) {
	admin, _ := setupAdmin(t)

	_, err := admin.JoinRing(context.Background(), &pb.JoinRingRequest{
		Addr:            "localhost:50052",
		ExpectedVersion: 0,
	})
	if err == nil {
		t.Fatal("expected error for missing node_id")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("got code=%v, want InvalidArgument", status.Code(err))
	}
}
