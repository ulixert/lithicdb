package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Mock transport ---

type mockTransport struct {
	mu     sync.Mutex
	pingFn func(addr string, msg *PingMessage) (*PingMessage, error)
	preqFn func(addr, targetID, targetAddr string) (bool, error)
	syncFn func(addr string, members []MemberState, rd *RingDescriptor) ([]MemberState, *RingDescriptor, error)
	calls  []string // recorded call descriptions
}

func (t *mockTransport) Ping(_ context.Context, addr string, msg *PingMessage) (*PingMessage, error) {
	t.mu.Lock()
	t.calls = append(t.calls, "ping:"+addr)
	fn := t.pingFn
	t.mu.Unlock()
	if fn != nil {
		return fn(addr, msg)
	}
	return &PingMessage{SenderID: "remote", SenderAddr: addr}, nil
}

func (t *mockTransport) PingReq(_ context.Context, addr, targetID, targetAddr string) (bool, error) {
	t.mu.Lock()
	t.calls = append(t.calls, "pingreq:"+addr+"->"+targetID)
	fn := t.preqFn
	t.mu.Unlock()
	if fn != nil {
		return fn(addr, targetID, targetAddr)
	}
	return true, nil
}

func (t *mockTransport) GossipSync(_ context.Context, addr string, members []MemberState, rd *RingDescriptor) ([]MemberState, *RingDescriptor, error) {
	t.mu.Lock()
	t.calls = append(t.calls, "sync:"+addr)
	fn := t.syncFn
	t.mu.Unlock()
	if fn != nil {
		return fn(addr, members, rd)
	}
	return nil, nil, nil
}

func (t *mockTransport) getCalls() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.calls))
	copy(out, t.calls)
	return out
}

// --- Test helpers ---

func newTestMembership(nodeID string, tp Transport) *Membership {
	cfg := DefaultClusterConfig(nodeID, nodeID+":9090")
	cfg.GossipInterval = 50 * time.Millisecond
	cfg.PingTimeout = 20 * time.Millisecond
	cfg.SuspectTimeout = 100 * time.Millisecond
	return NewMembership(cfg, tp)
}

// --- Merge tests ---

func TestMerge_NewNode(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	remote := MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1}
	m.mu.Lock()
	m.mergeLocked(remote)
	m.mu.Unlock()

	members := m.GetMembers()
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
}

func TestMerge_HigherIncarnationWins(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Add node-2 at incarnation 5, Alive.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 5})
	m.mu.Unlock()

	// Update with incarnation 10, Suspect — should produce a liveness event.
	m.mu.Lock()
	ev := m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Suspect, Incarnation: 10})
	m.mu.Unlock()

	if ev == nil {
		t.Fatal("expected liveness event for Alive→Suspect transition")
	}

	members := m.GetMembers()
	for _, ms := range members {
		if ms.NodeID == "node-2" {
			if ms.Liveness != Suspect {
				t.Fatalf("expected Suspect, got %v", ms.Liveness)
			}
			if ms.Incarnation != 10 {
				t.Fatalf("expected incarnation 10, got %d", ms.Incarnation)
			}
			return
		}
	}
	t.Fatal("node-2 not found in members")
}

func TestMerge_LowerIncarnationIgnored(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 10})
	m.mu.Unlock()

	// Try to apply a stale update — should be ignored (no event).
	m.mu.Lock()
	ev := m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Dead, Incarnation: 5})
	m.mu.Unlock()

	if ev != nil {
		t.Fatal("expected stale incarnation to be ignored (no event)")
	}
	if !m.IsAlive("node-2") {
		t.Fatal("expected node-2 to still be alive")
	}
}

func TestMerge_SameIncarnation_HigherLivenessWins(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 5})
	m.mu.Unlock()

	// Same incarnation, but Suspect > Alive — should produce liveness event.
	m.mu.Lock()
	ev := m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Suspect, Incarnation: 5})
	m.mu.Unlock()

	if ev == nil {
		t.Fatal("expected liveness event for Alive→Suspect at same incarnation")
	}

	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			if ms.Liveness != Suspect {
				t.Fatalf("expected Suspect, got %v", ms.Liveness)
			}
			return
		}
	}
	t.Fatal("node-2 not found")
}

func TestMerge_SelfRefutation(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Someone says we're Suspect with incarnation >= ours.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-1", Addr: "node-1:9090", Liveness: Suspect, Incarnation: 1})
	m.mu.Unlock()

	// We should have refuted — our incarnation bumped and we're still Alive.
	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-1" {
			if ms.Liveness != Alive {
				t.Fatalf("expected self to remain Alive after refutation, got %v", ms.Liveness)
			}
			if ms.Incarnation <= 1 {
				t.Fatalf("expected incarnation to increment past 1, got %d", ms.Incarnation)
			}
			return
		}
	}
	t.Fatal("self not found in members")
}

// --- Broadcast retransmit buffer tests ---

func TestBroadcastBuffer_QueueAndDequeue(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.queueBroadcastLocked(MemberState{NodeID: "node-2", Liveness: Alive, Incarnation: 1})
	m.queueBroadcastLocked(MemberState{NodeID: "node-3", Liveness: Suspect, Incarnation: 2})
	updates := m.getBroadcastsLocked(10)
	m.mu.Unlock()

	if len(updates) != 2 {
		t.Fatalf("expected 2 broadcasts, got %d", len(updates))
	}
}

func TestBroadcastBuffer_RetransmitDecrement(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.queueBroadcastLocked(MemberState{NodeID: "node-2", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	// Drain broadcasts repeatedly until exhausted.
	for i := range 100 {
		m.mu.Lock()
		updates := m.getBroadcastsLocked(10)
		remaining := len(m.broadcasts)
		m.mu.Unlock()

		if len(updates) == 0 && remaining == 0 {
			if i == 0 {
				t.Fatal("broadcast should have been available at least once")
			}
			return // success — eventually exhausted
		}
	}
	t.Fatal("broadcast was never exhausted after 100 rounds")
}

func TestBroadcastBuffer_OnlySentItemsDecremented(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Queue 3 broadcasts.
	m.mu.Lock()
	m.queueBroadcastLocked(MemberState{NodeID: "node-2", Liveness: Alive, Incarnation: 1})
	m.queueBroadcastLocked(MemberState{NodeID: "node-3", Liveness: Alive, Incarnation: 1})
	m.queueBroadcastLocked(MemberState{NodeID: "node-4", Liveness: Alive, Incarnation: 1})

	// Fetch with limit=1 — only first item should be decremented.
	initialRetransmits := m.broadcasts[2].retransmits
	m.getBroadcastsLocked(1)
	// Third item (index 1 after shift since first may be removed) should
	// retain its original retransmit count.
	var node4Retransmits int
	for _, b := range m.broadcasts {
		if b.state.NodeID == "node-4" {
			node4Retransmits = b.retransmits
			break
		}
	}
	m.mu.Unlock()

	if node4Retransmits != initialRetransmits {
		t.Fatalf("expected node-4 retransmits=%d (unchanged), got %d", initialRetransmits, node4Retransmits)
	}
}

// --- Probe tests ---

func TestProbe_DirectPingSuccess(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return &PingMessage{SenderID: "node-2", SenderAddr: addr}, nil
		},
	}
	m := newTestMembership("node-1", tp)

	// Add a peer.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	m.probe()

	if !m.IsAlive("node-2") {
		t.Fatal("expected node-2 to remain Alive after successful ping")
	}
}

func TestProbe_DirectFail_IndirectSuccess(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			// node-2 doesn't respond to direct pings, but node-3 does.
			if addr == "10.0.0.2:9090" {
				return nil, errors.New("timeout")
			}
			return &PingMessage{SenderID: "node-3", SenderAddr: addr}, nil
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return true, nil // indirect ping succeeds
		},
	}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	// Force probe to target node-2.
	m.mu.Lock()
	m.probeOrder = []string{"node-2"}
	m.probeIdx = 0
	m.mu.Unlock()

	m.probe()

	// node-2 should still be Alive — indirect ping saved it.
	if !m.IsAlive("node-2") {
		t.Fatal("expected node-2 to remain Alive after indirect ping success")
	}
}

func TestProbe_AllFail_BecomesSuspect(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil // indirect also fails
		},
	}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	// Set probe order with all non-self members so probeOrderStale() doesn't rebuild.
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	m.probe()

	// node-2 should be Suspect.
	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			if ms.Liveness != Suspect {
				t.Fatalf("expected node-2 to be Suspect, got %v", ms.Liveness)
			}
			return
		}
	}
	t.Fatal("node-2 not found in members")
}

func TestSuspect_BecomesDeadAfterTimeout(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil
		},
	}
	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SuspectTimeout = 50 * time.Millisecond
	cfg.PingTimeout = 10 * time.Millisecond
	m := NewMembership(cfg, tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	// First probe: Alive → Suspect + starts timer.
	m.probe()

	// Wait for suspect timeout to fire.
	time.Sleep(100 * time.Millisecond)

	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			if ms.Liveness != Dead {
				t.Fatalf("expected node-2 to be Dead after suspect timeout, got %v", ms.Liveness)
			}
			return
		}
	}
	t.Fatal("node-2 not found in members")
}

func TestSuspect_Refutation(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil
		},
	}
	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SuspectTimeout = 5 * time.Second // long timeout so we can refute
	cfg.PingTimeout = 10 * time.Millisecond
	m := NewMembership(cfg, tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 5})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	// Probe fails → node-2 becomes Suspect.
	m.probe()

	// Receive an Alive update from node-2 with higher incarnation (refutation).
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 10})
	m.mu.Unlock()

	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			if ms.Liveness != Alive {
				t.Fatalf("expected node-2 to be Alive after refutation, got %v", ms.Liveness)
			}
			if ms.Incarnation != 10 {
				t.Fatalf("expected incarnation 10, got %d", ms.Incarnation)
			}
			return
		}
	}
	t.Fatal("node-2 not found")
}

// --- GossipSync test ---

func TestGossipSync_ExchangesMembership(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Add a local member.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	// Simulate incoming sync from node-3 that knows about node-4.
	remote := []MemberState{
		{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1},
		{NodeID: "node-4", Addr: "10.0.0.4:9090", Liveness: Alive, Incarnation: 1},
	}

	result, _ := m.HandleGossipSync(remote, nil)

	// Result should contain our full table.
	if len(result) < 3 {
		t.Fatalf("expected at least 3 members in sync response, got %d", len(result))
	}

	// We should now know about all 4 nodes.
	members := m.GetMembers()
	if len(members) != 4 {
		t.Fatalf("expected 4 members after sync, got %d", len(members))
	}
}

// --- Discovery test ---

func TestDiscovery_ViaSeedPeer(t *testing.T) {
	tp := &mockTransport{
		syncFn: func(addr string, members []MemberState, rd *RingDescriptor) ([]MemberState, *RingDescriptor, error) {
			return []MemberState{
				{NodeID: "seed", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1},
				{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1},
			}, nil, nil
		},
	}

	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SeedPeers = []string{"10.0.0.2:9090"}
	cfg.GossipInterval = 50 * time.Millisecond
	m := NewMembership(cfg, tp)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop()

	members := m.GetMembers()
	if len(members) != 3 {
		t.Fatalf("expected 3 members after seed discovery, got %d", len(members))
	}
}

// --- Ring descriptor tests ---

func TestApplyRingDescriptor_UpdatesRingState(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	rd := RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
			{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingJoining},
		},
	}
	if err := m.ApplyRingDescriptor(rd); err != nil {
		t.Fatalf("ApplyRingDescriptor: %v", err)
	}

	if m.RingStateOf("node-1") != RingActive {
		t.Fatalf("expected node-1 RingActive, got %v", m.RingStateOf("node-1"))
	}
	if m.RingStateOf("node-2") != RingJoining {
		t.Fatalf("expected node-2 RingJoining, got %v", m.RingStateOf("node-2"))
	}
	if m.RingStateOf("node-3") != RingNone {
		t.Fatalf("expected node-3 RingNone, got %v", m.RingStateOf("node-3"))
	}
}

func TestApplyRingDescriptor_HigherVersionWins(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	rd1 := RingDescriptor{
		Version: 2,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
		},
	}
	m.ApplyRingDescriptor(rd1)

	// Apply stale version — should be ignored.
	rd2 := RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingJoining},
		},
	}
	m.ApplyRingDescriptor(rd2)

	if m.RingStateOf("node-1") != RingActive {
		t.Fatalf("stale ring descriptor should have been ignored")
	}
	if m.GetRingDescriptor().Version != 2 {
		t.Fatalf("expected ring version 2, got %d", m.GetRingDescriptor().Version)
	}
}

func TestActiveRingMembers_FiltersCorrectly(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	rd := RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
			{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingJoining},
			{NodeID: "node-3", Addr: "10.0.0.3:9090", State: RingActive},
		},
	}
	m.ApplyRingDescriptor(rd)

	active := m.ActiveRingMembers()
	if len(active) != 2 {
		t.Fatalf("expected 2 active ring members, got %d", len(active))
	}

	ids := make(map[string]bool)
	for _, ms := range active {
		ids[ms.NodeID] = true
	}
	if !ids["node-1"] || !ids["node-3"] {
		t.Fatalf("expected node-1 and node-3, got %v", active)
	}
}

// --- HandlePing test ---

func TestHandlePing_MergesAndResponds(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	incoming := &PingMessage{
		SenderID:   "node-2",
		SenderAddr: "10.0.0.2:9090",
		Updates: []MemberState{
			{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1},
			{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1},
		},
	}

	resp := m.HandlePing(incoming)

	if resp.SenderID != "node-1" {
		t.Fatalf("expected sender node-1, got %q", resp.SenderID)
	}

	// We should now know about node-2 and node-3.
	if len(m.GetMembers()) != 3 {
		t.Fatalf("expected 3 members after ping, got %d", len(m.GetMembers()))
	}
}

// --- Ring descriptor gossip tests ---

func TestHandlePing_PropagatesRingDescriptor(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Incoming ping carries a ring descriptor.
	incoming := &PingMessage{
		SenderID:   "node-2",
		SenderAddr: "10.0.0.2:9090",
		Updates: []MemberState{
			{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1},
		},
		RingDesc: &RingDescriptor{
			Version: 3,
			Members: []RingMember{
				{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
				{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingActive},
			},
		},
	}

	resp := m.HandlePing(incoming)

	// Our ring descriptor should be updated.
	if m.GetRingDescriptor().Version != 3 {
		t.Fatalf("expected ring version 3 after ping, got %d", m.GetRingDescriptor().Version)
	}
	if m.RingStateOf("node-1") != RingActive {
		t.Fatalf("expected node-1 RingActive, got %v", m.RingStateOf("node-1"))
	}

	// Response should include our (now updated) ring descriptor.
	if resp.RingDesc == nil {
		t.Fatal("expected ring descriptor in response")
	}
	if resp.RingDesc.Version != 3 {
		t.Fatalf("expected ring version 3 in response, got %d", resp.RingDesc.Version)
	}
}

func TestGossipSync_PropagatesRingDescriptor(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	remoteRD := &RingDescriptor{
		Version: 5,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
		},
	}

	_, respRD := m.HandleGossipSync(nil, remoteRD)

	if m.GetRingDescriptor().Version != 5 {
		t.Fatalf("expected ring version 5 after sync, got %d", m.GetRingDescriptor().Version)
	}
	if respRD == nil || respRD.Version != 5 {
		t.Fatal("expected ring version 5 in sync response")
	}
}

func TestDiscovery_PropagatesRingDescriptor(t *testing.T) {
	tp := &mockTransport{
		syncFn: func(addr string, members []MemberState, rd *RingDescriptor) ([]MemberState, *RingDescriptor, error) {
			return []MemberState{
					{NodeID: "seed", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1},
				}, &RingDescriptor{
					Version: 7,
					Members: []RingMember{
						{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
						{NodeID: "seed", Addr: "10.0.0.2:9090", State: RingActive},
					},
				}, nil
		},
	}

	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SeedPeers = []string{"10.0.0.2:9090"}
	cfg.GossipInterval = 50 * time.Millisecond
	m := NewMembership(cfg, tp)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop()

	if m.GetRingDescriptor().Version != 7 {
		t.Fatalf("expected ring version 7 after seed discovery, got %d", m.GetRingDescriptor().Version)
	}
}

// --- IsRoutable test ---

func TestIsRoutable_SuspectIsRoutable(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.members["node-2"].Liveness = Suspect
	m.mu.Unlock()

	// Suspect should be routable but not "alive" in strict sense.
	if !m.IsRoutable("node-2") {
		t.Fatal("expected Suspect node to be routable")
	}
	if m.IsAlive("node-2") {
		t.Fatal("expected Suspect node to NOT be alive")
	}
}

func TestIsRoutable_DeadIsNotRoutable(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Dead, Incarnation: 1})
	m.mu.Unlock()

	if m.IsRoutable("node-2") {
		t.Fatal("expected Dead node to NOT be routable")
	}
}

// --- Liveness callback test ---

func TestOnLivenessChange_CalledOutsideLock(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil
		},
	}
	m := newTestMembership("node-1", tp)

	var mu sync.Mutex
	var changes []string
	m.OnLivenessChange(func(nodeID string, from, to LivenessState) {
		// Verify we can call GetMembers without deadlocking — this proves
		// the callback is fired outside the membership lock.
		members := m.GetMembers()
		mu.Lock()
		changes = append(changes, fmt.Sprintf("%s:%v->%v(members=%d)", nodeID, from, to, len(members)))
		mu.Unlock()
	})

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	m.probe()

	// Stop cancels suspect timers, ensuring no background callback fires.
	m.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(changes) < 1 {
		t.Fatal("expected at least one liveness change callback")
	}
	// First change should be node-2 going to suspect.
	if changes[0] != "node-2:alive->suspect(members=3)" {
		t.Fatalf("unexpected first change: %s", changes[0])
	}
}

func TestOnRingChange_CalledOutsideLock(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	var calledVersion uint64
	m.OnRingChange(func(rd RingDescriptor) {
		// Verify we can call GetMembers without deadlocking.
		_ = m.GetMembers()
		calledVersion = rd.Version
	})

	rd := RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-1", Addr: "node-1:9090", State: RingActive},
		},
	}
	m.ApplyRingDescriptor(rd)

	if calledVersion != 1 {
		t.Fatalf("expected ring change callback with version 1, got %d", calledVersion)
	}
}

// --- Concurrent safety test ---

func TestConcurrentSafety(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return &PingMessage{SenderID: "remote", SenderAddr: addr}, nil
		},
	}
	m := newTestMembership("node-1", tp)

	// Add several peers.
	for i := 2; i <= 10; i++ {
		m.mu.Lock()
		m.mergeLocked(MemberState{
			NodeID:      fmt.Sprintf("node-%d", i),
			Addr:        fmt.Sprintf("10.0.0.%d:9090", i),
			Liveness:    Alive,
			Incarnation: 1,
		})
		m.mu.Unlock()
	}

	var wg sync.WaitGroup
	const goroutines = 8
	const ops = 200

	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range ops {
				switch i % 6 {
				case 0:
					m.probe()
				case 1:
					m.GetMembers()
				case 2:
					m.IsAlive(fmt.Sprintf("node-%d", (i%9)+2))
				case 3:
					m.AliveMembers()
				case 4:
					m.HandlePing(&PingMessage{
						SenderID:   fmt.Sprintf("node-%d", (id%9)+2),
						SenderAddr: fmt.Sprintf("10.0.0.%d:9090", (id%9)+2),
					})
				case 5:
					m.IsRoutable(fmt.Sprintf("node-%d", (i%9)+2))
				}
			}
		}(g)
	}

	wg.Wait()
}

// --- mergeLocked preserves ring state ---

func TestMerge_PreservesRingState(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Add node-2 and set it to RingActive via descriptor.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 5})
	m.mu.Unlock()

	m.ApplyRingDescriptor(RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingActive},
		},
	})

	if m.RingStateOf("node-2") != RingActive {
		t.Fatalf("expected RingActive, got %v", m.RingStateOf("node-2"))
	}

	// Gossip from a node that hasn't seen the ring descriptor.
	// Higher incarnation but stale ring state (RingNone).
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Ring: RingNone, Incarnation: 10})
	m.mu.Unlock()

	// Ring state must NOT be overwritten by gossip.
	if m.RingStateOf("node-2") != RingActive {
		t.Fatalf("expected RingActive preserved after merge, got %v", m.RingStateOf("node-2"))
	}
	// But incarnation should be updated.
	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			if ms.Incarnation != 10 {
				t.Fatalf("expected incarnation 10, got %d", ms.Incarnation)
			}
			return
		}
	}
	t.Fatal("node-2 not found")
}

func TestMerge_NewNodeUsesDescriptorRingState(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	// Set up a ring descriptor that includes node-2 as Active.
	m.ApplyRingDescriptor(RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingActive},
		},
	})

	// Now learn about node-2 via gossip (which says RingNone).
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Ring: RingNone, Incarnation: 1})
	m.mu.Unlock()

	// Ring state should come from descriptor, not gossip.
	if m.RingStateOf("node-2") != RingActive {
		t.Fatalf("expected new node to get ring state from descriptor, got %v", m.RingStateOf("node-2"))
	}
}

// --- Dead node reaping ---

func TestReapDeadNodes(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil
		},
	}
	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SuspectTimeout = 10 * time.Millisecond
	cfg.PingTimeout = 5 * time.Millisecond
	cfg.DeadNodeReapTimeout = 50 * time.Millisecond
	m := NewMembership(cfg, tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	// Probe to make node-2 Suspect.
	m.probe()
	// Wait for suspect → dead timer.
	time.Sleep(30 * time.Millisecond)

	// node-2 should be Dead.
	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" && ms.Liveness != Dead {
			t.Fatalf("expected Dead, got %v", ms.Liveness)
		}
	}

	// node-2 is Dead+RingNone — should be reaped after timeout.
	time.Sleep(60 * time.Millisecond)
	m.reapDeadNodes()

	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			t.Fatal("expected node-2 to be reaped")
		}
	}
}

func TestReapDeadNodes_PreservesRingMembers(t *testing.T) {
	tp := &mockTransport{}
	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.DeadNodeReapTimeout = 1 * time.Millisecond
	m := NewMembership(cfg, tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	// Put node-2 in ring, then mark Dead.
	m.ApplyRingDescriptor(RingDescriptor{
		Version: 1,
		Members: []RingMember{
			{NodeID: "node-2", Addr: "10.0.0.2:9090", State: RingActive},
		},
	})

	m.mu.Lock()
	m.setLivenessLocked("node-2", Dead)
	m.mu.Unlock()

	time.Sleep(5 * time.Millisecond)
	m.reapDeadNodes()

	// Should NOT be reaped — still in ring.
	found := false
	for _, ms := range m.GetMembers() {
		if ms.NodeID == "node-2" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Dead+RingActive node should not be reaped")
	}
}

// --- Probe order generation counter ---

func TestProbeOrder_DetectsABAChange(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return &PingMessage{SenderID: "remote", SenderAddr: addr}, nil
		},
	}
	m := newTestMembership("node-1", tp)

	// Add 2 nodes and build probe order.
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.rebuildProbeOrderLocked()
	savedGen := m.probeOrderGen
	m.mu.Unlock()

	// Simulate ABA: kill one, add another. Count stays at 2.
	m.mu.Lock()
	m.members["node-2"].Liveness = Dead
	m.memberGen++
	m.mergeLocked(MemberState{NodeID: "node-4", Addr: "10.0.0.4:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	// probeOrderGen should differ from memberGen now.
	m.mu.RLock()
	currentGen := m.memberGen
	m.mu.RUnlock()

	if savedGen == currentGen {
		t.Fatal("expected memberGen to have changed after ABA scenario")
	}

	// nextProbeTarget should rebuild the order.
	target := m.nextProbeTarget()
	if target == nil {
		t.Fatal("expected a probe target")
	}
	// Should not get node-2 (dead).
	if target.NodeID == "node-2" {
		t.Fatal("should not probe dead node-2")
	}
}

// --- Gossip-learned liveness changes ---

func TestMerge_GossipDeadFiresCallback(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mu.Unlock()

	var mu sync.Mutex
	var changes []string
	m.OnLivenessChange(func(nodeID string, from, to LivenessState) {
		mu.Lock()
		changes = append(changes, fmt.Sprintf("%s:%v->%v", nodeID, from, to))
		mu.Unlock()
	})

	// Node C tells us node-2 is Dead with higher incarnation.
	incoming := &PingMessage{
		SenderID:   "node-3",
		SenderAddr: "10.0.0.3:9090",
		Updates: []MemberState{
			{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1},
			{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Dead, Incarnation: 5},
		},
	}
	m.HandlePing(incoming)

	mu.Lock()
	defer mu.Unlock()
	if len(changes) != 1 || changes[0] != "node-2:alive->dead" {
		t.Fatalf("expected [node-2:alive->dead] callback from gossip, got %v", changes)
	}

	// Verify deadSince tracking works for gossip-learned deaths.
	m.mu.RLock()
	_, tracked := m.deadSince["node-2"]
	m.mu.RUnlock()
	if !tracked {
		t.Fatal("expected gossip-learned Dead node to be tracked in deadSince")
	}
}

func TestMerge_GossipSuspectCancelsTimer(t *testing.T) {
	tp := &mockTransport{
		pingFn: func(addr string, msg *PingMessage) (*PingMessage, error) {
			return nil, errors.New("timeout")
		},
		preqFn: func(addr, targetID, targetAddr string) (bool, error) {
			return false, nil
		},
	}
	cfg := DefaultClusterConfig("node-1", "node-1:9090")
	cfg.SuspectTimeout = 5 * time.Second // long so it doesn't fire during test
	cfg.PingTimeout = 10 * time.Millisecond
	m := NewMembership(cfg, tp)

	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Alive, Incarnation: 1})
	m.mergeLocked(MemberState{NodeID: "node-3", Addr: "10.0.0.3:9090", Liveness: Alive, Incarnation: 1})
	m.probeOrder = []string{"node-2", "node-3"}
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
	m.mu.Unlock()

	// Local probe makes node-2 Suspect + starts suspect timer.
	m.probe()

	m.mu.RLock()
	_, hasTimer := m.suspectTimers["node-2"]
	m.mu.RUnlock()
	if !hasTimer {
		t.Fatal("expected suspect timer for node-2")
	}

	// Gossip tells us node-2 is Dead (higher incarnation, skips Suspect→Dead).
	m.mu.Lock()
	m.mergeLocked(MemberState{NodeID: "node-2", Addr: "10.0.0.2:9090", Liveness: Dead, Incarnation: 10})
	m.mu.Unlock()

	// Suspect timer should have been cancelled.
	m.mu.RLock()
	_, hasTimer = m.suspectTimers["node-2"]
	m.mu.RUnlock()
	if hasTimer {
		t.Fatal("expected suspect timer to be cancelled after gossip Dead")
	}
}

// --- Broadcast priority ---

func TestBroadcastBuffer_FreshestFirst(t *testing.T) {
	tp := &mockTransport{}
	m := newTestMembership("node-1", tp)

	m.mu.Lock()
	// Queue an "old" broadcast and drain it once to reduce its retransmit count.
	m.queueBroadcastLocked(MemberState{NodeID: "old-node", Liveness: Alive, Incarnation: 1})
	m.getBroadcastsLocked(10) // decrement old-node's retransmits by 1

	// Queue a "new" broadcast — it has full retransmit count.
	m.queueBroadcastLocked(MemberState{NodeID: "new-node", Liveness: Suspect, Incarnation: 1})

	// Fetch with limit=1 — should get the newer one (higher retransmit count).
	updates := m.getBroadcastsLocked(1)
	m.mu.Unlock()

	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].NodeID != "new-node" {
		t.Fatalf("expected freshest gossip (new-node) first, got %s", updates[0].NodeID)
	}
}
