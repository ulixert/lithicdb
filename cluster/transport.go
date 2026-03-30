package cluster

import "context"

// LivenessState represents a node's reachability as detected by SWIM.
type LivenessState int

const (
	Alive   LivenessState = iota // reachable, responding to probes
	Suspect                      // ping failed, indirect ping pending or timed out
	Dead                         // confirmed unreachable after SuspectTimeout
)

func (s LivenessState) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// RingState represents a node's role in the hash ring.
// Ring state only changes via explicit admin commands, never from SWIM.
type RingState int

const (
	RingNone    RingState = iota // discovered via SWIM, not yet in the ring
	RingJoining                  // in the ring, receives writes, skipped for reads
	RingActive                   // full participant in reads and writes
)

func (s RingState) String() string {
	switch s {
	case RingNone:
		return "none"
	case RingJoining:
		return "joining"
	case RingActive:
		return "active"
	default:
		return "unknown"
	}
}

// MemberState is the gossip state for a single cluster member.
// The Incarnation field is a monotonic counter used for CRDT-style
// conflict resolution: higher incarnation always wins on merge.
type MemberState struct {
	NodeID      string
	Addr        string
	Liveness    LivenessState
	Ring        RingState
	Incarnation uint64
}

// PingMessage is the payload for SWIM Ping and Ping-Ack exchanges.
// Updates are piggybacked gossip - recent state changes that should
// be disseminated across the cluster. RingDesc is piggybacked so all
// nodes converge on the latest ring descriptor within one gossip round.
type PingMessage struct {
	SenderID   string
	SenderAddr string
	Updates    []MemberState
	RingDesc   *RingDescriptor // nil if no descriptor to share
}

// RingDescriptor is a versioned snapshot of ring membership.
// The Version field is monotonically increasing and is incremented
// on every admin-initiated ring change.
type RingDescriptor struct {
	Version uint64
	Members []RingMember
}

// RingMember describes a single node's ring participation.
type RingMember struct {
	NodeID string
	Addr   string
	State  RingState
}

// Transport abstracts the network layer for SWIM protocol messages.
// This allows the SWIM logic to be tested with a mock transport
// (no real networking) while production uses gRPC.
type Transport interface {
	// Ping sends a SWIM probe to the target address and returns the
	// ack response. The msg contains piggybacked gossip updates.
	// Returns an error if the target is unreachable or times out.
	Ping(ctx context.Context, addr string, msg *PingMessage) (*PingMessage, error)

	// PingReq asks the node at addr to ping targetAddr on our behalf
	// (indirect ping). Returns true if the target responded to the
	// intermediary, false otherwise.
	PingReq(ctx context.Context, addr string, targetID, targetAddr string) (bool, error)

	// GossipSync performs a full state exchange with the node at addr.
	// Sends our full membership table and ring descriptor, receives theirs.
	// Used for initial discovery via seed peers.
	GossipSync(ctx context.Context, addr string, members []MemberState, ringDesc *RingDescriptor) ([]MemberState, *RingDescriptor, error)
}
