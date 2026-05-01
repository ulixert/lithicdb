// Package node provides the top-level orchestrator for a theseon cluster node.
// It wires together the storage engine, vector store, SWIM membership,
// coordinator, hinted handoff, and gRPC server into a single lifecycle.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/cluster/antientropy"
	"github.com/ulixert/theseon/cluster/hintedhandoff"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/metrics"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"github.com/ulixert/theseon/server"
	"github.com/ulixert/theseon/vector"
	"google.golang.org/protobuf/proto"
)

// Config configures a cluster node.
type Config struct {
	// Identity
	NodeID string // unique node identifier
	Addr   string // gRPC listen address (host:port)

	// Storage
	DataDir string // main database directory
	HintDir string // hint store directory (default: DataDir/hints)

	// Cluster
	SeedPeers   []string              // seed addresses for SWIM discovery
	Cluster     cluster.ClusterConfig // SWIM parameters
	Coord       cluster.CoordinatorConfig
	AntiEntropy cluster.AntiEntropyConfig

	// Vector
	Vector vector.VectorStoreConfig

	// Logging
	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.HintDir == "" {
		c.HintDir = filepath.Join(c.DataDir, "hints")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	// SWIM parameters: fill each zero field from defaults so partial
	// configs still boot. GossipInterval is the hottest — time.NewTicker(0) panics.
	dcc := cluster.DefaultClusterConfig(c.NodeID, c.Addr)
	if c.Cluster.GossipInterval == 0 {
		c.Cluster.GossipInterval = dcc.GossipInterval
	}
	if c.Cluster.PingTimeout == 0 {
		c.Cluster.PingTimeout = dcc.PingTimeout
	}
	if c.Cluster.SuspectTimeout == 0 {
		c.Cluster.SuspectTimeout = dcc.SuspectTimeout
	}
	if c.Cluster.IndirectPeers == 0 {
		c.Cluster.IndirectPeers = dcc.IndirectPeers
	}
	if c.Cluster.RetransmitMult == 0 {
		c.Cluster.RetransmitMult = dcc.RetransmitMult
	}
	if c.Cluster.MaxPiggyback == 0 {
		c.Cluster.MaxPiggyback = dcc.MaxPiggyback
	}
	if c.Cluster.MaxBroadcasts == 0 {
		c.Cluster.MaxBroadcasts = dcc.MaxBroadcasts
	}
	if c.Cluster.DeadNodeReapTimeout == 0 {
		c.Cluster.DeadNodeReapTimeout = dcc.DeadNodeReapTimeout
	}
	// Quorum parameters: fill each zero field from defaults.
	dco := cluster.DefaultCoordinatorConfig()
	if c.Coord.ReplicationFactor == 0 {
		c.Coord.ReplicationFactor = dco.ReplicationFactor
	}
	if c.Coord.WriteQuorum == 0 {
		c.Coord.WriteQuorum = dco.WriteQuorum
	}
	if c.Coord.ReadQuorum == 0 {
		c.Coord.ReadQuorum = dco.ReadQuorum
	}
	if c.Coord.PerReplicaTimeout == 0 {
		c.Coord.PerReplicaTimeout = dco.PerReplicaTimeout
	}
	// Anti-entropy: fill non-Enabled defaults so admin-triggered runs work
	// even when periodic AE is off.
	dae := cluster.DefaultAntiEntropyConfig()
	if c.AntiEntropy.Interval == 0 {
		c.AntiEntropy.Interval = dae.Interval
	}
	if c.AntiEntropy.Depth == 0 {
		c.AntiEntropy.Depth = dae.Depth
	}
	if c.AntiEntropy.Fanout == 0 {
		c.AntiEntropy.Fanout = dae.Fanout
	}
	if c.AntiEntropy.GracePeriod == 0 {
		c.AntiEntropy.GracePeriod = dae.GracePeriod
	}
	if c.AntiEntropy.MaxConcurrent == 0 {
		c.AntiEntropy.MaxConcurrent = dae.MaxConcurrent
	}
	if c.AntiEntropy.MaxRepairPerRound == 0 {
		c.AntiEntropy.MaxRepairPerRound = dae.MaxRepairPerRound
	}
	if c.AntiEntropy.ScanKeysPerTick == 0 {
		c.AntiEntropy.ScanKeysPerTick = dae.ScanKeysPerTick
	}
}

// New creates a new Node with the given configuration. Call Start to
// initialize and launch all components.
func New(cfg Config) *Node {
	return &Node{cfg: cfg}
}

// Node orchestrates all components of a theseon cluster node.
type Node struct {
	cfg         Config
	database    *db.DB
	vectorStore *vector.VectorStore
	clock       *hlc.Clock
	ring        *hashring.Ring
	membership  *cluster.Membership
	peerPool    *cluster.PeerPool
	coordinator *cluster.Coordinator
	hintStore   *hintedhandoff.Store
	drainer     *hintedhandoff.Drainer
	antiEntropy *antientropy.Manager
	srv         *server.Server
	listener    net.Listener
	logger      *slog.Logger
}

// Start initializes and starts all node components. On failure, any
// successfully opened resources are closed before returning.
func (n *Node) Start(ctx context.Context) error {
	n.cfg.defaults()
	n.logger = n.cfg.Logger

	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// 1. Open the main database.
	opts := db.DefaultOptions(n.cfg.DataDir)
	opts.Logger = n.logger
	opts.Mode = "cluster"
	database, err := db.Open(opts)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	n.database = database
	cleanups = append(cleanups, func() {
		if err := database.Close(); err != nil {
			n.logger.Error("close database", "err", err)
		}
	})

	// 2. Create the vector store.
	vs, err := vector.NewVectorStore(
		database,
		n.cfg.Vector,
		vector.WithLogger(n.logger),
		vector.WithMetrics(metrics.NewVectorAdapter()),
	)
	if err != nil {
		cleanup()
		return fmt.Errorf("create vector store: %w", err)
	}
	n.vectorStore = vs
	cleanups = append(cleanups, func() {
		if err := vs.Close(); err != nil {
			n.logger.Error("close vector store", "err", err)
		}
	})

	// 3. Create the TCP listener early to resolve :0 to an actual port.
	// SWIM membership needs the resolved address.
	lis, err := net.Listen("tcp", n.cfg.Addr)
	if err != nil {
		cleanup()
		return fmt.Errorf("listen %s: %w", n.cfg.Addr, err)
	}
	n.listener = lis
	n.cfg.Addr = lis.Addr().String()

	// 4. Create the HLC clock.
	n.clock = hlc.NewClock(n.cfg.NodeID, nil)

	// 5. Create the hash ring (empty - populated from ring descriptor).
	n.ring = hashring.New(150)

	// 6. Create SWIM membership (not started yet).
	clusterCfg := n.cfg.Cluster
	clusterCfg.NodeID = n.cfg.NodeID
	clusterCfg.Addr = n.cfg.Addr
	clusterCfg.SeedPeers = n.cfg.SeedPeers
	transport := cluster.NewGRPCTransport()
	n.membership = cluster.NewMembership(clusterCfg, transport)

	// 7. Create the peer pool for data-plane RPCs.
	n.peerPool = cluster.NewPeerPool(cluster.DefaultPeerPoolConfig())
	cleanups = append(cleanups, func() { n.peerPool.Close() })

	// 8. Create the coordinator.
	n.coordinator = cluster.NewCoordinator(
		n.cfg.Coord, n.cfg.NodeID, n.ring,
		n.membership, n.clock, database,
		n.peerPool, n.logger,
	)

	// 9. Create the hinted handoff store.
	hintStore, err := hintedhandoff.NewStore(hintedhandoff.StoreConfig{
		Dir:    n.cfg.HintDir,
		Logger: n.logger,
	})
	if err != nil {
		cleanup()
		return fmt.Errorf("create hint store: %w", err)
	}
	n.hintStore = hintStore
	cleanups = append(cleanups, func() {
		if err := hintStore.Close(); err != nil {
			n.logger.Error("close hint store", "err", err)
		}
	})

	// Wire hint store into coordinator.
	n.coordinator.SetHintStore(hintStore)

	// 10. Create the drainer with vector replay callbacks.
	n.drainer = hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:              hintStore,
		Dialer:             n.peerPool,
		Membership:         &membershipAdapter{n.membership},
		DecodeEnvelope:     envelopeDecoder,
		Logger:             n.logger,
		VectorWriteReplay:  vectorWriteReplayFunc,
		VectorDeleteReplay: vectorDeleteReplayFunc,
	})

	// 11. Construct the anti-entropy manager (always created so admin
	// trigger and the AE RPC service are available; periodic ticker
	// only runs when AntiEntropy.Enabled is true).
	aeMgr, err := antientropy.NewManager(antientropy.Config{
		Cfg:               n.cfg.AntiEntropy,
		SelfID:            n.cfg.NodeID,
		Ring:              n.ring,
		Membership:        &aeMembershipAdapter{n.membership},
		DB:                database,
		Dialer:            n.peerPool,
		Repairer:          n.coordinator,
		ReplicationFactor: n.cfg.Coord.ReplicationFactor,
		Logger:            n.logger,
	})
	if err != nil {
		cleanup()
		return fmt.Errorf("create anti-entropy manager: %w", err)
	}
	n.antiEntropy = aeMgr

	// Construct the AE service handler (server-side RPCs).
	aeSvc := antientropy.NewService(
		n.cfg.NodeID, n.ring, database,
		&aeMembershipAdapter{n.membership},
		n.cfg.Coord.ReplicationFactor, n.logger,
	)

	// Forward-compat: when MVCC-aware compaction lands
	// (currently tombstones are never GC'd — see compaction/executor.go:100),
	// this is where we'd assert tombstoneGrace > AE.Interval + safetyMargin
	// and fail Start. Today the check is a no-op log line.
	n.logger.Debug("anti-entropy tombstone-retention check skipped: tombstones currently never GC'd")

	// 12. Wire membership callbacks.
	n.membership.OnRingChange(func(rd cluster.RingDescriptor) {
		var nodes []hashring.Node
		for _, rm := range rd.Members {
			if rm.State == cluster.RingJoining || rm.State == cluster.RingActive {
				nodes = append(nodes, hashring.Node{ID: rm.NodeID, Addr: rm.Addr})
			}
		}
		n.ring.ReplaceMembers(nodes)
		n.logger.Info("ring rebuilt", "version", rd.Version, "members", len(nodes))
	})

	n.membership.OnLivenessChange(func(nodeID string, from, to cluster.LivenessState) {
		if from != cluster.Alive && to == cluster.Alive {
			n.drainer.TriggerDrain(nodeID)
			// Anti-entropy on recovery: complements hinted handoff for
			// hints that expired or were never stored.
			if n.antiEntropy != nil {
				n.antiEntropy.TriggerWith(nodeID, antientropy.TriggerRecovery)
			}
		}
	})

	// 13. Create the gRPC server with all options.
	n.srv = server.New(database, nil,
		server.WithMembership(n.membership),
		server.WithReplication(n.clock, database),
		server.WithCoordinator(n.coordinator),
		server.WithVectorStore(n.vectorStore),
		server.WithAntiEntropy(aeSvc),
	)

	// 14. Register AdminService.
	adminSrv := cluster.NewAdminServer(n.membership, n.cfg.NodeID, n.cfg.Addr, n.antiEntropy)
	n.srv.RegisterService(&pb.AdminService_ServiceDesc, adminSrv)

	// 15. Start gRPC server (non-blocking) on the already-created listener.
	go func() {
		if err := n.srv.Serve(lis); err != nil {
			n.logger.Error("gRPC server error", "err", err)
		}
	}()

	// 16. Start membership (SWIM begins, node becomes discoverable).
	if err := n.membership.Start(ctx); err != nil {
		n.srv.GracefulStop()
		cleanup()
		return fmt.Errorf("start membership: %w", err)
	}

	// 17. Start drainer.
	n.drainer.Start()

	// 18. Start anti-entropy (no-op if disabled).
	n.antiEntropy.Start()

	n.logger.Info("node started",
		"node_id", n.cfg.NodeID,
		"addr", n.cfg.Addr,
		"seeds", n.cfg.SeedPeers,
	)
	return nil
}

// Stop shuts down the node in reverse startup order, ensuring in-flight
// RPCs complete before dependencies are torn down.
func (n *Node) Stop() {
	n.logger.Info("node stopping", "node_id", n.cfg.NodeID)

	// 1. Stop accepting new RPCs, drain in-flight.
	if n.srv != nil {
		n.srv.GracefulStop()
	}
	// 2. Stop anti-entropy (waits for in-flight reconciles).
	if n.antiEntropy != nil {
		n.antiEntropy.Stop()
	}
	// 3. Stop drainer (waits for in-flight drains).
	if n.drainer != nil {
		n.drainer.Stop()
	}
	// 4. Stop SWIM probe loop.
	if n.membership != nil {
		n.membership.Stop()
	}
	// 5. Close peer connections.
	if n.peerPool != nil {
		n.peerPool.Close()
	}
	// 6. Close hint store.
	if n.hintStore != nil {
		if err := n.hintStore.Close(); err != nil {
			n.logger.Error("close hint store", "err", err)
		}
	}
	// 7. Close vector store (release HNSW memory).
	if n.vectorStore != nil {
		if err := n.vectorStore.Close(); err != nil {
			n.logger.Error("close vector store", "err", err)
		}
	}
	// 8. Close the main database.
	if n.database != nil {
		if err := n.database.Close(); err != nil {
			n.logger.Error("close database", "err", err)
		}
	}

	n.logger.Info("node stopped", "node_id", n.cfg.NodeID)
}

// Addr returns the actual listener address (resolved from :0 if needed).
func (n *Node) Addr() string {
	if n.listener != nil {
		return n.listener.Addr().String()
	}
	return n.cfg.Addr
}

// --- Adapters ---

// membershipAdapter bridges cluster.Membership to hintedhandoff.MembershipQuerier.
type membershipAdapter struct {
	m *cluster.Membership
}

func (a *membershipAdapter) IsAlive(nodeID string) bool {
	return a.m.IsAlive(nodeID)
}

func (a *membershipAdapter) GetMemberInfos() []hintedhandoff.MemberInfo {
	members := a.m.GetMembers()
	infos := make([]hintedhandoff.MemberInfo, len(members))
	for i, ms := range members {
		infos[i] = hintedhandoff.MemberInfo{NodeID: ms.NodeID, Addr: ms.Addr}
	}
	return infos
}

// aeMembershipAdapter bridges cluster.Membership to the antientropy
// MembershipQuerier and the antientropy.MembershipRingVersioner.
type aeMembershipAdapter struct {
	m *cluster.Membership
}

func (a *aeMembershipAdapter) IsAlive(nodeID string) bool { return a.m.IsAlive(nodeID) }

func (a *aeMembershipAdapter) Members() []antientropy.MemberInfo {
	members := a.m.GetMembers()
	infos := make([]antientropy.MemberInfo, len(members))
	for i, ms := range members {
		infos[i] = antientropy.MemberInfo{NodeID: ms.NodeID, Addr: ms.Addr}
	}
	return infos
}

func (a *aeMembershipAdapter) RingVersion() uint64 {
	return a.m.GetRingDescriptor().Version
}

// envelopeDecoder adapts cluster.DecodeEnvelope to hintedhandoff.EnvelopeDecoder.
func envelopeDecoder(b []byte) (hintedhandoff.DecodedEnvelope, error) {
	env, err := cluster.DecodeEnvelope(b)
	if err != nil {
		return hintedhandoff.DecodedEnvelope{}, err
	}
	return hintedhandoff.DecodedEnvelope{
		Timestamp: env.Timestamp,
		Deleted:   env.Deleted,
		Value:     env.Value,
	}, nil
}

// vectorWriteReplayFunc replays a vector write hint to a target replica.
func vectorWriteReplayFunc(ctx context.Context, client pb.InternalServiceClient, payload []byte) error {
	req := &pb.ReplicateVectorWriteRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("unmarshal vector write hint: %w", err)
	}
	_, err := client.ReplicateVectorWrite(ctx, req)
	return err
}

// vectorDeleteReplayFunc replays a vector delete hint to a target replica.
func vectorDeleteReplayFunc(ctx context.Context, client pb.InternalServiceClient, payload []byte) error {
	req := &pb.ReplicateVectorDeleteRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("unmarshal vector delete hint: %w", err)
	}
	_, err := client.ReplicateVectorDelete(ctx, req)
	return err
}
