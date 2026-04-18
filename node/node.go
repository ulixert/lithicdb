// Package node provides the top-level orchestrator for theseon cluster node.
// It wires together the storage engine, vector store, SWIM membership,
// coordinator, hinted handoff, and gRPC server into a single lifecycle.
package node

import (
	"log/slog"
	"net"
	"path/filepath"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/cluster/hintedhandoff"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/server"
	"github.com/ulixert/theseon/vector"
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
	SeedPeers []string              // seed addresses for SWIM discovery
	Cluster   cluster.ClusterConfig // SWIM parameters
	Coord     cluster.CoordinatorConfig

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
	srv         *server.Server
	listener    net.Listener
	logger      *slog.Logger
}
