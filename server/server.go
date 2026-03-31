// Package server provides a gRPC server wrapping a db.DB instance.
// The server translates between protobuf messages and the db package's
// Go API, exposing Put, Get, Delete, Scan, and BatchWrite over gRPC.
//
// When a Membership is provided, the server also registers the
// InternalService for SWIM protocol RPCs (Ping, PingReq, GossipSync)
// and optionally data-plane replication RPCs (ReplicateWrite,
// ReplicateWriteBatch, ReplicateRead).
//
// The server does not own the db.DB lifecycle - the caller opens and
// closes the database. This keeps the server testable and allows
// multiple gRPC services to share the same DB in future PRs.
package server

import (
	"net"

	"github.com/ulixert/lithicdb/cluster"
	"github.com/ulixert/lithicdb/db"
	"github.com/ulixert/lithicdb/hlc"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc"
)

// Server wraps a db.DB with gRPC handlers for the LithicDB service.
// Optionally serves InternalService for SWIM protocol if membership
// is configured. When a Coordinator is provided, client-facing RPCs
// (Put, Get, Delete) route through the quorum coordinator instead of
// writing directly to the local database.
type Server struct {
	pb.UnimplementedLithicDBServer

	db          *db.DB
	coordinator *cluster.Coordinator
	gs          *grpc.Server
}

// Option configures the Server.
type Option func(*serverConfig)

type serverConfig struct {
	membership  *cluster.Membership
	clock       *hlc.Clock
	database    *db.DB
	coordinator *cluster.Coordinator
}

// WithMembership configures the server to register the InternalService
// for SWIM protocol RPCs backed by the given Membership.
func WithMembership(m *cluster.Membership) Option {
	return func(c *serverConfig) {
		c.membership = m
	}
}

// WithReplication configures the InternalService with an HLC clock and
// database for data-plane replication RPCs (ReplicateWrite,
// ReplicateWriteBatch, ReplicateRead). Requires WithMembership.
func WithReplication(clock *hlc.Clock, database *db.DB) Option {
	return func(c *serverConfig) {
		c.clock = clock
		c.database = database
	}
}

// WithCoordinator enables cluster mode for client-facing RPCs. When
// set, Put/Get/Delete are routed through the quorum coordinator
// instead of writing directly to the local database.
func WithCoordinator(coord *cluster.Coordinator) Option {
	return func(c *serverConfig) {
		c.coordinator = coord
	}
}

// New creates a gRPC server that serves the LithicDB service backed
// by the given database. The caller retains ownership of the database
// and must close it after stopping the server.
func New(database *db.DB, grpcOpts []grpc.ServerOption, opts ...Option) *Server {
	var cfg serverConfig
	for _, o := range opts {
		o(&cfg)
	}

	gs := grpc.NewServer(grpcOpts...)
	s := &Server{db: database, coordinator: cfg.coordinator, gs: gs}
	pb.RegisterLithicDBServer(gs, s)

	if cfg.membership != nil {
		pb.RegisterInternalServiceServer(gs, &internalServer{
			membership: cfg.membership,
			clock:      cfg.clock,
			db:         cfg.database,
		})
	}

	return s
}

// Serve starts the gRPC server on the given listener. It blocks until
// the server is stopped via GracefulStop or the listener fails.
func (s *Server) Serve(lis net.Listener) error {
	return s.gs.Serve(lis)
}

// GracefulStop stops the gRPC server gracefully. It stops accepting
// new RPCs and blocks until all pending RPCs have completed.
func (s *Server) GracefulStop() {
	s.gs.GracefulStop()
}
