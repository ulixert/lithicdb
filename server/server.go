// Package server provides a gRPC server wrapping a db.DB instance.
// The server translates between protobuf messages and the db package's
// Go API, exposing Put, Get, Delete, Scan, and BatchWrite over gRPC.
//
// When a Membership is provided, the server also registers the
// InternalService for SWIM protocol RPCs (Ping, PingReq, GossipSync).
//
// The server does not own the db.DB lifecycle - the caller opens and
// closes the database. This keeps the server testable and allows
// multiple gRPC services to share the same DB in future PRs.
package server

import (
	"net"

	"github.com/ulixert/lithicdb/cluster"
	"github.com/ulixert/lithicdb/db"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc"
)

// Server wraps a db.DB with gRPC handlers for the LithicDB service.
// Optionally serves InternalService for SWIM protocol if membership
// is configured.
type Server struct {
	pb.UnimplementedLithicDBServer

	db *db.DB
	gs *grpc.Server
}

// Option configures the Server.
type Option func(*serverConfig)

type serverConfig struct {
	membership *cluster.Membership
}

// WithMembership configures the server to register the InternalService
// for SWIM protocol RPCs backed by the given Membership.
func WithMembership(m *cluster.Membership) Option {
	return func(c *serverConfig) {
		c.membership = m
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
	s := &Server{db: database, gs: gs}
	pb.RegisterLithicDBServer(gs, s)

	if cfg.membership != nil {
		pb.RegisterInternalServiceServer(gs, &internalServer{
			membership: cfg.membership,
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
