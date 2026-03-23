// Package server provides a gRPC server wrapping a db.DB instance.
// The server translates between protobuf messages and the db package's
// Go API, exposing Put, Get, Delete, Scan, and BatchWrite over gRPC.
//
// The server does not own the db.DB lifecycle - the caller opens and
// closes the database. This keeps the server testable and allows
// multiple gRPC services to share the same DB in future PRs.
package server

import (
	"net"

	"github.com/ulixert/lithicdb/db"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc"
)

// Server wraps a db.DB with gRPC handlers for the LithicDB service.
type Server struct {
	pb.UnimplementedLithicDBServer

	db *db.DB
	gs *grpc.Server
}

// New creates a gRPC server that serves the LithicDB service backed
// by the given database. The caller retains ownership of the database
// and must close it after stopping the server.
func New(database *db.DB, opts ...grpc.ServerOption) *Server {
	gs := grpc.NewServer(opts...)
	s := &Server{db: database, gs: gs}
	pb.RegisterLithicDBServer(gs, s)
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
