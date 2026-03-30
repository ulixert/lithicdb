// Command lithicdb starts a standalone gRPC server backed by a LithicDB
// storage engine. Graceful shutdown on SIGINT/SIGTERM drains in-flight
// RPCs before closing the database.
//
// Usage:
//
//	lithicdb -addr=:50051 -data-dir=./data
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ulixert/lithicdb/db"
	"github.com/ulixert/lithicdb/server"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "database directory")
	flag.Parse()

	opts := db.DefaultOptions(*dataDir)

	database, err := db.Open(opts)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		_ = database.Close()
		log.Fatalf("listen %s: %v", *addr, err)
	}

	srv := server.New(database, nil)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %v, shutting down...", sig)
		srv.GracefulStop()
	}()

	log.Printf("lithicdb listening on %s (data: %s)", *addr, *dataDir)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}

	// Serve returns after GracefulStop completes - safe to close DB.
	if err := database.Close(); err != nil {
		log.Printf("close database: %v", err)
	}
}
