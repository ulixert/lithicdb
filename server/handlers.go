package server

import (
	"context"

	"github.com/ulixert/lithicdb/kv"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := s.db.Put(req.Key, req.Value); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	val, found := s.db.Get(req.Key)
	if !found || val.Tombstone {
		return &pb.GetResponse{Found: false}, nil
	}
	return &pb.GetResponse{Value: val.Data, Found: true}, nil
}

func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := s.db.Delete(req.Key); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) Scan(_ *pb.ScanRequest, stream pb.LithicDB_ScanServer) error {
	iter := s.db.Scan()
	defer iter.Close()

	for iter.IsValid() {
		userKey := kv.UserKey(iter.Key())
		value := iter.Value()

		// Skip tombstones - deleted keys are not visible to clients.
		if value == nil {
			iter.Next()
			continue
		}

		if err := stream.Send(&pb.ScanResponse{
			Key:   userKey,
			Value: value,
		}); err != nil {
			// Client disconnected or context canceled.
			return err
		}

		iter.Next()
	}

	if err := iter.Err(); err != nil {
		return status.Errorf(codes.Internal, "scan: %v", err)
	}

	return nil
}

func (s *Server) BatchWrite(_ context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {
	batch := s.db.NewWriteBatch()

	for i, entry := range req.Entries {
		if len(entry.Key) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "entry %d: key must not be empty", i)
		}
		if entry.IsDelete {
			batch.Delete(entry.Key)
		} else {
			batch.Put(entry.Key, entry.Value)
		}
	}

	if err := batch.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "batch write: %v", err)
	}

	return &pb.BatchWriteResponse{}, nil
}
