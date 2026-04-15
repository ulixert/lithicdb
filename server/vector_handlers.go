package server

import (
	"context"
	"sync"
	"time"

	pb "github.com/ulixert/theseon/proto/theseonpb"
	"github.com/ulixert/theseon/vector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// localVersionGen generates monotonically increasing versions for standalone mode.
// Uses a mini local HLC: wall time advances when the clock advances,
// logical increments on ties.
type localVersionGen struct {
	mu    sync.Mutex
	lastW int64
	lastL uint32
}

func (g *localVersionGen) Next() vector.VectorVersion {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixNano()
	if now > g.lastW {
		g.lastW = now
		g.lastL = 0
	} else {
		g.lastL++
	}
	return vector.VectorVersion{WallTime: g.lastW, Logical: g.lastL}
}

func (s *Server) CreateCollection(ctx context.Context, req *pb.CreateCollectionRequest) (*pb.CreateCollectionResponse, error) {
	if s.vectorStore == nil {
		return nil, status.Error(codes.Unavailable, "vector store not configured")
	}
	cfg := vector.CollectionConfig{
		Dim:         int(req.Dim),
		Metric:      uint8(req.Metric),
		M:           int(req.M),
		EfConstruct: int(req.EfConstruct),
		EfSearch:    int(req.EfSearch),
		MaxVectors:  req.MaxVectors,
	}
	if err := s.vectorStore.CreateCollection(req.Name, cfg); err != nil {
		return nil, status.Errorf(codes.Internal, "create collection: %v", err)
	}
	return &pb.CreateCollectionResponse{}, nil
}

func (s *Server) VectorPut(ctx context.Context, req *pb.VectorPutRequest) (*pb.VectorPutResponse, error) {
	if s.vectorStore == nil {
		return nil, status.Error(codes.Unavailable, "vector store not configured")
	}
	if len(req.Id) != 16 {
		return nil, status.Error(codes.InvalidArgument, "id must be 16 bytes")
	}

	var id [16]byte
	copy(id[:], req.Id)

	if s.coordinator != nil {
		meta := protoMetadataToVector(req.Metadata)
		if err := s.coordinator.VectorWrite(ctx, req.Collection, id, req.Vector, meta); err != nil {
			return nil, status.Errorf(codes.Internal, "coordinator vector write: %v", err)
		}
		return &pb.VectorPutResponse{}, nil
	}

	// Standalone mode.
	meta := protoMetadataToVectorMeta(req.Metadata)
	ver := s.versionGen.Next()
	if err := s.vectorStore.Put(req.Collection, id, req.Vector, meta, ver); err != nil {
		return nil, status.Errorf(codes.Internal, "vector put: %v", err)
	}
	return &pb.VectorPutResponse{}, nil
}

// --- Proto conversion helpers ---

// protoMetadataToVector converts proto metadata to cluster.Metadata (for the coordinator path).
func protoMetadataToVector(m map[string][]byte) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		// Proto uses raw bytes. For now, treat all values as []byte.
		// Full type-aware serialization deferred to later iteration.
		result[k] = v
	}
	return result
}

// protoMetadataToVectorMeta converts proto metadata to vector.Metadata (for the standalone path).
func protoMetadataToVectorMeta(m map[string][]byte) vector.Metadata {
	if m == nil {
		return nil
	}
	result := make(vector.Metadata, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func vectorMetadataToProto(meta vector.Metadata) map[string][]byte {
	if meta == nil {
		return nil
	}
	result := make(map[string][]byte, len(meta))
	for k, v := range meta {
		switch val := v.(type) {
		case []byte:
			result[k] = val
		case string:
			result[k] = []byte(val)
		}
	}
	return result
}
