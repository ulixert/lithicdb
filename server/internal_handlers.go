package server

import (
	"context"
	"errors"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hlc"
	"github.com/ulixert/theseon/metrics"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"github.com/ulixert/theseon/vector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// internalServer implements the InternalService gRPC service,
// delegating SWIM protocol handling to the Membership and
// data-plane replication to the local db.DB via HLC-stamped envelopes.
type internalServer struct {
	pb.UnimplementedInternalServiceServer
	membership  *cluster.Membership
	clock       *hlc.Clock          // nil in standalone mode
	db          *db.DB              // nil in standalone mode
	vectorStore *vector.VectorStore // nil if vector store not configured
}

func (s *internalServer) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	incoming := &cluster.PingMessage{
		SenderID:   req.SenderId,
		SenderAddr: req.SenderAddr,
		Updates:    protoToMemberStates(req.Updates),
		RingDesc:   protoToRingDesc(req.RingDescriptor),
	}

	resp := s.membership.HandlePing(incoming)

	return &pb.PingResponse{
		SenderId:       resp.SenderID,
		SenderAddr:     resp.SenderAddr,
		Updates:        memberStatesToProto(resp.Updates),
		RingDescriptor: ringDescToProto(resp.RingDesc),
	}, nil
}

func (s *internalServer) PingReq(_ context.Context, req *pb.PingReqRequest) (*pb.PingReqResponse, error) {
	ack := s.membership.HandlePingReq(req.TargetId, req.TargetAddr)
	return &pb.PingReqResponse{Ack: ack}, nil
}

func (s *internalServer) GossipSync(_ context.Context, req *pb.GossipSyncRequest) (*pb.GossipSyncResponse, error) {
	remote := protoToMemberStates(req.Members)
	remoteRD := protoToRingDesc(req.RingDescriptor)
	local, localRD := s.membership.HandleGossipSync(remote, remoteRD)
	return &pb.GossipSyncResponse{
		Members:        memberStatesToProto(local),
		RingDescriptor: ringDescToProto(localRD),
	}, nil
}

// --- Data-plane replication handlers ---

// ReplicateWrite stores a single key-value pair wrapped in an HLC envelope.
// The coordinator calls this on each replica after stamping the write with
// clock.Now(). The receiver calls clock.Update() to synchronize its HLC.
//
// Clock drift handling: if the received timestamp is too far from the local
// physical clock, the write is rejected. The coordinator won't count this
// replica's ack. Anti-entropy will repair the divergence later.
func (s *internalServer) ReplicateWrite(_ context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_write"))
	defer timer.ObserveDuration()
	if s.clock == nil || s.db == nil {
		return nil, status.Error(codes.Unavailable, "replication not configured")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if req.Timestamp == nil {
		return nil, status.Error(codes.InvalidArgument, "timestamp is required")
	}

	ts := protoToHLCTimestamp(req.Timestamp)

	if err := s.clock.Update(ts); err != nil {
		if errors.Is(err, hlc.ErrClockDrift) {
			slog.Warn("clock drift on replicated write",
				"remote_wall", ts.WallTime,
				"remote_node", ts.NodeID,
			)
		}
		return nil, status.Errorf(codes.Internal, "clock update: %v", err)
	}

	encoded, err := cluster.EncodeEnvelope(cluster.Envelope{
		Timestamp: ts,
		Deleted:   req.Deleted,
		Value:     req.Value,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode envelope: %v", err)
	}

	if err := s.db.Put(req.Key, encoded); err != nil {
		return nil, status.Errorf(codes.Internal, "db put: %v", err)
	}

	return &pb.ReplicateWriteResponse{}, nil
}

// ReplicateWriteBatch atomically stores multiple key-value pairs as envelopes.
// The clock is synchronized once with the highest timestamp in the batch,
// rather than per-entry, to avoid inflating the logical counter by N.
func (s *internalServer) ReplicateWriteBatch(_ context.Context, req *pb.ReplicateWriteBatchRequest) (*pb.ReplicateWriteBatchResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_write_batch"))
	defer timer.ObserveDuration()
	if s.clock == nil || s.db == nil {
		return nil, status.Error(codes.Unavailable, "replication not configured")
	}

	// Validate all entries and find the max timestamp.
	var maxTS hlc.Timestamp
	for i, entry := range req.Entries {
		if len(entry.Key) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "entry %d: key must not be empty", i)
		}
		if entry.Timestamp == nil {
			return nil, status.Errorf(codes.InvalidArgument, "entry %d: timestamp is required", i)
		}
		ts := protoToHLCTimestamp(entry.Timestamp)
		if maxTS.IsZero() || maxTS.Less(ts) {
			maxTS = ts
		}
	}

	// Synchronize clock once with the highest timestamp in the batch.
	if err := s.clock.Update(maxTS); err != nil {
		if errors.Is(err, hlc.ErrClockDrift) {
			slog.Warn("clock drift on replicated batch write",
				"remote_wall", maxTS.WallTime,
				"remote_node", maxTS.NodeID,
			)
		}
		return nil, status.Errorf(codes.Internal, "clock update: %v", err)
	}

	// Encode and batch all entries.
	batch := s.db.NewWriteBatch()
	for i, entry := range req.Entries {
		ts := protoToHLCTimestamp(entry.Timestamp)
		encoded, err := cluster.EncodeEnvelope(cluster.Envelope{
			Timestamp: ts,
			Deleted:   entry.Deleted,
			Value:     entry.Value,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "entry %d: encode envelope: %v", i, err)
		}

		batch.Put(entry.Key, encoded)
	}

	if err := batch.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "batch commit: %v", err)
	}

	return &pb.ReplicateWriteBatchResponse{}, nil
}

// ReplicateRead returns the envelope metadata for a single key. Used by
// the coordinator for quorum reads and read repair.
func (s *internalServer) ReplicateRead(_ context.Context, req *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_read"))
	defer timer.ObserveDuration()
	if s.clock == nil || s.db == nil {
		return nil, status.Error(codes.Unavailable, "replication not configured")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	val, found := s.db.Get(req.Key)
	if !found {
		return &pb.ReplicateReadResponse{Found: false}, nil
	}

	env, err := cluster.DecodeEnvelope(val.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode envelope: %v", err)
	}

	return &pb.ReplicateReadResponse{
		Value:     env.Value,
		Timestamp: hlcTimestampToProto(env.Timestamp),
		Found:     true,
		Deleted:   env.Deleted,
	}, nil
}

// --- Vector replication handlers ---

func (s *internalServer) ReplicateVectorWrite(_ context.Context, req *pb.ReplicateVectorWriteRequest) (*pb.ReplicateVectorWriteResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_vector_write"))
	defer timer.ObserveDuration()
	if s.vectorStore == nil {
		return nil, status.Error(codes.Unavailable, "vector store not configured")
	}
	if req.Timestamp == nil {
		return nil, status.Error(codes.InvalidArgument, "timestamp is required")
	}
	if len(req.Id) != 16 {
		return nil, status.Error(codes.InvalidArgument, "id must be 16 bytes")
	}

	// Config digest validation.
	cfg, err := s.vectorStore.GetCollectionConfig(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection %q: %v", req.Collection, err)
	}
	localDigest := vector.ConfigDigest(cfg)
	if req.ConfigDigest != 0 && req.ConfigDigest != localDigest {
		return nil, status.Errorf(codes.FailedPrecondition,
			"config digest mismatch for collection %q: local=%d, remote=%d",
			req.Collection, localDigest, req.ConfigDigest)
	}

	ts := protoToHLCTimestamp(req.Timestamp)
	if s.clock != nil {
		if err := s.clock.Update(ts); err != nil {
			return nil, status.Errorf(codes.Internal, "clock update: %v", err)
		}
	}

	var id [16]byte
	copy(id[:], req.Id)
	ver := vector.VectorVersion{WallTime: ts.WallTime, Logical: ts.Logical}
	meta := protoMetadataToVectorMeta(req.Metadata)

	if err := s.vectorStore.Put(req.Collection, id, req.Vector, meta, ver); err != nil {
		return nil, status.Errorf(codes.Internal, "vector put: %v", err)
	}

	return &pb.ReplicateVectorWriteResponse{}, nil
}

func (s *internalServer) ReplicateVectorDelete(_ context.Context, req *pb.ReplicateVectorDeleteRequest) (*pb.ReplicateVectorDeleteResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_vector_delete"))
	defer timer.ObserveDuration()
	if s.vectorStore == nil {
		return nil, status.Error(codes.Unavailable, "vector store not configured")
	}
	if req.Timestamp == nil {
		return nil, status.Error(codes.InvalidArgument, "timestamp is required")
	}
	if len(req.Id) != 16 {
		return nil, status.Error(codes.InvalidArgument, "id must be 16 bytes")
	}

	// Config digest validation.
	cfg, err := s.vectorStore.GetCollectionConfig(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection %q: %v", req.Collection, err)
	}
	localDigest := vector.ConfigDigest(cfg)
	if req.ConfigDigest != 0 && req.ConfigDigest != localDigest {
		return nil, status.Errorf(codes.FailedPrecondition,
			"config digest mismatch for collection %q", req.Collection)
	}

	ts := protoToHLCTimestamp(req.Timestamp)
	if s.clock != nil {
		if err := s.clock.Update(ts); err != nil {
			return nil, status.Errorf(codes.Internal, "clock update: %v", err)
		}
	}

	var id [16]byte
	copy(id[:], req.Id)
	ver := vector.VectorVersion{WallTime: ts.WallTime, Logical: ts.Logical}

	if err := s.vectorStore.Delete(req.Collection, id, ver); err != nil {
		return nil, status.Errorf(codes.Internal, "vector delete: %v", err)
	}

	return &pb.ReplicateVectorDeleteResponse{}, nil
}

func (s *internalServer) ReplicateVectorSearch(ctx context.Context, req *pb.ReplicateVectorSearchRequest) (*pb.ReplicateVectorSearchResponse, error) {
	timer := prometheus.NewTimer(metrics.ClusterRPCDuration.WithLabelValues("replicate_vector_search"))
	defer timer.ObserveDuration()
	if s.vectorStore == nil {
		return nil, status.Error(codes.Unavailable, "vector store not configured")
	}

	// Config digest validation.
	cfg, err := s.vectorStore.GetCollectionConfig(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection %q: %v", req.Collection, err)
	}
	localDigest := vector.ConfigDigest(cfg)
	if req.ConfigDigest != 0 && req.ConfigDigest != localDigest {
		return nil, status.Errorf(codes.FailedPrecondition,
			"config digest mismatch for collection %q", req.Collection)
	}

	// Collection readiness check.
	if !s.vectorStore.CollectionReady(req.Collection) {
		return nil, status.Errorf(codes.Unavailable,
			"collection %q is rebuilding", req.Collection)
	}

	// Do NOT call clock.Update() — reads don't update clocks.
	var opts *vector.SearchOptions
	if req.EfSearch > 0 {
		opts = &vector.SearchOptions{EfSearch: int(req.EfSearch)}
	}
	results, err := s.vectorStore.Search(req.Collection, req.Query, int(req.K), opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "vector search: %v", err)
	}

	resp := &pb.ReplicateVectorSearchResponse{
		Results: make([]*pb.VectorSearchResult, len(results)),
	}
	for i, r := range results {
		resp.Results[i] = &pb.VectorSearchResult{
			Id:              r.ID[:],
			Vector:          r.Vector,
			Distance:        r.Distance,
			VersionWallTime: r.Version.WallTime,
			VersionLogical:  r.Version.Logical,
		}
	}
	return resp, nil
}

// --- Proto ↔ domain conversion (server-side) ---

func memberStatesToProto(states []cluster.MemberState) []*pb.MemberUpdateProto {
	if states == nil {
		return nil
	}
	out := make([]*pb.MemberUpdateProto, len(states))
	for i, s := range states {
		out[i] = &pb.MemberUpdateProto{
			NodeId:      s.NodeID,
			Addr:        s.Addr,
			Liveness:    int32(s.Liveness),
			RingState:   int32(s.Ring),
			Incarnation: s.Incarnation,
		}
	}
	return out
}

func protoToMemberStates(protos []*pb.MemberUpdateProto) []cluster.MemberState {
	if protos == nil {
		return nil
	}
	out := make([]cluster.MemberState, len(protos))
	for i, p := range protos {
		out[i] = cluster.MemberState{
			NodeID:      p.NodeId,
			Addr:        p.Addr,
			Liveness:    cluster.LivenessState(p.Liveness),
			Ring:        cluster.RingState(p.RingState),
			Incarnation: p.Incarnation,
		}
	}
	return out
}

func ringDescToProto(rd *cluster.RingDescriptor) *pb.RingDescriptorProto {
	if rd == nil {
		return nil
	}
	members := make([]*pb.RingMemberProto, len(rd.Members))
	for i, rm := range rd.Members {
		members[i] = &pb.RingMemberProto{
			NodeId:    rm.NodeID,
			Addr:      rm.Addr,
			RingState: int32(rm.State),
		}
	}
	return &pb.RingDescriptorProto{
		Version: rd.Version,
		Members: members,
	}
}

func protoToRingDesc(p *pb.RingDescriptorProto) *cluster.RingDescriptor {
	if p == nil {
		return nil
	}
	members := make([]cluster.RingMember, len(p.Members))
	for i, rm := range p.Members {
		members[i] = cluster.RingMember{
			NodeID: rm.NodeId,
			Addr:   rm.Addr,
			State:  cluster.RingState(rm.RingState),
		}
	}
	return &cluster.RingDescriptor{
		Version: p.Version,
		Members: members,
	}
}

func hlcTimestampToProto(ts hlc.Timestamp) *pb.HLCTimestamp {
	return &pb.HLCTimestamp{
		WallTime: ts.WallTime,
		Logical:  ts.Logical,
		NodeId:   ts.NodeID,
	}
}

func protoToHLCTimestamp(p *pb.HLCTimestamp) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: p.WallTime,
		Logical:  p.Logical,
		NodeID:   p.NodeId,
	}
}
