package cluster

import (
	"context"
	"testing"

	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
)

// TestApplyRepair_PreservesEnvelope_Local exercises invariant #2 on the
// local path: a repair with source HLC + deleted flag must write an
// envelope carrying that exact (wall, logical, nodeID, deleted), never
// a newly-stamped clock.Now() value.
func TestApplyRepair_PreservesEnvelope_Local(t *testing.T) {
	coord, _, cleanup := setupCoordinator(t, DefaultCoordinatorConfig())
	defer cleanup()

	key := []byte("repair-local-key")
	value := []byte("repair-value")
	sourceTS := hlc.Timestamp{
		WallTime: 1_234_567_890,
		Logical:  42,
		NodeID:   "source-node-xyz",
	}

	ctx, cancel := context.WithTimeout(context.Background(), coord.cfg.PerReplicaTimeout)
	defer cancel()

	// Local-path repair: targetID == selfID writes directly to localDB.
	if err := coord.ApplyRepair(ctx, coord.selfID, "ignored-addr", key, value, sourceTS, false); err != nil {
		t.Fatalf("ApplyRepair: %v", err)
	}

	raw, found := coord.localDB.Get(key)
	if !found {
		t.Fatal("local repair did not write the key")
	}
	env, err := DecodeEnvelope(raw.Data)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Timestamp.WallTime != sourceTS.WallTime ||
		env.Timestamp.Logical != sourceTS.Logical ||
		env.Timestamp.NodeID != sourceTS.NodeID {
		t.Errorf("timestamp not preserved: got %+v want %+v", env.Timestamp, sourceTS)
	}
	if env.Deleted {
		t.Error("deleted bit flipped unexpectedly")
	}
	if string(env.Value) != string(value) {
		t.Errorf("value: got %q want %q", env.Value, value)
	}
}

// TestApplyRepair_PreservesTombstone_Local ensures a tombstone repair
// (Deleted=true) encodes an envelope with the deleted bit set and the
// exact source HLC, not a fresh clock.Now().
func TestApplyRepair_PreservesTombstone_Local(t *testing.T) {
	coord, _, cleanup := setupCoordinator(t, DefaultCoordinatorConfig())
	defer cleanup()

	key := []byte("repair-tombstone-key")
	sourceTS := hlc.Timestamp{
		WallTime: 9_999_999_999,
		Logical:  7,
		NodeID:   "tombstone-origin",
	}

	ctx, cancel := context.WithTimeout(context.Background(), coord.cfg.PerReplicaTimeout)
	defer cancel()

	if err := coord.ApplyRepair(ctx, coord.selfID, "", key, nil, sourceTS, true); err != nil {
		t.Fatalf("ApplyRepair tombstone: %v", err)
	}

	raw, found := coord.localDB.Get(key)
	if !found {
		t.Fatal("tombstone repair not written")
	}
	env, err := DecodeEnvelope(raw.Data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !env.Deleted {
		t.Error("deleted bit not set")
	}
	if env.Timestamp.WallTime != sourceTS.WallTime ||
		env.Timestamp.Logical != sourceTS.Logical ||
		env.Timestamp.NodeID != sourceTS.NodeID {
		t.Errorf("timestamp not preserved on tombstone: got %+v want %+v", env.Timestamp, sourceTS)
	}
}

// TestApplyRepair_PreservesEnvelope_Remote verifies the remote path
// sends the source HLC fields verbatim on the wire via ReplicateWrite.
// The mock client captures the request and the test asserts field
// equality — no re-stamping through the coordinator's clock.
func TestApplyRepair_PreservesEnvelope_Remote(t *testing.T) {
	coord, dialer, cleanup := setupCoordinator(t, DefaultCoordinatorConfig())
	defer cleanup()

	key := []byte("repair-remote-key")
	value := []byte("remote-payload")
	sourceTS := hlc.Timestamp{
		WallTime: 555_555_555,
		Logical:  99,
		NodeID:   "remote-origin",
	}

	var captured *pb.ReplicateWriteRequest
	dialer.setClient("127.0.0.1:9002", &mockClient{
		writeFn: func(_ context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
			captured = req
			return &pb.ReplicateWriteResponse{}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), coord.cfg.PerReplicaTimeout)
	defer cancel()

	if err := coord.ApplyRepair(ctx, "node-2", "127.0.0.1:9002", key, value, sourceTS, false); err != nil {
		t.Fatalf("ApplyRepair remote: %v", err)
	}

	if captured == nil {
		t.Fatal("remote write was not sent")
	}
	if captured.Timestamp == nil {
		t.Fatal("ReplicateWrite missing timestamp")
	}
	if captured.Timestamp.WallTime != sourceTS.WallTime ||
		captured.Timestamp.Logical != sourceTS.Logical ||
		captured.Timestamp.NodeId != sourceTS.NodeID {
		t.Errorf("wire timestamp not preserved: got wall=%d logical=%d node=%q; want wall=%d logical=%d node=%q",
			captured.Timestamp.WallTime, captured.Timestamp.Logical, captured.Timestamp.NodeId,
			sourceTS.WallTime, sourceTS.Logical, sourceTS.NodeID)
	}
	if captured.Deleted {
		t.Error("deleted bit flipped")
	}
	if string(captured.Value) != string(value) {
		t.Errorf("value: got %q want %q", captured.Value, value)
	}
}
