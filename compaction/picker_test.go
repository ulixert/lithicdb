package compaction

import (
	"fmt"
	"testing"

	"github.com/ulixert/theseon/kv"
	"github.com/ulixert/theseon/sstable"
)

// makeHandle creates a TableHandle with a Reader containing the given
// key range and file size. Used for picker tests without real files.
func makeHandle(t *testing.T, dir string, id uint64, firstUserKey, lastUserKey string, seq uint64) *sstable.TableHandle {
	t.Helper()

	b := sstable.NewBuilder(dir, id, 4096)
	b.Add(kv.MakeInternalKey([]byte(firstUserKey), seq), kv.NewValue([]byte("v")))
	if firstUserKey != lastUserKey {
		b.Add(kv.MakeInternalKey([]byte(lastUserKey), seq-1), kv.NewValue([]byte("v")))
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("build SSTable %d: %v", id, err)
	}

	reader, err := sstable.OpenReader(dir, id)
	if err != nil {
		t.Fatalf("open SSTable %d: %v", id, err)
	}

	return sstable.NewTableHandle(reader, dir)
}

func TestPickCompaction_L0Trigger(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.L0CompactionTrigger = 4

	state := &LevelState{
		Levels: make([][]*sstable.TableHandle, cfg.MaxLevels),
	}

	// Add 4 L0 files — should trigger
	for i := uint64(1); i <= 4; i++ {
		state.L0 = append(state.L0, makeHandle(t, dir, i, "a", "z", i*10))
	}

	task := PickCompaction(state, cfg)
	if task == nil {
		t.Fatal("expected compaction task for 4 L0 files")
	}
	if task.InputLevel != 0 || task.OutputLevel != 1 {
		t.Errorf("levels = %d→%d, want 0→1", task.InputLevel, task.OutputLevel)
	}
	if len(task.Inputs) != 4 {
		t.Errorf("inputs = %d, want 4", len(task.Inputs))
	}
}

func TestPickCompaction_BelowTrigger(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.L0CompactionTrigger = 4

	state := &LevelState{
		Levels: make([][]*sstable.TableHandle, cfg.MaxLevels),
	}

	// Only 3 L0 files — below trigger
	for i := uint64(1); i <= 3; i++ {
		state.L0 = append(state.L0, makeHandle(t, dir, i, "a", "z", i*10))
	}

	task := PickCompaction(state, cfg)
	if task != nil {
		t.Error("expected no compaction for 3 L0 files (trigger=4)")
	}
}

func TestPickCompaction_L0WithOverlappingL1(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.L0CompactionTrigger = 2

	state := &LevelState{
		Levels: make([][]*sstable.TableHandle, cfg.MaxLevels),
	}

	// L0 files covering [a, m]
	state.L0 = append(state.L0,
		makeHandle(t, dir, 1, "a", "f", 20),
		makeHandle(t, dir, 2, "d", "m", 10),
	)

	// L1 files: [a, g] overlaps, [n, z] does not
	state.Levels[1] = []*sstable.TableHandle{
		makeHandle(t, dir, 3, "a", "g", 5),
		makeHandle(t, dir, 4, "n", "z", 3),
	}

	task := PickCompaction(state, cfg)
	if task == nil {
		t.Fatal("expected compaction task")
	}

	if len(task.Inputs) != 2 {
		t.Errorf("inputs = %d, want 2 (all L0)", len(task.Inputs))
	}

	// Only the [a, g] L1 file should be included (overlaps with [a, m])
	if len(task.Overlapping) != 1 {
		t.Fatalf("overlapping = %d, want 1", len(task.Overlapping))
	}
	if task.Overlapping[0].Reader.ID() != 3 {
		t.Errorf("overlapping ID = %d, want 3", task.Overlapping[0].Reader.ID())
	}
}

func TestPickCompaction_NoCompactionNeeded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.L0CompactionTrigger = 4

	state := &LevelState{
		Levels: make([][]*sstable.TableHandle, cfg.MaxLevels),
	}

	task := PickCompaction(state, cfg)
	if task != nil {
		t.Error("expected no compaction for empty state")
	}
}

func TestPickCompaction_AllInputs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.L0CompactionTrigger = 2

	state := &LevelState{
		Levels: make([][]*sstable.TableHandle, cfg.MaxLevels),
	}

	state.L0 = append(state.L0,
		makeHandle(t, dir, 1, "a", "z", 20),
		makeHandle(t, dir, 2, "a", "z", 10),
	)

	state.Levels[1] = []*sstable.TableHandle{
		makeHandle(t, dir, 3, "a", "m", 5),
		makeHandle(t, dir, 4, "n", "z", 3),
	}

	task := PickCompaction(state, cfg)
	if task == nil {
		t.Fatal("expected compaction task")
	}

	all := task.AllInputs()
	if len(all) != 4 {
		t.Errorf("AllInputs = %d, want 4", len(all))
	}

	// Verify all IDs present
	ids := make(map[uint64]bool)
	for _, h := range all {
		ids[h.Reader.ID()] = true
	}
	for _, id := range []uint64{1, 2, 3, 4} {
		if !ids[id] {
			t.Errorf("missing SSTable ID %d in AllInputs", id)
		}
	}
}

func TestFindOverlapping_NoOverlap(t *testing.T) {
	dir := t.TempDir()

	level := []*sstable.TableHandle{
		makeHandle(t, dir, 1, "a", "c", 10),
		makeHandle(t, dir, 2, "d", "f", 8),
	}

	// Query range [x, z] doesn't overlap any L1 file
	minKey := kv.MakeInternalKey([]byte("x"), 100)
	maxKey := kv.MakeInternalKey([]byte("z"), 1)

	result := findOverlapping(level, minKey, maxKey)
	if len(result) != 0 {
		t.Errorf("expected 0 overlapping, got %d", len(result))
	}
}

func TestFindOverlapping_PartialOverlap(t *testing.T) {
	dir := t.TempDir()

	level := []*sstable.TableHandle{
		makeHandle(t, dir, 1, "a", "d", 10),
		makeHandle(t, dir, 2, "e", "h", 8),
		makeHandle(t, dir, 3, "i", "z", 6),
	}

	// Query range [c, f] overlaps [a,d] and [e,h]
	minKey := kv.MakeInternalKey([]byte("c"), 100)
	maxKey := kv.MakeInternalKey([]byte("f"), 1)

	result := findOverlapping(level, minKey, maxKey)
	if len(result) != 2 {
		t.Fatalf("expected 2 overlapping, got %d", len(result))
	}

	ids := []uint64{result[0].Reader.ID(), result[1].Reader.ID()}
	if ids[0] != 1 || ids[1] != 2 {
		t.Errorf("overlapping IDs = %v, want [1, 2]", ids)
	}
}

func TestKeyRange(t *testing.T) {
	dir := t.TempDir()

	handles := []*sstable.TableHandle{
		makeHandle(t, dir, 1, "c", "f", 10),
		makeHandle(t, dir, 2, "a", "d", 8),
		makeHandle(t, dir, 3, "e", "z", 6),
	}

	minKey, maxKey := keyRange(handles)

	minUser := string(kv.UserKey(minKey))
	maxUser := string(kv.UserKey(maxKey))

	if minUser != "a" {
		t.Errorf("minKey user = %q, want %q", minUser, "a")
	}
	if maxUser != "z" {
		t.Errorf("maxKey user = %q, want %q", maxUser, "z")
	}
}

func TestExecute_Basic(t *testing.T) {
	dir := t.TempDir()

	// Create two L0 SSTables with overlapping keys
	h1 := makeHandle(t, dir, 1, "a", "c", 20)
	h2 := makeHandle(t, dir, 2, "b", "d", 10)

	task := &CompactionTask{
		InputLevel:  0,
		OutputLevel: 1,
		Inputs:      []*sstable.TableHandle{h1, h2},
		MinKey:      h1.Reader.FirstKey(),
		MaxKey:      h2.Reader.LastKey(),
	}

	seq := uint64(100)
	alloc := &testAllocator{next: seq}

	result, err := Execute(task, dir, 4096, 1024*1024, alloc, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.NewTables) == 0 {
		t.Fatal("expected at least one output SSTable")
	}

	// Verify all keys are readable from the output
	for _, key := range []string{"a", "b", "c", "d"} {
		found := false
		for _, reader := range result.NewTables {
			val, ok, err := reader.Get([]byte(key))
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			if ok {
				found = true
				if string(val.Data) != "v" {
					t.Errorf("Get(%q) = %q, want %q", key, val.Data, "v")
				}
				break
			}
		}
		if !found {
			t.Errorf("key %q not found in compaction output", key)
		}
	}
}

func TestExecute_ManyKeys(t *testing.T) {
	dir := t.TempDir()

	// Build 3 SSTables with overlapping ranges
	handles := make([]*sstable.TableHandle, 3)
	for i := 0; i < 3; i++ {
		id := uint64(i + 1)
		b := sstable.NewBuilder(dir, id, 4096)
		for j := 0; j < 50; j++ {
			key := fmt.Sprintf("key-%04d", i*30+j) // overlapping ranges
			b.Add(kv.MakeInternalKey([]byte(key), id*100+uint64(j)), kv.NewValue([]byte("v")))
		}
		b.Finish()
		reader, _ := sstable.OpenReader(dir, id)
		handles[i] = sstable.NewTableHandle(reader, dir)
	}

	minKey, maxKey := keyRange(handles)

	task := &CompactionTask{
		InputLevel:  0,
		OutputLevel: 1,
		Inputs:      handles,
		MinKey:      minKey,
		MaxKey:      maxKey,
	}

	result, err := Execute(task, dir, 4096, 1024*1024, &testAllocator{next: 1000}, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.NewTables) == 0 {
		t.Fatal("expected output SSTables")
	}

	t.Logf("compacted 3 inputs → %d outputs", len(result.NewTables))
}

// testAllocator provides sequential IDs for testing.
type testAllocator struct {
	next uint64
}

func (a *testAllocator) NextID() uint64 {
	a.next++
	return a.next
}
