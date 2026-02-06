package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// writeVolumeManifest writes a VolumeManifest directly to the store for
// validation testing. Mirrors writeManifest in reader_test.go.
func writeVolumeManifest(ctx context.Context, t *testing.T, store Store, m *VolumeManifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := volumeManifestPath(m.VolumeID, m.SnapshotID)
	if err := store.Put(ctx, p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// validVolumeManifest returns a minimal valid VolumeManifest for mutation in tests.
func validVolumeManifest(volumeID VolumeID, totalLength int64) *VolumeManifest {
	return &VolumeManifest{
		SchemaName:    volumeManifestSchemaName,
		FormatVersion: volumeManifestFormatVersion,
		VolumeID:      volumeID,
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		TotalLength:   totalLength,
		Blocks: []BlockRef{
			{Offset: 0, Length: 10, Path: volumeBlockPath(volumeID, 0, 10)},
		},
	}
}

// newTestVolume is a convenience helper that creates a Volume and t.Fatal's on error.
func newTestVolume(t *testing.T, id VolumeID, factory StoreFactory, totalLength int64, opts ...VolumeOption) Volume {
	t.Helper()
	vol, err := NewVolume(id, factory, totalLength, opts...)
	if err != nil {
		t.Fatalf("NewVolume failed: %v", err)
	}
	return vol
}

// stageBlock is a convenience helper that stages data and t.Fatal's on error.
func stageBlock(t *testing.T, vol Volume, offset int64, data []byte) BlockRef {
	t.Helper()
	blk, err := vol.StageWriteAt(t.Context(), offset, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("StageWriteAt(offset=%d) failed: %v", offset, err)
	}
	return blk
}

// commitBlocks is a convenience helper that commits blocks and t.Fatal's on error.
func commitBlocks(t *testing.T, vol Volume, blocks []BlockRef, meta Metadata) *VolumeSnapshot {
	t.Helper()
	snap, err := vol.Commit(t.Context(), blocks, meta)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	return snap
}

// =============================================================================
// Existing smoke tests (17 tests — kept as-is)
// =============================================================================

func TestNewVolume_Success(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vol == nil {
		t.Fatal("expected non-nil volume")
	}
	if vol.ID() != "test-vol" {
		t.Errorf("expected ID 'test-vol', got %q", vol.ID())
	}
}

func TestNewVolume_NilFactory_ReturnsError(t *testing.T) {
	_, err := NewVolume("test-vol", nil, 1024)
	if err == nil {
		t.Fatal("expected error for nil factory")
	}
}

func TestNewVolume_ZeroTotalLength_ReturnsError(t *testing.T) {
	_, err := NewVolume("test-vol", NewMemoryFactory(), 0)
	if err == nil {
		t.Fatal("expected error for zero total length")
	}
}

func TestNewVolume_NilStore_ReturnsError(t *testing.T) {
	factory := func() (Store, error) { return nil, nil } //nolint:nilnil // intentionally testing nil store guard
	_, err := NewVolume("test-vol", factory, 1024)
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestNewVolume_EmptyID_ReturnsError(t *testing.T) {
	_, err := NewVolume("", NewMemoryFactory(), 1024)
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
}

func TestVolume_StageCommitReadAt_EndToEnd(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()
	data := []byte("hello world") // 11 bytes

	// Stage a block at offset 0.
	blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	if blk.Offset != 0 {
		t.Errorf("expected offset 0, got %d", blk.Offset)
	}
	if blk.Length != int64(len(data)) {
		t.Errorf("expected length %d, got %d", len(data), blk.Length)
	}

	// Commit.
	snap, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{"source": "test"})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if snap.ID == "" {
		t.Fatal("expected non-empty snapshot ID")
	}
	if len(snap.Manifest.Blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(snap.Manifest.Blocks))
	}

	// ReadAt.
	got, err := vol.ReadAt(ctx, snap.ID, 0, int64(len(data)))
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("expected %q, got %q", data, got)
	}
}

func TestVolume_CumulativeManifest(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()

	// First snapshot: block at [0, 50).
	data1 := bytes.Repeat([]byte("A"), 50)
	blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data1))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	snap1, err := vol.Commit(ctx, []BlockRef{blk1}, Metadata{})
	if err != nil {
		t.Fatalf("Commit 1 failed: %v", err)
	}
	if len(snap1.Manifest.Blocks) != 1 {
		t.Fatalf("expected 1 block in snap1, got %d", len(snap1.Manifest.Blocks))
	}

	// Second snapshot: block at [50, 100).
	data2 := bytes.Repeat([]byte("B"), 50)
	blk2, err := vol.StageWriteAt(ctx, 50, bytes.NewReader(data2))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	snap2, err := vol.Commit(ctx, []BlockRef{blk2}, Metadata{})
	if err != nil {
		t.Fatalf("Commit 2 failed: %v", err)
	}

	// Second snapshot manifest must contain BOTH blocks (cumulative).
	if len(snap2.Manifest.Blocks) != 2 {
		t.Fatalf("expected 2 blocks in snap2 (cumulative), got %d", len(snap2.Manifest.Blocks))
	}

	// Parent should reference snap1.
	if snap2.Manifest.ParentSnapshotID != snap1.ID {
		t.Errorf("expected parent %s, got %s", snap1.ID, snap2.Manifest.ParentSnapshotID)
	}
}

func TestVolume_ReadAt_SpanningBlocks(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()

	// Stage two adjacent blocks.
	data1 := []byte("AAAAAAAAAA") // 10 bytes at offset 0
	data2 := []byte("BBBBBBBBBB") // 10 bytes at offset 10

	blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data1))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	blk2, err := vol.StageWriteAt(ctx, 10, bytes.NewReader(data2))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}

	snap, err := vol.Commit(ctx, []BlockRef{blk1, blk2}, Metadata{})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read spanning both blocks: [5, 15).
	got, err := vol.ReadAt(ctx, snap.ID, 5, 10)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	expected := append([]byte("AAAAA"), []byte("BBBBB")...)
	if !bytes.Equal(got, expected) {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestVolume_ReadAt_MissingRange_ReturnsErrRangeMissing(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()

	// Stage and commit a block at [0, 10).
	blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("0123456789")))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	snap, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Try to read [5, 15) — partially uncommitted.
	_, err = vol.ReadAt(ctx, snap.ID, 5, 10)
	if !errors.Is(err, ErrRangeMissing) {
		t.Errorf("expected ErrRangeMissing, got: %v", err)
	}
}

func TestVolume_Commit_NilMetadata_ReturnsError(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()
	blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}

	_, err = vol.Commit(ctx, []BlockRef{blk}, nil)
	if err == nil {
		t.Fatal("expected error for nil metadata")
	}
}

func TestVolume_Commit_EmptyBlockPath_ReturnsError(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A BlockRef with an empty path should be rejected.
	_, err = vol.Commit(t.Context(), []BlockRef{{Offset: 0, Length: 10, Path: ""}}, Metadata{})
	if err == nil {
		t.Fatal("expected error for empty block path")
	}
}

func TestVolume_Commit_EmptyBlocks_ReturnsError(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = vol.Commit(t.Context(), []BlockRef{}, Metadata{})
	if err == nil {
		t.Fatal("expected error for empty blocks")
	}
}

func TestVolume_Commit_OverlappingBlocks_ReturnsErrOverlappingBlocks(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := t.Context()

	// Stage two overlapping blocks: [0, 10) and [5, 15).
	blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(bytes.Repeat([]byte("A"), 10)))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}
	blk2, err := vol.StageWriteAt(ctx, 5, bytes.NewReader(bytes.Repeat([]byte("B"), 10)))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}

	_, err = vol.Commit(ctx, []BlockRef{blk1, blk2}, Metadata{})
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Errorf("expected ErrOverlappingBlocks, got: %v", err)
	}
}

func TestVolume_Latest_EmptyVolume_ReturnsErrNoSnapshots(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = vol.Latest(t.Context())
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots, got: %v", err)
	}
}

func TestVolume_Snapshot_NotFound_ReturnsErrNotFound(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = vol.Snapshot(t.Context(), "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestVolume_StageWriteAt_WithChecksum(t *testing.T) {
	vol, err := NewVolume("test-vol", NewMemoryFactory(), 100,
		WithVolumeChecksum(NewMD5Checksum()),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blk, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("checksum-test")))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}

	if blk.Checksum == "" {
		t.Error("expected non-empty checksum when WithVolumeChecksum configured")
	}
}

func TestVolume_ID_ReturnsVolumeID(t *testing.T) {
	vol, err := NewVolume("my-volume", NewMemoryFactory(), 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vol.ID() != "my-volume" {
		t.Errorf("expected 'my-volume', got %q", vol.ID())
	}
}

// =============================================================================
// A. StageWriteAt edge cases
// =============================================================================

func TestVolume_StageWriteAt_NegativeOffset_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	_, err := vol.StageWriteAt(t.Context(), -1, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
}

func TestVolume_StageWriteAt_ExceedsBounds_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// 10 bytes at offset 95 → [95, 105) exceeds totalLength=100.
	_, err := vol.StageWriteAt(t.Context(), 95, bytes.NewReader(bytes.Repeat([]byte("X"), 10)))
	if err == nil {
		t.Fatal("expected error for block exceeding volume bounds")
	}
}

func TestVolume_StageWriteAt_EmptyReader_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	_, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}

func TestVolume_StageWriteAt_ExactFit(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 10)
	// 10 bytes at offset 0 → [0, 10) exactly fills totalLength=10.
	blk, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader(bytes.Repeat([]byte("X"), 10)))
	if err != nil {
		t.Fatalf("expected success for exact fit, got: %v", err)
	}
	if blk.Offset != 0 || blk.Length != 10 {
		t.Errorf("expected offset=0 length=10, got offset=%d length=%d", blk.Offset, blk.Length)
	}
}

func TestVolume_StageWriteAt_PathLayout(t *testing.T) {
	vol := newTestVolume(t, "my-vol", NewMemoryFactory(), 100)
	blk, err := vol.StageWriteAt(t.Context(), 5, bytes.NewReader(bytes.Repeat([]byte("X"), 20)))
	if err != nil {
		t.Fatalf("StageWriteAt failed: %v", err)
	}

	expected := "volumes/my-vol/data/5-20.bin"
	if blk.Path != expected {
		t.Errorf("expected path %q, got %q", expected, blk.Path)
	}
}

// =============================================================================
// B. Commit validation
// =============================================================================

func TestVolume_Commit_DuplicateBlocks_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	ctx := t.Context()

	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	// Re-commit the same block (no new blocks) → error.
	_, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{})
	if err == nil {
		t.Fatal("expected error when committing only duplicate blocks")
	}
}

func TestVolume_Commit_BlockExceedsBounds_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// Construct a BlockRef that exceeds bounds.
	bad := BlockRef{Offset: 90, Length: 20, Path: volumeBlockPath("test-vol", 90, 20)}
	_, err := vol.Commit(t.Context(), []BlockRef{bad}, Metadata{})
	if err == nil {
		t.Fatal("expected error for block exceeding volume bounds")
	}
}

func TestVolume_Commit_NegativeBlockOffset_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	bad := BlockRef{Offset: -1, Length: 10, Path: volumeBlockPath("test-vol", -1, 10)}
	_, err := vol.Commit(t.Context(), []BlockRef{bad}, Metadata{})
	if err == nil {
		t.Fatal("expected error for negative block offset")
	}
}

func TestVolume_Commit_ZeroBlockLength_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	bad := BlockRef{Offset: 0, Length: 0, Path: volumeBlockPath("test-vol", 0, 0)}
	_, err := vol.Commit(t.Context(), []BlockRef{bad}, Metadata{})
	if err == nil {
		t.Fatal("expected error for zero block length")
	}
}

func TestVolume_Commit_PathMismatch_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// Stage data at [0, 10) but give it a wrong path.
	bad := BlockRef{Offset: 0, Length: 10, Path: "volumes/other-vol/data/0-10.bin"}
	_, err := vol.Commit(t.Context(), []BlockRef{bad}, Metadata{})
	if err == nil {
		t.Fatal("expected error for path mismatch")
	}
}

func TestVolume_Commit_EmptyMetadata_Succeeds(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap, err := vol.Commit(t.Context(), []BlockRef{blk}, Metadata{})
	if err != nil {
		t.Fatalf("expected success with empty Metadata{}, got: %v", err)
	}
	if snap.ID == "" {
		t.Fatal("expected non-empty snapshot ID")
	}
}

// =============================================================================
// C. Multi-snapshot progression
// =============================================================================

func TestVolume_ThreeSnapshots_CumulativeProgression(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 90)
	ctx := t.Context()

	// Snap 1: [0, 30)
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 30))
	snap1 := commitBlocks(t, vol, []BlockRef{blk1}, Metadata{})
	if len(snap1.Manifest.Blocks) != 1 {
		t.Fatalf("snap1: expected 1 block, got %d", len(snap1.Manifest.Blocks))
	}
	if snap1.Manifest.ParentSnapshotID != "" {
		t.Errorf("snap1: expected no parent, got %q", snap1.Manifest.ParentSnapshotID)
	}

	// Snap 2: [30, 60)
	blk2 := stageBlock(t, vol, 30, bytes.Repeat([]byte("B"), 30))
	snap2 := commitBlocks(t, vol, []BlockRef{blk2}, Metadata{})
	if len(snap2.Manifest.Blocks) != 2 {
		t.Fatalf("snap2: expected 2 cumulative blocks, got %d", len(snap2.Manifest.Blocks))
	}
	if snap2.Manifest.ParentSnapshotID != snap1.ID {
		t.Errorf("snap2: expected parent %s, got %s", snap1.ID, snap2.Manifest.ParentSnapshotID)
	}

	// Snap 3: [60, 90)
	blk3 := stageBlock(t, vol, 60, bytes.Repeat([]byte("C"), 30))
	snap3 := commitBlocks(t, vol, []BlockRef{blk3}, Metadata{})
	if len(snap3.Manifest.Blocks) != 3 {
		t.Fatalf("snap3: expected 3 cumulative blocks, got %d", len(snap3.Manifest.Blocks))
	}
	if snap3.Manifest.ParentSnapshotID != snap2.ID {
		t.Errorf("snap3: expected parent %s, got %s", snap2.ID, snap3.Manifest.ParentSnapshotID)
	}

	// Read full volume from snap3.
	got, err := vol.ReadAt(ctx, snap3.ID, 0, 90)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	expected := append(bytes.Repeat([]byte("A"), 30), bytes.Repeat([]byte("B"), 30)...)
	expected = append(expected, bytes.Repeat([]byte("C"), 30)...)
	if !bytes.Equal(got, expected) {
		t.Error("full volume read did not match expected data after 3 snapshots")
	}
}

func TestVolume_SparseBlocks_WithGaps(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	ctx := t.Context()

	// Stage non-contiguous blocks: [0, 10) and [50, 60).
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	blk2 := stageBlock(t, vol, 50, bytes.Repeat([]byte("B"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, Metadata{})

	if len(snap.Manifest.Blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(snap.Manifest.Blocks))
	}

	// Read committed ranges — both should succeed.
	got1, err := vol.ReadAt(ctx, snap.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadAt [0,10) failed: %v", err)
	}
	if !bytes.Equal(got1, bytes.Repeat([]byte("A"), 10)) {
		t.Error("block [0,10) data mismatch")
	}

	got2, err := vol.ReadAt(ctx, snap.ID, 50, 10)
	if err != nil {
		t.Fatalf("ReadAt [50,60) failed: %v", err)
	}
	if !bytes.Equal(got2, bytes.Repeat([]byte("B"), 10)) {
		t.Error("block [50,60) data mismatch")
	}

	// Gap [10, 50) should return ErrRangeMissing.
	_, err = vol.ReadAt(ctx, snap.ID, 10, 40)
	if !errors.Is(err, ErrRangeMissing) {
		t.Errorf("expected ErrRangeMissing for gap, got: %v", err)
	}
}

func TestVolume_DensePacking_Adjacent(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	ctx := t.Context()

	// Pack the full volume: [0,25) [25,50) [50,75) [75,100).
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 25))
	blk2 := stageBlock(t, vol, 25, bytes.Repeat([]byte("B"), 25))
	blk3 := stageBlock(t, vol, 50, bytes.Repeat([]byte("C"), 25))
	blk4 := stageBlock(t, vol, 75, bytes.Repeat([]byte("D"), 25))
	snap := commitBlocks(t, vol, []BlockRef{blk1, blk2, blk3, blk4}, Metadata{})

	if len(snap.Manifest.Blocks) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(snap.Manifest.Blocks))
	}

	// Full volume read.
	got, err := vol.ReadAt(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatalf("ReadAt full failed: %v", err)
	}
	expected := bytes.Repeat([]byte("A"), 25)
	expected = append(expected, bytes.Repeat([]byte("B"), 25)...)
	expected = append(expected, bytes.Repeat([]byte("C"), 25)...)
	expected = append(expected, bytes.Repeat([]byte("D"), 25)...)
	if !bytes.Equal(got, expected) {
		t.Error("dense packing full read data mismatch")
	}
}

func TestVolume_Snapshots_SortedByCreatedAt(t *testing.T) {
	store := NewMemory()
	factory := NewMemoryFactoryFrom(store)
	ctx := t.Context()

	// Write manifests with explicit, well-separated timestamps to avoid
	// sort.Slice non-determinism when timestamps are equal.
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, snapID := range []VolumeSnapshotID{"snap-c", "snap-a", "snap-b"} {
		m := &VolumeManifest{
			SchemaName:    volumeManifestSchemaName,
			FormatVersion: volumeManifestFormatVersion,
			VolumeID:      "test-vol",
			SnapshotID:    snapID,
			CreatedAt:     t0.Add(time.Duration(i) * time.Hour),
			Metadata:      Metadata{},
			TotalLength:   100,
			Blocks:        []BlockRef{},
		}
		writeVolumeManifest(ctx, t, store, m)
	}

	vol := newTestVolume(t, "test-vol", factory, 100)
	snaps, err := vol.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snaps))
	}

	// Verify non-decreasing CreatedAt order (the actual sorting invariant).
	for i := 1; i < len(snaps); i++ {
		if snaps[i].Manifest.CreatedAt.Before(snaps[i-1].Manifest.CreatedAt) {
			t.Errorf("snapshot[%d] (%s) has earlier timestamp than snapshot[%d] (%s)",
				i, snaps[i].Manifest.CreatedAt, i-1, snaps[i-1].Manifest.CreatedAt)
		}
	}

	// With distinct timestamps, order should be: snap-c (t0), snap-a (t0+1h), snap-b (t0+2h).
	if snaps[0].ID != "snap-c" || snaps[1].ID != "snap-a" || snaps[2].ID != "snap-b" {
		t.Errorf("expected order [snap-c, snap-a, snap-b], got [%s, %s, %s]",
			snaps[0].ID, snaps[1].ID, snaps[2].ID)
	}
}

// =============================================================================
// D. Resume pattern
// =============================================================================

func TestVolume_Resume_NewInstance_LoadsLatest(t *testing.T) {
	store := NewMemory()
	factory := NewMemoryFactoryFrom(store)

	// Instance 1: create a snapshot.
	vol1 := newTestVolume(t, "test-vol", factory, 100)
	blk := stageBlock(t, vol1, 0, bytes.Repeat([]byte("A"), 10))
	snap1 := commitBlocks(t, vol1, []BlockRef{blk}, Metadata{"step": "first"})

	// Instance 2: new Volume on same store should see the snapshot.
	vol2 := newTestVolume(t, "test-vol", factory, 100)
	latest, err := vol2.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest on new instance failed: %v", err)
	}
	if latest.ID != snap1.ID {
		t.Errorf("expected snapshot %s, got %s", snap1.ID, latest.ID)
	}
}

func TestVolume_Resume_ContinueStaging(t *testing.T) {
	store := NewMemory()
	factory := NewMemoryFactoryFrom(store)

	// Instance 1: commit block [0, 10).
	vol1 := newTestVolume(t, "test-vol", factory, 100)
	blk1 := stageBlock(t, vol1, 0, bytes.Repeat([]byte("A"), 10))
	commitBlocks(t, vol1, []BlockRef{blk1}, Metadata{})

	// Instance 2: resume, stage [10, 20), commit → cumulative manifest has 2 blocks.
	vol2 := newTestVolume(t, "test-vol", factory, 100)
	blk2 := stageBlock(t, vol2, 10, bytes.Repeat([]byte("B"), 10))
	snap2 := commitBlocks(t, vol2, []BlockRef{blk2}, Metadata{})

	if len(snap2.Manifest.Blocks) != 2 {
		t.Fatalf("expected 2 cumulative blocks after resume, got %d", len(snap2.Manifest.Blocks))
	}

	// Verify both blocks readable from snap2.
	got, err := vol2.ReadAt(t.Context(), snap2.ID, 0, 20)
	if err != nil {
		t.Fatalf("ReadAt after resume failed: %v", err)
	}
	expected := append(bytes.Repeat([]byte("A"), 10), bytes.Repeat([]byte("B"), 10)...)
	if !bytes.Equal(got, expected) {
		t.Error("data mismatch after resume")
	}
}

// =============================================================================
// E. ReadAt comprehensive
// =============================================================================

func TestVolume_ReadAt_ExactBlockBoundary(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	data := []byte("0123456789")
	blk := stageBlock(t, vol, 0, data)
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	got, err := vol.ReadAt(t.Context(), snap.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadAt exact boundary failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("expected %q, got %q", data, got)
	}
}

func TestVolume_ReadAt_WithinSingleBlock(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	data := []byte("0123456789")
	blk := stageBlock(t, vol, 0, data)
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	// Read [2, 7) within [0, 10).
	got, err := vol.ReadAt(t.Context(), snap.ID, 2, 5)
	if err != nil {
		t.Fatalf("ReadAt within single block failed: %v", err)
	}
	if !bytes.Equal(got, []byte("23456")) {
		t.Errorf("expected %q, got %q", "23456", got)
	}
}

func TestVolume_ReadAt_FullVolume(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 20)
	data1 := bytes.Repeat([]byte("A"), 10)
	data2 := bytes.Repeat([]byte("B"), 10)
	blk1 := stageBlock(t, vol, 0, data1)
	blk2 := stageBlock(t, vol, 10, data2)
	snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, Metadata{})

	got, err := vol.ReadAt(t.Context(), snap.ID, 0, 20)
	if err != nil {
		t.Fatalf("ReadAt full volume failed: %v", err)
	}
	expected := make([]byte, 0, 20)
	expected = append(expected, data1...)
	expected = append(expected, data2...)
	if !bytes.Equal(got, expected) {
		t.Error("full volume read data mismatch")
	}
}

func TestVolume_ReadAt_GapAtStart_ReturnsErrRangeMissing(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// Block at [10, 20), gap at [0, 10).
	blk := stageBlock(t, vol, 10, bytes.Repeat([]byte("A"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	_, err := vol.ReadAt(t.Context(), snap.ID, 0, 5)
	if !errors.Is(err, ErrRangeMissing) {
		t.Errorf("expected ErrRangeMissing for gap at start, got: %v", err)
	}
}

func TestVolume_ReadAt_GapInMiddle_ReturnsErrRangeMissing(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// Blocks at [0, 5) and [10, 15), gap at [5, 10).
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))
	blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 5))
	snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, Metadata{})

	_, err := vol.ReadAt(t.Context(), snap.ID, 0, 15)
	if !errors.Is(err, ErrRangeMissing) {
		t.Errorf("expected ErrRangeMissing for gap in middle, got: %v", err)
	}
}

func TestVolume_ReadAt_NegativeOffset_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	_, err := vol.ReadAt(t.Context(), snap.ID, -1, 5)
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
}

func TestVolume_ReadAt_ZeroLength_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	_, err := vol.ReadAt(t.Context(), snap.ID, 0, 0)
	if err == nil {
		t.Fatal("expected error for zero length")
	}
}

// =============================================================================
// F. Overlap validation deep dive
// =============================================================================

func TestVolume_Commit_AdjacentBlocks_Valid(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// [0, 10) and [10, 20) are adjacent, not overlapping.
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))

	snap, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, Metadata{})
	if err != nil {
		t.Fatalf("expected adjacent blocks to succeed, got: %v", err)
	}
	if len(snap.Manifest.Blocks) != 2 {
		t.Errorf("expected 2 blocks, got %d", len(snap.Manifest.Blocks))
	}
}

func TestVolume_Commit_ContainedBlock_Overlap(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// [0, 20) contains [5, 10).
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 20))
	blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 5))

	_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, Metadata{})
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Errorf("expected ErrOverlappingBlocks for contained block, got: %v", err)
	}
}

func TestVolume_Commit_SameStartOffset_Overlap(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// [5, 10) and [5, 15) share the same start offset.
	blk1 := stageBlock(t, vol, 5, bytes.Repeat([]byte("A"), 5))
	blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))

	_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, Metadata{})
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Errorf("expected ErrOverlappingBlocks for same-start-offset blocks, got: %v", err)
	}
}

func TestVolume_Commit_OverlapWithExisting_Rejected(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)

	// Snap 1: commit [0, 10).
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	commitBlocks(t, vol, []BlockRef{blk1}, Metadata{})

	// Snap 2: try to commit [5, 15) which overlaps with existing [0, 10).
	blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))
	_, err := vol.Commit(t.Context(), []BlockRef{blk2}, Metadata{})
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Errorf("expected ErrOverlappingBlocks for overlap with existing, got: %v", err)
	}
}

func TestVolume_Commit_ThreeBlockOverlap(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	// [0, 10) + [5, 15) + [12, 20) — multiple overlaps.
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))
	blk3 := stageBlock(t, vol, 12, bytes.Repeat([]byte("C"), 8))

	_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2, blk3}, Metadata{})
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Errorf("expected ErrOverlappingBlocks for three-block overlap, got: %v", err)
	}
}

// =============================================================================
// G. Manifest validation on load
// =============================================================================

func TestVolume_Snapshot_InvalidManifest_MissingSchemaName(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	m.SchemaName = ""
	writeVolumeManifest(t.Context(), t, store, m)

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_MissingVolumeID(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	snapID := m.SnapshotID
	m.VolumeID = ""
	// Write to the correct path (using "test-vol" for lookup, not the empty VolumeID).
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := volumeManifestPath("test-vol", snapID)
	if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err = vol.Snapshot(t.Context(), snapID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_MissingSnapshotID(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	snapID := m.SnapshotID
	m.SnapshotID = ""
	// We must write at the correct path (using the original snapID for path lookup).
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := volumeManifestPath("test-vol", snapID)
	if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err = vol.Snapshot(t.Context(), snapID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_ZeroCreatedAt(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	m.CreatedAt = time.Time{}
	writeVolumeManifest(t.Context(), t, store, m)

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_NilMetadata(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	m.Metadata = nil
	writeVolumeManifest(t.Context(), t, store, m)

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_NilBlocks(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	m.Blocks = nil
	writeVolumeManifest(t.Context(), t, store, m)

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestVolume_Snapshot_InvalidManifest_NegativeTotalLength(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 100)
	m.TotalLength = -1
	m.Blocks = []BlockRef{} // avoid block bounds failure
	writeVolumeManifest(t.Context(), t, store, m)

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

// =============================================================================
// H. Cross-validation on load
// =============================================================================

func TestVolume_Snapshot_VolumeIDMismatch_ReturnsError(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("other-vol", 100)
	// Write under "test-vol" path but with VolumeID="other-vol".
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := volumeManifestPath("test-vol", m.SnapshotID)
	if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err = vol.Snapshot(t.Context(), m.SnapshotID)
	if err == nil {
		t.Fatal("expected error for volume ID mismatch")
	}
}

func TestVolume_Snapshot_TotalLengthMismatch_ReturnsError(t *testing.T) {
	store := NewMemory()
	m := validVolumeManifest("test-vol", 200) // manifest says 200
	writeVolumeManifest(t.Context(), t, store, m)

	// Volume created with totalLength=100, but manifest says 200.
	vol := newTestVolume(t, "test-vol", NewMemoryFactoryFrom(store), 100)
	_, err := vol.Snapshot(t.Context(), m.SnapshotID)
	if err == nil {
		t.Fatal("expected error for total length mismatch")
	}
}

// =============================================================================
// I. Checksum
// =============================================================================

func TestVolume_Commit_WithChecksum_AlgorithmPersisted(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100,
		WithVolumeChecksum(NewMD5Checksum()),
	)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	if snap.Manifest.ChecksumAlgorithm != "md5" {
		t.Errorf("expected checksum algorithm 'md5', got %q", snap.Manifest.ChecksumAlgorithm)
	}
	if blk.Checksum == "" {
		t.Error("expected non-empty block checksum")
	}
}

func TestVolume_Commit_WithoutChecksum_NoAlgorithm(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	if snap.Manifest.ChecksumAlgorithm != "" {
		t.Errorf("expected empty checksum algorithm, got %q", snap.Manifest.ChecksumAlgorithm)
	}
	if blk.Checksum != "" {
		t.Errorf("expected empty block checksum, got %q", blk.Checksum)
	}
}

// =============================================================================
// J. Manifest round-trip
// =============================================================================

func TestVolume_ManifestRoundTrip_AllFieldsPreserved(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 100,
		WithVolumeChecksum(NewMD5Checksum()),
	)
	ctx := t.Context()

	// First snap for parent chain.
	blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
	snap1 := commitBlocks(t, vol, []BlockRef{blk1}, Metadata{"key": "value"})

	// Second snap.
	blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))
	snap2 := commitBlocks(t, vol, []BlockRef{blk2}, Metadata{"step": "two"})

	// Load snap2 from store and verify all fields.
	loaded, err := vol.Snapshot(ctx, snap2.ID)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	m := loaded.Manifest
	if m.SchemaName != volumeManifestSchemaName {
		t.Errorf("schema_name: expected %q, got %q", volumeManifestSchemaName, m.SchemaName)
	}
	if m.FormatVersion != volumeManifestFormatVersion {
		t.Errorf("format_version: expected %q, got %q", volumeManifestFormatVersion, m.FormatVersion)
	}
	if m.VolumeID != "test-vol" {
		t.Errorf("volume_id: expected %q, got %q", "test-vol", m.VolumeID)
	}
	if m.SnapshotID != snap2.ID {
		t.Errorf("snapshot_id: expected %q, got %q", snap2.ID, m.SnapshotID)
	}
	if m.CreatedAt.IsZero() {
		t.Error("created_at: expected non-zero")
	}
	if m.TotalLength != 100 {
		t.Errorf("total_length: expected 100, got %d", m.TotalLength)
	}
	if len(m.Blocks) != 2 {
		t.Fatalf("blocks: expected 2 (cumulative), got %d", len(m.Blocks))
	}
	if m.ParentSnapshotID != snap1.ID {
		t.Errorf("parent_snapshot_id: expected %q, got %q", snap1.ID, m.ParentSnapshotID)
	}
	if m.ChecksumAlgorithm != "md5" {
		t.Errorf("checksum_algorithm: expected %q, got %q", "md5", m.ChecksumAlgorithm)
	}
	if m.Metadata["step"] != "two" {
		t.Errorf("metadata[step]: expected 'two', got %v", m.Metadata["step"])
	}
}

// =============================================================================
// K. FS store
// =============================================================================

func TestVolume_FSStore_StageCommitReadAt(t *testing.T) {
	tmpDir := t.TempDir()
	vol := newTestVolume(t, "test-vol", NewFSFactory(tmpDir), 100)
	ctx := t.Context()

	data := []byte("filesystem-backed-volume-data")
	blk := stageBlock(t, vol, 0, data)
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{"store": "fs"})

	got, err := vol.ReadAt(ctx, snap.ID, 0, int64(len(data)))
	if err != nil {
		t.Fatalf("ReadAt on FS store failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("expected %q, got %q", data, got)
	}
}

// =============================================================================
// L. Fault injection
// =============================================================================

func TestVolume_Commit_ManifestPutError_NoSnapshot(t *testing.T) {
	fs := newFaultStore(NewMemory())
	vol := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)
	ctx := t.Context()

	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))

	// Inject error on manifest write.
	fs.SetPutError(errInjectedPut, "manifest.json")

	_, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{})
	if err == nil {
		t.Fatal("expected error when manifest write fails")
	}

	// Clear error and verify no snapshot is visible.
	fs.SetPutError(nil)
	_, err = vol.Latest(ctx)
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots after failed commit, got: %v", err)
	}
}

func TestVolume_StageWriteAt_PutError_ReturnsError(t *testing.T) {
	fs := newFaultStore(NewMemory())
	vol := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)

	// Inject error on all Put calls.
	fs.SetPutError(errInjectedPut)

	_, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error when store Put fails")
	}
}

// =============================================================================
// Integer overflow safety tests
// =============================================================================

func TestVolume_StageWriteAt_OverflowOffset_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 1024)

	// offset + length would overflow int64 if computed naively.
	_, err := vol.StageWriteAt(t.Context(), math.MaxInt64, bytes.NewReader([]byte("A")))
	if err == nil {
		t.Fatal("expected error for offset that would overflow, got nil")
	}
}

func TestVolume_ReadAt_OverflowOffset_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 1024)
	ctx := t.Context()

	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 1024))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	// offset + length would overflow int64.
	_, err := vol.ReadAt(ctx, snap.ID, math.MaxInt64-5, 10)
	if err == nil {
		t.Fatal("expected error for read offset that would overflow, got nil")
	}
}

func TestVolume_Commit_OverflowBlockRef_ReturnsError(t *testing.T) {
	vol := newTestVolume(t, "test-vol", NewMemoryFactory(), 1024)

	// Craft a BlockRef where Offset + Length overflows int64.
	overflowBlock := BlockRef{
		Offset: math.MaxInt64 - 5,
		Length: 10,
		Path:   volumeBlockPath("test-vol", math.MaxInt64-5, 10),
	}

	_, err := vol.Commit(t.Context(), []BlockRef{overflowBlock}, Metadata{})
	if err == nil {
		t.Fatal("expected error for block ref that would overflow, got nil")
	}
}

func TestVolume_ValidateManifest_OverflowBlock_ReturnsError(t *testing.T) {
	m := validVolumeManifest("test-vol", 1024)
	m.Blocks = []BlockRef{
		{Offset: math.MaxInt64 - 5, Length: 10, Path: "some/path.bin"},
	}

	err := validateVolumeManifest(m)
	if err == nil {
		t.Fatal("expected manifest validation error for block that would overflow, got nil")
	}
}

func TestVolume_ValidateNoOverlaps_OverflowPrevEnd_Detected(t *testing.T) {
	// Two blocks where prevEnd = Offset + Length would overflow.
	// Block 1 starts near MaxInt64; block 2 starts at offset 0.
	// Naive prevEnd would wrap to a small number and miss the overlap.
	blocks := []BlockRef{
		{Offset: 0, Length: 100, Path: "a.bin"},
		{Offset: math.MaxInt64 - 10, Length: 20, Path: "b.bin"},
	}

	// These blocks don't actually overlap (they're far apart),
	// so this should pass. The key is that it doesn't panic or give
	// wrong results due to overflow in the prevEnd calculation.
	err := validateNoOverlaps(blocks)
	if err != nil {
		t.Fatalf("expected no overlap for non-overlapping blocks, got: %v", err)
	}
}

func TestVolume_ValidateNoOverlaps_AdjacentAtHighOffset(t *testing.T) {
	// Adjacent blocks at high offsets where Offset + Length approaches MaxInt64.
	blocks := []BlockRef{
		{Offset: math.MaxInt64 - 20, Length: 10, Path: "a.bin"},
		{Offset: math.MaxInt64 - 10, Length: 10, Path: "b.bin"},
	}

	err := validateNoOverlaps(blocks)
	if err != nil {
		t.Fatalf("expected no overlap for adjacent blocks at high offset, got: %v", err)
	}
}

func TestVolume_ValidateNoOverlaps_OverlapAtHighOffset(t *testing.T) {
	// Overlapping blocks at high offsets where naive arithmetic overflows.
	blocks := []BlockRef{
		{Offset: math.MaxInt64 - 20, Length: 15, Path: "a.bin"},
		{Offset: math.MaxInt64 - 10, Length: 10, Path: "b.bin"},
	}

	err := validateNoOverlaps(blocks)
	if !errors.Is(err, ErrOverlappingBlocks) {
		t.Fatalf("expected ErrOverlappingBlocks for overlapping blocks at high offset, got: %v", err)
	}
}
