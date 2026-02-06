package lode

import (
	"bytes"
	"errors"
	"testing"
)

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
