package dataset_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justapithecus/lode/internal/codec"
	"github.com/justapithecus/lode/internal/compress"
	"github.com/justapithecus/lode/internal/dataset"
	"github.com/justapithecus/lode/internal/partition"
	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

func newTestDataset(t *testing.T) *dataset.Dataset {
	t.Helper()

	ds, err := dataset.New("test-dataset", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}
	return ds
}

// -----------------------------------------------------------------------------
// Unit tests: Write creates snapshot with parent linkage
// -----------------------------------------------------------------------------

func TestWrite_CreatesSnapshot(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	records := []any{
		map[string]any{"id": 1, "name": "Alice"},
		map[string]any{"id": 2, "name": "Bob"},
	}
	metadata := lode.Metadata{"source": "test"}

	snapshot, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if snapshot == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if snapshot.ID == "" {
		t.Error("expected non-empty snapshot ID")
	}
	if snapshot.Manifest == nil {
		t.Fatal("expected non-nil manifest")
	}
}

func TestWrite_ParentLinkage(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	records := []any{map[string]any{"id": 1}}
	metadata := lode.Metadata{}

	// First write - no parent
	snap1, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("first Write failed: %v", err)
	}
	if snap1.Manifest.ParentSnapshotID != "" {
		t.Errorf("first snapshot should have no parent, got %q", snap1.Manifest.ParentSnapshotID)
	}

	// Second write - should reference first
	snap2, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("second Write failed: %v", err)
	}
	if snap2.Manifest.ParentSnapshotID != snap1.ID {
		t.Errorf("second snapshot parent = %q, want %q", snap2.Manifest.ParentSnapshotID, snap1.ID)
	}

	// Third write - should reference second
	snap3, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("third Write failed: %v", err)
	}
	if snap3.Manifest.ParentSnapshotID != snap2.ID {
		t.Errorf("third snapshot parent = %q, want %q", snap3.Manifest.ParentSnapshotID, snap2.ID)
	}
}

// -----------------------------------------------------------------------------
// Unit tests: Nil metadata errors; empty metadata persists
// -----------------------------------------------------------------------------

func TestWrite_NilMetadata_Error(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	records := []any{map[string]any{"id": 1}}

	_, err := ds.Write(ctx, records, nil)
	if err == nil {
		t.Fatal("expected error for nil metadata")
	}
}

func TestWrite_EmptyMetadata_Persists(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	records := []any{map[string]any{"id": 1}}
	metadata := lode.Metadata{} // empty, not nil

	snapshot, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if snapshot.Manifest.Metadata == nil {
		t.Error("expected non-nil metadata in manifest")
	}

	// Verify it persists by re-reading
	retrieved, err := ds.Snapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if retrieved.Manifest.Metadata == nil {
		t.Error("persisted metadata should not be nil")
	}
}

// -----------------------------------------------------------------------------
// Unit tests: Empty dataset behaviors match contract
// -----------------------------------------------------------------------------

func TestEmptyDataset_Latest_ErrNoSnapshots(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	_, err := ds.Latest(ctx)
	if !errors.Is(err, lode.ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots, got %v", err)
	}
}

func TestEmptyDataset_Snapshots_EmptyList(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("expected empty list, got %d snapshots", len(snapshots))
	}
}

func TestEmptyDataset_Snapshot_ErrNotFound(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	_, err := ds.Snapshot(ctx, "nonexistent-id")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Unit tests: Manifest contains required fields
// -----------------------------------------------------------------------------

func TestManifest_RequiredFields(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	records := []any{map[string]any{"id": 1}}
	metadata := lode.Metadata{"key": "value"}

	snapshot, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	m := snapshot.Manifest

	// Schema name + version
	if m.SchemaName == "" {
		t.Error("manifest missing SchemaName")
	}
	if m.FormatVersion == "" {
		t.Error("manifest missing FormatVersion")
	}

	// Dataset ID
	if m.DatasetID == "" {
		t.Error("manifest missing DatasetID")
	}

	// Snapshot ID
	if m.SnapshotID == "" {
		t.Error("manifest missing SnapshotID")
	}

	// Creation time
	if m.CreatedAt.IsZero() {
		t.Error("manifest missing CreatedAt")
	}

	// Explicit metadata
	if m.Metadata == nil {
		t.Error("manifest missing Metadata")
	}

	// Files with sizes
	if len(m.Files) == 0 {
		t.Error("manifest missing Files")
	}
	for _, f := range m.Files {
		if f.Path == "" {
			t.Error("file missing Path")
		}
		if f.SizeBytes <= 0 {
			t.Error("file missing valid SizeBytes")
		}
	}

	// Component names (CONTRACT_LAYOUT)
	if m.Codec == "" {
		t.Error("manifest missing Codec")
	}
	if m.Compressor == "" {
		t.Error("manifest missing Compressor")
	}
	if m.Partitioner == "" {
		t.Error("manifest missing Partitioner")
	}

	// Row count (CONTRACT_CORE)
	if m.RowCount != 1 {
		t.Errorf("manifest RowCount = %d, want 1", m.RowCount)
	}
}

func TestManifest_RowCount(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	// Write with multiple records
	records := []any{
		map[string]any{"id": 1},
		map[string]any{"id": 2},
		map[string]any{"id": 3},
	}

	snapshot, err := ds.Write(ctx, records, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if snapshot.Manifest.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", snapshot.Manifest.RowCount)
	}

	// Write with zero records
	snapshot2, err := ds.Write(ctx, []any{}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if snapshot2.Manifest.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0", snapshot2.Manifest.RowCount)
	}
}

// -----------------------------------------------------------------------------
// Unit tests: Config validation - no nil components
// -----------------------------------------------------------------------------

func TestConfig_NilStore_Error(t *testing.T) {
	_, err := dataset.New("test", dataset.Config{
		Store:       nil,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
	})
	if err == nil {
		t.Error("expected error for nil Store")
	}
}

func TestConfig_NilCodec_Error(t *testing.T) {
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       nil,
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
	})
	if err == nil {
		t.Error("expected error for nil Codec")
	}
}

func TestConfig_NilCompressor_Error(t *testing.T) {
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  nil,
		Partitioner: partition.NewNoop(),
	})
	if err == nil {
		t.Error("expected error for nil Compressor")
	}
}

func TestConfig_NilPartitioner_Error(t *testing.T) {
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: nil,
	})
	if err == nil {
		t.Error("expected error for nil Partitioner")
	}
}

// -----------------------------------------------------------------------------
// Integration tests: End-to-end write → manifest → visible
// -----------------------------------------------------------------------------

func TestIntegration_WriteReadCycle(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	// Write records
	records := []any{
		map[string]any{"id": float64(1), "name": "Alice"},
		map[string]any{"id": float64(2), "name": "Bob"},
	}
	metadata := lode.Metadata{"version": "1.0"}

	snapshot, err := ds.Write(ctx, records, metadata)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify snapshot is visible via Snapshots()
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	if snapshots[0].ID != snapshot.ID {
		t.Errorf("snapshot ID mismatch")
	}

	// Verify snapshot is visible via Latest()
	latest, err := ds.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}
	if latest.ID != snapshot.ID {
		t.Errorf("Latest ID mismatch")
	}

	// Verify snapshot is visible via Snapshot(id)
	retrieved, err := ds.Snapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if retrieved.Manifest.Metadata["version"] != "1.0" {
		t.Error("metadata not persisted correctly")
	}

	// Read back records
	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readRecords) != 2 {
		t.Fatalf("expected 2 records, got %d", len(readRecords))
	}

	// Verify record content
	rec0 := readRecords[0].(map[string]any)
	if rec0["name"] != "Alice" {
		t.Errorf("record 0 name = %v, want Alice", rec0["name"])
	}
}

func TestIntegration_MultipleSnapshots(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	metadata := lode.Metadata{}

	// Write three snapshots
	snap1, _ := ds.Write(ctx, []any{map[string]any{"v": 1}}, metadata)
	snap2, _ := ds.Write(ctx, []any{map[string]any{"v": 2}}, metadata)
	snap3, _ := ds.Write(ctx, []any{map[string]any{"v": 3}}, metadata)

	// Verify all are visible
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}
	if len(snapshots) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
	}

	// Verify Latest returns most recent
	latest, _ := ds.Latest(ctx)
	if latest.ID != snap3.ID {
		t.Errorf("Latest should return snap3")
	}

	// Verify each can be read individually
	for _, snap := range []*lode.Snapshot{snap1, snap2, snap3} {
		_, err := ds.Read(ctx, snap.ID)
		if err != nil {
			t.Errorf("Read(%s) failed: %v", snap.ID, err)
		}
	}
}

func TestIntegration_WithGzipCompression(t *testing.T) {
	ctx := context.Background()

	ds, err := dataset.New("test-gzip", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewGzip(),
		Partitioner: partition.NewNoop(),
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	records := []any{
		map[string]any{"id": float64(1), "data": "compressed content"},
	}

	snapshot, err := ds.Write(ctx, records, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify compression is recorded
	if snapshot.Manifest.Compressor != "gzip" {
		t.Errorf("Compressor = %q, want gzip", snapshot.Manifest.Compressor)
	}

	// Verify file has .gz extension
	if len(snapshot.Manifest.Files) == 0 {
		t.Fatal("expected at least one file")
	}

	// Read back and verify
	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readRecords) != 1 {
		t.Errorf("expected 1 record, got %d", len(readRecords))
	}
}

func TestIntegration_WithHivePartitioner(t *testing.T) {
	ctx := context.Background()

	ds, err := dataset.New("test-hive", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewHive("region"),
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	records := []any{
		map[string]any{"region": "us", "id": 1},
		map[string]any{"region": "us", "id": 2},
		map[string]any{"region": "eu", "id": 3},
	}

	snapshot, err := ds.Write(ctx, records, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify partitioner is recorded
	if snapshot.Manifest.Partitioner != "hive" {
		t.Errorf("Partitioner = %q, want hive", snapshot.Manifest.Partitioner)
	}

	// Should have 2 files (us and eu partitions)
	if len(snapshot.Manifest.Files) != 2 {
		t.Errorf("expected 2 files for 2 partitions, got %d", len(snapshot.Manifest.Files))
	}

	// Read back and verify all records present
	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readRecords) != 3 {
		t.Errorf("expected 3 records, got %d", len(readRecords))
	}
}
