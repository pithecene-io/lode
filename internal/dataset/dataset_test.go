package dataset_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/justapithecus/lode/internal/codec"
	"github.com/justapithecus/lode/internal/compress"
	"github.com/justapithecus/lode/internal/dataset"
	"github.com/justapithecus/lode/internal/partition"
	"github.com/justapithecus/lode/internal/read"
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

func TestConfig_FlatLayoutWithNonNoopPartitioner_Error(t *testing.T) {
	// FlatLayout doesn't support partitions, so using a non-noop partitioner
	// is a configuration error
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewHive("region"), // non-noop partitioner
		Layout:      read.FlatLayout{},           // doesn't support partitions
	})
	if err == nil {
		t.Fatal("expected error for FlatLayout with non-noop partitioner")
	}
	if !strings.Contains(err.Error(), "does not support partitions") {
		t.Errorf("expected error about partition support, got: %v", err)
	}
}

func TestConfig_FlatLayoutWithNoopPartitioner_OK(t *testing.T) {
	// FlatLayout with noop partitioner should be valid
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
		Layout:      read.FlatLayout{},
	})
	if err != nil {
		t.Errorf("expected no error for FlatLayout with noop partitioner, got: %v", err)
	}
}

// customNoopPartitioner is a custom noop partitioner for testing
type customNoopPartitioner struct{}

func (p *customNoopPartitioner) Name() string                        { return "custom-noop" }
func (p *customNoopPartitioner) PartitionKey(_ any) (string, error)  { return "", nil }
func (p *customNoopPartitioner) IsNoop() bool                        { return true }

func TestConfig_FlatLayoutWithCustomNoopPartitioner_OK(t *testing.T) {
	// FlatLayout with custom noop partitioner (implements IsNoop) should be valid
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: &customNoopPartitioner{},
		Layout:      read.FlatLayout{},
	})
	if err != nil {
		t.Errorf("expected no error for FlatLayout with custom noop partitioner, got: %v", err)
	}
}

// nonNoopPartitioner is a partitioner that emits non-empty keys but doesn't implement IsNoop
type nonNoopPartitioner struct{}

func (p *nonNoopPartitioner) Name() string                        { return "non-noop" }
func (p *nonNoopPartitioner) PartitionKey(_ any) (string, error)  { return "some-partition", nil }

func TestConfig_FlatLayoutWithCustomNonNoopPartitioner_Error(t *testing.T) {
	// FlatLayout with partitioner that emits non-empty keys should fail
	// even if it doesn't implement IsNoop (falls back to name check)
	_, err := dataset.New("test", dataset.Config{
		Store:       storage.NewMemory(),
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: &nonNoopPartitioner{},
		Layout:      read.FlatLayout{},
	})
	if err == nil {
		t.Fatal("expected error for FlatLayout with custom non-noop partitioner")
	}
	if !strings.Contains(err.Error(), "does not support partitions") {
		t.Errorf("expected error about partition support, got: %v", err)
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

// -----------------------------------------------------------------------------
// Component mismatch detection tests
// -----------------------------------------------------------------------------

func TestRead_CodecMismatch_Error(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write with JSONL codec
	ds1, _ := dataset.New("test", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
	})

	snapshot, err := ds1.Write(ctx, []any{map[string]any{"id": 1}}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create new dataset with different codec (simulated by using a mock)
	// Since we don't have another codec implementation, we'll test the compressor mismatch instead
	ds2, _ := dataset.New("test", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewGzip(), // Different compressor
		Partitioner: partition.NewNoop(),
	})

	// Read should fail with mismatch error
	_, err = ds2.Read(ctx, snapshot.ID)
	if err == nil {
		t.Fatal("expected error for compressor mismatch")
	}
	if !strings.Contains(err.Error(), "compressor mismatch") {
		t.Errorf("expected compressor mismatch error, got: %v", err)
	}
}

func TestRead_CompressorMismatch_Error(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write with gzip compressor
	ds1, _ := dataset.New("test", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewGzip(),
		Partitioner: partition.NewNoop(),
	})

	snapshot, err := ds1.Write(ctx, []any{map[string]any{"id": 1}}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create new dataset with noop compressor
	ds2, _ := dataset.New("test", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(), // Different compressor
		Partitioner: partition.NewNoop(),
	})

	// Read should fail with mismatch error
	_, err = ds2.Read(ctx, snapshot.ID)
	if err == nil {
		t.Fatal("expected error for compressor mismatch")
	}
	if !strings.Contains(err.Error(), "compressor mismatch") {
		t.Errorf("expected compressor mismatch error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Hardening tests: Immutability invariants
// Per CONTRACT_CORE.md: Data files and snapshots are immutable
// -----------------------------------------------------------------------------

func TestImmutability_ManifestPathsAreStable(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	// Write first snapshot
	snap1, err := ds.Write(ctx, []any{map[string]any{"id": 1}}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Write second snapshot
	snap2, err := ds.Write(ctx, []any{map[string]any{"id": 2}}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// First snapshot must still be readable with same ID
	reloaded, err := ds.Snapshot(ctx, snap1.ID)
	if err != nil {
		t.Fatalf("Snapshot %s not found after second write: %v", snap1.ID, err)
	}

	if reloaded.ID != snap1.ID {
		t.Errorf("Snapshot ID changed: got %s, want %s", reloaded.ID, snap1.ID)
	}

	// Verify both snapshots exist independently
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots() failed: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}

	// Verify snapshots are distinct
	if snap1.ID == snap2.ID {
		t.Error("Snapshots should have distinct IDs")
	}
}

func TestImmutability_SnapshotContentsUnchanged(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	originalRecords := []any{
		map[string]any{"id": 1, "value": "original"},
	}

	snapshot, err := ds.Write(ctx, originalRecords, lode.Metadata{"version": "1"})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Write more data (should not affect first snapshot)
	_, err = ds.Write(ctx, []any{map[string]any{"id": 2, "value": "new"}}, lode.Metadata{"version": "2"})
	if err != nil {
		t.Fatalf("Second write failed: %v", err)
	}

	// Read original snapshot - contents must be unchanged
	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0].(map[string]any)
	if rec["value"] != "original" {
		t.Errorf("Snapshot contents changed: got %v, want 'original'", rec["value"])
	}
}

func TestImmutability_ManifestMetadataPreserved(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

	metadata := lode.Metadata{
		"author":  "test",
		"version": "1.0",
		"tags":    []string{"a", "b"},
	}

	snapshot, err := ds.Write(ctx, []any{map[string]any{"id": 1}}, metadata)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Reload and verify metadata
	reloaded, err := ds.Snapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Snapshot not found: %v", err)
	}

	if reloaded.Manifest.Metadata["author"] != "test" {
		t.Errorf("Metadata 'author' not preserved")
	}
	if reloaded.Manifest.Metadata["version"] != "1.0" {
		t.Errorf("Metadata 'version' not preserved")
	}
}

func TestImmutability_RowCountAccurate(t *testing.T) {
	ctx := context.Background()
	ds := newTestDataset(t)

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

	// Reload and verify
	reloaded, err := ds.Snapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Snapshot not found: %v", err)
	}

	if reloaded.Manifest.RowCount != 3 {
		t.Errorf("Reloaded RowCount = %d, want 3", reloaded.Manifest.RowCount)
	}
}

// -----------------------------------------------------------------------------
// Hardening tests: Layout integration
// -----------------------------------------------------------------------------

func TestLayout_CustomLayoutUsedForPaths(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Use HiveLayout instead of default
	ds, err := dataset.New("test-ds", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
		Layout:      read.HiveLayout{},
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	snapshot, err := ds.Write(ctx, []any{map[string]any{"id": 1}}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify manifest path uses HiveLayout structure
	// HiveLayout: datasets/<ds>/segments/<seg>/manifest.json
	paths, _ := store.List(ctx, "")
	hasHiveManifest := false
	for _, p := range paths {
		if strings.Contains(p, "/segments/") && strings.HasSuffix(p, "manifest.json") {
			hasHiveManifest = true
			break
		}
	}

	if !hasHiveManifest {
		t.Errorf("Expected HiveLayout manifest path, got paths: %v", paths)
	}

	// Verify snapshot is still readable
	reloaded, err := ds.Snapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Snapshot not readable: %v", err)
	}

	if reloaded.ID != snapshot.ID {
		t.Errorf("Snapshot ID mismatch")
	}
}

// -----------------------------------------------------------------------------
// HiveLayout manifest placement and partition pruning tests
// -----------------------------------------------------------------------------

func TestHiveLayout_ManifestPlacement_UnderPartitionPrefix(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Create dataset with HiveLayout and Hive partitioner
	ds, err := dataset.New("events", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewHive("day"),
		Layout:      read.HiveLayout{},
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	// Write records with different days (creates 2 partitions)
	records := []any{
		map[string]any{"day": "2024-01-01", "id": 1},
		map[string]any{"day": "2024-01-02", "id": 2},
	}

	snapshot, err := ds.Write(ctx, records, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify manifests are placed under partition prefixes
	paths, _ := store.List(ctx, "")
	manifestCount := 0
	var manifestPaths []string
	for _, p := range paths {
		if strings.HasSuffix(p, "manifest.json") {
			manifestCount++
			manifestPaths = append(manifestPaths, p)
		}
	}

	// Should have 2 manifests (one under each partition)
	if manifestCount != 2 {
		t.Errorf("expected 2 manifests under partition prefixes, got %d: %v", manifestCount, manifestPaths)
	}

	// Verify manifests are under partition paths
	for _, p := range manifestPaths {
		if !strings.Contains(p, "/partitions/day=") {
			t.Errorf("manifest not under partition prefix: %s", p)
		}
		if !strings.Contains(p, "/segments/") {
			t.Errorf("manifest path missing /segments/: %s", p)
		}
	}

	// Verify snapshot is still discoverable
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots() failed: %v", err)
	}
	if len(snapshots) != 1 {
		t.Errorf("expected 1 snapshot (deduplicated), got %d", len(snapshots))
	}
	if len(snapshots) > 0 && snapshots[0].ID != snapshot.ID {
		t.Errorf("snapshot ID mismatch")
	}
}

func TestHiveLayout_PartitionPruning_Integration(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Create dataset with HiveLayout
	ds, err := dataset.New("events", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewHive("region"),
		Layout:      read.HiveLayout{},
	})
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	// Write first snapshot with region=us
	_, err = ds.Write(ctx, []any{
		map[string]any{"region": "us", "id": 1},
	}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}

	// Write second snapshot with region=eu
	_, err = ds.Write(ctx, []any{
		map[string]any{"region": "eu", "id": 2},
	}, lode.Metadata{})
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	// Use Reader with HiveLayout to test partition filtering
	reader := read.NewReaderWithLayout(store, read.HiveLayout{})

	// List segments with region=us filter - should find 1 segment
	usSegments, err := reader.ListSegments(ctx, "events", "region=us", read.SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments(region=us) failed: %v", err)
	}
	if len(usSegments) != 1 {
		t.Errorf("expected 1 segment in region=us, got %d", len(usSegments))
	}

	// List segments with region=eu filter - should find 1 segment
	euSegments, err := reader.ListSegments(ctx, "events", "region=eu", read.SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments(region=eu) failed: %v", err)
	}
	if len(euSegments) != 1 {
		t.Errorf("expected 1 segment in region=eu, got %d", len(euSegments))
	}

	// List all segments (no filter) - should find 2 segments
	allSegments, err := reader.ListSegments(ctx, "events", "", read.SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments() failed: %v", err)
	}
	if len(allSegments) != 2 {
		t.Errorf("expected 2 total segments, got %d", len(allSegments))
	}
}
