package read

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

func TestNewReader_NilStore(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil store")
		}
	}()
	NewReader(nil)
}

func TestListDatasets_Empty(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(datasets) != 0 {
		t.Errorf("expected empty list, got %d datasets", len(datasets))
	}
}

func TestListDatasets_WithManifests(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifests for two datasets
	writeTestManifest(t, ctx, store, "dataset-a", "snap-1")
	writeTestManifest(t, ctx, store, "dataset-b", "snap-1")

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(datasets) != 2 {
		t.Errorf("expected 2 datasets, got %d", len(datasets))
	}
}

func TestListDatasets_Limit(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifests for three datasets
	writeTestManifest(t, ctx, store, "dataset-a", "snap-1")
	writeTestManifest(t, ctx, store, "dataset-b", "snap-1")
	writeTestManifest(t, ctx, store, "dataset-c", "snap-1")

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(datasets) != 2 {
		t.Errorf("expected 2 datasets with limit, got %d", len(datasets))
	}
}

func TestListDatasets_IgnoresDataWithoutManifest(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write only a data file without manifest
	err := store.Put(ctx, "datasets/orphan/snapshots/snap-1/data/file.json", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	// Write a proper manifest for another dataset
	writeTestManifest(t, ctx, store, "valid", "snap-1")

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only the dataset with manifest should be returned
	if len(datasets) != 1 {
		t.Errorf("expected 1 dataset, got %d", len(datasets))
	}
	if len(datasets) > 0 && datasets[0] != "valid" {
		t.Errorf("expected dataset 'valid', got %q", datasets[0])
	}
}

func TestListSegments_DatasetNotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	// Per CONTRACT_READ_API.md: return ErrNotFound if dataset doesn't exist
	_, err := reader.ListSegments(context.Background(), "nonexistent", "", SegmentListOptions{})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound for nonexistent dataset, got %v", err)
	}
}

func TestListSegments_WithManifests(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	writeTestManifest(t, ctx, store, "mydata", "snap-1")
	writeTestManifest(t, ctx, store, "mydata", "snap-2")

	reader := NewReader(store)
	segments, err := reader.ListSegments(ctx, "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(segments) != 2 {
		t.Errorf("expected 2 segments, got %d", len(segments))
	}
}

func TestListSegments_IgnoresSegmentsWithoutManifest(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write a proper manifest
	writeTestManifest(t, ctx, store, "mydata", "snap-1")

	// Write only a data file without manifest
	err := store.Put(ctx, "datasets/mydata/snapshots/orphan/data/file.json", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	segments, err := reader.ListSegments(ctx, "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only segment with manifest should be returned
	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
}

func TestGetManifest_NotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	_, err := reader.GetManifest(context.Background(), "missing", SegmentRef{ID: "snap-1"})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetManifest_Success(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	writeTestManifest(t, ctx, store, "mydata", "snap-1")

	reader := NewReader(store)
	manifest, err := reader.GetManifest(ctx, "mydata", SegmentRef{ID: "snap-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if manifest.DatasetID != "mydata" {
		t.Errorf("expected dataset ID 'mydata', got %q", manifest.DatasetID)
	}
	if manifest.SnapshotID != "snap-1" {
		t.Errorf("expected snapshot ID 'snap-1', got %q", manifest.SnapshotID)
	}
}

func TestGetManifest_Malformed(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write invalid JSON
	err := store.Put(ctx, "datasets/bad/snapshots/snap-1/manifest.json", bytes.NewReader([]byte("not json")))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	_, err = reader.GetManifest(ctx, "bad", SegmentRef{ID: "snap-1"})
	if err == nil {
		t.Error("expected error for malformed manifest")
	}
}

func TestOpenObject_NotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	_, err := reader.OpenObject(context.Background(), ObjectRef{
		Dataset: "missing",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    "datasets/missing/snapshots/snap-1/data/file.json",
	})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestOpenObject_Success(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write a data file
	content := []byte(`{"test": "data"}`)
	fullPath := "datasets/mydata/snapshots/snap-1/data/file.json"
	err := store.Put(ctx, fullPath, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	rc, err := reader.OpenObject(ctx, ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    fullPath, // Full storage key
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if buf.String() != string(content) {
		t.Errorf("expected %q, got %q", string(content), buf.String())
	}
}

func TestObjectReaderAt_Success(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write a data file
	content := []byte("0123456789abcdef")
	fullPath := "datasets/mydata/snapshots/snap-1/data/file.bin"
	err := store.Put(ctx, fullPath, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	ra, err := reader.ObjectReaderAt(ctx, ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    fullPath, // Full storage key
	})
	if err != nil {
		t.Fatalf("ObjectReaderAt failed: %v", err)
	}
	defer ra.Close()

	// Verify size
	if ra.Size() != int64(len(content)) {
		t.Errorf("Size() = %d, want %d", ra.Size(), len(content))
	}

	// Read at offset
	buf := make([]byte, 4)
	n, err := ra.ReadAt(buf, 4)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 4 || !bytes.Equal(buf, []byte("4567")) {
		t.Errorf("ReadAt = %d, %q; want 4, %q", n, buf, "4567")
	}
}

func TestObjectReaderAt_RepeatedAccess(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	content := []byte("hello world test data")
	fullPath := "datasets/mydata/snapshots/snap-1/data/file.bin"
	err := store.Put(ctx, fullPath, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	ra, err := reader.ObjectReaderAt(ctx, ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    fullPath, // Full storage key
	})
	if err != nil {
		t.Fatalf("ObjectReaderAt failed: %v", err)
	}
	defer ra.Close()

	buf := make([]byte, 5)

	// Read same position multiple times
	for i := 0; i < 3; i++ {
		n, err := ra.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt iteration %d failed: %v", i, err)
		}
		if n != 5 || !bytes.Equal(buf, []byte("hello")) {
			t.Errorf("ReadAt iteration %d = %d, %q; want 5, %q", i, n, buf, "hello")
		}
	}

	// Read different position
	n, err := ra.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt(6) failed: %v", err)
	}
	if n != 5 || !bytes.Equal(buf, []byte("world")) {
		t.Errorf("ReadAt(6) = %d, %q; want 5, %q", n, buf, "world")
	}
}

func TestObjectReaderAt_NotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	_, err := reader.ObjectReaderAt(context.Background(), ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    "datasets/mydata/snapshots/snap-1/data/missing.bin",
	})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListPartitions_DatasetNotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	// Per CONTRACT_READ_API.md: return ErrNotFound if dataset doesn't exist
	_, err := reader.ListPartitions(context.Background(), "nonexistent", PartitionListOptions{})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound for nonexistent dataset, got %v", err)
	}
}

func TestListPartitions_WithPartitionedData(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest with partitioned files
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/day=2024-01-01/file.json", SizeBytes: 100},
			{Path: "datasets/mydata/snapshots/snap-1/data/day=2024-01-02/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive-dt",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	partitions, err := reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(partitions))
	}
}

func TestListPartitions_NoInference(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest with unpartitioned files
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "noop",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	partitions, err := reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No partitions should be inferred from unpartitioned data
	if len(partitions) != 0 {
		t.Errorf("expected 0 partitions (no inference), got %d", len(partitions))
	}
}

// -----------------------------------------------------------------------------
// Order-independence tests (per CONTRACT_STORAGE.md: ordering is unspecified)
// -----------------------------------------------------------------------------

func TestListDatasets_OrderIndependent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifests for multiple datasets
	expected := []lode.DatasetID{"alpha", "beta", "gamma"}
	for _, id := range expected {
		writeTestManifest(t, ctx, store, string(id), "snap-1")
	}

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all expected datasets are present (order-independent)
	if !datasetIDsEqual(datasets, expected) {
		t.Errorf("expected datasets %v, got %v", expected, datasets)
	}
}

func TestListSegments_OrderIndependent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write multiple segments
	expected := []lode.SnapshotID{"snap-a", "snap-b", "snap-c"}
	for _, id := range expected {
		writeTestManifest(t, ctx, store, "mydata", string(id))
	}

	reader := NewReader(store)
	segments, err := reader.ListSegments(ctx, "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all expected segments are present (order-independent)
	got := make([]lode.SnapshotID, len(segments))
	for i, s := range segments {
		got[i] = s.ID
	}
	if !snapshotIDsEqual(got, expected) {
		t.Errorf("expected segments %v, got %v", expected, got)
	}
}

func TestListPartitions_OrderIndependent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest with multiple partitions
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/region=us/file.json", SizeBytes: 100},
			{Path: "datasets/mydata/snapshots/snap-1/data/region=eu/file.json", SizeBytes: 100},
			{Path: "datasets/mydata/snapshots/snap-1/data/region=ap/file.json", SizeBytes: 100},
		},
		RowCount:    30,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	partitions, err := reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all expected partitions are present (order-independent)
	expected := []string{"region=us", "region=eu", "region=ap"}
	got := make([]string, len(partitions))
	for i, p := range partitions {
		got[i] = p.Path
	}
	if !stringsEqual(got, expected) {
		t.Errorf("expected partitions %v, got %v", expected, got)
	}
}

// -----------------------------------------------------------------------------
// Empty dataset edge cases (Task 2 requirements)
// -----------------------------------------------------------------------------

func TestListSegments_DatasetWithoutManifests_ReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Create a dataset directory structure without any manifests
	// This simulates a dataset that has files but no committed segments
	err := store.Put(ctx, "datasets/empty-dataset/readme.txt", bytes.NewReader([]byte("placeholder")))
	if err != nil {
		t.Fatalf("failed to write placeholder: %v", err)
	}

	reader := NewReader(store)
	// Per CONTRACT_READ_API.md: dataset without manifests = doesn't exist
	_, err = reader.ListSegments(ctx, "empty-dataset", "", SegmentListOptions{})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound for dataset without manifests, got %v", err)
	}
}

func TestListPartitions_DatasetWithNoSegments_ReturnsNotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	// Dataset doesn't exist at all
	_, err := reader.ListPartitions(context.Background(), "nonexistent", PartitionListOptions{})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListSegments_WithPartitionFilter_DatasetNotFound(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	// Query with partition filter on nonexistent dataset
	_, err := reader.ListSegments(context.Background(), "nonexistent", "day=2024-01-01", SegmentListOptions{})
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Partition filter tests (Task 2 requirements)
// -----------------------------------------------------------------------------

func TestListSegments_WithPartitionFilter(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Segment 1: has partition day=2024-01-01
	manifest1 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/day=2024-01-01/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive-dt",
	}

	// Segment 2: has partition day=2024-01-02
	manifest2 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-2",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-2/data/day=2024-01-02/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive-dt",
	}

	// Segment 3: has both partitions
	manifest3 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-3",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-3/data/day=2024-01-01/file.json", SizeBytes: 100},
			{Path: "datasets/mydata/snapshots/snap-3/data/day=2024-01-02/file.json", SizeBytes: 100},
		},
		RowCount:    20,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive-dt",
	}

	for _, m := range []*lode.Manifest{manifest1, manifest2, manifest3} {
		data, _ := json.Marshal(m)
		path := "datasets/mydata/snapshots/" + string(m.SnapshotID) + "/manifest.json"
		if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
			t.Fatalf("failed to write manifest: %v", err)
		}
	}

	reader := NewReader(store)

	// Filter by day=2024-01-01 should return snap-1 and snap-3
	segments, err := reader.ListSegments(ctx, "mydata", "day=2024-01-01", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(segments) != 2 {
		t.Errorf("expected 2 segments with day=2024-01-01, got %d", len(segments))
	}

	got := make([]lode.SnapshotID, len(segments))
	for i, s := range segments {
		got[i] = s.ID
	}
	expected := []lode.SnapshotID{"snap-1", "snap-3"}
	if !snapshotIDsEqual(got, expected) {
		t.Errorf("expected segments %v, got %v", expected, got)
	}
}

func TestListSegments_WithPartitionFilter_NoMatch(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write a manifest with a different partition
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/day=2024-01-01/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive-dt",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)

	// Filter by non-existent partition
	segments, err := reader.ListSegments(ctx, "mydata", "day=2024-12-31", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(segments) != 0 {
		t.Errorf("expected 0 segments, got %d", len(segments))
	}
}

// -----------------------------------------------------------------------------
// Aggregated partitions across segments
// -----------------------------------------------------------------------------

func TestListPartitions_AggregatesAcrossSegments(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Segment 1: partition A
	manifest1 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-1/data/region=us/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive",
	}

	// Segment 2: partition B
	manifest2 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "snap-2",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/snapshots/snap-2/data/region=eu/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive",
	}

	for _, m := range []*lode.Manifest{manifest1, manifest2} {
		data, _ := json.Marshal(m)
		path := "datasets/mydata/snapshots/" + string(m.SnapshotID) + "/manifest.json"
		if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
			t.Fatalf("failed to write manifest: %v", err)
		}
	}

	reader := NewReader(store)
	partitions, err := reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should aggregate partitions from both segments
	if len(partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(partitions))
	}

	got := make([]string, len(partitions))
	for i, p := range partitions {
		got[i] = p.Path
	}
	expected := []string{"region=us", "region=eu"}
	if !stringsEqual(got, expected) {
		t.Errorf("expected partitions %v, got %v", expected, got)
	}
}

func TestListPartitions_DeduplicatesAcrossSegments(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Both segments have the same partition
	for _, snapID := range []string{"snap-1", "snap-2"} {
		manifest := &lode.Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "mydata",
			SnapshotID:    lode.SnapshotID(snapID),
			CreatedAt:     time.Now().UTC(),
			Metadata:      lode.Metadata{},
			Files: []lode.FileRef{
				{Path: "datasets/mydata/snapshots/" + snapID + "/data/region=us/file.json", SizeBytes: 100},
			},
			RowCount:    10,
			Codec:       "jsonl",
			Compressor:  "noop",
			Partitioner: "hive",
		}
		data, _ := json.Marshal(manifest)
		path := "datasets/mydata/snapshots/" + snapID + "/manifest.json"
		if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
			t.Fatalf("failed to write manifest: %v", err)
		}
	}

	reader := NewReader(store)
	partitions, err := reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should deduplicate - only one "region=us" partition
	if len(partitions) != 1 {
		t.Errorf("expected 1 partition (deduplicated), got %d", len(partitions))
	}
}

// -----------------------------------------------------------------------------
// Helper functions for order-independent comparison
// -----------------------------------------------------------------------------

func datasetIDsEqual(a, b []lode.DatasetID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[lode.DatasetID]int)
	for _, id := range a {
		m[id]++
	}
	for _, id := range b {
		m[id]--
		if m[id] < 0 {
			return false
		}
	}
	return true
}

func snapshotIDsEqual(a, b []lode.SnapshotID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[lode.SnapshotID]int)
	for _, id := range a {
		m[id]++
	}
	for _, id := range b {
		m[id]--
		if m[id] < 0 {
			return false
		}
	}
	return true
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Layout-specific error tests
// -----------------------------------------------------------------------------

func TestListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReaderWithLayout(store, FlatLayout{})

	_, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrDatasetsNotModeled) {
		t.Errorf("expected ErrDatasetsNotModeled, got: %v", err)
	}
}

func TestListDatasets_DefaultLayout_EmptyStorage_ReturnsEmptyList(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty list for truly empty storage (not an error)
	if len(datasets) != 0 {
		t.Errorf("expected empty list, got %d datasets", len(datasets))
	}
}

// -----------------------------------------------------------------------------
// Custom layout discovery tests - prove discovery behavior changes with layout
// -----------------------------------------------------------------------------

func TestListDatasets_HiveLayout_FindsDatasets(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifests using HiveLayout structure
	// HiveLayout: datasets/<ds>/segments/<seg>/manifest.json
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "hive-ds",
		SnapshotID:    "seg-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	data, _ := json.Marshal(manifest)
	_ = store.Put(ctx, "datasets/hive-ds/segments/seg-1/manifest.json", bytes.NewReader(data))

	// Create reader with HiveLayout
	reader := NewReaderWithLayout(store, HiveLayout{})

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(datasets) != 1 {
		t.Errorf("expected 1 dataset, got %d", len(datasets))
	}
	if len(datasets) > 0 && datasets[0] != "hive-ds" {
		t.Errorf("expected dataset %q, got %q", "hive-ds", datasets[0])
	}
}

func TestListSegments_HiveLayout_FindsSegments(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest using HiveLayout structure
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "hive-ds",
		SnapshotID:    "seg-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	data, _ := json.Marshal(manifest)
	_ = store.Put(ctx, "datasets/hive-ds/segments/seg-1/manifest.json", bytes.NewReader(data))

	reader := NewReaderWithLayout(store, HiveLayout{})

	segments, err := reader.ListSegments(ctx, "hive-ds", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
	if len(segments) > 0 && segments[0].ID != "seg-1" {
		t.Errorf("expected segment %q, got %q", "seg-1", segments[0].ID)
	}
}

func TestListSegments_FlatLayout_FindsSegments(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest using FlatLayout structure: <dataset>/<segment>/manifest.json
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "flat-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	data, _ := json.Marshal(manifest)
	_ = store.Put(ctx, "flat-ds/snap-1/manifest.json", bytes.NewReader(data))

	reader := NewReaderWithLayout(store, FlatLayout{})

	// FlatLayout doesn't support ListDatasets, but ListSegments should work
	segments, err := reader.ListSegments(ctx, "flat-ds", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
	if len(segments) > 0 && segments[0].ID != "snap-1" {
		t.Errorf("expected segment %q, got %q", "snap-1", segments[0].ID)
	}
}

func TestDiscovery_LayoutMismatch_NoResults(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest using DefaultLayout structure
	writeTestManifest(t, ctx, store, "default-ds", "snap-1")

	// Try to read with HiveLayout - should find nothing
	// because the paths don't match HiveLayout's expected structure
	reader := NewReaderWithLayout(store, HiveLayout{})

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// HiveLayout won't recognize DefaultLayout paths
	if len(datasets) != 0 {
		t.Errorf("expected 0 datasets (layout mismatch), got %d", len(datasets))
	}
}

// -----------------------------------------------------------------------------
// HiveLayout Prefix Pruning Integration Tests
// Verify true prefix pruning without scanning all manifests
// -----------------------------------------------------------------------------

func TestHiveLayout_PartitionPruning_OnlyListsTargetPartition(t *testing.T) {
	ctx := context.Background()

	// trackingStore that records list prefixes
	store := &prefixTrackingStore{
		memory: storage.NewMemory(),
	}

	// Write manifests in two different partitions using HiveLayout structure
	// Partition day=2024-01-01 contains seg-1
	manifest1 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "seg-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/partitions/day=2024-01-01/segments/seg-1/data/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive",
	}
	data1, _ := json.Marshal(manifest1)
	_ = store.memory.Put(ctx, "datasets/mydata/partitions/day=2024-01-01/segments/seg-1/manifest.json", bytes.NewReader(data1))

	// Partition day=2024-01-02 contains seg-2
	manifest2 := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "mydata",
		SnapshotID:    "seg-2",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files: []lode.FileRef{
			{Path: "datasets/mydata/partitions/day=2024-01-02/segments/seg-2/data/file.json", SizeBytes: 100},
		},
		RowCount:    10,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "hive",
	}
	data2, _ := json.Marshal(manifest2)
	_ = store.memory.Put(ctx, "datasets/mydata/partitions/day=2024-01-02/segments/seg-2/manifest.json", bytes.NewReader(data2))

	reader := NewReaderWithLayout(store, HiveLayout{})

	// List segments with partition filter - should use prefix pruning
	store.listPrefixes = nil // clear
	segments, err := reader.ListSegments(ctx, "mydata", "day=2024-01-01", SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}

	// Verify prefix used was partition-specific (true prefix pruning)
	if len(store.listPrefixes) != 1 {
		t.Errorf("expected 1 list call, got %d", len(store.listPrefixes))
	}
	expectedPrefix := "datasets/mydata/partitions/day=2024-01-01/segments/"
	if len(store.listPrefixes) > 0 && store.listPrefixes[0] != expectedPrefix {
		t.Errorf("expected list prefix %q, got %q", expectedPrefix, store.listPrefixes[0])
	}

	// Should only find seg-1 (in the requested partition)
	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
	if len(segments) > 0 && segments[0].ID != "seg-1" {
		t.Errorf("expected segment seg-1, got %s", segments[0].ID)
	}
}

// prefixTrackingStore wraps a store to track list prefixes
type prefixTrackingStore struct {
	memory       *storage.Memory
	listPrefixes []string
}

func (p *prefixTrackingStore) Put(ctx context.Context, path string, r io.Reader) error {
	return p.memory.Put(ctx, path, r)
}
func (p *prefixTrackingStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	return p.memory.Get(ctx, path)
}
func (p *prefixTrackingStore) Exists(ctx context.Context, path string) (bool, error) {
	return p.memory.Exists(ctx, path)
}
func (p *prefixTrackingStore) List(ctx context.Context, prefix string) ([]string, error) {
	p.listPrefixes = append(p.listPrefixes, prefix)
	return p.memory.List(ctx, prefix)
}
func (p *prefixTrackingStore) Delete(ctx context.Context, path string) error {
	return p.memory.Delete(ctx, path)
}

func TestDiscovery_ManifestDriven_OnlyCommittedVisible(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write a data file without a manifest (uncommitted)
	_ = store.Put(ctx, "datasets/uncommitted/snapshots/snap-1/data/file.json", bytes.NewReader([]byte(`{}`)))

	// Write a committed dataset with manifest
	writeTestManifest(t, ctx, store, "committed", "snap-1")

	reader := NewReader(store)

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only the committed dataset should be visible
	if len(datasets) != 1 {
		t.Errorf("expected 1 dataset, got %d", len(datasets))
	}
	if len(datasets) > 0 && datasets[0] != "committed" {
		t.Errorf("expected dataset %q, got %q", "committed", datasets[0])
	}
}

func TestDiscovery_ManifestPresence_CommitSignal(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write data file first (simulating incomplete write)
	_ = store.Put(ctx, "datasets/mydata/snapshots/snap-1/data/file.json", bytes.NewReader([]byte(`{}`)))

	reader := NewReader(store)

	// Should not find the dataset (no manifest = not committed)
	segments, err := reader.ListSegments(ctx, "mydata", "", SegmentListOptions{})
	if err == nil || !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound for uncommitted dataset, got: %v", err)
	}

	// Now write the manifest (commit signal)
	writeTestManifest(t, ctx, store, "mydata", "snap-1")

	// Now the segment should be visible
	segments, err = reader.ListSegments(ctx, "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error after commit: %v", err)
	}

	if len(segments) != 1 {
		t.Errorf("expected 1 segment after commit, got %d", len(segments))
	}
}

// -----------------------------------------------------------------------------
// Manifest validation error propagation tests (CONTRACT_ERRORS.md)
// "ListSegments returns error (not skip) when manifest validation fails"
// "ListPartitions returns error (not skip) when manifest validation fails"
// -----------------------------------------------------------------------------

func TestListSegments_ManifestValidationError_PropagatesError(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write an invalid manifest (missing required fields)
	// Include files with a partition to trigger manifest loading during partition filter
	invalidManifest := map[string]any{
		// Missing schema_name, format_version, codec, compressor, partitioner
		"dataset_id":  "mydata",
		"snapshot_id": "snap-1",
		"created_at":  time.Now().UTC(),
		"metadata":    map[string]any{},
		"files": []any{
			map[string]any{"path": "datasets/mydata/snapshots/snap-1/data/region=us/file.json", "size_bytes": 100},
		},
		"row_count": 10,
	}

	data, _ := json.Marshal(invalidManifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write invalid manifest: %v", err)
	}

	reader := NewReader(store)

	// ListSegments with a partition filter triggers manifest loading and validation
	// Per CONTRACT_ERRORS.md: "ListSegments returns error (not skip) when manifest validation fails"
	_, err = reader.ListSegments(ctx, "mydata", "region=us", SegmentListOptions{})
	if err == nil {
		t.Fatal("expected error for invalid manifest, got nil")
	}

	// Error should be related to manifest validation
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestListPartitions_ManifestValidationError_PropagatesError(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write an invalid manifest (missing required fields)
	invalidManifest := map[string]any{
		// Missing schema_name, format_version, codec, compressor, partitioner
		"dataset_id":  "mydata",
		"snapshot_id": "snap-1",
		"created_at":  time.Now().UTC(),
		"metadata":    map[string]any{},
		"files": []any{
			map[string]any{"path": "datasets/mydata/snapshots/snap-1/data/region=us/file.json", "size_bytes": 100},
		},
		"row_count": 10,
	}

	data, _ := json.Marshal(invalidManifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write invalid manifest: %v", err)
	}

	reader := NewReader(store)

	// ListPartitions should return error, not skip the invalid manifest
	_, err = reader.ListPartitions(ctx, "mydata", PartitionListOptions{})
	if err == nil {
		t.Fatal("expected error for invalid manifest, got nil")
	}

	// Error should be related to manifest validation
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Helper for writing test manifests
// -----------------------------------------------------------------------------

// writeTestManifest writes a minimal valid manifest to storage.
func writeTestManifest(t *testing.T, ctx context.Context, store lode.Store, dataset, snapshot string) {
	t.Helper()

	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     lode.DatasetID(dataset),
		SnapshotID:    lode.SnapshotID(snapshot),
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}

	path := "datasets/" + dataset + "/snapshots/" + snapshot + "/manifest.json"
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}
}
