package read

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestListSegments_Empty(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	segments, err := reader.ListSegments(context.Background(), "nonexistent", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty list for nonexistent dataset (not error per contract)
	if len(segments) != 0 {
		t.Errorf("expected empty list, got %d segments", len(segments))
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
		Path:    "data/file.json",
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
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/data/file.json", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	rc, err := reader.OpenObject(ctx, ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    "data/file.json",
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

func TestObjectReaderAt_NotSupported(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	_, err := reader.ObjectReaderAt(context.Background(), ObjectRef{
		Dataset: "mydata",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    "data/file.json",
	})
	if !errors.Is(err, ErrRangeReadNotSupported) {
		t.Errorf("expected ErrRangeReadNotSupported, got %v", err)
	}
}

func TestListPartitions_Empty(t *testing.T) {
	store := storage.NewMemory()
	reader := NewReader(store)

	// No datasets exist
	partitions, err := reader.ListPartitions(context.Background(), "nonexistent", PartitionListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(partitions) != 0 {
		t.Errorf("expected empty list, got %d partitions", len(partitions))
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
