package read

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

// -----------------------------------------------------------------------------
// Cross-adapter test harness
//
// These tests verify that the read API produces identical results regardless
// of whether the underlying store is FS or Memory. Per CONTRACT_READ_API.md,
// the API must be adapter-agnostic.
// -----------------------------------------------------------------------------

// adapterTestCase holds a test store and its name for cross-adapter testing.
type adapterTestCase struct {
	name  string
	store lode.Store
}

// setupCrossAdapterStores creates both FS and Memory stores with identical data.
// Returns a cleanup function that must be called when done.
func setupCrossAdapterStores(t *testing.T) ([]adapterTestCase, func()) {
	t.Helper()

	// Create Memory store
	memStore := storage.NewMemory()

	// Create FS store
	tmpDir := t.TempDir()
	fsStore, err := storage.NewFS(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FS store: %v", err)
	}

	return []adapterTestCase{
		{name: "Memory", store: memStore},
		{name: "FS", store: fsStore},
	}, func() {
		// TempDir cleanup is automatic
	}
}

// writeIdenticalData writes the same test data to all stores.
func writeIdenticalData(t *testing.T, ctx context.Context, stores []adapterTestCase) {
	t.Helper()

	// Create manifests for two datasets
	manifest1 := createTestManifest("dataset-alpha", "snap-1", []lode.FileRef{
		{Path: "datasets/dataset-alpha/snapshots/snap-1/data/region=us/file.json", SizeBytes: 100},
		{Path: "datasets/dataset-alpha/snapshots/snap-1/data/region=eu/file.json", SizeBytes: 150},
	})

	manifest2 := createTestManifest("dataset-alpha", "snap-2", []lode.FileRef{
		{Path: "datasets/dataset-alpha/snapshots/snap-2/data/region=us/file.json", SizeBytes: 200},
		{Path: "datasets/dataset-alpha/snapshots/snap-2/data/region=ap/file.json", SizeBytes: 250},
	})

	manifest3 := createTestManifest("dataset-beta", "snap-1", []lode.FileRef{
		{Path: "datasets/dataset-beta/snapshots/snap-1/data/file.json", SizeBytes: 300},
	})

	// Data content for range read tests
	dataContent := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	for _, tc := range stores {
		// Write manifests
		writeManifest(t, ctx, tc.store, "dataset-alpha", "snap-1", manifest1)
		writeManifest(t, ctx, tc.store, "dataset-alpha", "snap-2", manifest2)
		writeManifest(t, ctx, tc.store, "dataset-beta", "snap-1", manifest3)

		// Write data files
		writeData(t, ctx, tc.store, "datasets/dataset-alpha/snapshots/snap-1/data/region=us/file.json", dataContent)
		writeData(t, ctx, tc.store, "datasets/dataset-alpha/snapshots/snap-1/data/region=eu/file.json", dataContent)
		writeData(t, ctx, tc.store, "datasets/dataset-alpha/snapshots/snap-2/data/region=us/file.json", dataContent)
		writeData(t, ctx, tc.store, "datasets/dataset-alpha/snapshots/snap-2/data/region=ap/file.json", dataContent)
		writeData(t, ctx, tc.store, "datasets/dataset-beta/snapshots/snap-1/data/file.json", dataContent)
	}
}

func createTestManifest(dataset, snapshot string, files []lode.FileRef) *lode.Manifest {
	return &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     lode.DatasetID(dataset),
		SnapshotID:    lode.SnapshotID(snapshot),
		CreatedAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Metadata:      lode.Metadata{"test": "data"},
		Files:         files,
		RowCount:      int64(len(files) * 10),
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "hive",
	}
}

func writeManifest(t *testing.T, ctx context.Context, store lode.Store, dataset, snapshot string, m *lode.Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	path := "datasets/" + dataset + "/snapshots/" + snapshot + "/manifest.json"
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatalf("failed to write manifest to %s: %v", path, err)
	}
}

func writeData(t *testing.T, ctx context.Context, store lode.Store, path string, data []byte) {
	t.Helper()
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatalf("failed to write data to %s: %v", path, err)
	}
}

// -----------------------------------------------------------------------------
// Manifest discovery parity tests
// -----------------------------------------------------------------------------

func TestCrossAdapter_ListDatasets(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	var results [][]lode.DatasetID
	for _, tc := range stores {
		reader := NewReader(tc.store)
		datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
		if err != nil {
			t.Fatalf("[%s] ListDatasets failed: %v", tc.name, err)
		}
		results = append(results, datasets)
	}

	// Compare results (order-independent)
	if len(results) < 2 {
		t.Fatal("need at least 2 adapters to compare")
	}

	baseline := results[0]
	for i := 1; i < len(results); i++ {
		if !datasetIDsEqual(baseline, results[i]) {
			t.Errorf("ListDatasets mismatch between %s and %s:\n  %s: %v\n  %s: %v",
				stores[0].name, stores[i].name,
				stores[0].name, baseline,
				stores[i].name, results[i])
		}
	}

	// Verify expected count
	if len(baseline) != 2 {
		t.Errorf("expected 2 datasets, got %d", len(baseline))
	}
}

func TestCrossAdapter_ListSegments(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	var results [][]SegmentRef
	for _, tc := range stores {
		reader := NewReader(tc.store)
		segments, err := reader.ListSegments(ctx, "dataset-alpha", "", SegmentListOptions{})
		if err != nil {
			t.Fatalf("[%s] ListSegments failed: %v", tc.name, err)
		}
		results = append(results, segments)
	}

	// Compare results
	baseline := results[0]
	for i := 1; i < len(results); i++ {
		if !segmentRefsEqual(baseline, results[i]) {
			t.Errorf("ListSegments mismatch between %s and %s", stores[0].name, stores[i].name)
		}
	}

	if len(baseline) != 2 {
		t.Errorf("expected 2 segments, got %d", len(baseline))
	}
}

func TestCrossAdapter_ListPartitions(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	var results [][]PartitionRef
	for _, tc := range stores {
		reader := NewReader(tc.store)
		partitions, err := reader.ListPartitions(ctx, "dataset-alpha", PartitionListOptions{})
		if err != nil {
			t.Fatalf("[%s] ListPartitions failed: %v", tc.name, err)
		}
		results = append(results, partitions)
	}

	// Compare results
	baseline := results[0]
	for i := 1; i < len(results); i++ {
		if !partitionRefsEqual(baseline, results[i]) {
			t.Errorf("ListPartitions mismatch between %s and %s:\n  %s: %v\n  %s: %v",
				stores[0].name, stores[i].name,
				stores[0].name, partitionPaths(baseline),
				stores[i].name, partitionPaths(results[i]))
		}
	}

	// dataset-alpha has: region=us, region=eu, region=ap
	if len(baseline) != 3 {
		t.Errorf("expected 3 partitions, got %d: %v", len(baseline), partitionPaths(baseline))
	}
}

func TestCrossAdapter_GetManifest(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	var results []*lode.Manifest
	for _, tc := range stores {
		reader := NewReader(tc.store)
		manifest, err := reader.GetManifest(ctx, "dataset-alpha", SegmentRef{ID: "snap-1"})
		if err != nil {
			t.Fatalf("[%s] GetManifest failed: %v", tc.name, err)
		}
		results = append(results, manifest)
	}

	// Compare key fields
	baseline := results[0]
	for i := 1; i < len(results); i++ {
		m := results[i]
		if baseline.DatasetID != m.DatasetID {
			t.Errorf("DatasetID mismatch: %s vs %s", baseline.DatasetID, m.DatasetID)
		}
		if baseline.SnapshotID != m.SnapshotID {
			t.Errorf("SnapshotID mismatch: %s vs %s", baseline.SnapshotID, m.SnapshotID)
		}
		if baseline.RowCount != m.RowCount {
			t.Errorf("RowCount mismatch: %d vs %d", baseline.RowCount, m.RowCount)
		}
		if len(baseline.Files) != len(m.Files) {
			t.Errorf("Files count mismatch: %d vs %d", len(baseline.Files), len(m.Files))
		}
	}
}

// -----------------------------------------------------------------------------
// Object access parity tests
// -----------------------------------------------------------------------------

func TestCrossAdapter_OpenObject(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	objRef := ObjectRef{
		Dataset: "dataset-alpha",
		Segment: SegmentRef{ID: "snap-1"},
		Path:    "data/region=us/file.json",
	}

	var contents [][]byte
	for _, tc := range stores {
		reader := NewReader(tc.store)
		rc, err := reader.OpenObject(ctx, objRef)
		if err != nil {
			t.Fatalf("[%s] OpenObject failed: %v", tc.name, err)
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("[%s] ReadAll failed: %v", tc.name, err)
		}
		contents = append(contents, data)
	}

	// Compare contents
	baseline := contents[0]
	for i := 1; i < len(contents); i++ {
		if !bytes.Equal(baseline, contents[i]) {
			t.Errorf("OpenObject content mismatch between %s and %s", stores[0].name, stores[i].name)
		}
	}
}

func TestCrossAdapter_ReadRange(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	path := "datasets/dataset-alpha/snapshots/snap-1/data/region=us/file.json"

	testCases := []struct {
		name   string
		offset int64
		length int64
	}{
		{"start", 0, 10},
		{"middle", 10, 10},
		{"end", 26, 10},
		{"full", 0, 36},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var results [][]byte
			for _, adapter := range stores {
				adp := NewStoreAdapter(adapter.store)
				data, err := adp.ReadRange(ctx, ObjectKey(path), tc.offset, tc.length)
				if err != nil {
					t.Fatalf("[%s] ReadRange failed: %v", adapter.name, err)
				}
				results = append(results, data)
			}

			baseline := results[0]
			for i := 1; i < len(results); i++ {
				if !bytes.Equal(baseline, results[i]) {
					t.Errorf("ReadRange mismatch between %s and %s:\n  %s: %q\n  %s: %q",
						stores[0].name, stores[i].name,
						stores[0].name, baseline,
						stores[i].name, results[i])
				}
			}
		})
	}
}

func TestCrossAdapter_ReaderAt(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	path := "datasets/dataset-beta/snapshots/snap-1/data/file.json"

	// Test reading at various offsets
	offsets := []int64{0, 5, 15, 30}

	for _, offset := range offsets {
		t.Run("offset_"+string(rune('0'+offset)), func(t *testing.T) {
			var results [][]byte
			for _, adapter := range stores {
				adp := NewStoreAdapter(adapter.store)
				ra, err := adp.ReaderAt(ctx, ObjectKey(path))
				if err != nil {
					t.Fatalf("[%s] ReaderAt failed: %v", adapter.name, err)
				}

				buf := make([]byte, 5)
				n, err := ra.ReadAt(buf, offset)
				_ = ra.Close()
				if err != nil && err != io.EOF {
					t.Fatalf("[%s] ReadAt failed: %v", adapter.name, err)
				}
				results = append(results, buf[:n])
			}

			baseline := results[0]
			for i := 1; i < len(results); i++ {
				if !bytes.Equal(baseline, results[i]) {
					t.Errorf("ReaderAt mismatch at offset %d between %s and %s",
						offset, stores[0].name, stores[i].name)
				}
			}
		})
	}
}

func TestCrossAdapter_ReaderAt_Size(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	writeIdenticalData(t, ctx, stores)

	path := "datasets/dataset-alpha/snapshots/snap-1/data/region=eu/file.json"

	var sizes []int64
	for _, tc := range stores {
		adp := NewStoreAdapter(tc.store)
		ra, err := adp.ReaderAt(ctx, ObjectKey(path))
		if err != nil {
			t.Fatalf("[%s] ReaderAt failed: %v", tc.name, err)
		}
		sizes = append(sizes, ra.Size())
		_ = ra.Close()
	}

	baseline := sizes[0]
	for i := 1; i < len(sizes); i++ {
		if baseline != sizes[i] {
			t.Errorf("Size mismatch between %s and %s: %d vs %d",
				stores[0].name, stores[i].name, baseline, sizes[i])
		}
	}
}

// -----------------------------------------------------------------------------
// Error handling parity tests
// -----------------------------------------------------------------------------

func TestCrossAdapter_NotFound_Parity(t *testing.T) {
	ctx := context.Background()
	stores, cleanup := setupCrossAdapterStores(t)
	defer cleanup()

	// Don't write any data - test missing resources

	for _, tc := range stores {
		reader := NewReader(tc.store)

		// ListDatasets on empty store should return empty, not error
		datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
		if err != nil {
			t.Errorf("[%s] ListDatasets on empty store failed: %v", tc.name, err)
		}
		if len(datasets) != 0 {
			t.Errorf("[%s] expected empty datasets, got %d", tc.name, len(datasets))
		}

		// GetManifest on missing should return ErrNotFound
		_, err = reader.GetManifest(ctx, "missing", SegmentRef{ID: "snap-1"})
		if err == nil {
			t.Errorf("[%s] GetManifest on missing should error", tc.name)
		}

		// OpenObject on missing should return ErrNotFound
		_, err = reader.OpenObject(ctx, ObjectRef{
			Dataset: "missing",
			Segment: SegmentRef{ID: "snap-1"},
			Path:    "data/file.json",
		})
		if err == nil {
			t.Errorf("[%s] OpenObject on missing should error", tc.name)
		}
	}
}

// -----------------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------------

func segmentRefsEqual(a, b []SegmentRef) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[lode.SnapshotID]int)
	for _, s := range a {
		m[s.ID]++
	}
	for _, s := range b {
		m[s.ID]--
		if m[s.ID] < 0 {
			return false
		}
	}
	return true
}

func partitionRefsEqual(a, b []PartitionRef) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, p := range a {
		m[p.Path]++
	}
	for _, p := range b {
		m[p.Path]--
		if m[p.Path] < 0 {
			return false
		}
	}
	return true
}

func partitionPaths(refs []PartitionRef) []string {
	paths := make([]string, len(refs))
	for i, r := range refs {
		paths[i] = r.Path
	}
	return paths
}
