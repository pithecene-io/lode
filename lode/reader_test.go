package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// G2: Manifest validation tests
// -----------------------------------------------------------------------------

func TestDatasetReader_GetManifest_InvalidManifest_MissingSchemaName(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		// SchemaName missing
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{},
		RowCount:      0,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_MissingFormatVersion(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName: "lode-manifest",
		// FormatVersion missing
		DatasetID:   "test-ds",
		SnapshotID:  "snap-1",
		CreatedAt:   time.Now().UTC(),
		Metadata:    Metadata{},
		Files:       []FileRef{},
		RowCount:    0,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_NilMetadata(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      nil, // nil not allowed
		Files:         []FileRef{},
		RowCount:      0,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_MissingDatasetID(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		// DatasetID missing
		SnapshotID:  "snap-1",
		CreatedAt:   time.Now().UTC(),
		Metadata:    Metadata{},
		Files:       []FileRef{},
		RowCount:    0,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	// Manually write with a path that includes dataset ID
	data, _ := json.Marshal(manifest)
	_ = store.Put(ctx, "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data))

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_MissingSnapshotID(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		// SnapshotID missing
		CreatedAt:   time.Now().UTC(),
		Metadata:    Metadata{},
		Files:       []FileRef{},
		RowCount:    0,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	// Use empty ManifestRef since manifest has no ID
	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: ""})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_ZeroCreatedAt(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		// CreatedAt zero value (not set)
		Metadata:    Metadata{},
		Files:       []FileRef{},
		RowCount:    0,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_NilFiles(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         nil, // nil not allowed (use empty slice)
		RowCount:      0,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_NegativeRowCount(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{},
		RowCount:      -1, // negative not allowed
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_MissingCompressor(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{},
		RowCount:      0,
		// Compressor missing
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_MissingPartitioner(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{},
		RowCount:      0,
		Compressor:    "noop",
		// Partitioner missing
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_EmptyFilePath(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files: []FileRef{
			{Path: "", SizeBytes: 100}, // empty path not allowed
		},
		RowCount:    1,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_GetManifest_InvalidManifest_NegativeFileSize(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files: []FileRef{
			{Path: "data/file.bin", SizeBytes: -100}, // negative size not allowed
		},
		RowCount:    1,
		Compressor:  "noop",
		Partitioner: "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_ListManifests_InvalidManifest_DeferredToGetManifest(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// Write an invalid manifest (missing required fields).
	manifest := &Manifest{
		// SchemaName missing - invalid
		DatasetID:  "test-ds",
		SnapshotID: "snap-1",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	// ListManifests extracts refs from paths without loading manifests.
	// Validation is deferred to GetManifest.
	refs, err := reader.ListManifests(ctx, "test-ds", "", ManifestListOptions{})
	if err != nil {
		t.Fatalf("ListManifests should succeed (path-only): %v", err)
	}
	if len(refs) == 0 {
		t.Fatal("expected at least one manifest ref")
	}

	// GetManifest triggers validation and returns the error.
	_, err = reader.GetManifest(ctx, "test-ds", refs[0])
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid from GetManifest, got: %v", err)
	}
}

// TestDatasetReader_ListManifests_NoGetCalls verifies that ListManifests
// extracts ManifestRefs from paths without downloading any manifests.
func TestDatasetReader_ListManifests_NoGetCalls(t *testing.T) {
	fs := newFaultStore(NewMemory())

	// Write a dataset with 3 snapshots.
	ds, err := NewDataset("test-ds", newFaultStoreFactory(fs), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := ds.Write(t.Context(), R(D{"i": i}), Metadata{}); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := NewDatasetReader(newFaultStoreFactory(fs))
	if err != nil {
		t.Fatal(err)
	}

	fs.Reset()

	refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}

	// ListManifests should use 1 List call and 0 Get calls.
	getCalls := fs.GetCalls()
	if len(getCalls) != 0 {
		t.Fatalf("expected 0 Get calls from ListManifests, got %d: %v", len(getCalls), getCalls)
	}
	listCalls := fs.ListCalls()
	if len(listCalls) != 1 {
		t.Fatalf("expected 1 List call, got %d", len(listCalls))
	}
}

// TestDatasetReader_ListPartitions_NoGetCalls verifies that ListPartitions
// extracts partitions from store paths without downloading any manifests.
func TestDatasetReader_ListPartitions_NoGetCalls(t *testing.T) {
	fs := newFaultStore(NewMemory())

	ds, err := NewDataset("test-ds", newFaultStoreFactory(fs),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	records := R(
		D{"id": 1, "day": "2024-01-15"},
		D{"id": 2, "day": "2024-01-16"},
		D{"id": 3, "day": "2024-01-17"},
	)
	if _, err := ds.Write(t.Context(), records, Metadata{}); err != nil {
		t.Fatal(err)
	}

	reader, err := NewDatasetReader(newFaultStoreFactory(fs), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	fs.Reset()

	partitions, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(partitions))
	}

	// ListPartitions should use 1 List call and 0 Get calls.
	getCalls := fs.GetCalls()
	if len(getCalls) != 0 {
		t.Fatalf("expected 0 Get calls from ListPartitions, got %d: %v", len(getCalls), getCalls)
	}
	listCalls := fs.ListCalls()
	if len(listCalls) != 1 {
		t.Fatalf("expected 1 List call, got %d", len(listCalls))
	}
}

// -----------------------------------------------------------------------------
// G3: ErrNoManifests test
// -----------------------------------------------------------------------------

func TestDatasetReader_ListDatasets_ErrNoManifests(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// Write data files but no manifest
	err := store.Put(ctx, "datasets/test-ds/snapshots/snap-1/data/file.txt", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.ListDatasets(ctx, DatasetListOptions{})
	if !errors.Is(err, ErrNoManifests) {
		t.Errorf("expected ErrNoManifests, got: %v", err)
	}
}

func TestDatasetReader_ListDatasets_EmptyStorage(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("expected no error for empty storage, got: %v", err)
	}
	if len(datasets) != 0 {
		t.Errorf("expected empty list, got: %v", datasets)
	}
}

func TestDatasetReader_ListDatasets_WithValidManifest(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	manifest := &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{},
		RowCount:      0,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(datasets) != 1 || datasets[0] != "test-ds" {
		t.Errorf("expected [test-ds], got: %v", datasets)
	}
}

// -----------------------------------------------------------------------------
// G4: Layout-specific tests
// -----------------------------------------------------------------------------

func TestDatasetReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithLayout(NewFlatLayout()))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.ListDatasets(ctx, DatasetListOptions{})
	if !errors.Is(err, ErrDatasetsNotModeled) {
		t.Errorf("expected ErrDatasetsNotModeled, got: %v", err)
	}
}

func TestDatasetReader_ListDatasets_FSStore_EmptyStorage_ReturnsEmptyList(t *testing.T) {
	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	reader, err := NewDatasetReader(NewFSFactory(tmpDir))
	if err != nil {
		t.Fatal(err)
	}

	datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
	if err != nil {
		t.Fatalf("expected no error for empty FS storage, got: %v", err)
	}
	if len(datasets) != 0 {
		t.Errorf("expected empty list, got: %v", datasets)
	}
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// NewMemoryFactoryFrom creates a StoreFactory that returns an existing store.
func NewMemoryFactoryFrom(store Store) StoreFactory {
	return func() (Store, error) {
		return store, nil
	}
}

func writeManifest(ctx context.Context, t *testing.T, store Store, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := "datasets/" + string(m.DatasetID) + "/snapshots/" + string(m.SnapshotID) + "/manifest.json"
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}
