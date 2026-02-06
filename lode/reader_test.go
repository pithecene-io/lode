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

func TestDatasetReader_ListManifests_InvalidManifest_WithPartitionFilter_ReturnsError(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// Write an invalid manifest (missing required fields)
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

	_, err = reader.ListManifests(ctx, "test-ds", "some-partition", ManifestListOptions{})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestDatasetReader_ListManifests_InvalidManifest_WithoutPartitionFilter_ReturnsError(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// Write an invalid manifest (missing required fields)
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

	// Per CONTRACT_READ_API.md, invalid manifests must return an error
	// even when no partition filter is specified
	_, err = reader.ListManifests(ctx, "test-ds", "", ManifestListOptions{})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
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
