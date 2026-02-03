package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// G2: Manifest validation tests
// -----------------------------------------------------------------------------

func TestReader_GetManifest_InvalidManifest_MissingSchemaName(t *testing.T) {
	ctx := context.Background()
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

	reader, err := NewReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", SegmentRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestReader_GetManifest_InvalidManifest_MissingFormatVersion(t *testing.T) {
	ctx := context.Background()
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

	reader, err := NewReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", SegmentRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestReader_GetManifest_InvalidManifest_NilMetadata(t *testing.T) {
	ctx := context.Background()
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

	reader, err := NewReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetManifest(ctx, "test-ds", SegmentRef{ID: "snap-1"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

func TestReader_ListSegments_InvalidManifest_ReturnsError(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()

	// Write an invalid manifest (missing required fields)
	manifest := &Manifest{
		// SchemaName missing - invalid
		DatasetID:  "test-ds",
		SnapshotID: "snap-1",
	}
	writeManifest(ctx, t, store, manifest)

	reader, err := NewReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	// Must provide partition filter to trigger manifest loading/validation.
	// Without partition filter, ListSegments only parses paths without loading manifests.
	_, err = reader.ListSegments(ctx, "test-ds", "some-partition", SegmentListOptions{})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// G3: ErrNoManifests test
// -----------------------------------------------------------------------------

func TestReader_ListDatasets_ErrNoManifests(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()

	// Write data files but no manifest
	err := store.Put(ctx, "datasets/test-ds/snapshots/snap-1/data/file.txt", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.ListDatasets(ctx, DatasetListOptions{})
	if !errors.Is(err, ErrNoManifests) {
		t.Errorf("expected ErrNoManifests, got: %v", err)
	}
}

func TestReader_ListDatasets_EmptyStorage(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()

	reader, err := NewReader(NewMemoryFactoryFrom(store))
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

func TestReader_ListDatasets_WithValidManifest(t *testing.T) {
	ctx := context.Background()
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

	reader, err := NewReader(NewMemoryFactoryFrom(store))
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
