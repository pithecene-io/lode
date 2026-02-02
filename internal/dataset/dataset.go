// Package dataset provides the core Dataset implementation.
package dataset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/justapithecus/lode/internal/read"
	"github.com/justapithecus/lode/lode"
)

const (
	// ManifestSchemaName is the canonical schema name for Lode manifests.
	ManifestSchemaName = "lode-manifest"

	// ManifestFormatVersion is the current manifest format version.
	ManifestFormatVersion = "1.0.0"
)

// Config holds the configuration for a Dataset.
// Per CONTRACT_LAYOUT.md, all components must be non-nil.
type Config struct {
	Store       lode.Store
	Codec       lode.Codec
	Compressor  lode.Compressor
	Partitioner lode.Partitioner
	// Layout determines path topology. If nil, DefaultLayout is used.
	Layout read.Layout
}

// Validate checks that all required components are set.
func (c *Config) Validate() error {
	if c.Store == nil {
		return errors.New("dataset: Store is required")
	}
	if c.Codec == nil {
		return errors.New("dataset: Codec is required")
	}
	if c.Compressor == nil {
		return errors.New("dataset: Compressor is required")
	}
	if c.Partitioner == nil {
		return errors.New("dataset: Partitioner is required")
	}
	return nil
}

// Dataset implements lode.Dataset.
type Dataset struct {
	id          lode.DatasetID
	store       lode.Store
	codec       lode.Codec
	compressor  lode.Compressor
	partitioner lode.Partitioner
	layout      read.Layout
}

// New creates a new Dataset with the given configuration.
// Returns an error if any required component is nil.
// If Layout is nil, DefaultLayout is used.
func New(id lode.DatasetID, cfg Config) (*Dataset, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	layout := cfg.Layout
	if layout == nil {
		layout = read.DefaultLayout{}
	}

	return &Dataset{
		id:          id,
		store:       cfg.Store,
		codec:       cfg.Codec,
		compressor:  cfg.Compressor,
		partitioner: cfg.Partitioner,
		layout:      layout,
	}, nil
}

// ID returns the dataset's unique identifier.
func (d *Dataset) ID() lode.DatasetID {
	return d.id
}

// Write commits new data and metadata as an immutable snapshot.
// Per CONTRACT_WRITE_API.md:
// - metadata must be non-nil (nil returns error)
// - empty metadata {} is valid and persisted explicitly
// - new snapshot references previous snapshot as parent (if any)
func (d *Dataset) Write(ctx context.Context, records []any, metadata lode.Metadata) (*lode.Snapshot, error) {
	// CONTRACT: nil metadata is invalid
	if metadata == nil {
		return nil, errors.New("dataset: metadata must be non-nil (use empty map {} for no metadata)")
	}

	// Get parent snapshot (if any) for linear history
	var parentID lode.SnapshotID
	latest, err := d.Latest(ctx)
	if err != nil && !errors.Is(err, lode.ErrNoSnapshots) {
		return nil, fmt.Errorf("dataset: failed to get latest snapshot: %w", err)
	}
	if latest != nil {
		parentID = latest.ID
	}

	// Generate snapshot ID
	snapshotID := lode.SnapshotID(generateID())

	// Group records by partition
	partitions, err := d.partitionRecords(records)
	if err != nil {
		return nil, fmt.Errorf("dataset: partitioning failed: %w", err)
	}

	// Write data files (before manifest per CONTRACT_STORAGE.md)
	var files []lode.FileRef
	for partKey, partRecords := range partitions {
		fileRef, err := d.writeDataFile(ctx, snapshotID, partKey, partRecords)
		if err != nil {
			return nil, fmt.Errorf("dataset: failed to write data file: %w", err)
		}
		files = append(files, fileRef)
	}

	// Sort files for deterministic manifests
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	// Create manifest
	manifest := &lode.Manifest{
		SchemaName:       ManifestSchemaName,
		FormatVersion:    ManifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       snapshotID,
		CreatedAt:        time.Now().UTC(),
		Metadata:         metadata,
		Files:            files,
		ParentSnapshotID: parentID,
		RowCount:         int64(len(records)),
		// MinTimestamp/MaxTimestamp: omitted when not applicable
		Codec:       d.codec.Name(),
		Compressor:  d.compressor.Name(),
		Partitioner: d.partitioner.Name(),
	}

	// Write manifest (commit signal per CONTRACT_STORAGE.md)
	if err := d.writeManifest(ctx, snapshotID, manifest); err != nil {
		return nil, fmt.Errorf("dataset: failed to write manifest: %w", err)
	}

	return &lode.Snapshot{
		ID:       snapshotID,
		Manifest: manifest,
	}, nil
}

// Snapshot retrieves a specific snapshot by ID.
// Per CONTRACT_WRITE_API.md: returns ErrNotFound for missing snapshot.
func (d *Dataset) Snapshot(ctx context.Context, id lode.SnapshotID) (*lode.Snapshot, error) {
	manifestPath := d.layout.ManifestPath(d.id, id)

	rc, err := d.store.Get(ctx, manifestPath)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return nil, lode.ErrNotFound
		}
		return nil, fmt.Errorf("dataset: failed to get manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var manifest lode.Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("dataset: failed to decode manifest: %w", err)
	}

	return &lode.Snapshot{
		ID:       id,
		Manifest: &manifest,
	}, nil
}

// Snapshots lists all committed snapshots.
// Per CONTRACT_WRITE_API.md: returns empty list (no error) for empty dataset.
func (d *Dataset) Snapshots(ctx context.Context) ([]*lode.Snapshot, error) {
	prefix := d.layout.SegmentsPrefix(d.id)

	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("dataset: failed to list snapshots: %w", err)
	}

	// Find unique snapshot IDs from manifest paths using layout
	seen := make(map[lode.SnapshotID]bool)
	var snapshots []*lode.Snapshot

	for _, p := range paths {
		// Use layout to check if this is a valid manifest path
		if !d.layout.IsManifest(p) {
			continue
		}

		// Extract snapshot ID using layout
		snapshotID := d.layout.ParseSegmentID(p)
		if snapshotID == "" || seen[snapshotID] {
			continue
		}
		seen[snapshotID] = true

		snapshot, err := d.Snapshot(ctx, snapshotID)
		if err != nil {
			return nil, fmt.Errorf("dataset: failed to load snapshot %s: %w", snapshotID, err)
		}
		snapshots = append(snapshots, snapshot)
	}

	// Sort by creation time for consistent ordering
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Manifest.CreatedAt.Before(snapshots[j].Manifest.CreatedAt)
	})

	return snapshots, nil
}

// Read retrieves all records from a specific snapshot.
// Returns an error if the snapshot was written with different codec/compressor
// than the dataset is currently configured with.
func (d *Dataset) Read(ctx context.Context, id lode.SnapshotID) ([]any, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}

	// Validate that manifest components match dataset config
	// This ensures we use the correct codec/compressor for decoding
	if err := d.validateComponentsMatch(snapshot.Manifest); err != nil {
		return nil, err
	}

	var allRecords []any
	for _, fileRef := range snapshot.Manifest.Files {
		records, err := d.readDataFile(ctx, fileRef.Path)
		if err != nil {
			return nil, fmt.Errorf("dataset: failed to read data file %s: %w", fileRef.Path, err)
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

// validateComponentsMatch checks that the manifest's recorded components
// match the dataset's current configuration. This prevents silent data
// corruption when reading snapshots written with different components.
func (d *Dataset) validateComponentsMatch(m *lode.Manifest) error {
	if m.Codec != d.codec.Name() {
		return fmt.Errorf("dataset: codec mismatch: snapshot uses %q but dataset configured with %q",
			m.Codec, d.codec.Name())
	}
	if m.Compressor != d.compressor.Name() {
		return fmt.Errorf("dataset: compressor mismatch: snapshot uses %q but dataset configured with %q",
			m.Compressor, d.compressor.Name())
	}
	// Partitioner doesn't affect reading, only organization, so we don't validate it
	return nil
}

// Latest returns the most recently committed snapshot.
// Per CONTRACT_WRITE_API.md: returns ErrNoSnapshots for empty dataset.
func (d *Dataset) Latest(ctx context.Context) (*lode.Snapshot, error) {
	snapshots, err := d.Snapshots(ctx)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, lode.ErrNoSnapshots
	}

	// Return the last (most recent) snapshot
	return snapshots[len(snapshots)-1], nil
}

// partitionRecords groups records by partition key.
func (d *Dataset) partitionRecords(records []any) (map[string][]any, error) {
	partitions := make(map[string][]any)

	for _, record := range records {
		key, err := d.partitioner.PartitionKey(record)
		if err != nil {
			return nil, err
		}
		partitions[key] = append(partitions[key], record)
	}

	return partitions, nil
}

// writeDataFile writes records to a data file and returns the FileRef.
func (d *Dataset) writeDataFile(ctx context.Context, snapshotID lode.SnapshotID, partKey string, records []any) (lode.FileRef, error) {
	// Build file path using layout
	fileName := "data" + d.compressor.Extension()
	filePath := d.layout.DataFilePath(d.id, snapshotID, partKey, fileName)

	// Encode and compress data
	var buf bytes.Buffer
	compWriter, err := d.compressor.Compress(&buf)
	if err != nil {
		return lode.FileRef{}, err
	}

	if err := d.codec.Encode(compWriter, records); err != nil {
		_ = compWriter.Close()
		return lode.FileRef{}, err
	}

	if err := compWriter.Close(); err != nil {
		return lode.FileRef{}, err
	}

	// Write to store
	data := buf.Bytes()
	if err := d.store.Put(ctx, filePath, bytes.NewReader(data)); err != nil {
		return lode.FileRef{}, err
	}

	return lode.FileRef{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		// Checksum could be added here if needed
	}, nil
}

// readDataFile reads and decodes a data file.
func (d *Dataset) readDataFile(ctx context.Context, filePath string) ([]any, error) {
	rc, err := d.store.Get(ctx, filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	decompReader, err := d.compressor.Decompress(rc)
	if err != nil {
		return nil, err
	}
	defer func() { _ = decompReader.Close() }()

	return d.codec.Decode(decompReader)
}

// writeManifest writes the manifest to the store.
func (d *Dataset) writeManifest(ctx context.Context, snapshotID lode.SnapshotID, manifest *lode.Manifest) error {
	manifestPath := d.layout.ManifestPath(d.id, snapshotID)

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	return d.store.Put(ctx, manifestPath, bytes.NewReader(data))
}

// generateID creates a unique snapshot ID.
// Uses timestamp + random suffix for uniqueness.
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// Ensure Dataset implements lode.Dataset
var _ lode.Dataset = (*Dataset)(nil)
