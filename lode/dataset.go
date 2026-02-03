package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	manifestSchemaName    = "lode-manifest"
	manifestFormatVersion = "1.0.0"
)

// -----------------------------------------------------------------------------
// Dataset Configuration
// -----------------------------------------------------------------------------

// datasetConfig holds the resolved configuration for a dataset.
type datasetConfig struct {
	layout     layout
	compressor Compressor
	codec      Codec
}

// Option configures dataset or reader construction.
// Options implement methods for the constructors they support.
// Using an option with an unsupported constructor returns an error.
type Option interface {
	applyDataset(*datasetConfig) error
	applyReader(*readerConfig) error
}

// ErrOptionNotValidForReader indicates an option was used with NewReader
// that only applies to NewDataset.
var ErrOptionNotValidForReader = errors.New("option not valid for reader")

// ErrOptionNotValidForDataset indicates an option was used with NewDataset
// that only applies to NewReader.
var ErrOptionNotValidForDataset = errors.New("option not valid for dataset")

// layoutOption implements Option for WithLayout.
type layoutOption struct {
	layout layout
}

// WithLayout sets the layout for the dataset or reader.
// Layout configures both path topology AND partitioning.
// Default: NewDefaultLayout() (flat, no partitions).
func WithLayout(l layout) Option {
	return &layoutOption{layout: l}
}

func (o *layoutOption) applyDataset(cfg *datasetConfig) error {
	cfg.layout = o.layout
	return nil
}

func (o *layoutOption) applyReader(cfg *readerConfig) error {
	cfg.layout = o.layout
	return nil
}

// compressorOption implements Option for WithCompressor (dataset-only).
type compressorOption struct {
	compressor Compressor
}

// WithCompressor sets the compressor for the dataset.
// Default: NewNoOpCompressor().
// This option is only valid for NewDataset.
func WithCompressor(c Compressor) Option {
	return &compressorOption{compressor: c}
}

func (o *compressorOption) applyDataset(cfg *datasetConfig) error {
	cfg.compressor = o.compressor
	return nil
}

func (o *compressorOption) applyReader(*readerConfig) error {
	return fmt.Errorf("WithCompressor: %w", ErrOptionNotValidForReader)
}

// codecOption implements Option for WithCodec (dataset-only).
type codecOption struct {
	codec Codec
}

// WithCodec sets the codec for the dataset.
// Default: none (raw blob storage).
// This option is only valid for NewDataset.
//
// When a codec is set, Write expects structured records that will be
// serialized using the codec. When nil (default), Write expects a single
// []byte element and stores it as a raw blob.
func WithCodec(c Codec) Option {
	return &codecOption{codec: c}
}

func (o *codecOption) applyDataset(cfg *datasetConfig) error {
	cfg.codec = o.codec
	return nil
}

func (o *codecOption) applyReader(*readerConfig) error {
	return fmt.Errorf("WithCodec: %w", ErrOptionNotValidForReader)
}

// -----------------------------------------------------------------------------
// Dataset Implementation
// -----------------------------------------------------------------------------

// dataset implements the Dataset interface.
type dataset struct {
	id         DatasetID
	store      Store
	layout     layout
	compressor Compressor
	codec      Codec
}

// NewDataset creates a dataset with documented defaults.
//
// Default bundle (per PUBLIC_API.md):
//   - Layout: NewDefaultLayout() (flat, no partitions)
//   - Compressor: NewNoOpCompressor()
//   - Codec: none (raw blob storage)
//
// Use option functions to override defaults:
//   - WithLayout(l) to use a different layout (configures both paths AND partitioning)
//   - WithCompressor(c) to use compression
//   - WithCodec(c) to use structured records with a codec
func NewDataset(id DatasetID, factory StoreFactory, opts ...Option) (Dataset, error) {
	if factory == nil {
		return nil, errors.New("lode: store factory is required")
	}

	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if store == nil {
		return nil, errors.New("lode: store factory returned nil store")
	}

	cfg := &datasetConfig{
		layout:     NewDefaultLayout(),
		compressor: NewNoOpCompressor(),
		codec:      nil,
	}

	for _, opt := range opts {
		if err := opt.applyDataset(cfg); err != nil {
			return nil, fmt.Errorf("lode: %w", err)
		}
	}

	if cfg.layout == nil {
		return nil, errors.New("lode: layout must not be nil")
	}
	if cfg.compressor == nil {
		return nil, errors.New("lode: compressor must not be nil")
	}
	// Raw blob mode (no codec) cannot use partitioning - there are no record fields to extract keys from
	if cfg.codec == nil && !cfg.layout.partitioner().isNoop() {
		return nil, errors.New("lode: raw blob mode (no codec) requires a layout with noop partitioner")
	}

	return &dataset{
		id:         id,
		store:      store,
		layout:     cfg.layout,
		compressor: cfg.compressor,
		codec:      cfg.codec,
	}, nil
}

func (d *dataset) ID() DatasetID {
	return d.id
}

func (d *dataset) Write(ctx context.Context, data []any, metadata Metadata) (*Snapshot, error) {
	if metadata == nil {
		return nil, errors.New("lode: metadata must be non-nil (use empty map {} for no metadata)")
	}

	var parentID SnapshotID
	latest, err := d.Latest(ctx)
	if err != nil && !errors.Is(err, ErrNoSnapshots) {
		return nil, fmt.Errorf("lode: failed to get latest snapshot: %w", err)
	}
	if latest != nil {
		parentID = latest.ID
	}

	snapshotID := SnapshotID(generateID())

	var files []FileRef
	var rowCount int64
	var partitionKeys []string
	var codecName string

	if d.codec == nil {
		// Raw blob mode
		if len(data) != 1 {
			return nil, errors.New("lode: raw blob mode requires exactly one data element")
		}
		blob, ok := data[0].([]byte)
		if !ok {
			return nil, fmt.Errorf("lode: raw blob mode requires []byte, got %T", data[0])
		}

		fileRef, err := d.writeRawBlob(ctx, snapshotID, blob)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to write blob: %w", err)
		}
		files = []FileRef{fileRef}
		rowCount = 1
		partitionKeys = []string{""}
		codecName = ""
	} else {
		// Structured records mode
		partitions, err := d.partitionRecords(data)
		if err != nil {
			return nil, fmt.Errorf("lode: partitioning failed: %w", err)
		}

		for partKey, partRecords := range partitions {
			fileRef, err := d.writeDataFile(ctx, snapshotID, partKey, partRecords)
			if err != nil {
				return nil, fmt.Errorf("lode: failed to write data file: %w", err)
			}
			files = append(files, fileRef)
			partitionKeys = append(partitionKeys, partKey)
		}

		rowCount = int64(len(data))
		codecName = d.codec.Name()
	}

	// Extract timestamps from records that implement Timestamped
	minTs, maxTs := extractTimestamps(data)

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	manifest := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       snapshotID,
		CreatedAt:        time.Now().UTC(),
		Metadata:         metadata,
		Files:            files,
		ParentSnapshotID: parentID,
		RowCount:         rowCount,
		MinTimestamp:     minTs,
		MaxTimestamp:     maxTs,
		Codec:            codecName,
		Compressor:       d.compressor.Name(),
		Partitioner:      d.layout.partitioner().name(),
	}

	if err := d.writeManifests(ctx, snapshotID, manifest, partitionKeys); err != nil {
		return nil, fmt.Errorf("lode: failed to write manifest: %w", err)
	}

	return &Snapshot{
		ID:       snapshotID,
		Manifest: manifest,
	}, nil
}

func (d *dataset) Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	manifestPath := d.layout.manifestPath(d.id, id)

	rc, err := d.store.Get(ctx, manifestPath)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return d.findSnapshotByID(ctx, id)
		}
		return nil, fmt.Errorf("lode: failed to get manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var manifest Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("lode: failed to decode manifest: %w", err)
	}

	return &Snapshot{ID: id, Manifest: &manifest}, nil
}

func (d *dataset) Snapshots(ctx context.Context) ([]*Snapshot, error) {
	prefix := d.layout.segmentsPrefix(d.id)

	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}

	seen := make(map[SnapshotID]bool)
	var snapshots []*Snapshot

	for _, p := range paths {
		if !d.layout.isManifest(p) {
			continue
		}

		snapshotID := d.layout.parseSegmentID(p)
		if snapshotID == "" || seen[snapshotID] {
			continue
		}
		seen[snapshotID] = true

		snapshot, err := d.loadSnapshotFromPath(ctx, snapshotID, p)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to load snapshot %s: %w", snapshotID, err)
		}
		snapshots = append(snapshots, snapshot)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Manifest.CreatedAt.Before(snapshots[j].Manifest.CreatedAt)
	})

	return snapshots, nil
}

func (d *dataset) Read(ctx context.Context, id SnapshotID) ([]any, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := d.validateComponentsMatch(snapshot.Manifest); err != nil {
		return nil, err
	}

	if d.codec == nil {
		if len(snapshot.Manifest.Files) != 1 {
			return nil, fmt.Errorf("lode: raw blob snapshot must have exactly one file, got %d", len(snapshot.Manifest.Files))
		}
		data, err := d.readRawBlob(ctx, snapshot.Manifest.Files[0].Path)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read blob %s: %w", snapshot.Manifest.Files[0].Path, err)
		}
		return []any{data}, nil
	}

	var allRecords []any
	for _, fileRef := range snapshot.Manifest.Files {
		records, err := d.readDataFile(ctx, fileRef.Path)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read data file %s: %w", fileRef.Path, err)
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

func (d *dataset) Latest(ctx context.Context) (*Snapshot, error) {
	snapshots, err := d.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNoSnapshots
	}
	return snapshots[len(snapshots)-1], nil
}

func (d *dataset) partitionRecords(records []any) (map[string][]any, error) {
	partitions := make(map[string][]any)
	part := d.layout.partitioner()

	for _, record := range records {
		key, err := part.partitionKey(record)
		if err != nil {
			return nil, err
		}
		partitions[key] = append(partitions[key], record)
	}

	return partitions, nil
}

func (d *dataset) writeRawBlob(ctx context.Context, snapshotID SnapshotID, data []byte) (FileRef, error) {
	fileName := "blob" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	var buf bytes.Buffer
	compWriter, err := d.compressor.Compress(&buf)
	if err != nil {
		return FileRef{}, err
	}

	if _, err := compWriter.Write(data); err != nil {
		_ = compWriter.Close()
		return FileRef{}, err
	}

	if err := compWriter.Close(); err != nil {
		return FileRef{}, err
	}

	compressedData := buf.Bytes()
	if err := d.store.Put(ctx, filePath, bytes.NewReader(compressedData)); err != nil {
		return FileRef{}, err
	}

	return FileRef{
		Path:      filePath,
		SizeBytes: int64(len(compressedData)),
	}, nil
}

func (d *dataset) writeDataFile(ctx context.Context, snapshotID SnapshotID, partKey string, records []any) (FileRef, error) {
	fileName := "data" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, partKey, fileName)

	var buf bytes.Buffer
	compWriter, err := d.compressor.Compress(&buf)
	if err != nil {
		return FileRef{}, err
	}

	if err := d.codec.Encode(compWriter, records); err != nil {
		_ = compWriter.Close()
		return FileRef{}, err
	}

	if err := compWriter.Close(); err != nil {
		return FileRef{}, err
	}

	data := buf.Bytes()
	if err := d.store.Put(ctx, filePath, bytes.NewReader(data)); err != nil {
		return FileRef{}, err
	}

	return FileRef{
		Path:      filePath,
		SizeBytes: int64(len(data)),
	}, nil
}

func (d *dataset) readRawBlob(ctx context.Context, filePath string) ([]byte, error) {
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

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(decompReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *dataset) readDataFile(ctx context.Context, filePath string) ([]any, error) {
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

func (d *dataset) writeManifests(ctx context.Context, snapshotID SnapshotID, manifest *Manifest, partitionKeys []string) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	pathSet := make(map[string]bool)
	var manifestPaths []string

	hasPartitions := false
	for _, pk := range partitionKeys {
		if pk != "" {
			hasPartitions = true
			break
		}
	}

	if hasPartitions && d.layout.supportsPartitions() {
		canonicalPath := d.layout.manifestPath(d.id, snapshotID)

		var firstNonEmptyPart string
		for _, pk := range partitionKeys {
			if pk != "" {
				firstNonEmptyPart = pk
				break
			}
		}

		if firstNonEmptyPart != "" {
			firstPartPath := d.layout.manifestPathInPartition(d.id, snapshotID, firstNonEmptyPart)

			if firstPartPath != canonicalPath {
				for _, pk := range partitionKeys {
					if pk != "" {
						partPath := d.layout.manifestPathInPartition(d.id, snapshotID, pk)
						if !pathSet[partPath] {
							pathSet[partPath] = true
							manifestPaths = append(manifestPaths, partPath)
						}
					}
				}
			}
		}
	}

	if len(manifestPaths) == 0 {
		manifestPaths = []string{d.layout.manifestPath(d.id, snapshotID)}
	}

	for _, path := range manifestPaths {
		if err := d.store.Put(ctx, path, bytes.NewReader(data)); err != nil {
			return err
		}
	}

	return nil
}

func (d *dataset) validateComponentsMatch(m *Manifest) error {
	var expectedCodec string
	if d.codec != nil {
		expectedCodec = d.codec.Name()
	}
	if m.Codec != expectedCodec {
		return fmt.Errorf("lode: codec mismatch: snapshot uses %q but dataset configured with %q",
			m.Codec, expectedCodec)
	}
	if m.Compressor != d.compressor.Name() {
		return fmt.Errorf("lode: compressor mismatch: snapshot uses %q but dataset configured with %q",
			m.Compressor, d.compressor.Name())
	}
	return nil
}

func (d *dataset) loadSnapshotFromPath(ctx context.Context, id SnapshotID, manifestPath string) (*Snapshot, error) {
	rc, err := d.store.Get(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	var manifest Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	return &Snapshot{ID: id, Manifest: &manifest}, nil
}

func (d *dataset) findSnapshotByID(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	prefix := d.layout.segmentsPrefix(d.id)
	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list for snapshot search: %w", err)
	}

	for _, p := range paths {
		if !d.layout.isManifest(p) {
			continue
		}
		foundID := d.layout.parseSegmentID(p)
		if foundID == id {
			return d.loadSnapshotFromPath(ctx, id, p)
		}
	}

	return nil, ErrNotFound
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// extractTimestamps iterates over records and extracts min/max timestamps
// from records that implement the Timestamped interface.
// Returns nil pointers if no records implement Timestamped.
func extractTimestamps(data []any) (minTs, maxTs *time.Time) {
	var hasTimestamp bool

	for _, record := range data {
		ts, ok := record.(Timestamped)
		if !ok {
			continue
		}

		t := ts.Timestamp()
		if !hasTimestamp {
			minTs = &t
			maxTs = &t
			hasTimestamp = true
			continue
		}

		if t.Before(*minTs) {
			minTs = &t
		}
		if t.After(*maxTs) {
			maxTs = &t
		}
	}

	return minTs, maxTs
}
