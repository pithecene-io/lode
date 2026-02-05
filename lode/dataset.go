package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
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
	checksum   Checksum
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

// hiveLayoutOption implements Option for WithHiveLayout.
type hiveLayoutOption struct {
	keys []string
}

// WithHiveLayout creates a Hive (partition-first) layout option with the specified partition keys.
//
// This is the preferred way to configure Hive layout for fluent callsites.
// At least one partition key is required; validation occurs when the option is applied.
//
// Example:
//
//	ds, err := lode.NewDataset("events", factory,
//	    lode.WithHiveLayout("day", "region"),
//	    lode.WithCodec(lode.NewJSONLCodec()),
//	)
func WithHiveLayout(keys ...string) Option {
	return &hiveLayoutOption{keys: keys}
}

func (o *hiveLayoutOption) applyDataset(cfg *datasetConfig) error {
	l, err := NewHiveLayout(o.keys...)
	if err != nil {
		return err
	}
	cfg.layout = l
	return nil
}

func (o *hiveLayoutOption) applyReader(cfg *readerConfig) error {
	l, err := NewHiveLayout(o.keys...)
	if err != nil {
		return err
	}
	cfg.layout = l
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

// checksumOption implements Option for WithChecksum (dataset-only).
type checksumOption struct {
	checksum Checksum
}

// WithChecksum sets the checksum algorithm for the dataset.
// Default: none (no checksums).
// This option is only valid for NewDataset.
//
// When a checksum is configured, checksums are computed during write
// and recorded in the manifest for each data file.
func WithChecksum(c Checksum) Option {
	return &checksumOption{checksum: c}
}

func (o *checksumOption) applyDataset(cfg *datasetConfig) error {
	cfg.checksum = o.checksum
	return nil
}

func (o *checksumOption) applyReader(*readerConfig) error {
	return fmt.Errorf("WithChecksum: %w", ErrOptionNotValidForReader)
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
	checksum   Checksum
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
//   - WithChecksum(c) to enable file checksums
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
		checksum:   cfg.checksum,
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
	if d.checksum != nil {
		manifest.ChecksumAlgorithm = d.checksum.Name()
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

func (d *dataset) StreamWrite(ctx context.Context, metadata Metadata) (StreamWriter, error) {
	if metadata == nil {
		return nil, errors.New("lode: metadata must be non-nil (use empty map {} for no metadata)")
	}
	if d.codec != nil {
		return nil, ErrCodecConfigured
	}

	// Determine parent snapshot
	var parentID SnapshotID
	latest, err := d.Latest(ctx)
	if err != nil && !errors.Is(err, ErrNoSnapshots) {
		return nil, fmt.Errorf("lode: failed to get latest snapshot: %w", err)
	}
	if latest != nil {
		parentID = latest.ID
	}

	snapshotID := SnapshotID(generateID())
	fileName := "blob" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	// Create pipe for streaming to store
	pr, pw := io.Pipe()

	// Wrap with counting writer to track compressed size
	cw := &countingWriter{w: pw}

	// Set up base writer, optionally with checksum
	var baseWriter io.Writer = cw
	var hasher HashWriter
	if d.checksum != nil {
		hasher = d.checksum.NewHasher()
		baseWriter = io.MultiWriter(cw, hasher)
	}

	// Wrap with compression
	compWriter, err := d.compressor.Compress(baseWriter)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create compressor: %w", err)
	}

	// Start store.Put in background
	putDone := make(chan error, 1)
	go func() {
		putDone <- d.store.Put(ctx, filePath, pr)
	}()

	return &streamWriter{
		ds:          d,
		ctx:         ctx,
		metadata:    metadata,
		snapshotID:  snapshotID,
		parentID:    parentID,
		filePath:    filePath,
		pipeWriter:  pw,
		compWriter:  compWriter,
		countWriter: cw,
		hasher:      hasher,
		putDone:     putDone,
	}, nil
}

func (d *dataset) StreamWriteRecords(ctx context.Context, records RecordIterator, metadata Metadata) (*Snapshot, error) {
	if metadata == nil {
		return nil, errors.New("lode: metadata must be non-nil (use empty map {} for no metadata)")
	}
	if records == nil {
		return nil, ErrNilIterator
	}
	if d.codec == nil {
		return nil, errors.New("lode: StreamWriteRecords requires a codec")
	}
	streamCodec, ok := d.codec.(StreamingRecordCodec)
	if !ok {
		return nil, ErrCodecNotStreamable
	}
	// StreamWriteRecords writes to a single file and cannot partition records
	// since that would require buffering to determine partition keys
	if !d.layout.partitioner().isNoop() {
		return nil, ErrPartitioningNotSupported
	}

	// Determine parent snapshot
	var parentID SnapshotID
	latest, err := d.Latest(ctx)
	if err != nil && !errors.Is(err, ErrNoSnapshots) {
		return nil, fmt.Errorf("lode: failed to get latest snapshot: %w", err)
	}
	if latest != nil {
		parentID = latest.ID
	}

	snapshotID := SnapshotID(generateID())
	fileName := "data" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	// Create pipe for streaming to store
	pr, pw := io.Pipe()

	// Wrap with counting writer to track compressed size
	cw := &countingWriter{w: pw}

	// Set up base writer, optionally with checksum
	var baseWriter io.Writer = cw
	var hasher HashWriter
	if d.checksum != nil {
		hasher = d.checksum.NewHasher()
		baseWriter = io.MultiWriter(cw, hasher)
	}

	// Wrap with compression
	compWriter, err := d.compressor.Compress(baseWriter)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create compressor: %w", err)
	}

	// Create streaming encoder
	encoder, err := streamCodec.NewStreamEncoder(compWriter)
	if err != nil {
		_ = compWriter.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create stream encoder: %w", err)
	}

	// Start store.Put in background
	putDone := make(chan error, 1)
	go func() {
		putDone <- d.store.Put(ctx, filePath, pr)
	}()

	// Stream records through encoder, tracking count and timestamps
	var rowCount int64
	var minTs, maxTs *time.Time

	for records.Next() {
		record := records.Record()

		if err := encoder.WriteRecord(record); err != nil {
			// Abort on error
			_ = encoder.Close()
			_ = compWriter.Close()
			_ = pw.CloseWithError(err)
			<-putDone
			_ = d.store.Delete(ctx, filePath)
			return nil, fmt.Errorf("lode: failed to write record: %w", err)
		}

		rowCount++

		// Track timestamps from Timestamped records
		if ts, ok := record.(Timestamped); ok {
			t := ts.Timestamp()
			if minTs == nil || t.Before(*minTs) {
				minTs = &t
			}
			if maxTs == nil || t.After(*maxTs) {
				maxTs = &t
			}
		}
	}

	// Check for iterator errors
	if err := records.Err(); err != nil {
		_ = encoder.Close()
		_ = compWriter.Close()
		_ = pw.CloseWithError(err)
		<-putDone
		_ = d.store.Delete(ctx, filePath)
		return nil, fmt.Errorf("lode: record iterator error: %w", err)
	}

	// Close encoder (finalizes format)
	if err := encoder.Close(); err != nil {
		_ = compWriter.Close()
		_ = pw.CloseWithError(err)
		<-putDone
		_ = d.store.Delete(ctx, filePath)
		return nil, fmt.Errorf("lode: failed to close encoder: %w", err)
	}

	// Close compression (flushes final data)
	if err := compWriter.Close(); err != nil {
		_ = pw.CloseWithError(err)
		<-putDone
		_ = d.store.Delete(ctx, filePath)
		return nil, fmt.Errorf("lode: failed to close compressor: %w", err)
	}

	// Close pipe (signals EOF to store.Put)
	if err := pw.Close(); err != nil {
		<-putDone
		_ = d.store.Delete(ctx, filePath)
		return nil, fmt.Errorf("lode: failed to close pipe: %w", err)
	}

	// Wait for store.Put to complete
	if err := <-putDone; err != nil {
		_ = d.store.Delete(ctx, filePath)
		return nil, fmt.Errorf("lode: failed to write data: %w", err)
	}

	// Build file reference with optional checksum
	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: cw.n,
	}
	if hasher != nil {
		fileRef.Checksum = hasher.Sum()
	}

	// Build manifest
	manifest := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       snapshotID,
		CreatedAt:        time.Now().UTC(),
		Metadata:         metadata,
		Files:            []FileRef{fileRef},
		ParentSnapshotID: parentID,
		RowCount:         rowCount,
		MinTimestamp:     minTs,
		MaxTimestamp:     maxTs,
		Codec:            d.codec.Name(),
		Compressor:       d.compressor.Name(),
		Partitioner:      d.layout.partitioner().name(),
	}
	if d.checksum != nil {
		manifest.ChecksumAlgorithm = d.checksum.Name()
	}

	if err := d.writeManifests(ctx, snapshotID, manifest, []string{""}); err != nil {
		_ = d.store.Delete(ctx, filePath) // best-effort cleanup
		return nil, fmt.Errorf("lode: failed to write manifest: %w", err)
	}

	return &Snapshot{
		ID:       snapshotID,
		Manifest: manifest,
	}, nil
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

	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: int64(len(compressedData)),
	}

	// Compute checksum on stored (compressed) bytes
	if d.checksum != nil {
		hasher := d.checksum.NewHasher()
		_, _ = hasher.Write(compressedData)
		fileRef.Checksum = hasher.Sum()
	}

	return fileRef, nil
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

	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: int64(len(data)),
	}

	// Compute checksum on stored (compressed) bytes
	if d.checksum != nil {
		hasher := d.checksum.NewHasher()
		_, _ = hasher.Write(data)
		fileRef.Checksum = hasher.Sum()
	}

	return fileRef, nil
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

// -----------------------------------------------------------------------------
// StreamWriter Implementation
// -----------------------------------------------------------------------------

// streamWriter implements StreamWriter for raw binary streaming writes.
type streamWriter struct {
	ds          *dataset
	ctx         context.Context
	metadata    Metadata
	snapshotID  SnapshotID
	parentID    SnapshotID
	filePath    string
	pipeWriter  *io.PipeWriter
	compWriter  io.WriteCloser
	countWriter *countingWriter
	hasher      HashWriter
	putDone     chan error

	mu        sync.Mutex
	committed bool
	aborted   bool
	closed    bool
	writeErr  error

	// putDrainOnce ensures exactly one consumer drains putDone
	putDrainOnce sync.Once
	putErr       error // cached error from putDone
}

func (sw *streamWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	if sw.closed || sw.aborted || sw.committed {
		sw.mu.Unlock()
		return 0, errors.New("lode: stream is closed")
	}
	if sw.writeErr != nil {
		sw.mu.Unlock()
		return 0, sw.writeErr
	}
	sw.mu.Unlock()

	n, err = sw.compWriter.Write(p)
	if err != nil {
		sw.mu.Lock()
		sw.writeErr = err
		sw.mu.Unlock()
	}
	return n, err
}

// drainPutDone ensures exactly one consumer receives from putDone.
// Safe to call from multiple goroutines; subsequent calls return cached result.
func (sw *streamWriter) drainPutDone() error {
	sw.putDrainOnce.Do(func() {
		sw.putErr = <-sw.putDone
	})
	return sw.putErr
}

func (sw *streamWriter) Commit(ctx context.Context) (*Snapshot, error) {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream already committed")
	}
	if sw.aborted || sw.closed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream is closed")
	}
	// Mark as closed to prevent concurrent commits, but not committed until success
	sw.closed = true
	sw.mu.Unlock()

	// Close compression writer (flushes final data)
	if err := sw.compWriter.Close(); err != nil {
		_ = sw.pipeWriter.CloseWithError(err)
		_ = sw.drainPutDone() // drain for cleanup; error irrelevant
		_ = sw.ds.store.Delete(ctx, sw.filePath)
		return nil, fmt.Errorf("lode: failed to close compressor: %w", err)
	}

	// Close pipe writer (signals EOF to store.Put)
	if err := sw.pipeWriter.Close(); err != nil {
		_ = sw.drainPutDone() // drain for cleanup; error irrelevant
		_ = sw.ds.store.Delete(ctx, sw.filePath)
		return nil, fmt.Errorf("lode: failed to close pipe: %w", err)
	}

	// Wait for store.Put to complete
	if err := sw.drainPutDone(); err != nil {
		_ = sw.ds.store.Delete(ctx, sw.filePath) // best-effort cleanup
		return nil, fmt.Errorf("lode: failed to write data: %w", err)
	}

	// Build file reference with optional checksum
	fileRef := FileRef{
		Path:      sw.filePath,
		SizeBytes: sw.countWriter.n,
	}
	if sw.hasher != nil {
		fileRef.Checksum = sw.hasher.Sum()
	}

	// Build manifest
	manifest := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        sw.ds.id,
		SnapshotID:       sw.snapshotID,
		CreatedAt:        time.Now().UTC(),
		Metadata:         sw.metadata,
		Files:            []FileRef{fileRef},
		ParentSnapshotID: sw.parentID,
		RowCount:         1,
		Codec:            "",
		Compressor:       sw.ds.compressor.Name(),
		Partitioner:      sw.ds.layout.partitioner().name(),
	}
	if sw.ds.checksum != nil {
		manifest.ChecksumAlgorithm = sw.ds.checksum.Name()
	}

	if err := sw.ds.writeManifests(ctx, sw.snapshotID, manifest, []string{""}); err != nil {
		_ = sw.ds.store.Delete(ctx, sw.filePath) // best-effort cleanup
		return nil, fmt.Errorf("lode: failed to write manifest: %w", err)
	}

	// Mark committed only after full success
	sw.mu.Lock()
	sw.committed = true
	sw.mu.Unlock()

	return &Snapshot{
		ID:       sw.snapshotID,
		Manifest: manifest,
	}, nil
}

func (sw *streamWriter) Abort(ctx context.Context) error {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return errors.New("lode: cannot abort committed stream")
	}
	if sw.aborted {
		sw.mu.Unlock()
		return nil // already aborted
	}
	sw.aborted = true
	sw.mu.Unlock()

	// Close pipe with error to cancel store.Put
	_ = sw.pipeWriter.CloseWithError(errors.New("stream aborted"))

	// Wait for store.Put to complete (safe: drainPutDone uses sync.Once)
	_ = sw.drainPutDone() // error irrelevant during abort

	// Best-effort cleanup of partial object
	_ = sw.ds.store.Delete(ctx, sw.filePath)

	return nil
}

func (sw *streamWriter) Close() error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return nil
	}
	sw.closed = true
	alreadyCommitted := sw.committed
	alreadyAborted := sw.aborted
	sw.mu.Unlock()

	if alreadyCommitted || alreadyAborted {
		return nil
	}

	// Close without commit = abort
	return sw.Abort(sw.ctx)
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}
