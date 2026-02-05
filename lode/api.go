// Package lode provides dataset snapshots, manifests, and metadata for object
// storage systems.
//
// Lode focuses on persistence structure: datasets, immutable snapshots, and
// explicit metadata. It does not implement query execution or background
// processing.
package lode

import (
	"context"
	"io"
	"time"
)

// -----------------------------------------------------------------------------
// Core types
// -----------------------------------------------------------------------------

// DatasetID uniquely identifies a dataset and is stable for its lifetime.
type DatasetID string

// SnapshotID uniquely identifies an immutable snapshot within a dataset.
type SnapshotID string

// Metadata holds user-defined key-value pairs stored with a snapshot.
type Metadata map[string]any

// D is a shorthand alias for map-based records in examples and callsites.
// It is equivalent to map[string]any.
type D = map[string]any

// R converts record literals into the []any form expected by Dataset.Write.
// This is a convenience helper for callsites and examples.
func R(records ...D) []any {
	out := make([]any, len(records))
	for i, r := range records {
		out[i] = r
	}
	return out
}

// Timestamped is implemented by records that have an associated timestamp.
// When records passed to Dataset.Write implement this interface, the manifest's
// MinTimestamp and MaxTimestamp fields are automatically computed from the data.
// Records that do not implement this interface are ignored for timestamp tracking.
type Timestamped interface {
	Timestamp() time.Time
}

// -----------------------------------------------------------------------------
// Manifest
// -----------------------------------------------------------------------------

// Manifest describes the complete contents of a snapshot.
//
// A manifest is self-contained and includes the dataset ID, snapshot ID,
// format version, metadata, and all data file references.
type Manifest struct {
	// SchemaName identifies the manifest schema (e.g., "lode-manifest").
	SchemaName string `json:"schema_name"`

	// FormatVersion identifies the manifest schema version.
	FormatVersion string `json:"format_version"`

	// DatasetID identifies the dataset this manifest belongs to.
	DatasetID DatasetID `json:"dataset_id"`

	// SnapshotID uniquely identifies this snapshot.
	SnapshotID SnapshotID `json:"snapshot_id"`

	// CreatedAt records when the snapshot was committed.
	CreatedAt time.Time `json:"created_at"`

	// Metadata contains user-provided key-value pairs.
	Metadata Metadata `json:"metadata"`

	// Files lists all data files comprising this snapshot.
	Files []FileRef `json:"files"`

	// ParentSnapshotID optionally references the previous snapshot.
	ParentSnapshotID SnapshotID `json:"parent_snapshot_id,omitempty"`

	// RowCount is the total number of data units in this snapshot.
	RowCount int64 `json:"row_count"`

	// MinTimestamp is the earliest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MinTimestamp *time.Time `json:"min_timestamp,omitempty"`

	// MaxTimestamp is the latest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MaxTimestamp *time.Time `json:"max_timestamp,omitempty"`

	// Codec records the codec used to serialize structured data (e.g., "jsonl").
	// Omitted when no codec is configured.
	Codec string `json:"codec,omitempty"`

	// Compressor records the compression format (e.g., "gzip", "noop").
	Compressor string `json:"compressor"`

	// Partitioner records the partitioning strategy (e.g., "hive-dt", "noop").
	Partitioner string `json:"partitioner"`

	// ChecksumAlgorithm records the checksum algorithm used (e.g., "md5").
	// Omitted when no checksum is configured.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
}

// FileRef describes a single data file within a snapshot.
type FileRef struct {
	// Path is the relative path to the file within the dataset.
	Path string `json:"path"`

	// SizeBytes is the file size in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// Checksum is an optional integrity hash.
	Checksum string `json:"checksum,omitempty"`
}

// -----------------------------------------------------------------------------
// Snapshot
// -----------------------------------------------------------------------------

// Snapshot represents an immutable point-in-time view of a dataset.
type Snapshot struct {
	// ID uniquely identifies this snapshot.
	ID SnapshotID

	// Manifest describes the snapshot's contents.
	Manifest *Manifest
}

// -----------------------------------------------------------------------------
// Store interface
// -----------------------------------------------------------------------------

// Store abstracts the underlying object storage system.
//
// Implementations may target filesystems, S3, or other object stores.
// The interface is intentionally minimal to avoid backend-specific leakage.
type Store interface {
	// Put writes data to the given path.
	Put(ctx context.Context, path string, r io.Reader) error

	// Get retrieves data from the given path.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// Exists checks whether a path exists.
	Exists(ctx context.Context, path string) (bool, error)

	// List returns paths under the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the path if it exists.
	Delete(ctx context.Context, path string) error

	// ReadRange reads a byte range from the given path.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)

	// ReaderAt returns an io.ReaderAt for random access reads.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}

// StoreFactory creates a Store. Used for deferred store construction.
type StoreFactory func() (Store, error)

// -----------------------------------------------------------------------------
// Codec interface
// -----------------------------------------------------------------------------

// Codec handles serialization and deserialization of records.
//
// Codecs are pluggable and orthogonal to storage, compression, and partitioning.
type Codec interface {
	// Name returns the codec identifier (for example, "jsonl" or "parquet").
	Name() string

	// Encode writes records to the given writer.
	Encode(w io.Writer, records []any) error

	// Decode reads records from the given reader.
	Decode(r io.Reader) ([]any, error)
}

// -----------------------------------------------------------------------------
// Streaming codec interfaces
// -----------------------------------------------------------------------------

// StreamingRecordCodec is implemented by codecs that support streaming record encoding.
//
// StreamWriteRecords requires a codec that implements this interface. Codecs that
// do not support streaming should not implement this interface.
type StreamingRecordCodec interface {
	Codec

	// NewStreamEncoder creates a streaming encoder that writes to w.
	NewStreamEncoder(w io.Writer) (RecordStreamEncoder, error)
}

// RecordStreamEncoder writes records one at a time to a stream.
type RecordStreamEncoder interface {
	// WriteRecord encodes and writes a single record.
	WriteRecord(record any) error

	// Close finalizes the stream and flushes any buffered data.
	Close() error
}

// -----------------------------------------------------------------------------
// Compressor interface
// -----------------------------------------------------------------------------

// Compressor handles compression and decompression of data streams.
//
// Compressors are pluggable and orthogonal to storage, codecs, and partitioning.
type Compressor interface {
	// Name returns the compressor identifier (for example, "gzip", "zstd", "none").
	Name() string

	// Extension returns the file extension (for example, ".gz", ".zst", "").
	Extension() string

	// Compress wraps a writer with compression.
	Compress(w io.Writer) (io.WriteCloser, error)

	// Decompress wraps a reader with decompression.
	Decompress(r io.Reader) (io.ReadCloser, error)
}

// -----------------------------------------------------------------------------
// Checksum interface
// -----------------------------------------------------------------------------

// Checksum computes integrity checksums for data files.
//
// Checksums are optional and configured via WithChecksum. When configured,
// checksums are computed during write and recorded in manifests.
type Checksum interface {
	// Name returns the checksum algorithm identifier (e.g., "md5", "sha256").
	Name() string

	// NewHasher returns a new hash.Hash for computing checksums.
	// The returned Hash can be used as an io.Writer to accumulate data.
	NewHasher() HashWriter
}

// HashWriter combines hash computation with io.Writer.
// Write data to accumulate the hash, then call Sum to get the result.
type HashWriter interface {
	io.Writer

	// Sum returns the computed checksum as a hex-encoded string.
	Sum() string
}

// -----------------------------------------------------------------------------
// Dataset interface
// -----------------------------------------------------------------------------

// Dataset provides operations on a named collection of snapshots.
//
// A dataset is a logical container for related data. All writes to a dataset
// produce immutable snapshots.
type Dataset interface {
	// ID returns the dataset's unique identifier.
	ID() DatasetID

	// Write commits new data and metadata as an immutable snapshot.
	Write(ctx context.Context, data []any, metadata Metadata) (*Snapshot, error)

	// Snapshot retrieves a specific snapshot by ID.
	Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)

	// Snapshots lists all committed snapshots.
	Snapshots(ctx context.Context) ([]*Snapshot, error)

	// Read retrieves all data units from a specific snapshot.
	Read(ctx context.Context, id SnapshotID) ([]any, error)

	// Latest returns the most recently committed snapshot.
	Latest(ctx context.Context) (*Snapshot, error)

	// StreamWrite returns a StreamWriter for single-pass streaming of a binary payload.
	// Returns an error if metadata is nil or if a codec is configured.
	StreamWrite(ctx context.Context, metadata Metadata) (StreamWriter, error)

	// StreamWriteRecords consumes records via a pull-based iterator and streams them
	// through a streaming-capable codec. Returns an error if metadata is nil or if
	// the configured codec does not support streaming.
	StreamWriteRecords(ctx context.Context, records RecordIterator, metadata Metadata) (*Snapshot, error)
}

// -----------------------------------------------------------------------------
// StreamWriter interface
// -----------------------------------------------------------------------------

// StreamWriter supports single-pass streaming writes of binary data.
//
// A StreamWriter writes bytes directly to the final object path. The manifest
// is written only on Commit. If Close is called before Commit, the stream is
// aborted and no snapshot is created.
type StreamWriter interface {
	// Write writes bytes to the stream. Implements io.Writer.
	Write(p []byte) (n int, err error)

	// Commit finalizes the stream and writes the manifest.
	// Returns the new snapshot on success.
	Commit(ctx context.Context) (*Snapshot, error)

	// Abort discards the stream without creating a snapshot.
	// Attempts best-effort cleanup of partial objects.
	Abort(ctx context.Context) error

	// Close closes the stream. If Commit was not called, behaves as Abort.
	Close() error
}

// -----------------------------------------------------------------------------
// RecordIterator interface
// -----------------------------------------------------------------------------

// RecordIterator provides pull-based iteration over records.
//
// The typical usage pattern is:
//
//	for iter.Next() {
//	    record := iter.Record()
//	    // process record
//	}
//	if err := iter.Err(); err != nil { ... }
type RecordIterator interface {
	Next() bool  // Advances to the next record. Returns false when exhausted.
	Record() any // Returns the current record. Only valid after Next returns true.
	Err() error  // Returns any error encountered during iteration.
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

// Error sentinel values for common conditions.
var (
	// ErrNotFound indicates a requested resource does not exist.
	ErrNotFound = errNotFound{}

	// ErrNoSnapshots indicates a dataset has no committed snapshots.
	ErrNoSnapshots = errNoSnapshots{}

	// ErrPathExists indicates an attempt to write to an existing path.
	ErrPathExists = errPathExists{}

	// ErrNoManifests indicates storage contains objects but no valid manifests.
	ErrNoManifests = errNoManifests{}

	// ErrRangeReadNotSupported indicates the store does not support range reads.
	ErrRangeReadNotSupported = errRangeReadNotSupported{}

	// ErrCodecConfigured indicates StreamWrite was called with a codec configured.
	ErrCodecConfigured = errCodecConfigured{}

	// ErrCodecNotStreamable indicates the configured codec does not support streaming.
	ErrCodecNotStreamable = errCodecNotStreamable{}

	// ErrNilIterator indicates a nil iterator was passed to StreamWriteRecords.
	ErrNilIterator = errNilIterator{}

	// ErrPartitioningNotSupported indicates StreamWriteRecords was called with partitioning configured.
	ErrPartitioningNotSupported = errPartitioningNotSupported{}
)

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }

type errNoSnapshots struct{}

func (errNoSnapshots) Error() string { return "no snapshots" }

type errPathExists struct{}

func (errPathExists) Error() string { return "path exists" }

type errNoManifests struct{}

func (errNoManifests) Error() string { return "no manifests found (storage contains objects)" }

type errRangeReadNotSupported struct{}

func (errRangeReadNotSupported) Error() string { return "range read not supported" }

type errCodecConfigured struct{}

func (errCodecConfigured) Error() string {
	return "StreamWrite requires no codec; use StreamWriteRecords for structured data"
}

type errCodecNotStreamable struct{}

func (errCodecNotStreamable) Error() string { return "codec does not support streaming" }

type errNilIterator struct{}

func (errNilIterator) Error() string { return "records iterator must be non-nil" }

type errPartitioningNotSupported struct{}

func (errPartitioningNotSupported) Error() string {
	return "StreamWriteRecords does not support partitioning; use Write for partitioned data"
}

// -----------------------------------------------------------------------------
// Reader interface
// -----------------------------------------------------------------------------

// SegmentRef references an immutable segment (snapshot) within a dataset.
type SegmentRef struct {
	// ID is the segment/snapshot identifier.
	ID SnapshotID

	// Partition is the partition path where this segment's manifest resides.
	// For partition-first layouts (e.g., HiveLayout), this is populated during
	// discovery. Empty for segment-first layouts (e.g., DefaultLayout).
	Partition string
}

// PartitionRef references a partition within a dataset.
type PartitionRef struct {
	// Path is the partition path (e.g., "day=2024-01-01/source=foo").
	Path string
}

// ObjectRef references a data object within a segment.
type ObjectRef struct {
	// Dataset is the dataset containing this object.
	Dataset DatasetID

	// Segment is the segment containing this object.
	Segment SegmentRef

	// Path is the full storage key for the object.
	Path string
}

// DatasetListOptions controls dataset listing.
type DatasetListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int
}

// PartitionListOptions controls partition listing.
type PartitionListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int
}

// SegmentListOptions controls segment listing.
type SegmentListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int
}

// Reader provides read operations over stored datasets.
//
// Reader is a façade over storage and layout that performs no interpretation.
// Per CONTRACT_READ_API.md: "Lode's read API exposes stored facts, not interpretations.
// Planning and meaning belong to consumers."
type Reader interface {
	// ListDatasets returns all dataset IDs found in storage.
	// Returns ErrDatasetsNotModeled if the layout doesn't support dataset enumeration.
	ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error)

	// ListPartitions returns partition paths found across all committed segments.
	// Returns ErrNotFound if the dataset does not exist.
	ListPartitions(ctx context.Context, dataset DatasetID, opts PartitionListOptions) ([]PartitionRef, error)

	// ListSegments returns committed segments (snapshots) within a dataset.
	// Returns ErrNotFound if the dataset does not exist.
	ListSegments(ctx context.Context, dataset DatasetID, partition string, opts SegmentListOptions) ([]SegmentRef, error)

	// GetManifest loads the manifest for a specific segment.
	// Returns ErrNotFound if the dataset or segment does not exist.
	GetManifest(ctx context.Context, dataset DatasetID, seg SegmentRef) (*Manifest, error)

	// OpenObject returns a reader for a data object.
	// The caller must close the reader when done.
	OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)

	// ReaderAt returns an io.ReaderAt for random access reads on a data object.
	// Returns ErrRangeReadNotSupported if the underlying store does not support range reads.
	ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error)
}

// ErrDatasetsNotModeled indicates that the current layout does not support
// dataset enumeration.
var ErrDatasetsNotModeled = errDatasetsNotModeled{}

type errDatasetsNotModeled struct{}

func (errDatasetsNotModeled) Error() string { return "datasets not modeled by this layout" }
