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

	// RowCount is the total number of records in this snapshot.
	RowCount int64 `json:"row_count"`

	// MinTimestamp is the earliest timestamp in the snapshot (if records are timestamped).
	// Omitted when not applicable.
	MinTimestamp *time.Time `json:"min_timestamp,omitempty"`

	// MaxTimestamp is the latest timestamp in the snapshot (if records are timestamped).
	// Omitted when not applicable.
	MaxTimestamp *time.Time `json:"max_timestamp,omitempty"`

	// Codec records the codec used to serialize records (e.g., "jsonl").
	Codec string `json:"codec"`

	// Compressor records the compression format (e.g., "gzip", "noop").
	Compressor string `json:"compressor"`

	// Partitioner records the partitioning strategy (e.g., "hive-dt", "noop").
	Partitioner string `json:"partitioner"`
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
}

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
// Partitioner interface
// -----------------------------------------------------------------------------

// Partitioner determines how records are organized into files.
//
// Partitioners are pluggable and orthogonal to storage, codecs, and compression.
type Partitioner interface {
	// Name returns the partitioner identifier (for example, "hive-dt", "none").
	Name() string

	// PartitionKey returns the partition path component for a record.
	PartitionKey(record any) (string, error)
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
	Write(ctx context.Context, records []any, metadata Metadata) (*Snapshot, error)

	// Snapshot retrieves a specific snapshot by ID.
	Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)

	// Snapshots lists all committed snapshots.
	Snapshots(ctx context.Context) ([]*Snapshot, error)

	// Read retrieves all records from a specific snapshot.
	Read(ctx context.Context, id SnapshotID) ([]any, error)

	// Latest returns the most recently committed snapshot.
	Latest(ctx context.Context) (*Snapshot, error)
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
)

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }

type errNoSnapshots struct{}

func (errNoSnapshots) Error() string { return "no snapshots" }

type errPathExists struct{}

func (errPathExists) Error() string { return "path exists" }
