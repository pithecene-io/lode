package read

import (
	"context"
	"io"

	"github.com/justapithecus/lode/lode"
)

// API defines the Lode read interface.
//
// This is a façade over storage and layout that performs no interpretation.
// Per CONTRACT_READ_API.md: "Lode's read API exposes stored facts, not interpretations.
// Planning and meaning belong to consumers."
//
// Discovery is manifest-driven. Segment visibility is determined by manifest presence.
type API interface {
	// ListDatasets returns all dataset IDs found in storage.
	// Returns an empty slice (not error) if no datasets exist.
	ListDatasets(ctx context.Context, opts DatasetListOptions) ([]lode.DatasetID, error)

	// ListPartitions returns partition paths found across all committed segments.
	// Per ambiguity resolution: partitions are discovered from manifests,
	// aggregated across all committed segments (snapshots) in the dataset.
	// Returns an empty slice (not error) if no partitions exist.
	// Returns ErrNotFound if the dataset does not exist.
	ListPartitions(ctx context.Context, dataset lode.DatasetID, opts PartitionListOptions) ([]PartitionRef, error)

	// ListSegments returns committed segments (snapshots) within a dataset.
	// Per CONTRACT_READ_API.md: manifest presence = commit signal.
	// Segments without manifests are ignored.
	// If partition is non-empty, filters to segments containing that partition.
	// Returns an empty slice (not error) if no segments exist.
	// Returns ErrNotFound if the dataset does not exist.
	ListSegments(ctx context.Context, dataset lode.DatasetID, partition PartitionPath, opts SegmentListOptions) ([]SegmentRef, error)

	// GetManifest loads the manifest for a specific segment.
	// Returns ErrNotFound if the dataset or segment does not exist.
	// Returns an error if the manifest is malformed.
	GetManifest(ctx context.Context, dataset lode.DatasetID, seg SegmentRef) (*lode.Manifest, error)

	// OpenObject returns a reader for a data object.
	// The caller must close the reader when done.
	// Returns ErrNotFound if the object does not exist.
	OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)

	// ObjectReaderAt returns a random-access reader for a data object.
	// Supports repeated access without re-reading the full object.
	// The caller must close the reader when done.
	// Returns ErrNotFound if the object does not exist.
	ObjectReaderAt(ctx context.Context, obj ObjectRef) (ReaderAt, error)
}
