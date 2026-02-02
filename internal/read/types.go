// Package read provides internal read API types and implementation.
//
// Per AGENTS.md, these types are internal and not part of the public lode API.
// The read API exposes stored facts, not interpretations.
package read

import (
	"errors"

	"github.com/justapithecus/lode/lode"
)

// ErrDatasetsNotModeled indicates that the current layout does not support
// dataset enumeration. Some layouts (e.g., FlatLayout) don't have a common
// prefix for datasets, making enumeration impossible without listing entire storage.
//
// This error is returned by ListDatasets when the layout's
// SupportsDatasetEnumeration returns false.
var ErrDatasetsNotModeled = errors.New("datasets not modeled by this layout")

// ObjectKey identifies an object in storage.
type ObjectKey string

// ObjectInfo contains metadata about a stored object.
type ObjectInfo struct {
	// Key is the object's storage key.
	Key ObjectKey

	// SizeBytes is the object size in bytes.
	SizeBytes int64
}

// ObjectRef references a data object within a segment.
type ObjectRef struct {
	// Dataset is the dataset containing this object.
	Dataset lode.DatasetID

	// Segment is the segment containing this object.
	Segment SegmentRef

	// Path is the full storage key for the object.
	// This matches the FileRef.Path stored in manifests.
	// Example: "datasets/mydata/snapshots/snap-1/data/file.json"
	Path string
}

// SegmentRef references an immutable segment (snapshot) within a dataset.
// Per ambiguity resolution: Segment == Snapshot (1:1 mapping).
type SegmentRef struct {
	// ID is the segment/snapshot identifier.
	ID lode.SnapshotID

	// Partition is the partition path where this segment's manifest resides.
	// For partition-first layouts (e.g., HiveLayout), this is populated during
	// discovery and used to construct the correct manifest path.
	// Empty for segment-first layouts (e.g., DefaultLayout).
	Partition PartitionPath
}

// PartitionRef references a partition within a dataset.
type PartitionRef struct {
	// Path is the partition path (e.g., "day=2024-01-01/source=foo").
	Path string
}

// PartitionPath is a partition path filter.
// Empty string means all partitions.
type PartitionPath string

// ListOptions controls listing behavior.
type ListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int
}

// ListPage contains a page of listing results.
type ListPage struct {
	// Keys contains the matching object keys.
	Keys []ObjectKey

	// HasMore indicates if more results are available.
	HasMore bool
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
