// Package read provides internal read API types and implementation.
//
// Per AGENTS.md, these types are internal and not part of the public lode API.
// The read API exposes stored facts, not interpretations.
package read

import "github.com/justapithecus/lode/lode"

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

	// Path is the object path relative to the segment.
	Path string
}

// SegmentRef references an immutable segment (snapshot) within a dataset.
// Per ambiguity resolution: Segment == Snapshot (1:1 mapping).
type SegmentRef struct {
	// ID is the segment/snapshot identifier.
	ID lode.SnapshotID
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
