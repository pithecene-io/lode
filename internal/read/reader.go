package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/justapithecus/lode/lode"
)

// ErrRangeReadNotSupported indicates that range reads are not supported.
// This error is returned until ReadStorage adapters are implemented.
var ErrRangeReadNotSupported = errors.New("range read not supported by this store")

// Reader implements the read API using a storage backend.
//
// Reader is a façade that maps API calls to storage operations.
// It performs no interpretation or planning.
type Reader struct {
	store   lode.Store
	adapter *StoreAdapter
	layout  Layout
}

// NewReader creates a Reader backed by the given store with the default layout.
// If the store supports range reads (e.g., storage.FS or storage.Memory),
// those capabilities will be automatically detected and enabled.
func NewReader(store lode.Store) *Reader {
	return NewReaderWithLayout(store, DefaultLayout{})
}

// NewReaderWithLayout creates a Reader with a custom layout strategy.
// Per CONTRACT_READ_API.md, alternative layouts are valid provided:
//   - Manifests remain discoverable via listing
//   - Object paths in manifests are accurate and resolvable
//   - Commit semantics (manifest presence = visibility) are preserved
func NewReaderWithLayout(store lode.Store, layout Layout) *Reader {
	if store == nil {
		panic("read: store is required")
	}
	if layout == nil {
		panic("read: layout is required")
	}
	return &Reader{
		store:   store,
		adapter: NewStoreAdapter(store),
		layout:  layout,
	}
}

// ListDatasets returns all dataset IDs found in storage.
//
// Returns ErrDatasetsNotModeled if the layout doesn't support dataset enumeration
// (e.g., FlatLayout). An empty list is returned only for truly empty storage.
func (r *Reader) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]lode.DatasetID, error) {
	// Check if the layout supports dataset enumeration
	if !r.layout.SupportsDatasetEnumeration() {
		return nil, ErrDatasetsNotModeled
	}

	// List all paths under the datasets prefix
	paths, err := r.store.List(ctx, r.layout.DatasetsPrefix())
	if err != nil {
		return nil, err
	}

	// Extract unique dataset IDs from manifest paths.
	// Only datasets with at least one manifest are considered to exist.
	seen := make(map[lode.DatasetID]bool)
	var datasets []lode.DatasetID

	for _, p := range paths {
		if !r.layout.IsManifest(p) {
			continue
		}

		datasetID := r.layout.ParseDatasetID(p)
		if datasetID == "" || seen[datasetID] {
			continue
		}
		seen[datasetID] = true
		datasets = append(datasets, datasetID)

		if opts.Limit > 0 && len(datasets) >= opts.Limit {
			break
		}
	}

	return datasets, nil
}

// ListPartitions returns partition paths found across all committed segments.
// Returns ErrNotFound if the dataset does not exist.
func (r *Reader) ListPartitions(ctx context.Context, dataset lode.DatasetID, opts PartitionListOptions) ([]PartitionRef, error) {
	// ListSegments returns ErrNotFound if dataset doesn't exist
	segments, err := r.ListSegments(ctx, dataset, "", SegmentListOptions{})
	if err != nil {
		return nil, err
	}

	// Dataset exists but has no segments is now impossible (ErrNotFound would be returned)
	// But handle it defensively
	if len(segments) == 0 {
		return nil, nil
	}

	// Aggregate partitions from all manifests
	seen := make(map[string]bool)
	var partitions []PartitionRef

	for _, seg := range segments {
		manifest, err := r.GetManifest(ctx, dataset, seg)
		if err != nil {
			// Per CONTRACT_READ_API.md: manifests are authoritative.
			// If a manifest exists but cannot be read, return an error.
			return nil, fmt.Errorf("failed to load manifest for segment %s: %w", seg.ID, err)
		}

		for _, f := range manifest.Files {
			partPath := r.layout.ExtractPartitionPath(f.Path)
			if partPath == "" || seen[partPath] {
				continue
			}
			seen[partPath] = true
			partitions = append(partitions, PartitionRef{Path: partPath})

			if opts.Limit > 0 && len(partitions) >= opts.Limit {
				return partitions, nil
			}
		}
	}

	return partitions, nil
}

// ListSegments returns committed segments (snapshots) within a dataset.
// Returns ErrNotFound if the dataset does not exist (has no committed segments).
// When partition is non-empty and the layout supports partition-first access
// (e.g., HiveLayout), uses prefix pruning to list only segments in that partition.
func (r *Reader) ListSegments(ctx context.Context, dataset lode.DatasetID, partition PartitionPath, opts SegmentListOptions) ([]SegmentRef, error) {
	// Use partition-aware prefix for layouts that support it (enables true prefix pruning)
	prefix := r.layout.SegmentsPrefixForPartition(dataset, partition)
	paths, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Extract segment IDs from manifest paths
	// Per CONTRACT_READ_API.md: manifest presence = commit signal
	var segments []SegmentRef
	seen := make(map[lode.SnapshotID]bool)
	hasAnyManifest := false

	for _, p := range paths {
		if !r.layout.IsManifest(p) {
			continue
		}
		hasAnyManifest = true

		segmentID := r.layout.ParseSegmentID(p)
		if segmentID == "" || seen[segmentID] {
			continue
		}

		// Extract partition from manifest path (for partition-first layouts)
		manifestPartition := r.layout.ParsePartitionFromManifest(p)

		// If partition filter is specified and layout doesn't do prefix-based pruning,
		// verify the segment contains data in that partition
		if partition != "" && manifestPartition == "" {
			// Segment-first layout: need to check manifest contents
			manifest, err := r.loadManifest(ctx, p)
			if err != nil {
				// Per CONTRACT_READ_API.md: manifests are authoritative.
				// If a manifest exists but cannot be read, return an error.
				return nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
			}
			if !r.segmentContainsPartition(manifest, string(partition)) {
				continue
			}
		}
		// For partition-first layouts (manifestPartition != ""), the prefix pruning
		// already ensured we're only listing manifests in the target partition

		seen[segmentID] = true
		segments = append(segments, SegmentRef{
			ID:        segmentID,
			Partition: manifestPartition,
		})

		if opts.Limit > 0 && len(segments) >= opts.Limit {
			break
		}
	}

	// Per CONTRACT_READ_API.md: return ErrNotFound if dataset doesn't exist
	// A dataset exists if it has at least one committed segment (manifest)
	if !hasAnyManifest {
		return nil, lode.ErrNotFound
	}

	return segments, nil
}

// segmentContainsPartition checks if a manifest contains files in the given partition.
func (r *Reader) segmentContainsPartition(m *lode.Manifest, partition string) bool {
	for _, f := range m.Files {
		partPath := r.layout.ExtractPartitionPath(f.Path)
		if partPath == partition || strings.HasPrefix(partPath, partition+"/") {
			return true
		}
	}
	return false
}

// GetManifest loads the manifest for a specific segment.
// If seg.Partition is set (for partition-first layouts like HiveLayout),
// the manifest is loaded from the partition-specific path.
func (r *Reader) GetManifest(ctx context.Context, dataset lode.DatasetID, seg SegmentRef) (*lode.Manifest, error) {
	manifestPath := r.layout.ManifestPathInPartition(dataset, seg.ID, seg.Partition)
	return r.loadManifest(ctx, manifestPath)
}

// loadManifest loads, parses, and validates a manifest from the given path.
// Per CONTRACT_READ_API.md, manifests must be readable in a single small object fetch.
func (r *Reader) loadManifest(ctx context.Context, manifestPath string) (*lode.Manifest, error) {
	rc, err := r.store.Get(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	var manifest lode.Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	// Validate manifest per CONTRACT_CORE.md requirements
	if err := ValidateManifest(&manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// OpenObject returns a reader for a data object.
// obj.Path must be the full storage key (as stored in manifest FileRef.Path).
func (r *Reader) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	return r.store.Get(ctx, obj.Path)
}

// ObjectReaderAt returns a random-access reader for a data object.
// obj.Path must be the full storage key (as stored in manifest FileRef.Path).
// Returns ErrRangeReadNotSupported if the underlying store does not support range reads.
// Per CONTRACT_READ_API.md, range reads must be true range reads, not simulated.
func (r *Reader) ObjectReaderAt(ctx context.Context, obj ObjectRef) (ReaderAt, error) {
	return r.adapter.ReaderAt(ctx, ObjectKey(obj.Path))
}

// Ensure Reader implements API
var _ API = (*Reader)(nil)
