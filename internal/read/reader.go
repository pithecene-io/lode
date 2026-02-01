package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/justapithecus/lode/lode"
)

// Layout constants matching internal/dataset layout.
const (
	datasetsDir     = "datasets"
	snapshotsDir    = "snapshots"
	manifestFile    = "manifest.json"
	dataDir         = "data"
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
}

// NewReader creates a Reader backed by the given store.
// If the store supports range reads (e.g., storage.FS or storage.Memory),
// those capabilities will be automatically detected and enabled.
func NewReader(store lode.Store) *Reader {
	if store == nil {
		panic("read: store is required")
	}
	return &Reader{
		store:   store,
		adapter: NewStoreAdapter(store),
	}
}

// ListDatasets returns all dataset IDs found in storage.
func (r *Reader) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]lode.DatasetID, error) {
	// List all paths under datasets/
	paths, err := r.store.List(ctx, datasetsDir+"/")
	if err != nil {
		return nil, err
	}

	// Extract unique dataset IDs from manifest paths.
	// Only datasets with at least one manifest are considered to exist.
	seen := make(map[lode.DatasetID]bool)
	var datasets []lode.DatasetID

	for _, p := range paths {
		if path.Base(p) != manifestFile {
			continue
		}

		// Path format: datasets/<dataset_id>/snapshots/<snapshot_id>/manifest.json
		parts := strings.Split(p, "/")
		if len(parts) < 4 || parts[0] != datasetsDir {
			continue
		}

		datasetID := lode.DatasetID(parts[1])
		if seen[datasetID] {
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
func (r *Reader) ListPartitions(ctx context.Context, dataset lode.DatasetID, opts PartitionListOptions) ([]PartitionRef, error) {
	// First verify dataset exists by listing segments
	segments, err := r.ListSegments(ctx, dataset, "", SegmentListOptions{})
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		// Dataset exists but has no committed segments
		return nil, nil
	}

	// Aggregate partitions from all manifests
	seen := make(map[string]bool)
	var partitions []PartitionRef

	for _, seg := range segments {
		manifest, err := r.GetManifest(ctx, dataset, seg)
		if err != nil {
			// Skip segments with unreadable manifests
			continue
		}

		for _, f := range manifest.Files {
			partPath := extractPartitionPath(f.Path)
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

// extractPartitionPath extracts the partition path component from a file path.
// File paths have format: datasets/<id>/snapshots/<id>/data/[partition/]filename
// Returns empty string if no partition.
func extractPartitionPath(filePath string) string {
	parts := strings.Split(filePath, "/")

	// Find the "data" component
	dataIdx := -1
	for i, p := range parts {
		if p == dataDir {
			dataIdx = i
			break
		}
	}

	if dataIdx < 0 || dataIdx >= len(parts)-1 {
		return ""
	}

	// Everything between "data" and the filename is the partition path
	partParts := parts[dataIdx+1 : len(parts)-1]
	if len(partParts) == 0 {
		return ""
	}

	return strings.Join(partParts, "/")
}

// ListSegments returns committed segments (snapshots) within a dataset.
func (r *Reader) ListSegments(ctx context.Context, dataset lode.DatasetID, partition PartitionPath, opts SegmentListOptions) ([]SegmentRef, error) {
	// List all paths under the dataset's snapshots directory
	prefix := path.Join(datasetsDir, string(dataset), snapshotsDir) + "/"
	paths, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Extract segment IDs from manifest paths
	// Per CONTRACT_READ_API.md: manifest presence = commit signal
	var segments []SegmentRef
	seen := make(map[lode.SnapshotID]bool)

	for _, p := range paths {
		if path.Base(p) != manifestFile {
			continue
		}

		// Path format: datasets/<dataset_id>/snapshots/<snapshot_id>/manifest.json
		dir := path.Dir(p)
		segmentID := lode.SnapshotID(path.Base(dir))

		if seen[segmentID] {
			continue
		}

		// If partition filter is specified, check if segment contains it
		if partition != "" {
			manifest, err := r.loadManifest(ctx, p)
			if err != nil {
				continue
			}
			if !segmentContainsPartition(manifest, string(partition)) {
				continue
			}
		}

		seen[segmentID] = true
		segments = append(segments, SegmentRef{ID: segmentID})

		if opts.Limit > 0 && len(segments) >= opts.Limit {
			break
		}
	}

	return segments, nil
}

// segmentContainsPartition checks if a manifest contains files in the given partition.
func segmentContainsPartition(m *lode.Manifest, partition string) bool {
	for _, f := range m.Files {
		partPath := extractPartitionPath(f.Path)
		if partPath == partition || strings.HasPrefix(partPath, partition+"/") {
			return true
		}
	}
	return false
}

// GetManifest loads the manifest for a specific segment.
func (r *Reader) GetManifest(ctx context.Context, dataset lode.DatasetID, seg SegmentRef) (*lode.Manifest, error) {
	manifestPath := path.Join(datasetsDir, string(dataset), snapshotsDir, string(seg.ID), manifestFile)
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
func (r *Reader) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	// Construct the full path from ObjectRef
	objPath := path.Join(datasetsDir, string(obj.Dataset), snapshotsDir, string(obj.Segment.ID), obj.Path)
	return r.store.Get(ctx, objPath)
}

// ObjectReaderAt returns a random-access reader for a data object.
// Returns ErrRangeReadNotSupported if the underlying store does not support range reads.
// Per CONTRACT_READ_API.md, range reads must be true range reads, not simulated.
func (r *Reader) ObjectReaderAt(ctx context.Context, obj ObjectRef) (ReaderAt, error) {
	objPath := path.Join(datasetsDir, string(obj.Dataset), snapshotsDir, string(obj.Segment.ID), obj.Path)
	return r.adapter.ReaderAt(ctx, ObjectKey(objPath))
}

// Ensure Reader implements API
var _ API = (*Reader)(nil)
