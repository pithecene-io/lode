package read

import (
	"path"
	"strings"

	"github.com/justapithecus/lode/lode"
)

// Layout abstracts storage path construction for both reads and writes.
//
// Per CONTRACT_LAYOUT.md, Layout is the unified abstraction that governs:
//   - Path topology for datasets, segments, manifests, and objects
//   - Whether and how partition semantics are encoded into paths
//
// Per CONTRACT_READ_API.md, layouts must ensure:
//   - Manifests remain discoverable via listing
//   - Object paths in manifests are accurate and resolvable
//   - Commit semantics (manifest presence = visibility) are preserved
//
// Alternative layouts (e.g., partitions nested inside segments) are valid
// provided these invariants hold.
type Layout interface {
	// -------------------------------------------------------------------------
	// Capability methods
	// -------------------------------------------------------------------------

	// SupportsDatasetEnumeration returns true if this layout supports listing
	// all datasets via a common prefix. Layouts that don't model datasets as
	// a first-class concept (e.g., FlatLayout) return false.
	//
	// When false, ListDatasets returns ErrDatasetsNotModeled instead of
	// attempting enumeration.
	SupportsDatasetEnumeration() bool

	// SupportsPartitions returns true if this layout supports partitioned data.
	// Layouts that return false (e.g., FlatLayout) should only be used with
	// noop partitioners. Using a non-noop partitioner with such layouts is an error.
	SupportsPartitions() bool

	// -------------------------------------------------------------------------
	// Discovery methods (read-side)
	// -------------------------------------------------------------------------

	// DatasetsPrefix returns the storage prefix for listing all datasets.
	// Only meaningful when SupportsDatasetEnumeration returns true.
	DatasetsPrefix() string

	// SegmentsPrefix returns the storage prefix for listing segments in a dataset.
	SegmentsPrefix(dataset lode.DatasetID) string

	// SegmentsPrefixForPartition returns the storage prefix for listing segments
	// within a specific partition. For partition-first layouts (e.g., HiveLayout),
	// this enables true prefix pruning without scanning all manifests.
	// For segment-first layouts (e.g., DefaultLayout), this returns the same as
	// SegmentsPrefix since partition filtering must happen post-listing.
	// partition may be empty, in which case this behaves like SegmentsPrefix.
	SegmentsPrefixForPartition(dataset lode.DatasetID, partition PartitionPath) string

	// IsManifest returns true if the path is a valid manifest location.
	IsManifest(p string) bool

	// ParseDatasetID extracts the dataset ID from a manifest path.
	// Returns empty string if the path is not a valid manifest path.
	ParseDatasetID(manifestPath string) lode.DatasetID

	// ParseSegmentID extracts the segment ID from a manifest path.
	// Returns empty string if the path is not a valid manifest path.
	ParseSegmentID(manifestPath string) lode.SnapshotID

	// ParsePartitionFromManifest extracts the partition path from a manifest path.
	// For partition-first layouts (e.g., HiveLayout), returns the partition portion.
	// For segment-first layouts (e.g., DefaultLayout), returns empty string.
	ParsePartitionFromManifest(manifestPath string) PartitionPath

	// ExtractPartitionPath extracts the partition path from a file path.
	// Returns empty string if no partition.
	ExtractPartitionPath(filePath string) string

	// -------------------------------------------------------------------------
	// Path construction methods (write-side)
	// -------------------------------------------------------------------------

	// ManifestPath returns the storage path for a segment's manifest.
	// For segment-first layouts, this is the canonical location.
	// For partition-first layouts, this returns the unpartitioned fallback.
	ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string

	// ManifestPathInPartition returns the manifest path for a segment within a partition.
	// For partition-first layouts (e.g., HiveLayout), manifests are placed under
	// partition prefixes to enable true prefix pruning.
	// For segment-first layouts (e.g., DefaultLayout), this returns the same as
	// ManifestPath (partition is ignored).
	ManifestPathInPartition(dataset lode.DatasetID, segment lode.SnapshotID, partition PartitionPath) string

	// DataFilePath returns the storage path for a data file within a segment.
	// partition may be empty for unpartitioned data.
	// filename is the base name of the data file (e.g., "data.jsonl.gz").
	DataFilePath(dataset lode.DatasetID, segment lode.SnapshotID, partition, filename string) string
}

// DefaultLayout implements the reference layout from CONTRACT_READ_API.md:
//
//	/datasets/<dataset>/snapshots/<segment_id>/
//	  manifest.json
//	  /data/
//	    [partition/]filename
//
// This layout nests partitions inside data directories within segments.
type DefaultLayout struct{}

// Default layout constants.
const (
	defaultDatasetsDir  = "datasets"
	defaultSnapshotsDir = "snapshots"
	defaultManifestFile = "manifest.json"
	defaultDataDir      = "data"
)

// SupportsDatasetEnumeration returns true - DefaultLayout has a datasets prefix.
func (l DefaultLayout) SupportsDatasetEnumeration() bool {
	return true
}

// SupportsPartitions returns true - DefaultLayout supports partitions nested in data/.
func (l DefaultLayout) SupportsPartitions() bool {
	return true
}

// DatasetsPrefix returns "datasets/".
func (l DefaultLayout) DatasetsPrefix() string {
	return defaultDatasetsDir + "/"
}

// SegmentsPrefix returns "datasets/<dataset>/snapshots/".
func (l DefaultLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir) + "/"
}

// SegmentsPrefixForPartition returns "datasets/<dataset>/snapshots/".
// DefaultLayout is segment-first, so partition filtering happens post-listing.
func (l DefaultLayout) SegmentsPrefixForPartition(dataset lode.DatasetID, _ PartitionPath) string {
	return l.SegmentsPrefix(dataset)
}

// ManifestPath returns "datasets/<dataset>/snapshots/<segment>/manifest.json".
func (l DefaultLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir, string(segment), defaultManifestFile)
}

// ManifestPathInPartition returns the same as ManifestPath.
// DefaultLayout is segment-first, so partition does not affect manifest placement.
func (l DefaultLayout) ManifestPathInPartition(dataset lode.DatasetID, segment lode.SnapshotID, _ PartitionPath) string {
	return l.ManifestPath(dataset, segment)
}

// IsManifest returns true if the path matches the canonical manifest location:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// This is path-aware to prevent stray manifest.json files from polluting
// dataset discovery. Per CONTRACT_READ_API.md, "manifest presence = commit signal"
// applies only to manifests in the correct location.
func (l DefaultLayout) IsManifest(p string) bool {
	return l.isValidManifestPath(p)
}

// ParseDatasetID extracts dataset ID from path format:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// Returns empty string if the path doesn't match the canonical layout.
// This ensures only manifests in /snapshots/ directories count toward
// dataset existence, per "manifest presence = commit signal" rule.
func (l DefaultLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	if !l.isValidManifestPath(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.DatasetID(parts[1])
}

// ParseSegmentID extracts segment ID from path format:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// Returns empty string if the path doesn't match the canonical layout.
func (l DefaultLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	if !l.isValidManifestPath(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.SnapshotID(parts[3])
}

// ParsePartitionFromManifest returns empty string.
// DefaultLayout is segment-first; manifests are not under partition prefixes.
func (l DefaultLayout) ParsePartitionFromManifest(_ string) PartitionPath {
	return ""
}

// isValidManifestPath checks if path matches:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
func (l DefaultLayout) isValidManifestPath(p string) bool {
	parts := strings.Split(p, "/")
	// Must be exactly: datasets / <dataset> / snapshots / <segment> / manifest.json
	if len(parts) != 5 {
		return false
	}
	return parts[0] == defaultDatasetsDir &&
		parts[1] != "" &&
		parts[2] == defaultSnapshotsDir &&
		parts[3] != "" &&
		parts[4] == defaultManifestFile
}

// ExtractPartitionPath extracts the partition path from a file path.
// File paths have format: datasets/<id>/snapshots/<id>/data/[partition/]filename
// Returns empty string if no partition.
func (l DefaultLayout) ExtractPartitionPath(filePath string) string {
	parts := strings.Split(filePath, "/")

	// Find the "data" component
	dataIdx := -1
	for i, p := range parts {
		if p == defaultDataDir {
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

// DataFilePath returns "datasets/<dataset>/snapshots/<segment>/data/[partition/]filename".
func (l DefaultLayout) DataFilePath(dataset lode.DatasetID, segment lode.SnapshotID, partition, filename string) string {
	if partition == "" {
		return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir, string(segment), defaultDataDir, filename)
	}
	return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir, string(segment), defaultDataDir, partition, filename)
}

// Ensure DefaultLayout implements Layout.
var _ Layout = DefaultLayout{}

// -----------------------------------------------------------------------------
// HiveLayout
// -----------------------------------------------------------------------------

// HiveLayout implements a partition-first layout where partitions exist at the
// dataset level and segments are nested within partitions:
//
//	/datasets/<dataset>/partitions/<k=v>/<k=v>/segments/<segment_id>/
//	  manifest.json
//	  /data/
//	    filename
//
// This layout is optimized for partition pruning at the storage layer, as each
// partition is a separate prefix. For unpartitioned data, files go directly in
// a "segments" directory without partition nesting.
//
// Per CONTRACT_READ_API.md, this is an alternative layout valid provided:
//   - Manifests remain discoverable via listing
//   - Object paths in manifests are accurate and resolvable
//   - Commit semantics (manifest presence = visibility) are preserved
type HiveLayout struct{}

// Hive layout constants.
const (
	hivePartitionsDir = "partitions"
	hiveSegmentsDir   = "segments"
)

// SupportsDatasetEnumeration returns true - HiveLayout has a datasets prefix.
func (l HiveLayout) SupportsDatasetEnumeration() bool {
	return true
}

// SupportsPartitions returns true - HiveLayout is optimized for partitioned data.
func (l HiveLayout) SupportsPartitions() bool {
	return true
}

// DatasetsPrefix returns "datasets/".
func (l HiveLayout) DatasetsPrefix() string {
	return defaultDatasetsDir + "/"
}

// SegmentsPrefix returns "datasets/<dataset>/".
// For HiveLayout, we must list the entire dataset to find all segments
// across all partitions.
func (l HiveLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	return path.Join(defaultDatasetsDir, string(dataset)) + "/"
}

// SegmentsPrefixForPartition returns a partition-specific prefix for true prefix pruning.
// When partition is non-empty: "datasets/<dataset>/partitions/<partition>/segments/"
// When partition is empty: "datasets/<dataset>/" (list all segments)
// This enables partition-first layouts to avoid scanning all manifests.
func (l HiveLayout) SegmentsPrefixForPartition(dataset lode.DatasetID, partition PartitionPath) string {
	if partition == "" {
		return l.SegmentsPrefix(dataset)
	}
	// True prefix pruning: list only segments within the specified partition
	return path.Join(defaultDatasetsDir, string(dataset), hivePartitionsDir, string(partition), hiveSegmentsDir) + "/"
}

// ManifestPath returns the path for a segment's manifest (unpartitioned case).
// For HiveLayout, this returns: datasets/<dataset>/segments/<segment>/manifest.json
// For partitioned manifests, use ManifestPathInPartition.
func (l HiveLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	return path.Join(defaultDatasetsDir, string(dataset), hiveSegmentsDir, string(segment), defaultManifestFile)
}

// ManifestPathInPartition returns the manifest path within a partition.
// When partition is non-empty: datasets/<dataset>/partitions/<partition>/segments/<segment>/manifest.json
// When partition is empty: datasets/<dataset>/segments/<segment>/manifest.json
// This enables true prefix pruning for HiveLayout.
func (l HiveLayout) ManifestPathInPartition(dataset lode.DatasetID, segment lode.SnapshotID, partition PartitionPath) string {
	if partition == "" {
		return l.ManifestPath(dataset, segment)
	}
	return path.Join(defaultDatasetsDir, string(dataset), hivePartitionsDir, string(partition), hiveSegmentsDir, string(segment), defaultManifestFile)
}

// IsManifest returns true if the path is a valid HiveLayout manifest location.
// Valid patterns:
//   - datasets/<dataset>/segments/<segment>/manifest.json (unpartitioned)
//   - datasets/<dataset>/partitions/.../segments/<segment>/manifest.json (partitioned)
func (l HiveLayout) IsManifest(p string) bool {
	parts := strings.Split(p, "/")
	if len(parts) < 4 {
		return false
	}

	// Must start with datasets/<dataset>
	if parts[0] != defaultDatasetsDir || parts[1] == "" {
		return false
	}

	// Must end with segments/<segment>/manifest.json
	if parts[len(parts)-1] != defaultManifestFile {
		return false
	}

	// Find "segments" - it must be followed by exactly <segment>/manifest.json
	for i := 2; i < len(parts)-2; i++ {
		if parts[i] == hiveSegmentsDir && parts[i+2] == defaultManifestFile {
			return parts[i+1] != "" // segment ID must be non-empty
		}
	}

	return false
}

// ParseDatasetID extracts dataset ID from a HiveLayout manifest path.
func (l HiveLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	if !l.IsManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.DatasetID(parts[1])
}

// ParseSegmentID extracts segment ID from a HiveLayout manifest path.
func (l HiveLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	if !l.IsManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")

	// Find "segments" and return the next part
	for i := 2; i < len(parts)-2; i++ {
		if parts[i] == hiveSegmentsDir {
			return lode.SnapshotID(parts[i+1])
		}
	}
	return ""
}

// ParsePartitionFromManifest extracts partition from a HiveLayout manifest path.
// For paths like: datasets/<ds>/partitions/<partition>/segments/<seg>/manifest.json
// Returns the partition portion between "partitions/" and "segments/".
// Returns empty string for unpartitioned paths.
func (l HiveLayout) ParsePartitionFromManifest(manifestPath string) PartitionPath {
	if !l.IsManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")

	// Find "partitions" start index
	partitionsIdx := -1
	for i := 2; i < len(parts); i++ {
		if parts[i] == hivePartitionsDir {
			partitionsIdx = i
			break
		}
	}

	if partitionsIdx < 0 {
		return "" // no partitions in path
	}

	// Find "segments" end index
	segmentsIdx := -1
	for i := partitionsIdx + 1; i < len(parts); i++ {
		if parts[i] == hiveSegmentsDir {
			segmentsIdx = i
			break
		}
	}

	if segmentsIdx < 0 || segmentsIdx <= partitionsIdx+1 {
		return "" // no partition values between partitions/ and segments/
	}

	// Partition path is everything between partitions/ and segments/
	return PartitionPath(strings.Join(parts[partitionsIdx+1:segmentsIdx], "/"))
}

// ExtractPartitionPath extracts the partition path from a HiveLayout file path.
// For HiveLayout, partitions are between datasets/<dataset>/partitions/ and /segments/
func (l HiveLayout) ExtractPartitionPath(filePath string) string {
	parts := strings.Split(filePath, "/")

	// Find "partitions" start index
	partitionsIdx := -1
	for i := 2; i < len(parts); i++ {
		if parts[i] == hivePartitionsDir {
			partitionsIdx = i
			break
		}
	}

	if partitionsIdx < 0 {
		return "" // no partitions
	}

	// Find "segments" end index
	segmentsIdx := -1
	for i := partitionsIdx + 1; i < len(parts); i++ {
		if parts[i] == hiveSegmentsDir {
			segmentsIdx = i
			break
		}
	}

	if segmentsIdx < 0 || segmentsIdx <= partitionsIdx+1 {
		return "" // no partition values between partitions/ and segments/
	}

	// Partition path is everything between partitions/ and segments/
	return strings.Join(parts[partitionsIdx+1:segmentsIdx], "/")
}

// DataFilePath returns the path for a data file in HiveLayout.
// Structure: datasets/<dataset>/[partitions/<k=v>/...]segments/<segment>/data/filename
func (l HiveLayout) DataFilePath(dataset lode.DatasetID, segment lode.SnapshotID, partition, filename string) string {
	if partition == "" {
		return path.Join(defaultDatasetsDir, string(dataset), hiveSegmentsDir, string(segment), defaultDataDir, filename)
	}
	return path.Join(defaultDatasetsDir, string(dataset), hivePartitionsDir, partition, hiveSegmentsDir, string(segment), defaultDataDir, filename)
}

// Ensure HiveLayout implements Layout.
var _ Layout = HiveLayout{}

// -----------------------------------------------------------------------------
// FlatLayout
// -----------------------------------------------------------------------------

// FlatLayout implements a minimal flat layout for simple use cases:
//
//	/<dataset>/<segment>/
//	  manifest.json
//	  /data/
//	    filename
//
// This layout has no partition support and no "datasets" prefix, making it
// simpler for single-dataset scenarios or testing.
type FlatLayout struct{}

// SupportsDatasetEnumeration returns false - FlatLayout has no common datasets prefix.
// Datasets in FlatLayout are at the root level and cannot be enumerated without
// listing the entire storage.
func (l FlatLayout) SupportsDatasetEnumeration() bool {
	return false
}

// SupportsPartitions returns false - FlatLayout does not support partitioned data.
// Using a non-noop partitioner with FlatLayout is a configuration error.
func (l FlatLayout) SupportsPartitions() bool {
	return false
}

// DatasetsPrefix returns "" (not meaningful for FlatLayout).
// Check SupportsDatasetEnumeration before using this.
func (l FlatLayout) DatasetsPrefix() string {
	return ""
}

// SegmentsPrefix returns "<dataset>/".
func (l FlatLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	return string(dataset) + "/"
}

// SegmentsPrefixForPartition returns "<dataset>/".
// FlatLayout doesn't support partitions, so partition parameter is ignored.
func (l FlatLayout) SegmentsPrefixForPartition(dataset lode.DatasetID, _ PartitionPath) string {
	return l.SegmentsPrefix(dataset)
}

// ManifestPath returns "<dataset>/<segment>/manifest.json".
func (l FlatLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	return path.Join(string(dataset), string(segment), defaultManifestFile)
}

// ManifestPathInPartition returns the same as ManifestPath.
// FlatLayout doesn't support partitions.
func (l FlatLayout) ManifestPathInPartition(dataset lode.DatasetID, segment lode.SnapshotID, _ PartitionPath) string {
	return l.ManifestPath(dataset, segment)
}

// IsManifest returns true if path matches <dataset>/<segment>/manifest.json.
func (l FlatLayout) IsManifest(p string) bool {
	parts := strings.Split(p, "/")
	return len(parts) == 3 &&
		parts[0] != "" &&
		parts[1] != "" &&
		parts[2] == defaultManifestFile
}

// ParseDatasetID extracts dataset ID from flat layout path.
func (l FlatLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	if !l.IsManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.DatasetID(parts[0])
}

// ParseSegmentID extracts segment ID from flat layout path.
func (l FlatLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	if !l.IsManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.SnapshotID(parts[1])
}

// ParsePartitionFromManifest returns empty string.
// FlatLayout doesn't support partitions.
func (l FlatLayout) ParsePartitionFromManifest(_ string) PartitionPath {
	return ""
}

// ExtractPartitionPath returns "" (flat layout has no partition support).
func (l FlatLayout) ExtractPartitionPath(_ string) string {
	return ""
}

// DataFilePath returns "<dataset>/<segment>/data/filename".
// Partition is ignored in FlatLayout.
func (l FlatLayout) DataFilePath(dataset lode.DatasetID, segment lode.SnapshotID, _, filename string) string {
	return path.Join(string(dataset), string(segment), defaultDataDir, filename)
}

// Ensure FlatLayout implements Layout.
var _ Layout = FlatLayout{}
