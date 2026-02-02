package read

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/justapithecus/lode/lode"
)

func TestDefaultLayout_DatasetsPrefix(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.DatasetsPrefix()
	want := "datasets/"
	if got != want {
		t.Errorf("DatasetsPrefix() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_SegmentsPrefix(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.SegmentsPrefix("my-dataset")
	want := "datasets/my-dataset/snapshots/"
	if got != want {
		t.Errorf("SegmentsPrefix() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_ManifestPath(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.ManifestPath("my-dataset", "snap-1")
	want := "datasets/my-dataset/snapshots/snap-1/manifest.json"
	if got != want {
		t.Errorf("ManifestPath() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_IsManifest(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want bool
	}{
		// Valid canonical paths
		{"datasets/foo/snapshots/bar/manifest.json", true},
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", true},
		// Invalid: wrong structure (stray manifests)
		{"manifest.json", false},
		{"some/path/manifest.json", false},
		{"datasets/foo/misc/manifest.json", false},
		{"datasets/foo/snapshots/manifest.json", false}, // missing segment
		{"datasets/foo/snapshots/bar/data/manifest.json", false}, // too deep
		// Invalid: wrong filename
		{"datasets/foo/snapshots/bar/data.json", false},
		{"datasets/foo/snapshots/bar/manifest.txt", false},
		// Invalid: empty
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.IsManifest(tt.path)
			if got != tt.want {
				t.Errorf("IsManifest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseDatasetID(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.DatasetID
	}{
		// Valid canonical paths
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", "my-dataset"},
		{"datasets/foo/snapshots/bar/manifest.json", "foo"},
		// Invalid: wrong structure
		{"snapshots/bar/manifest.json", ""},
		{"datasets/manifest.json", ""},
		{"datasets/foo/misc/manifest.json", ""},         // missing /snapshots/
		{"datasets/foo/snapshots/manifest.json", ""},    // missing segment
		{"datasets/foo/snapshots/bar/data/manifest.json", ""}, // too deep
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseDatasetID(tt.path)
			if got != tt.want {
				t.Errorf("ParseDatasetID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseSegmentID(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.SnapshotID
	}{
		// Valid canonical paths
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", "snap-1"},
		{"datasets/foo/snapshots/bar/manifest.json", "bar"},
		// Invalid: wrong structure
		{"some/path/seg-id/manifest.json", ""},
		{"datasets/foo/misc/manifest.json", ""},
		{"datasets/foo/snapshots/manifest.json", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseSegmentID(tt.path)
			if got != tt.want {
				t.Errorf("ParseSegmentID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_DataFilePath(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		dataset   lode.DatasetID
		segment   lode.SnapshotID
		partition string
		filename  string
		want      string
	}{
		// Without partition
		{"ds", "snap", "", "data.jsonl", "datasets/ds/snapshots/snap/data/data.jsonl"},
		{"mydata", "s1", "", "data.gz", "datasets/mydata/snapshots/s1/data/data.gz"},
		// With partition
		{"ds", "snap", "day=2024-01-01", "data.jsonl", "datasets/ds/snapshots/snap/data/day=2024-01-01/data.jsonl"},
		{"ds", "snap", "day=2024-01-01/hour=12", "data.gz", "datasets/ds/snapshots/snap/data/day=2024-01-01/hour=12/data.gz"},
	}

	for _, tt := range tests {
		name := tt.want
		t.Run(name, func(t *testing.T) {
			got := layout.DataFilePath(tt.dataset, tt.segment, tt.partition, tt.filename)
			if got != tt.want {
				t.Errorf("DataFilePath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ExtractPartitionPath(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want string
	}{
		// With partition
		{"datasets/ds/snapshots/snap/data/day=2024-01-01/file.json", "day=2024-01-01"},
		{"datasets/ds/snapshots/snap/data/day=2024-01-01/hour=12/file.json", "day=2024-01-01/hour=12"},
		// Without partition
		{"datasets/ds/snapshots/snap/data/file.json", ""},
		// No data directory
		{"datasets/ds/snapshots/snap/file.json", ""},
		// Empty
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ExtractPartitionPath(tt.path)
			if got != tt.want {
				t.Errorf("ExtractPartitionPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNewReaderWithLayout_NilLayout(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewReaderWithLayout(store, nil) did not panic")
		}
	}()

	store := &mockStore{}
	NewReaderWithLayout(store, nil)
}

// mockStore implements lode.Store for testing
type mockStore struct{}

func (m *mockStore) Put(_ context.Context, _ string, _ io.Reader) error     { return nil }
func (m *mockStore) Get(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (m *mockStore) Exists(_ context.Context, _ string) (bool, error)       { return false, nil }
func (m *mockStore) List(_ context.Context, _ string) ([]string, error)     { return nil, nil }
func (m *mockStore) Delete(_ context.Context, _ string) error               { return nil }

// -----------------------------------------------------------------------------
// HiveLayout Tests
// -----------------------------------------------------------------------------

func TestHiveLayout_DatasetsPrefix(t *testing.T) {
	layout := HiveLayout{}
	got := layout.DatasetsPrefix()
	want := "datasets/"
	if got != want {
		t.Errorf("DatasetsPrefix() = %q, want %q", got, want)
	}
}

func TestHiveLayout_SegmentsPrefix(t *testing.T) {
	layout := HiveLayout{}
	got := layout.SegmentsPrefix("mydata")
	want := "datasets/mydata/"
	if got != want {
		t.Errorf("SegmentsPrefix() = %q, want %q", got, want)
	}
}

func TestHiveLayout_ManifestPath(t *testing.T) {
	layout := HiveLayout{}
	got := layout.ManifestPath("mydata", "snap-1")
	want := "datasets/mydata/segments/snap-1/manifest.json"
	if got != want {
		t.Errorf("ManifestPath() = %q, want %q", got, want)
	}
}

func TestHiveLayout_IsManifest(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		path string
		want bool
	}{
		// Valid unpartitioned
		{"datasets/ds/segments/seg/manifest.json", true},
		// Valid partitioned
		{"datasets/ds/partitions/day=2024/segments/seg/manifest.json", true},
		{"datasets/ds/partitions/day=2024/hour=12/segments/seg/manifest.json", true},
		// Invalid
		{"datasets/ds/snapshots/seg/manifest.json", false}, // wrong dir name
		{"datasets/ds/segments/manifest.json", false},      // missing segment ID
		{"ds/segments/seg/manifest.json", false},           // missing datasets/
		{"manifest.json", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.IsManifest(tt.path)
			if got != tt.want {
				t.Errorf("IsManifest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestHiveLayout_ParseDatasetID(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		path string
		want lode.DatasetID
	}{
		{"datasets/ds/segments/seg/manifest.json", "ds"},
		{"datasets/mydata/partitions/day=2024/segments/s1/manifest.json", "mydata"},
		{"invalid/path", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseDatasetID(tt.path)
			if got != tt.want {
				t.Errorf("ParseDatasetID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestHiveLayout_ParseSegmentID(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		path string
		want lode.SnapshotID
	}{
		{"datasets/ds/segments/seg/manifest.json", "seg"},
		{"datasets/ds/partitions/day=2024/segments/s1/manifest.json", "s1"},
		{"invalid/path", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseSegmentID(tt.path)
			if got != tt.want {
				t.Errorf("ParseSegmentID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestHiveLayout_ExtractPartitionPath(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		path string
		want string
	}{
		// With partition
		{"datasets/ds/partitions/day=2024/segments/seg/data/f.json", "day=2024"},
		{"datasets/ds/partitions/day=2024/hour=12/segments/seg/data/f.json", "day=2024/hour=12"},
		// Without partition
		{"datasets/ds/segments/seg/data/f.json", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ExtractPartitionPath(tt.path)
			if got != tt.want {
				t.Errorf("ExtractPartitionPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestHiveLayout_DataFilePath(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		partition string
		want      string
	}{
		{"", "datasets/ds/segments/seg/data/file.json"},
		{"day=2024", "datasets/ds/partitions/day=2024/segments/seg/data/file.json"},
		{"day=2024/hour=12", "datasets/ds/partitions/day=2024/hour=12/segments/seg/data/file.json"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := layout.DataFilePath("ds", "seg", tt.partition, "file.json")
			if got != tt.want {
				t.Errorf("DataFilePath() = %q, want %q", got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// HiveLayout Prefix Pruning Tests
// Verify true prefix pruning without scanning all manifests
// -----------------------------------------------------------------------------

func TestHiveLayout_SegmentsPrefixForPartition_TruePrefixPruning(t *testing.T) {
	layout := HiveLayout{}
	tests := []struct {
		dataset   lode.DatasetID
		partition PartitionPath
		want      string
		desc      string
	}{
		// Without partition - list entire dataset
		{"mydata", "", "datasets/mydata/", "empty partition lists all"},
		// With single partition - true prefix pruning
		{"mydata", "day=2024-01-01", "datasets/mydata/partitions/day=2024-01-01/segments/", "single partition"},
		// With nested partitions - true prefix pruning
		{"mydata", "day=2024-01-01/hour=12", "datasets/mydata/partitions/day=2024-01-01/hour=12/segments/", "nested partitions"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := layout.SegmentsPrefixForPartition(tt.dataset, tt.partition)
			if got != tt.want {
				t.Errorf("SegmentsPrefixForPartition(%q, %q) = %q, want %q",
					tt.dataset, tt.partition, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_SegmentsPrefixForPartition_NoChange(t *testing.T) {
	layout := DefaultLayout{}

	// DefaultLayout should return same prefix regardless of partition
	// (partition filtering happens post-listing)
	withPartition := layout.SegmentsPrefixForPartition("mydata", "day=2024-01-01")
	withoutPartition := layout.SegmentsPrefixForPartition("mydata", "")
	expected := "datasets/mydata/snapshots/"

	if withPartition != expected {
		t.Errorf("SegmentsPrefixForPartition with partition = %q, want %q", withPartition, expected)
	}
	if withoutPartition != expected {
		t.Errorf("SegmentsPrefixForPartition without partition = %q, want %q", withoutPartition, expected)
	}
}

func TestFlatLayout_SegmentsPrefixForPartition_NoChange(t *testing.T) {
	layout := FlatLayout{}

	// FlatLayout should return same prefix regardless of partition
	// (FlatLayout doesn't support partitions)
	withPartition := layout.SegmentsPrefixForPartition("mydata", "day=2024-01-01")
	withoutPartition := layout.SegmentsPrefixForPartition("mydata", "")
	expected := "mydata/"

	if withPartition != expected {
		t.Errorf("SegmentsPrefixForPartition with partition = %q, want %q", withPartition, expected)
	}
	if withoutPartition != expected {
		t.Errorf("SegmentsPrefixForPartition without partition = %q, want %q", withoutPartition, expected)
	}
}

// -----------------------------------------------------------------------------
// FlatLayout Tests
// -----------------------------------------------------------------------------

func TestFlatLayout_SupportsDatasetEnumeration(t *testing.T) {
	layout := FlatLayout{}
	if layout.SupportsDatasetEnumeration() {
		t.Error("FlatLayout.SupportsDatasetEnumeration() = true, want false")
	}
}

func TestFlatLayout_DatasetsPrefix(t *testing.T) {
	layout := FlatLayout{}
	got := layout.DatasetsPrefix()
	if got != "" {
		t.Errorf("DatasetsPrefix() = %q, want empty", got)
	}
}

func TestFlatLayout_SegmentsPrefix(t *testing.T) {
	layout := FlatLayout{}
	got := layout.SegmentsPrefix("mydata")
	want := "mydata/"
	if got != want {
		t.Errorf("SegmentsPrefix() = %q, want %q", got, want)
	}
}

func TestFlatLayout_ManifestPath(t *testing.T) {
	layout := FlatLayout{}
	got := layout.ManifestPath("mydata", "snap-1")
	want := "mydata/snap-1/manifest.json"
	if got != want {
		t.Errorf("ManifestPath() = %q, want %q", got, want)
	}
}

func TestFlatLayout_IsManifest(t *testing.T) {
	layout := FlatLayout{}
	tests := []struct {
		path string
		want bool
	}{
		{"ds/seg/manifest.json", true},
		{"mydata/snap-1/manifest.json", true},
		// Invalid
		{"datasets/ds/seg/manifest.json", false}, // too many parts
		{"ds/manifest.json", false},               // missing segment
		{"manifest.json", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.IsManifest(tt.path)
			if got != tt.want {
				t.Errorf("IsManifest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestFlatLayout_ParseDatasetID(t *testing.T) {
	layout := FlatLayout{}
	tests := []struct {
		path string
		want lode.DatasetID
	}{
		{"ds/seg/manifest.json", "ds"},
		{"mydata/s1/manifest.json", "mydata"},
		{"invalid", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseDatasetID(tt.path)
			if got != tt.want {
				t.Errorf("ParseDatasetID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestFlatLayout_DataFilePath(t *testing.T) {
	layout := FlatLayout{}
	// Partition is ignored in FlatLayout
	got := layout.DataFilePath("ds", "seg", "ignored", "file.json")
	want := "ds/seg/data/file.json"
	if got != want {
		t.Errorf("DataFilePath() = %q, want %q", got, want)
	}
}

// -----------------------------------------------------------------------------
// Custom Layout Tests - Prove Reader honors custom layouts
// -----------------------------------------------------------------------------

// customLayout implements Layout with a different path structure:
// custom/<dataset>/segs/<segment>/meta.json
type customLayout struct {
	datasetsPrefix string
	listCalls      []string
	getManifestReq []string
}

func (c *customLayout) SupportsDatasetEnumeration() bool {
	return c.datasetsPrefix != ""
}

func (c *customLayout) SupportsPartitions() bool {
	return true // Custom layout supports partitions for testing
}

func (c *customLayout) DatasetsPrefix() string {
	return c.datasetsPrefix
}

func (c *customLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	prefix := "custom/" + string(dataset) + "/segs/"
	c.listCalls = append(c.listCalls, prefix)
	return prefix
}

func (c *customLayout) SegmentsPrefixForPartition(dataset lode.DatasetID, _ PartitionPath) string {
	return c.SegmentsPrefix(dataset)
}

func (c *customLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	path := "custom/" + string(dataset) + "/segs/" + string(segment) + "/meta.json"
	c.getManifestReq = append(c.getManifestReq, path)
	return path
}

func (c *customLayout) ManifestPathInPartition(dataset lode.DatasetID, segment lode.SnapshotID, _ PartitionPath) string {
	return c.ManifestPath(dataset, segment)
}

func (c *customLayout) IsManifest(p string) bool {
	// Custom layout uses meta.json instead of manifest.json
	parts := splitPath(p)
	if len(parts) != 5 {
		return false
	}
	return parts[0] == "custom" && parts[2] == "segs" && parts[4] == "meta.json"
}

func (c *customLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	parts := splitPath(manifestPath)
	if len(parts) != 5 || parts[0] != "custom" || parts[2] != "segs" || parts[4] != "meta.json" {
		return ""
	}
	return lode.DatasetID(parts[1])
}

func (c *customLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	parts := splitPath(manifestPath)
	if len(parts) != 5 || parts[0] != "custom" || parts[2] != "segs" || parts[4] != "meta.json" {
		return ""
	}
	return lode.SnapshotID(parts[3])
}

func (c *customLayout) ParsePartitionFromManifest(_ string) PartitionPath {
	return "" // Custom layout doesn't support partitions
}

func (c *customLayout) ExtractPartitionPath(_ string) string {
	return "" // Custom layout doesn't support partitions
}

func (c *customLayout) DataFilePath(dataset lode.DatasetID, segment lode.SnapshotID, partition, filename string) string {
	// custom/<dataset>/segs/<segment>/files/[partition/]filename
	base := "custom/" + string(dataset) + "/segs/" + string(segment) + "/files/"
	if partition == "" {
		return base + filename
	}
	return base + partition + "/" + filename
}

func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	var parts []string
	for _, part := range strings.Split(p, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

// trackingStore records calls for verification
type trackingStore struct {
	listPrefix string
	listResult []string
	getCalls   []string
}

func (t *trackingStore) Put(_ context.Context, _ string, _ io.Reader) error { return nil }
func (t *trackingStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	t.getCalls = append(t.getCalls, path)
	return nil, lode.ErrNotFound
}
func (t *trackingStore) Exists(_ context.Context, _ string) (bool, error) { return false, nil }
func (t *trackingStore) List(_ context.Context, prefix string) ([]string, error) {
	t.listPrefix = prefix
	return t.listResult, nil
}
func (t *trackingStore) Delete(_ context.Context, _ string) error { return nil }

func TestReader_UsesCustomLayout_ListDatasets(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"custom/ds1/segs/seg1/meta.json",
			"custom/ds2/segs/seg2/meta.json",
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Verify layout's DatasetsPrefix was used
	if store.listPrefix != "custom/" {
		t.Errorf("Expected list prefix %q, got %q", "custom/", store.listPrefix)
	}

	// Verify layout's IsManifest and ParseDatasetID were used
	if len(datasets) != 2 {
		t.Errorf("Expected 2 datasets, got %d", len(datasets))
	}
}

func TestReader_UsesCustomLayout_ListSegments(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"custom/mydata/segs/seg-a/meta.json",
			"custom/mydata/segs/seg-b/meta.json",
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	segments, err := reader.ListSegments(context.Background(), "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}

	// Verify layout's SegmentsPrefix was called with correct dataset
	expectedPrefix := "custom/mydata/segs/"
	if store.listPrefix != expectedPrefix {
		t.Errorf("Expected list prefix %q, got %q", expectedPrefix, store.listPrefix)
	}

	// Verify layout's IsManifest and ParseSegmentID were used
	if len(segments) != 2 {
		t.Errorf("Expected 2 segments, got %d", len(segments))
	}
	if segments[0].ID != "seg-a" {
		t.Errorf("Expected segment ID %q, got %q", "seg-a", segments[0].ID)
	}
}

func TestReader_UsesCustomLayout_GetManifest(t *testing.T) {
	store := &trackingStore{}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	// This will fail with ErrNotFound, but we just want to verify the path
	_, _ = reader.GetManifest(context.Background(), "mydata", SegmentRef{ID: "seg-1"})

	// Verify layout's ManifestPath was called
	if len(store.getCalls) != 1 {
		t.Fatalf("Expected 1 Get call, got %d", len(store.getCalls))
	}

	expectedPath := "custom/mydata/segs/seg-1/meta.json"
	if store.getCalls[0] != expectedPath {
		t.Errorf("Expected Get path %q, got %q", expectedPath, store.getCalls[0])
	}
}

func TestReader_UsesCustomLayout_RejectsStrayManifests(t *testing.T) {
	// Store returns paths that don't match custom layout
	store := &trackingStore{
		listResult: []string{
			"custom/ds1/segs/seg1/meta.json",          // valid
			"custom/ds1/misc/meta.json",               // invalid - wrong structure
			"datasets/ds2/snapshots/s1/manifest.json", // invalid - wrong layout
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Only ds1 should be found (the valid one)
	if len(datasets) != 1 {
		t.Errorf("Expected 1 dataset (stray manifests rejected), got %d", len(datasets))
	}
	if len(datasets) > 0 && datasets[0] != "ds1" {
		t.Errorf("Expected dataset %q, got %q", "ds1", datasets[0])
	}
}

// -----------------------------------------------------------------------------
// Strict DefaultLayout Parsing Tests
// Ensure parsing remains strict and doesn't become permissive
// -----------------------------------------------------------------------------

func TestDefaultLayout_IsManifest_StrictParsing(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want bool
		desc string
	}{
		// Valid paths
		{"datasets/foo/snapshots/bar/manifest.json", true, "canonical path"},
		{"datasets/a/snapshots/b/manifest.json", true, "single char IDs"},

		// Invalid: trailing slashes
		{"datasets/foo/snapshots/bar/manifest.json/", false, "trailing slash"},

		// Invalid: empty segments
		{"datasets//snapshots/bar/manifest.json", false, "empty dataset"},
		{"datasets/foo/snapshots//manifest.json", false, "empty segment"},
		{"datasets/foo//bar/manifest.json", false, "missing snapshots"},

		// Invalid: wrong depth
		{"datasets/foo/snapshots/bar/baz/manifest.json", false, "too deep"},
		{"datasets/foo/manifest.json", false, "too shallow"},

		// Invalid: wrong structure words
		{"datasets/foo/snapshot/bar/manifest.json", false, "singular 'snapshot'"},
		{"dataset/foo/snapshots/bar/manifest.json", false, "singular 'dataset'"},
		{"datasets/foo/segments/bar/manifest.json", false, "wrong dir 'segments'"},

		// Invalid: wrong filename
		{"datasets/foo/snapshots/bar/Manifest.json", false, "capitalized"},
		{"datasets/foo/snapshots/bar/manifest.JSON", false, "uppercase ext"},
		{"datasets/foo/snapshots/bar/manifest", false, "no extension"},

		// Edge cases: special characters in IDs
		{"datasets/foo-bar/snapshots/snap_1/manifest.json", true, "hyphen and underscore in IDs"},
		{"datasets/foo.bar/snapshots/snap.1/manifest.json", true, "dots in IDs"},

		// Edge cases that should be rejected
		{"datasets/foo/snapshots/bar/data/manifest.json", false, "manifest in data/"},
		{"/datasets/foo/snapshots/bar/manifest.json", false, "leading slash"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := layout.IsManifest(tt.path)
			if got != tt.want {
				t.Errorf("IsManifest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseDatasetID_StrictParsing(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.DatasetID
		desc string
	}{
		// Valid
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", "my-dataset", "canonical"},
		{"datasets/a/snapshots/b/manifest.json", "a", "single char"},

		// Invalid paths should return empty
		{"datasets/foo/snapshots/bar/manifest.json/extra", "", "extra path component"},
		{"datasets//snapshots/bar/manifest.json", "", "empty dataset ID"},
		{"datasets/foo/segments/bar/manifest.json", "", "wrong structure"},
		{"foo/snapshots/bar/manifest.json", "", "missing datasets prefix"},
		{"datasets/foo/snapshots/bar/data.json", "", "wrong filename"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := layout.ParseDatasetID(tt.path)
			if got != tt.want {
				t.Errorf("ParseDatasetID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseSegmentID_StrictParsing(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.SnapshotID
		desc string
	}{
		// Valid
		{"datasets/ds/snapshots/seg-123/manifest.json", "seg-123", "canonical"},
		{"datasets/a/snapshots/b/manifest.json", "b", "single char"},

		// Invalid paths should return empty
		{"datasets/ds/snapshots//manifest.json", "", "empty segment ID"},
		{"datasets/ds/snapshots/seg/extra/manifest.json", "", "extra path component"},
		{"datasets/ds/segments/seg/manifest.json", "", "wrong structure"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := layout.ParseSegmentID(tt.path)
			if got != tt.want {
				t.Errorf("ParseSegmentID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

// TestDefaultLayout_RejectsStrayManifests verifies that stray manifest.json files
// outside the canonical /snapshots/ structure don't create false dataset existence.
// This is the regression test for the "layout parsing too permissive" bug.
func TestDefaultLayout_RejectsStrayManifests(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"datasets/ds1/snapshots/seg1/manifest.json", // valid
			"datasets/ds2/misc/manifest.json",           // invalid - missing /snapshots/
			"datasets/ds3/snapshots/manifest.json",      // invalid - missing segment
			"datasets/ds4/snapshots/seg/sub/manifest.json", // invalid - too deep
		},
	}

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Only ds1 should be found
	if len(datasets) != 1 {
		t.Errorf("Expected 1 dataset (stray manifests rejected), got %d: %v", len(datasets), datasets)
	}
	if len(datasets) > 0 && datasets[0] != "ds1" {
		t.Errorf("Expected dataset %q, got %q", "ds1", datasets[0])
	}
}
