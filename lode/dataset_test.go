package lode

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// G4: Raw blob + partitioning rejection test
// -----------------------------------------------------------------------------

func TestNewDataset_RawBlobWithPartitioner_ReturnsError(t *testing.T) {
	// Per CONTRACT_LAYOUT.md and the implementation, raw blob mode (no codec)
	// cannot use partitioning because there are no record fields to extract keys from.
	//
	// NewHiveLayout creates a layout with a non-noop partitioner (hive partitioning).
	// Using it without a codec should return an error.

	hiveLayout, err := NewHiveLayout("day") // Non-noop partitioner
	if err != nil {
		t.Fatalf("NewHiveLayout failed: %v", err)
	}

	_, err = NewDataset("test-ds", NewMemoryFactory(), WithLayout(hiveLayout))
	if err == nil {
		t.Fatal("expected error for raw blob mode with partitioner, got nil")
	}
	if !strings.Contains(err.Error(), "raw blob mode") {
		t.Errorf("expected error message about raw blob mode, got: %v", err)
	}
}

func TestNewDataset_RawBlobWithDefaultLayout_Success(t *testing.T) {
	// DefaultLayout uses noop partitioner, so raw blob mode should work.
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatalf("expected success for raw blob with default layout, got: %v", err)
	}
	if ds == nil {
		t.Fatal("expected non-nil dataset")
	}
}

func TestNewHiveLayout_ZeroKeys_ReturnsError(t *testing.T) {
	// NewHiveLayout requires at least one partition key
	_, err := NewHiveLayout()
	if err == nil {
		t.Fatal("expected error for zero keys, got nil")
	}
	if !strings.Contains(err.Error(), "at least one partition key") {
		t.Errorf("expected 'at least one partition key' in error, got: %v", err)
	}
}

func TestNewHiveLayout_WithKeys_Success(t *testing.T) {
	layout, err := NewHiveLayout("day")
	if err != nil {
		t.Fatalf("NewHiveLayout failed: %v", err)
	}
	if layout == nil {
		t.Fatal("expected non-nil layout")
	}
}

func TestWithHiveLayout_ZeroKeys_ReturnsError(t *testing.T) {
	// WithHiveLayout validates on apply, so error comes from NewDataset
	_, err := NewDataset("test-ds", NewMemoryFactory(), WithHiveLayout(), WithCodec(NewJSONLCodec()))
	if err == nil {
		t.Fatal("expected error for zero keys, got nil")
	}
	if !strings.Contains(err.Error(), "at least one partition key") {
		t.Errorf("expected 'at least one partition key' in error, got: %v", err)
	}
}

func TestWithHiveLayout_WithKeys_Success(t *testing.T) {
	// Fluent API - single error check
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithHiveLayout("day"), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatalf("NewDataset with WithHiveLayout failed: %v", err)
	}
	if ds == nil {
		t.Fatal("expected non-nil dataset")
	}
}

func TestWithHiveLayout_WithReader_Success(t *testing.T) {
	// Fluent API - single error check
	reader, err := NewReader(NewMemoryFactory(), WithHiveLayout("day"))
	if err != nil {
		t.Fatalf("NewReader with WithHiveLayout failed: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
}

func TestNewDataset_NilFactory_ReturnsError(t *testing.T) {
	_, err := NewDataset("test-ds", nil)
	if err == nil {
		t.Fatal("expected error for nil factory, got nil")
	}
}

func TestNewDataset_FactoryReturnsNil_ReturnsError(t *testing.T) {
	nilFactory := func() (Store, error) {
		return nil, nil //nolint:nilnil // intentionally testing nil store with nil error
	}

	_, err := NewDataset("test-ds", nilFactory)
	if err == nil {
		t.Fatal("expected error for factory returning nil, got nil")
	}
}

// -----------------------------------------------------------------------------
// Option validation tests
// -----------------------------------------------------------------------------

func TestReader_WithCompressor_ReturnsError(t *testing.T) {
	// WithCompressor is a dataset-only option
	_, err := NewReader(NewMemoryFactory(), WithCompressor(NewNoOpCompressor()))
	if err == nil {
		t.Fatal("expected error for WithCompressor on reader, got nil")
	}
	if !strings.Contains(err.Error(), "not valid for reader") {
		t.Errorf("expected 'not valid for reader' error, got: %v", err)
	}
}

func TestReader_WithCodec_ReturnsError(t *testing.T) {
	// WithCodec is a dataset-only option
	_, err := NewReader(NewMemoryFactory(), WithCodec(&testCodec{}))
	if err == nil {
		t.Fatal("expected error for WithCodec on reader, got nil")
	}
	if !strings.Contains(err.Error(), "not valid for reader") {
		t.Errorf("expected 'not valid for reader' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Empty dataset behavior tests
// -----------------------------------------------------------------------------

func TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.Latest(context.Background())
	if err != ErrNoSnapshots {
		t.Errorf("expected ErrNoSnapshots, got: %v", err)
	}
}

func TestDataset_Snapshots_EmptyDataset_ReturnsEmptyList(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	snapshots, err := ds.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("expected empty list, got %d snapshots", len(snapshots))
	}
}

func TestDataset_Snapshot_EmptyDataset_ReturnsErrNotFound(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.Snapshot(context.Background(), "nonexistent-id")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Write validation tests
// -----------------------------------------------------------------------------

func TestDataset_Write_NilMetadata_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.Write(context.Background(), []any{[]byte("data")}, nil)
	if err == nil {
		t.Fatal("expected error for nil metadata, got nil")
	}
	if !strings.Contains(err.Error(), "metadata must be non-nil") {
		t.Errorf("expected metadata error, got: %v", err)
	}
}

func TestDataset_RawBlobWrite_MultipleElements_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	// Raw blob mode requires exactly one []byte element
	_, err = ds.Write(context.Background(), []any{[]byte("one"), []byte("two")}, Metadata{})
	if err == nil {
		t.Fatal("expected error for multiple elements in raw blob mode, got nil")
	}
	if !strings.Contains(err.Error(), "exactly one data element") {
		t.Errorf("expected 'exactly one data element' error, got: %v", err)
	}
}

func TestDataset_RawBlobWrite_WrongType_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	// Raw blob mode requires []byte, not string
	_, err = ds.Write(context.Background(), []any{"not a byte slice"}, Metadata{})
	if err == nil {
		t.Fatal("expected error for wrong type in raw blob mode, got nil")
	}
	if !strings.Contains(err.Error(), "requires []byte") {
		t.Errorf("expected '[]byte' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Timestamped interface tests
// -----------------------------------------------------------------------------

func TestDataset_Write_TimestampedRecords_ComputesMinMax(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	ts3 := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC)

	records := []any{
		&timestampedRecord{ID: "a", Time: ts1},
		&timestampedRecord{ID: "b", Time: ts2},
		&timestampedRecord{ID: "c", Time: ts3},
	}

	snap, err := ds.Write(context.Background(), records, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	if snap.Manifest.MinTimestamp == nil {
		t.Fatal("expected MinTimestamp to be set")
	}
	if snap.Manifest.MaxTimestamp == nil {
		t.Fatal("expected MaxTimestamp to be set")
	}

	if !snap.Manifest.MinTimestamp.Equal(ts1) {
		t.Errorf("expected MinTimestamp %v, got %v", ts1, *snap.Manifest.MinTimestamp)
	}
	if !snap.Manifest.MaxTimestamp.Equal(ts2) {
		t.Errorf("expected MaxTimestamp %v, got %v", ts2, *snap.Manifest.MaxTimestamp)
	}
}

func TestDataset_Write_NonTimestampedRecords_OmitsMinMax(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	// Plain maps don't implement Timestamped
	records := []any{
		D{"id": "a", "value": 1},
		D{"id": "b", "value": 2},
	}

	snap, err := ds.Write(context.Background(), records, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	if snap.Manifest.MinTimestamp != nil {
		t.Errorf("expected MinTimestamp to be nil, got %v", snap.Manifest.MinTimestamp)
	}
	if snap.Manifest.MaxTimestamp != nil {
		t.Errorf("expected MaxTimestamp to be nil, got %v", snap.Manifest.MaxTimestamp)
	}
}

func TestDataset_Write_RawBlob_OmitsTimestamps(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	snap, err := ds.Write(context.Background(), []any{[]byte("blob data")}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	if snap.Manifest.MinTimestamp != nil {
		t.Errorf("expected MinTimestamp to be nil for raw blob, got %v", snap.Manifest.MinTimestamp)
	}
	if snap.Manifest.MaxTimestamp != nil {
		t.Errorf("expected MaxTimestamp to be nil for raw blob, got %v", snap.Manifest.MaxTimestamp)
	}
}

func TestDataset_Write_SingleTimestampedRecord_SameMinMax(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	records := []any{&timestampedRecord{ID: "only", Time: ts}}

	snap, err := ds.Write(context.Background(), records, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	if snap.Manifest.MinTimestamp == nil || snap.Manifest.MaxTimestamp == nil {
		t.Fatal("expected both timestamps to be set")
	}
	if !snap.Manifest.MinTimestamp.Equal(ts) || !snap.Manifest.MaxTimestamp.Equal(ts) {
		t.Errorf("expected both timestamps to be %v", ts)
	}
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// timestampedRecord implements Timestamped for testing.
type timestampedRecord struct {
	ID   string    `json:"id"`
	Time time.Time `json:"time"`
}

func (r *timestampedRecord) Timestamp() time.Time {
	return r.Time
}

// testCodec is a simple codec for testing.
type testCodec struct{}

func (c *testCodec) Name() string { return "test-codec" }

func (c *testCodec) Encode(_ io.Writer, _ []any) error {
	return nil
}

func (c *testCodec) Decode(_ io.Reader) ([]any, error) {
	return nil, nil //nolint:nilnil // stub for testing
}
