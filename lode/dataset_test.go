package lode

import (
	"errors"
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
// G2-10, G2-11: Nil component rejection tests
// -----------------------------------------------------------------------------

func TestNewDataset_NilLayout_ReturnsError(t *testing.T) {
	_, err := NewDataset("test-ds", NewMemoryFactory(), WithLayout(nil))
	if err == nil {
		t.Fatal("expected error for nil layout, got nil")
	}
	if !strings.Contains(err.Error(), "layout must not be nil") {
		t.Errorf("expected 'layout must not be nil' error, got: %v", err)
	}
}

func TestNewDataset_NilCompressor_ReturnsError(t *testing.T) {
	_, err := NewDataset("test-ds", NewMemoryFactory(), WithCompressor(nil))
	if err == nil {
		t.Fatal("expected error for nil compressor, got nil")
	}
	if !strings.Contains(err.Error(), "compressor must not be nil") {
		t.Errorf("expected 'compressor must not be nil' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// G2-12: NewReader nil factory rejection
// -----------------------------------------------------------------------------

func TestNewReader_NilFactory_ReturnsError(t *testing.T) {
	_, err := NewReader(nil)
	if err == nil {
		t.Fatal("expected error for nil factory, got nil")
	}
	if !strings.Contains(err.Error(), "store factory is required") {
		t.Errorf("expected 'store factory is required' error, got: %v", err)
	}
}

func TestNewReader_FactoryReturnsNil_ReturnsError(t *testing.T) {
	nilFactory := func() (Store, error) {
		return nil, nil //nolint:nilnil // intentionally testing nil store with nil error
	}

	_, err := NewReader(nilFactory)
	if err == nil {
		t.Fatal("expected error for factory returning nil, got nil")
	}
	if !strings.Contains(err.Error(), "returned nil store") {
		t.Errorf("expected 'returned nil store' error, got: %v", err)
	}
}

func TestNewReader_NilLayout_ReturnsError(t *testing.T) {
	_, err := NewReader(NewMemoryFactory(), WithLayout(nil))
	if err == nil {
		t.Fatal("expected error for nil layout, got nil")
	}
	if !strings.Contains(err.Error(), "layout must not be nil") {
		t.Errorf("expected 'layout must not be nil' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// G2-13, G2-14: Codec/compressor mismatch on read
// -----------------------------------------------------------------------------

func TestDataset_Read_CodecMismatch_ReturnsError(t *testing.T) {
	store := NewMemory()

	// Write with JSONL codec
	dsWrite, err := NewDataset("test-ds", NewMemoryFactoryFrom(store), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := dsWrite.Write(t.Context(), []any{D{"id": "1"}}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Try to read with no codec (raw blob mode)
	dsRead, err := NewDataset("test-ds", NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dsRead.Read(t.Context(), snap.ID)
	if err == nil {
		t.Fatal("expected error for codec mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "codec mismatch") {
		t.Errorf("expected 'codec mismatch' error, got: %v", err)
	}
}

func TestDataset_Read_CompressorMismatch_ReturnsError(t *testing.T) {
	store := NewMemory()

	// Write with gzip compression
	dsWrite, err := NewDataset("test-ds", NewMemoryFactoryFrom(store), WithCompressor(NewGzipCompressor()))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := dsWrite.Write(t.Context(), []any{[]byte("data")}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Try to read with no compression (noop)
	dsRead, err := NewDataset("test-ds", NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dsRead.Read(t.Context(), snap.ID)
	if err == nil {
		t.Fatal("expected error for compressor mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "compressor mismatch") {
		t.Errorf("expected 'compressor mismatch' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// G2-15: Empty metadata explicitly valid and persisted
// -----------------------------------------------------------------------------

func TestDataset_Write_EmptyMetadata_ValidAndPersisted(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	// Write with explicitly empty metadata (not nil)
	snap, err := ds.Write(t.Context(), []any{[]byte("data")}, Metadata{})
	if err != nil {
		t.Fatalf("expected empty metadata to be valid, got error: %v", err)
	}

	// Verify metadata is persisted as empty object, not nil
	if snap.Manifest.Metadata == nil {
		t.Fatal("expected metadata to be non-nil empty map, got nil")
	}
	if len(snap.Manifest.Metadata) != 0 {
		t.Errorf("expected empty metadata, got %d keys", len(snap.Manifest.Metadata))
	}

	// Verify we can read it back and metadata is preserved
	retrieved, err := ds.Snapshot(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("failed to retrieve snapshot: %v", err)
	}
	if retrieved.Manifest.Metadata == nil {
		t.Fatal("retrieved metadata should be non-nil empty map, got nil")
	}
	if len(retrieved.Manifest.Metadata) != 0 {
		t.Errorf("retrieved metadata should be empty, got %d keys", len(retrieved.Manifest.Metadata))
	}
}

func TestDataset_StreamWrite_EmptyMetadata_ValidAndPersisted(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatalf("expected empty metadata to be valid, got error: %v", err)
	}

	_, err = sw.Write([]byte("streaming data"))
	if err != nil {
		t.Fatal(err)
	}

	snap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Verify metadata is persisted as empty object
	if snap.Manifest.Metadata == nil {
		t.Fatal("expected metadata to be non-nil empty map, got nil")
	}
	if len(snap.Manifest.Metadata) != 0 {
		t.Errorf("expected empty metadata, got %d keys", len(snap.Manifest.Metadata))
	}
}

func TestDataset_StreamWriteRecords_EmptyMetadata_ValidAndPersisted(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1"}}}
	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err != nil {
		t.Fatalf("expected empty metadata to be valid, got error: %v", err)
	}

	// Verify metadata is persisted as empty object
	if snap.Manifest.Metadata == nil {
		t.Fatal("expected metadata to be non-nil empty map, got nil")
	}
	if len(snap.Manifest.Metadata) != 0 {
		t.Errorf("expected empty metadata, got %d keys", len(snap.Manifest.Metadata))
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

func TestReader_WithChecksum_ReturnsError(t *testing.T) {
	// WithChecksum is a dataset-only option
	_, err := NewReader(NewMemoryFactory(), WithChecksum(NewMD5Checksum()))
	if err == nil {
		t.Fatal("expected error for WithChecksum on reader, got nil")
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

	_, err = ds.Latest(t.Context())
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots, got: %v", err)
	}
}

func TestDataset_Snapshots_EmptyDataset_ReturnsEmptyList(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	snapshots, err := ds.Snapshots(t.Context())
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

	_, err = ds.Snapshot(t.Context(), "nonexistent-id")
	if !errors.Is(err, ErrNotFound) {
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

	_, err = ds.Write(t.Context(), []any{[]byte("data")}, nil)
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
	_, err = ds.Write(t.Context(), []any{[]byte("one"), []byte("two")}, Metadata{})
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
	_, err = ds.Write(t.Context(), []any{"not a byte slice"}, Metadata{})
	if err == nil {
		t.Fatal("expected error for wrong type in raw blob mode, got nil")
	}
	if !strings.Contains(err.Error(), "requires []byte") {
		t.Errorf("expected '[]byte' error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// StreamWrite tests
// -----------------------------------------------------------------------------

func TestDataset_StreamWrite_Success(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{"source": "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	data := []byte("hello stream world")
	n, err := sw.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}

	// Commit and verify snapshot
	snap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if snap == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if snap.Manifest.RowCount != 1 {
		t.Errorf("expected RowCount 1, got %d", snap.Manifest.RowCount)
	}
	if len(snap.Manifest.Files) != 1 {
		t.Errorf("expected 1 file, got %d", len(snap.Manifest.Files))
	}
	if snap.Manifest.Metadata["source"] != "test" {
		t.Errorf("expected metadata source=test, got %v", snap.Manifest.Metadata)
	}

	// Verify snapshot is visible via Latest
	latest, err := ds.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}
	if latest.ID != snap.ID {
		t.Errorf("expected latest ID %s, got %s", snap.ID, latest.ID)
	}

	// Verify data can be read back
	readData, err := ds.Read(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readData) != 1 {
		t.Fatalf("expected 1 element, got %d", len(readData))
	}
	readBytes, ok := readData[0].([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", readData[0])
	}
	if string(readBytes) != string(data) {
		t.Errorf("expected %q, got %q", data, readBytes)
	}
}

func TestDataset_StreamWrite_Abort_NoManifest(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	_, err = sw.Write([]byte("partial data"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Abort
	err = sw.Abort(t.Context())
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Verify no snapshot visible
	_, err = ds.Latest(t.Context())
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots after abort, got: %v", err)
	}

	snapshots, err := ds.Snapshots(t.Context())
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots after abort, got %d", len(snapshots))
	}
}

func TestDataset_StreamWrite_CloseWithoutCommit_BehavesAsAbort(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	_, err = sw.Write([]byte("will be abandoned"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Close without commit
	err = sw.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify no snapshot visible
	_, err = ds.Latest(t.Context())
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots after close without commit, got: %v", err)
	}
}

func TestDataset_StreamWrite_NilMetadata_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.StreamWrite(t.Context(), nil)
	if err == nil {
		t.Fatal("expected error for nil metadata, got nil")
	}
	if !strings.Contains(err.Error(), "metadata must be non-nil") {
		t.Errorf("expected metadata error, got: %v", err)
	}
}

func TestDataset_StreamWrite_WithCodec_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.StreamWrite(t.Context(), Metadata{})
	if !errors.Is(err, ErrCodecConfigured) {
		t.Errorf("expected ErrCodecConfigured, got: %v", err)
	}
}

func TestDataset_StreamWrite_WriteAfterCommit_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Write after commit should fail
	_, err = sw.Write([]byte("more data"))
	if err == nil {
		t.Fatal("expected error writing after commit")
	}
}

func TestDataset_StreamWrite_WriteAfterAbort_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	err = sw.Abort(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Write after abort should fail
	_, err = sw.Write([]byte("more data"))
	if err == nil {
		t.Fatal("expected error writing after abort")
	}
}

func TestDataset_StreamWrite_DoubleCommit_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Second commit should fail
	_, err = sw.Commit(t.Context())
	if err == nil {
		t.Fatal("expected error on double commit")
	}
}

func TestDataset_StreamWrite_AbortAfterCommit_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Abort after commit should fail
	err = sw.Abort(t.Context())
	if err == nil {
		t.Fatal("expected error aborting after commit")
	}
}

func TestDataset_StreamWrite_ParentSnapshotLinked(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	// First write (regular Write)
	firstSnap, err := ds.Write(t.Context(), []any{[]byte("first")}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Second write (StreamWrite)
	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sw.Write([]byte("second"))
	secondSnap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if secondSnap.Manifest.ParentSnapshotID != firstSnap.ID {
		t.Errorf("expected parent %s, got %s", firstSnap.ID, secondSnap.Manifest.ParentSnapshotID)
	}
}

func TestDataset_StreamWrite_WithGzipCompression(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCompressor(NewGzipCompressor()))
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("compressible data that should be gzipped")
	_, err = sw.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	snap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify manifest indicates gzip compression
	if snap.Manifest.Compressor != "gzip" {
		t.Errorf("expected compressor 'gzip', got %q", snap.Manifest.Compressor)
	}

	// Verify file path ends with .gz
	if len(snap.Manifest.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(snap.Manifest.Files))
	}
	if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".gz") {
		t.Errorf("expected .gz extension, got %s", snap.Manifest.Files[0].Path)
	}

	// Verify data can be read back correctly
	readData, err := ds.Read(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	readBytes, ok := readData[0].([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", readData[0])
	}
	if string(readBytes) != string(data) {
		t.Errorf("expected %q, got %q", data, readBytes)
	}
}

func TestDataset_StreamWrite_MultipleWrites(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Write in chunks
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("-chunk2"),
		[]byte("-chunk3"),
	}
	for _, chunk := range chunks {
		_, err = sw.Write(chunk)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	snap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data can be read back as concatenated
	readData, err := ds.Read(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	readBytes, ok := readData[0].([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", readData[0])
	}
	expected := "chunk1-chunk2-chunk3"
	if string(readBytes) != expected {
		t.Errorf("expected %q, got %q", expected, readBytes)
	}
}

// -----------------------------------------------------------------------------
// StreamWriteRecords tests
// -----------------------------------------------------------------------------

func TestDataset_StreamWriteRecords_Success(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	records := []any{
		D{"id": "1", "value": "first"},
		D{"id": "2", "value": "second"},
		D{"id": "3", "value": "third"},
	}
	iter := &sliceIterator{records: records}

	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{"source": "stream"}, iter)
	if err != nil {
		t.Fatalf("StreamWriteRecords failed: %v", err)
	}

	if snap == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if snap.Manifest.RowCount != 3 {
		t.Errorf("expected RowCount 3, got %d", snap.Manifest.RowCount)
	}
	if snap.Manifest.Codec != "jsonl" {
		t.Errorf("expected codec 'jsonl', got %q", snap.Manifest.Codec)
	}
	if snap.Manifest.Metadata["source"] != "stream" {
		t.Errorf("expected metadata source=stream, got %v", snap.Manifest.Metadata)
	}

	// Verify snapshot is visible via Latest
	latest, err := ds.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}
	if latest.ID != snap.ID {
		t.Errorf("expected latest ID %s, got %s", snap.ID, latest.ID)
	}

	// Verify data can be read back
	readData, err := ds.Read(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readData) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readData))
	}
}

func TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError(t *testing.T) {
	// testCodec does not implement StreamingRecordCodec
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(&testCodec{}))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1"}}}
	_, err = ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if !errors.Is(err, ErrCodecNotStreamable) {
		t.Errorf("expected ErrCodecNotStreamable, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_NoCodec_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1"}}}
	_, err = ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err == nil {
		t.Fatal("expected error for no codec, got nil")
	}
	if !strings.Contains(err.Error(), "requires a codec") {
		t.Errorf("expected 'requires a codec' error, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_NilMetadata_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1"}}}
	_, err = ds.StreamWriteRecords(t.Context(), nil, iter)
	if err == nil {
		t.Fatal("expected error for nil metadata, got nil")
	}
	if !strings.Contains(err.Error(), "metadata must be non-nil") {
		t.Errorf("expected metadata error, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_NilIterator_ReturnsError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	_, err = ds.StreamWriteRecords(t.Context(), Metadata{}, nil)
	if err == nil {
		t.Fatal("expected error for nil iterator, got nil")
	}
	if !errors.Is(err, ErrNilIterator) {
		t.Errorf("expected ErrNilIterator, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError(t *testing.T) {
	// StreamWriteRecords cannot partition since it's single-pass streaming
	ds, err := NewDataset("test-ds", NewMemoryFactory(),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1", "day": "2024-01-01"}}}
	_, err = ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err == nil {
		t.Fatal("expected error for partitioning, got nil")
	}
	if !errors.Is(err, ErrPartitioningNotSupported) {
		t.Errorf("expected ErrPartitioningNotSupported, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_TimestampedRecords_ComputesMinMax(t *testing.T) {
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
	iter := &sliceIterator{records: records}

	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
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

func TestDataset_StreamWriteRecords_NonTimestampedRecords_OmitsMinMax(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	records := []any{
		D{"id": "a", "value": 1},
		D{"id": "b", "value": 2},
	}
	iter := &sliceIterator{records: records}

	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
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

func TestDataset_StreamWriteRecords_EmptyIterator(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{}}

	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err != nil {
		t.Fatalf("StreamWriteRecords failed: %v", err)
	}

	if snap.Manifest.RowCount != 0 {
		t.Errorf("expected RowCount 0, got %d", snap.Manifest.RowCount)
	}
}

func TestDataset_StreamWriteRecords_IteratorError(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	iterErr := errors.New("iterator failure")
	iter := &errorIterator{err: iterErr}

	_, err = ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err == nil {
		t.Fatal("expected error from iterator, got nil")
	}
	if !strings.Contains(err.Error(), "iterator failure") {
		t.Errorf("expected iterator failure error, got: %v", err)
	}

	// Verify no snapshot created
	_, err = ds.Latest(t.Context())
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots after iterator error, got: %v", err)
	}
}

func TestDataset_StreamWriteRecords_WithCompression(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(),
		WithCodec(NewJSONLCodec()),
		WithCompressor(NewGzipCompressor()))
	if err != nil {
		t.Fatal(err)
	}

	records := []any{
		D{"id": "1", "data": "some compressible content"},
		D{"id": "2", "data": "more compressible content"},
	}
	iter := &sliceIterator{records: records}

	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err != nil {
		t.Fatalf("StreamWriteRecords failed: %v", err)
	}

	if snap.Manifest.Compressor != "gzip" {
		t.Errorf("expected compressor 'gzip', got %q", snap.Manifest.Compressor)
	}
	if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".gz") {
		t.Errorf("expected .gz extension, got %s", snap.Manifest.Files[0].Path)
	}

	// Verify data can be read back
	readData, err := ds.Read(t.Context(), snap.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readData) != 2 {
		t.Errorf("expected 2 records, got %d", len(readData))
	}
}

func TestDataset_StreamWriteRecords_ParentSnapshotLinked(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	// First write (regular Write)
	firstSnap, err := ds.Write(t.Context(), []any{D{"id": "1"}}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Second write (StreamWriteRecords)
	iter := &sliceIterator{records: []any{D{"id": "2"}}}
	secondSnap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err != nil {
		t.Fatal(err)
	}

	if secondSnap.Manifest.ParentSnapshotID != firstSnap.ID {
		t.Errorf("expected parent %s, got %s", firstSnap.ID, secondSnap.Manifest.ParentSnapshotID)
	}
}

// -----------------------------------------------------------------------------
// Checksum tests
// -----------------------------------------------------------------------------

func TestDataset_Write_WithChecksum_RecordsChecksum(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	snap, err := ds.Write(t.Context(), []any{[]byte("hello world")}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Verify checksum algorithm recorded
	if snap.Manifest.ChecksumAlgorithm != "md5" {
		t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.Manifest.ChecksumAlgorithm)
	}

	// Verify file has checksum
	if len(snap.Manifest.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(snap.Manifest.Files))
	}
	if snap.Manifest.Files[0].Checksum == "" {
		t.Error("expected non-empty checksum")
	}
	// MD5 produces 32 hex characters
	if len(snap.Manifest.Files[0].Checksum) != 32 {
		t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
	}
}

func TestDataset_Write_WithoutChecksum_OmitsChecksum(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}

	snap, err := ds.Write(t.Context(), []any{[]byte("hello world")}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Verify no checksum algorithm recorded
	if snap.Manifest.ChecksumAlgorithm != "" {
		t.Errorf("expected empty ChecksumAlgorithm, got %q", snap.Manifest.ChecksumAlgorithm)
	}

	// Verify file has no checksum
	if snap.Manifest.Files[0].Checksum != "" {
		t.Errorf("expected empty checksum, got %q", snap.Manifest.Files[0].Checksum)
	}
}

func TestDataset_StreamWrite_WithChecksum_RecordsChecksum(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("streaming data"))
	if err != nil {
		t.Fatal(err)
	}

	snap, err := sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Verify checksum algorithm recorded
	if snap.Manifest.ChecksumAlgorithm != "md5" {
		t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.Manifest.ChecksumAlgorithm)
	}

	// Verify file has checksum
	if snap.Manifest.Files[0].Checksum == "" {
		t.Error("expected non-empty checksum")
	}
	if len(snap.Manifest.Files[0].Checksum) != 32 {
		t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
	}
}

func TestDataset_StreamWriteRecords_WithChecksum_RecordsChecksum(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(),
		WithCodec(NewJSONLCodec()),
		WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	iter := &sliceIterator{records: []any{D{"id": "1"}, D{"id": "2"}}}
	snap, err := ds.StreamWriteRecords(t.Context(), Metadata{}, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Verify checksum algorithm recorded
	if snap.Manifest.ChecksumAlgorithm != "md5" {
		t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.Manifest.ChecksumAlgorithm)
	}

	// Verify file has checksum
	if snap.Manifest.Files[0].Checksum == "" {
		t.Error("expected non-empty checksum")
	}
	if len(snap.Manifest.Files[0].Checksum) != 32 {
		t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
	}
}

func TestDataset_Checksum_ComputedOnCompressedData(t *testing.T) {
	// Two datasets with same data but different compression
	// should produce different checksums (since checksum is on stored bytes)
	dsNoComp, err := NewDataset("test-no-comp", NewMemoryFactory(),
		WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	dsGzip, err := NewDataset("test-gzip", NewMemoryFactory(),
		WithCompressor(NewGzipCompressor()),
		WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("compressible data that should produce different checksums")

	snapNoComp, err := dsNoComp.Write(t.Context(), []any{data}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	snapGzip, err := dsGzip.Write(t.Context(), []any{data}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Both should have checksums
	if snapNoComp.Manifest.Files[0].Checksum == "" {
		t.Error("expected checksum for no-compression")
	}
	if snapGzip.Manifest.Files[0].Checksum == "" {
		t.Error("expected checksum for gzip")
	}

	// Checksums should be different (stored bytes differ)
	if snapNoComp.Manifest.Files[0].Checksum == snapGzip.Manifest.Files[0].Checksum {
		t.Error("expected different checksums for different compression")
	}
}

func TestDataset_Checksum_SameDataProducesSameChecksum(t *testing.T) {
	ds, err := NewDataset("test-ds", NewMemoryFactory(), WithChecksum(NewMD5Checksum()))
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("consistent data")

	snap1, err := ds.Write(t.Context(), []any{data}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	snap2, err := ds.Write(t.Context(), []any{data}, Metadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Same data should produce same checksum
	if snap1.Manifest.Files[0].Checksum != snap2.Manifest.Files[0].Checksum {
		t.Errorf("expected same checksum for same data, got %s vs %s",
			snap1.Manifest.Files[0].Checksum, snap2.Manifest.Files[0].Checksum)
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

	snap, err := ds.Write(t.Context(), records, Metadata{})
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

	snap, err := ds.Write(t.Context(), records, Metadata{})
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

	snap, err := ds.Write(t.Context(), []any{[]byte("blob data")}, Metadata{})
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

	snap, err := ds.Write(t.Context(), records, Metadata{})
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

// sliceIterator implements RecordIterator for testing.
type sliceIterator struct {
	records []any
	index   int
	current any
}

func (s *sliceIterator) Next() bool {
	if s.index >= len(s.records) {
		return false
	}
	s.current = s.records[s.index]
	s.index++
	return true
}

func (s *sliceIterator) Record() any {
	return s.current
}

func (s *sliceIterator) Err() error {
	return nil
}

// errorIterator implements RecordIterator that returns an error.
type errorIterator struct {
	err error
}

func (e *errorIterator) Next() bool {
	return false
}

func (e *errorIterator) Record() any {
	return nil
}

func (e *errorIterator) Err() error {
	return e.err
}
