package lode

import (
	"context"
	"io"
	"strings"
	"testing"
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

	hiveLayout := NewHiveLayout("day") // Non-noop partitioner

	_, err := NewDataset("test-ds", NewMemoryFactory(), WithLayout(hiveLayout))
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

func TestNewDataset_NilFactory_ReturnsError(t *testing.T) {
	_, err := NewDataset("test-ds", nil)
	if err == nil {
		t.Fatal("expected error for nil factory, got nil")
	}
}

func TestNewDataset_FactoryReturnsNil_ReturnsError(t *testing.T) {
	nilFactory := func() (Store, error) {
		return nil, nil
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
// Test helpers
// -----------------------------------------------------------------------------

// testCodec is a simple codec for testing.
type testCodec struct{}

func (c *testCodec) Name() string { return "test-codec" }

func (c *testCodec) Encode(w io.Writer, records []any) error {
	return nil
}

func (c *testCodec) Decode(r io.Reader) ([]any, error) {
	return nil, nil
}
