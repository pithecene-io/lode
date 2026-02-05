package lode

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestParquetCodec_Name(t *testing.T) {
	codec, err := NewParquetCodec(ParquetSchema{})
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}
	if got := codec.Name(); got != "parquet" {
		t.Errorf("Name() = %q, want %q", got, "parquet")
	}
}

func TestParquetCodec_RoundTrip_BasicTypes(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
			{Name: "score", Type: ParquetFloat64},
			{Name: "active", Type: ParquetBool},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{"id": int64(1), "name": "alice", "score": 95.5, "active": true},
		map[string]any{"id": int64(2), "name": "bob", "score": 87.3, "active": false},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded) != len(records) {
		t.Fatalf("Decode() got %d records, want %d", len(decoded), len(records))
	}

	// Verify first record
	got := decoded[0].(map[string]any)
	if got["id"] != int64(1) {
		t.Errorf("record[0].id = %v, want 1", got["id"])
	}
	if got["name"] != "alice" {
		t.Errorf("record[0].name = %v, want alice", got["name"])
	}
	if got["active"] != true {
		t.Errorf("record[0].active = %v, want true", got["active"])
	}
}

func TestParquetCodec_RoundTrip_AllTypes(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond).UTC()
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "int32_field", Type: ParquetInt32},
			{Name: "int64_field", Type: ParquetInt64},
			{Name: "float32_field", Type: ParquetFloat32},
			{Name: "float64_field", Type: ParquetFloat64},
			{Name: "string_field", Type: ParquetString},
			{Name: "bool_field", Type: ParquetBool},
			{Name: "bytes_field", Type: ParquetBytes},
			{Name: "timestamp_field", Type: ParquetTimestamp},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{
			"int32_field":     int32(42),
			"int64_field":     int64(9999999999),
			"float32_field":   float32(3.14),
			"float64_field":   float64(2.71828),
			"string_field":    "hello",
			"bool_field":      true,
			"bytes_field":     []byte("binary data"),
			"timestamp_field": now,
		},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("Decode() got %d records, want 1", len(decoded))
	}

	got := decoded[0].(map[string]any)
	if got["int32_field"] != int32(42) {
		t.Errorf("int32_field = %v (%T), want 42", got["int32_field"], got["int32_field"])
	}
	if got["int64_field"] != int64(9999999999) {
		t.Errorf("int64_field = %v, want 9999999999", got["int64_field"])
	}
	if got["string_field"] != "hello" {
		t.Errorf("string_field = %v, want hello", got["string_field"])
	}
	if got["bool_field"] != true {
		t.Errorf("bool_field = %v, want true", got["bool_field"])
	}
}

func TestParquetCodec_EmptyRecords(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, []any{}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	// Should produce a valid parquet file
	if buf.Len() == 0 {
		t.Error("Encode() produced empty output for empty records")
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("Decode() got %d records, want 0", len(decoded))
	}
}

func TestParquetCodec_NullableFields(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString, Nullable: true},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{"id": int64(1), "name": "alice"},
		map[string]any{"id": int64(2), "name": nil},
		map[string]any{"id": int64(3)}, // missing field treated as nil
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded) != 3 {
		t.Fatalf("Decode() got %d records, want 3", len(decoded))
	}
}

func TestParquetCodec_MissingRequiredField_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString}, // required
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{"id": int64(1)}, // missing required "name"
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for missing required field")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_TypeMismatch_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{"id": "not an int"}, // wrong type
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for type mismatch")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_NotMapRecord_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		"not a map", // invalid record type
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for non-map record")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_Decode_InvalidFormat(t *testing.T) {
	codec, err := NewParquetCodec(ParquetSchema{
		Fields: []ParquetField{{Name: "id", Type: ParquetInt64}},
	})
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Empty data
	_, err = codec.Decode(bytes.NewReader([]byte{}))
	if !errors.Is(err, ErrInvalidFormat) {
		t.Errorf("Decode(empty) error = %v, want ErrInvalidFormat", err)
	}

	// Invalid data
	_, err = codec.Decode(bytes.NewReader([]byte("not parquet data")))
	if !errors.Is(err, ErrInvalidFormat) {
		t.Errorf("Decode(garbage) error = %v, want ErrInvalidFormat", err)
	}
}

func TestParquetCodec_ExtraFieldsIgnored(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	records := []any{
		map[string]any{
			"id":          int64(1),
			"extra_field": "ignored", // not in schema
		},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	got := decoded[0].(map[string]any)
	if _, exists := got["extra_field"]; exists {
		t.Error("extra_field should not be in decoded record")
	}
	if got["id"] != int64(1) {
		t.Errorf("id = %v, want 1", got["id"])
	}
}

func TestParquetCodec_WithOptions(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}

	// Test with different compression options
	compressions := []ParquetCompression{
		ParquetCompressionNone,
		ParquetCompressionSnappy,
		ParquetCompressionGzip,
	}

	for _, comp := range compressions {
		codec, err := NewParquetCodec(schema,
			WithParquetCompression(comp),
		)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1)},
			map[string]any{"id": int64(2)},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Errorf("Encode() with compression %d error = %v", comp, err)
			continue
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Errorf("Decode() with compression %d error = %v", comp, err)
			continue
		}

		if len(decoded) != 2 {
			t.Errorf("Decode() with compression %d got %d records, want 2", comp, len(decoded))
		}
	}
}

func TestParquetCodec_JSONNumberConversion(t *testing.T) {
	// When records come from JSON, numbers are float64
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "int32_field", Type: ParquetInt32},
			{Name: "int64_field", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Simulate JSON-decoded numbers (float64)
	records := []any{
		map[string]any{
			"int32_field": float64(42),
			"int64_field": float64(9999999999),
		},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	got := decoded[0].(map[string]any)
	if got["int32_field"] != int32(42) {
		t.Errorf("int32_field = %v (%T), want int32(42)", got["int32_field"], got["int32_field"])
	}
	if got["int64_field"] != int64(9999999999) {
		t.Errorf("int64_field = %v (%T), want int64(9999999999)", got["int64_field"], got["int64_field"])
	}
}

func TestParquetCodec_TimestampStringParsing(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "ts", Type: ParquetTimestamp},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Timestamp as RFC3339 string
	records := []any{
		map[string]any{
			"ts": "2024-01-15T10:30:00Z",
		},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	got := decoded[0].(map[string]any)
	ts, ok := got["ts"].(time.Time)
	if !ok {
		t.Fatalf("ts = %T, want time.Time", got["ts"])
	}
	if ts.Year() != 2024 || ts.Month() != 1 || ts.Day() != 15 {
		t.Errorf("ts = %v, want 2024-01-15", ts)
	}
}

func TestParquetCodec_LargeDataset(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "value", Type: ParquetString},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Generate 10k records
	records := make([]any, 10000)
	for i := range records {
		records[i] = map[string]any{
			"id":    int64(i),
			"value": "test value with some content to make it larger",
		}
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded) != 10000 {
		t.Errorf("Decode() got %d records, want 10000", len(decoded))
	}

	// Spot check
	first := decoded[0].(map[string]any)
	if first["id"] != int64(0) {
		t.Errorf("first record id = %v, want 0", first["id"])
	}
	last := decoded[9999].(map[string]any)
	if last["id"] != int64(9999) {
		t.Errorf("last record id = %v, want 9999", last["id"])
	}
}

// -----------------------------------------------------------------------------
// Schema Validation Tests
// -----------------------------------------------------------------------------

func TestParquetCodec_NewParquetCodec_InvalidFieldType(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetType(999)}, // invalid type
		},
	}
	_, err := NewParquetCodec(schema)
	if err == nil {
		t.Fatal("NewParquetCodec() expected error for invalid field type")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_NewParquetCodec_EmptyFieldName(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "", Type: ParquetInt64}, // empty name
		},
	}
	_, err := NewParquetCodec(schema)
	if err == nil {
		t.Fatal("NewParquetCodec() expected error for empty field name")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_NewParquetCodec_NegativeFieldType(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetType(-1)}, // negative type
		},
	}
	_, err := NewParquetCodec(schema)
	if err == nil {
		t.Fatal("NewParquetCodec() expected error for negative field type")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_NewParquetCodec_DuplicateFieldName(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "id", Type: ParquetString}, // duplicate name
		},
	}
	_, err := NewParquetCodec(schema)
	if err == nil {
		t.Fatal("NewParquetCodec() expected error for duplicate field name")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
	}
}

// -----------------------------------------------------------------------------
// Numeric Overflow Protection Tests
// -----------------------------------------------------------------------------

func TestParquetCodec_Int32Overflow_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "val", Type: ParquetInt32},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Value exceeds int32 max
	records := []any{
		map[string]any{"val": float64(3000000000)}, // > 2147483647
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for int32 overflow")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_Int64SafeRange_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "val", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Value exceeds safe integer range for float64 (2^53)
	// Use 1e18 which is much larger than 2^53 (≈9e15)
	records := []any{
		map[string]any{"val": float64(1e18)}, // 1e18 >> 2^53
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for int64 safe range exceeded")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

func TestParquetCodec_Float64NotInteger_Error(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "val", Type: ParquetInt32},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	// Float64 with fractional part
	records := []any{
		map[string]any{"val": float64(42.5)}, // not an integer
	}

	var buf bytes.Buffer
	err = codec.Encode(&buf, records)
	if err == nil {
		t.Fatal("Encode() expected error for non-integer float64")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
	}
}

// -----------------------------------------------------------------------------
// Dataset-Level Tests
// -----------------------------------------------------------------------------

func TestParquetCodec_StreamWriteRecords_ReturnsErrCodecNotStreamable(t *testing.T) {
	// Per CONTRACT_PARQUET.md: Parquet codec does NOT implement StreamingRecordCodec.
	// StreamWriteRecords with Parquet codec MUST return ErrCodecNotStreamable.

	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	ds, err := NewDataset("test-parquet", NewMemoryFactory(), WithCodec(codec))
	if err != nil {
		t.Fatalf("NewDataset failed: %v", err)
	}

	// Create a simple iterator
	iter := &parquetTestIterator{
		records: []any{
			map[string]any{"id": int64(1)},
		},
	}

	_, err = ds.StreamWriteRecords(t.Context(), iter, Metadata{})
	if err == nil {
		t.Fatal("StreamWriteRecords should return error for Parquet codec")
	}
	if !errors.Is(err, ErrCodecNotStreamable) {
		t.Errorf("StreamWriteRecords error = %v, want ErrCodecNotStreamable", err)
	}
}

func TestParquetCodec_DatasetWrite_Success(t *testing.T) {
	// Per CONTRACT_PARQUET.md: Callers MUST use Dataset.Write for Parquet encoding.

	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	ds, err := NewDataset("test-parquet-write", NewMemoryFactory(), WithCodec(codec))
	if err != nil {
		t.Fatalf("NewDataset failed: %v", err)
	}

	records := []any{
		map[string]any{"id": int64(1), "name": "alice"},
		map[string]any{"id": int64(2), "name": "bob"},
	}

	snapshot, err := ds.Write(t.Context(), records, Metadata{"source": "test"})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify manifest records codec
	if snapshot.Manifest.Codec != "parquet" {
		t.Errorf("Manifest.Codec = %q, want %q", snapshot.Manifest.Codec, "parquet")
	}

	// Verify row count
	if snapshot.Manifest.RowCount != 2 {
		t.Errorf("Manifest.RowCount = %d, want 2", snapshot.Manifest.RowCount)
	}

	// Read back and verify
	readRecords, err := ds.Read(t.Context(), snapshot.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(readRecords) != 2 {
		t.Errorf("Read returned %d records, want 2", len(readRecords))
	}
}

// parquetTestIterator is a simple RecordIterator for Parquet codec tests.
type parquetTestIterator struct {
	records []any
	index   int
	err     error
}

func (s *parquetTestIterator) Next() bool {
	if s.err != nil || s.index >= len(s.records) {
		return false
	}
	s.index++
	return true
}

func (s *parquetTestIterator) Record() any {
	if s.index == 0 || s.index > len(s.records) {
		return nil
	}
	return s.records[s.index-1]
}

func (s *parquetTestIterator) Err() error {
	return s.err
}
