package lode

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"
)

// -----------------------------------------------------------------------------
// Parquet Codec Types
// -----------------------------------------------------------------------------

// ParquetType enumerates supported Parquet logical types.
type ParquetType int

// Parquet type constants for schema field definitions.
const (
	ParquetInt32 ParquetType = iota
	ParquetInt64
	ParquetFloat32
	ParquetFloat64
	ParquetString
	ParquetBool
	ParquetBytes
	ParquetTimestamp
)

// ParquetField defines a single field in a Parquet schema.
type ParquetField struct {
	Name     string
	Type     ParquetType
	Nullable bool
}

// ParquetSchema defines the record structure for Parquet encoding.
type ParquetSchema struct {
	Fields []ParquetField
}

// ParquetCompression specifies internal Parquet compression.
type ParquetCompression int

// Parquet compression options for internal file compression.
const (
	ParquetCompressionNone ParquetCompression = iota
	ParquetCompressionSnappy
	ParquetCompressionGzip
)

// ParquetOption configures Parquet codec behavior.
type ParquetOption func(*parquetCodec)

// WithRowGroupSize sets the target row group size in bytes.
func WithRowGroupSize(bytes int64) ParquetOption {
	return func(c *parquetCodec) {
		c.rowGroupSize = bytes
	}
}

// WithParquetCompression sets internal Parquet compression.
func WithParquetCompression(codec ParquetCompression) ParquetOption {
	return func(c *parquetCodec) {
		c.compression = codec
	}
}

// -----------------------------------------------------------------------------
// Parquet Codec Implementation
// -----------------------------------------------------------------------------

// Error sentinels ErrSchemaViolation and ErrInvalidFormat are defined in api.go.

// parquetCodec implements Codec for Apache Parquet format.
type parquetCodec struct {
	schema       ParquetSchema
	rowGroupSize int64
	compression  ParquetCompression
	pqSchema     *parquet.Schema
	fieldOrder   []string // ordered field names matching schema columns
}

// NewParquetCodec creates a Parquet codec with the given schema.
//
// The schema defines the structure of records. All records must conform to this
// schema during encoding. Fields not in the schema are silently ignored.
//
// Parquet codec does NOT implement StreamingRecordCodec because Parquet files
// require a footer that references all row groups. Use Dataset.Write for
// batched encoding.
func NewParquetCodec(schema ParquetSchema, opts ...ParquetOption) Codec {
	c := &parquetCodec{
		schema:       schema,
		rowGroupSize: 128 * 1024 * 1024, // 128 MB default
		compression:  ParquetCompressionSnappy,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.pqSchema = buildParquetSchema(schema)

	// Extract field order from the built schema
	c.fieldOrder = make([]string, len(schema.Fields))
	for i, f := range c.pqSchema.Fields() {
		c.fieldOrder[i] = f.Name()
	}

	return c
}

func (c *parquetCodec) Name() string {
	return "parquet"
}

func (c *parquetCodec) Encode(w io.Writer, records []any) error {
	// Buffer to collect complete parquet file
	var buf bytes.Buffer

	// Create buffer for collecting rows
	rowBuf := parquet.NewBuffer(c.pqSchema)

	if len(records) > 0 {
		// Convert and write records to buffer
		for i, record := range records {
			row, err := c.recordToRow(record, i)
			if err != nil {
				return err
			}
			if _, err := rowBuf.WriteRows([]parquet.Row{row}); err != nil {
				return fmt.Errorf("parquet: write row %d: %w", i, err)
			}
		}
	}

	// Write buffer to parquet file
	pqWriter := parquet.NewWriter(&buf, c.pqSchema, c.getCompressionOption())
	if _, err := pqWriter.WriteRowGroup(rowBuf); err != nil {
		_ = pqWriter.Close()
		return fmt.Errorf("parquet: write row group: %w", err)
	}

	if err := pqWriter.Close(); err != nil {
		return fmt.Errorf("parquet: close writer: %w", err)
	}

	// Write buffered content to output
	_, err := io.Copy(w, &buf)
	return err
}

func (c *parquetCodec) Decode(r io.Reader) ([]any, error) {
	// Read all content into buffer (parquet needs seeking)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("parquet: read file: %w", err)
	}

	if len(data) == 0 {
		return nil, ErrInvalidFormat
	}

	// Open parquet file from buffer
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrInvalidFormat
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidFormat, err)
	}

	numRows := file.NumRows()
	if numRows == 0 {
		return []any{}, nil
	}

	// Read all rows
	reader := parquet.NewReader(file)
	defer func() { _ = reader.Close() }()

	records := make([]any, 0, numRows)
	rows := make([]parquet.Row, 100)
	for {
		n, err := reader.ReadRows(rows)
		if n > 0 {
			for i := 0; i < n; i++ {
				record := c.rowToRecord(rows[i])
				records = append(records, record)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parquet: read rows: %w", err)
		}
	}

	return records, nil
}

func (c *parquetCodec) getCompressionOption() parquet.WriterOption {
	switch c.compression {
	case ParquetCompressionSnappy:
		return parquet.Compression(&parquet.Snappy)
	case ParquetCompressionGzip:
		return parquet.Compression(&parquet.Gzip)
	default:
		return parquet.Compression(&parquet.Uncompressed)
	}
}

// getFieldByName returns the ParquetField for a given field name.
func (c *parquetCodec) getFieldByName(name string) ParquetField {
	for _, f := range c.schema.Fields {
		if f.Name == name {
			return f
		}
	}
	return ParquetField{}
}

// recordToRow converts a map record to a parquet Row.
// The row values must be in the same order as the schema fields.
func (c *parquetCodec) recordToRow(record any, index int) (parquet.Row, error) {
	m, ok := record.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: record %d is not map[string]any", ErrSchemaViolation, index)
	}

	// Build row in schema field order
	row := make(parquet.Row, len(c.fieldOrder))
	for i, fieldName := range c.fieldOrder {
		field := c.getFieldByName(fieldName)

		val, exists := m[fieldName]
		if !exists || val == nil {
			if !field.Nullable {
				return nil, fmt.Errorf("%w: record %d missing required field %q", ErrSchemaViolation, index, fieldName)
			}
			row[i] = parquet.NullValue().Level(0, 0, i)
			continue
		}

		pqVal, err := c.convertToParquetValue(val, field, index)
		if err != nil {
			return nil, err
		}
		defLevel := 1
		if !field.Nullable {
			defLevel = 0
		}
		row[i] = pqVal.Level(0, defLevel, i)
	}
	return row, nil
}

// rowToRecord converts a parquet Row back to a map.
func (c *parquetCodec) rowToRecord(row parquet.Row) map[string]any {
	record := make(map[string]any, len(c.fieldOrder))
	for i, fieldName := range c.fieldOrder {
		if i >= len(row) {
			continue
		}
		field := c.getFieldByName(fieldName)
		val := row[i]
		if val.IsNull() {
			record[fieldName] = nil
			continue
		}
		record[fieldName] = c.convertFromParquetValue(val, field)
	}
	return record
}

// convertToParquetValue converts a Go value to a parquet Value.
func (c *parquetCodec) convertToParquetValue(val any, field ParquetField, index int) (parquet.Value, error) {
	switch field.Type {
	case ParquetInt32:
		switch v := val.(type) {
		case int:
			return parquet.Int32Value(int32(v)), nil
		case int32:
			return parquet.Int32Value(v), nil
		case int64:
			return parquet.Int32Value(int32(v)), nil
		case float64: // JSON numbers
			return parquet.Int32Value(int32(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected int32, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetInt64:
		switch v := val.(type) {
		case int:
			return parquet.Int64Value(int64(v)), nil
		case int32:
			return parquet.Int64Value(int64(v)), nil
		case int64:
			return parquet.Int64Value(v), nil
		case float64: // JSON numbers
			return parquet.Int64Value(int64(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected int64, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetFloat32:
		switch v := val.(type) {
		case float32:
			return parquet.FloatValue(v), nil
		case float64:
			return parquet.FloatValue(float32(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected float32, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetFloat64:
		switch v := val.(type) {
		case float32:
			return parquet.DoubleValue(float64(v)), nil
		case float64:
			return parquet.DoubleValue(v), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected float64, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetString:
		switch v := val.(type) {
		case string:
			return parquet.ByteArrayValue([]byte(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected string, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetBool:
		switch v := val.(type) {
		case bool:
			return parquet.BooleanValue(v), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected bool, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetBytes:
		switch v := val.(type) {
		case []byte:
			return parquet.ByteArrayValue(v), nil
		case string:
			return parquet.ByteArrayValue([]byte(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected []byte, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetTimestamp:
		switch v := val.(type) {
		case time.Time:
			return parquet.Int64Value(v.UnixNano()), nil
		case string:
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return parquet.Value{}, fmt.Errorf("%w: record %d field %q: invalid timestamp: %w", ErrSchemaViolation, index, field.Name, err)
			}
			return parquet.Int64Value(t.UnixNano()), nil
		default:
			return parquet.Value{}, fmt.Errorf("%w: record %d field %q: expected time.Time, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	default:
		return parquet.Value{}, fmt.Errorf("%w: record %d field %q: unknown type %d", ErrSchemaViolation, index, field.Name, field.Type)
	}
}

// convertFromParquetValue converts a parquet Value back to a Go value.
func (c *parquetCodec) convertFromParquetValue(val parquet.Value, field ParquetField) any {
	switch field.Type {
	case ParquetInt32:
		return val.Int32()
	case ParquetInt64:
		return val.Int64()
	case ParquetFloat32:
		return val.Float()
	case ParquetFloat64:
		return val.Double()
	case ParquetString:
		return string(val.ByteArray())
	case ParquetBool:
		return val.Boolean()
	case ParquetBytes:
		return val.ByteArray()
	case ParquetTimestamp:
		return time.Unix(0, val.Int64()).UTC()
	default:
		return nil
	}
}

// buildParquetSchema creates a parquet-go schema from our schema definition.
func buildParquetSchema(schema ParquetSchema) *parquet.Schema {
	group := make(parquet.Group, len(schema.Fields))
	for _, field := range schema.Fields {
		group[field.Name] = buildFieldNode(field)
	}
	return parquet.NewSchema("record", group)
}

func buildFieldNode(field ParquetField) parquet.Node {
	var node parquet.Node

	switch field.Type {
	case ParquetInt32:
		node = parquet.Int(32)
	case ParquetInt64:
		node = parquet.Int(64)
	case ParquetFloat32:
		node = parquet.Leaf(parquet.FloatType)
	case ParquetFloat64:
		node = parquet.Leaf(parquet.DoubleType)
	case ParquetString:
		node = parquet.String()
	case ParquetBool:
		node = parquet.Leaf(parquet.BooleanType)
	case ParquetBytes:
		node = parquet.Leaf(parquet.ByteArrayType)
	case ParquetTimestamp:
		node = parquet.Timestamp(parquet.Nanosecond)
	default:
		node = parquet.String() // fallback
	}

	if field.Nullable {
		node = parquet.Optional(node)
	}

	return node
}
