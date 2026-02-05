# Lode Parquet Codec — Contract

This document defines the required semantics for Parquet codec integration.
It is authoritative for any Parquet codec implementation within Lode.

---

## Goals

1. Columnar storage format for efficient analytical queries.
2. Interoperability with common external readers (tested with basic primitives).
3. Schema-explicit encoding (no schema inference from data).
4. Codec interface compliance with Lode's existing abstractions.

---

## Non-goals

- Query execution or predicate pushdown.
- Automatic schema evolution or merging.
- Row-group-level partitioning within Lode (external readers handle this).
- Encryption or column-level access control.
- Nested types (structs, lists, maps) in initial implementation.

---

## Parquet Codec Interface

The Parquet codec MUST implement the `Codec` interface:

```go
type Codec interface {
    Name() string                             // Returns "parquet"
    Encode(w io.Writer, records []any) error  // Batch encoding
    Decode(r io.Reader) ([]any, error)        // Batch decoding
}
```

The Parquet codec MUST NOT implement `StreamingRecordCodec`. Parquet's footer
requirement makes true streaming impossible without full buffering.
`StreamWriteRecords` with a Parquet codec MUST return `ErrCodecNotStreamable`.

### Name

- `Name()` MUST return `"parquet"`.
- This name is recorded in manifests.

### Encode

- `Encode(w, records)` MUST write a valid Parquet file to `w`.
- The Parquet file MUST include the footer (requires buffering all data).
- Records MUST be encoded according to the configured schema.
- Empty records (`len(records) == 0`) MUST produce a valid Parquet file with zero rows.
- Encoding errors MUST be returned, not silently ignored.

### Decode

- `Decode(r)` MUST read a complete Parquet file from `r`.
- Returns records as `[]any` where each record is `map[string]any`.
- Field types are mapped according to the Type Mapping table below.
- Decode MUST support files written by this codec and standard Parquet writers.

---

## Schema Requirements

Parquet requires a schema. Lode Parquet codec MUST support explicit schema configuration.

### Schema Definition

Schemas are defined at codec construction time:

```go
codec, err := lode.NewParquetCodec(lode.ParquetSchema{
    Fields: []lode.ParquetField{
        {Name: "id", Type: lode.ParquetInt64},
        {Name: "name", Type: lode.ParquetString},
        {Name: "timestamp", Type: lode.ParquetTimestamp},
    },
})
if err != nil {
    // Handle schema validation error
}
```

### Schema Construction Validation

`NewParquetCodec` validates the schema and returns an error for:

- Invalid `ParquetType` values (out of range).
- Empty field names.
- Duplicate field names (would silently drop columns).

All schema validation errors wrap `ErrSchemaViolation`.

### Record Validation

- Records MUST contain all required (non-nullable) fields defined in the schema.
- **Extra fields**: Fields in records that are not in the schema MUST be silently ignored.
  This enables forward compatibility when record producers add fields.
- Missing required fields MUST return `ErrSchemaViolation`.
- Type mismatches (after coercion attempts) MUST return `ErrSchemaViolation`.

### No Schema Inference

- The codec MUST NOT infer schema from record data.
- Explicit schema is required for predictable, portable output.
- This aligns with Lode's principle: "stores facts, not interpretations."

---

## Type Mapping

### Supported Types

| Parquet Type         | Go Source Types (accepted)              | Decoded Go Type   |
|---------------------|----------------------------------------|-------------------|
| `ParquetInt32`      | `int`, `int32`, `int64`, `float64`     | `int32`           |
| `ParquetInt64`      | `int`, `int32`, `int64`, `float64`     | `int64`           |
| `ParquetFloat32`    | `float32`, `float64`                   | `float32`         |
| `ParquetFloat64`    | `float32`, `float64`                   | `float64`         |
| `ParquetString`     | `string`                               | `string`          |
| `ParquetBool`       | `bool`                                 | `bool`            |
| `ParquetBytes`      | `[]byte`, `string`                     | `[]byte`          |
| `ParquetTimestamp`  | `time.Time`, `string` (RFC3339)        | `time.Time`       |

### Explicit Type Coercion

The codec performs **explicit, documented type coercions** to support common patterns:

1. **JSON number conversion**: `float64` is accepted for integer fields because
   `encoding/json` decodes all numbers as `float64`. This enables interop with
   JSONL-decoded records.

2. **Timestamp string parsing**: RFC3339/RFC3339Nano strings are accepted for
   timestamp fields. Invalid timestamp strings return `ErrSchemaViolation`.

3. **Bytes from string**: `string` is accepted for bytes fields (converted via
   `[]byte(s)`).

These coercions are **not inference**—the schema still determines the output type.
Coercion only affects which input types are accepted.

### Numeric Overflow Protection

When coercing `float64` to integer types, the codec validates:

- **Truncation check**: `float64` values MUST be whole numbers (`math.Trunc(v) == v`).
  Non-integer values return `ErrSchemaViolation`.
- **Int32 range check**: Values MUST be within `[-2147483648, 2147483647]`.
  Out-of-range values return `ErrSchemaViolation`.
- **Int64 safe range check**: For `float64` → `int64`, values MUST be within
  `[-2^53, 2^53]` (the range where `float64` can represent integers exactly).
  Values outside this range return `ErrSchemaViolation`.

These checks prevent silent data corruption from numeric overflow or precision loss.

### Nullable Fields

- Fields MAY be marked as nullable in the schema.
- Nullable fields accept `nil` values and encode as Parquet null.
- Non-nullable fields with `nil` or missing values MUST return `ErrSchemaViolation`.

---

## Streaming Limitations

Parquet files require a footer that references row group metadata.
This makes true streaming impossible.

### Memory Behavior

- `Encode` buffers ALL records in memory before writing the Parquet file.
- `Decode` reads the ENTIRE file into memory before returning records.
- Memory usage scales linearly with data size.
- For large datasets, callers MUST chunk data into multiple snapshots.

### StreamWriteRecords Behavior

The Parquet codec MUST NOT implement `StreamingRecordCodec`:

- `StreamWriteRecords` with Parquet codec returns `ErrCodecNotStreamable`.
- Callers MUST use `Dataset.Write` for Parquet encoding.
- This is explicit: Parquet's format precludes streaming without buffering.

---

## Row Group Configuration

Row groups affect read performance and memory usage.

### Defaults

- Row group size is implementation-defined (typically library default).
- Single row group is acceptable for small datasets.

Row group configuration is an implementation detail and not exposed in the public API.

---

## Compression

Parquet supports internal compression per column chunk.

### Supported Compression Options

```go
const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy  // Default, good balance
    ParquetCompressionGzip    // Higher ratio, slower
)
```

Note: Zstd is not supported in initial implementation due to library constraints.

### Compression Layering

**Important**: Parquet internal compression is separate from Lode's `Compressor`.

- When using Parquet codec, set Lode's compressor to `"noop"`.
- Double compression (Parquet + Lode compressor) wastes CPU with minimal benefit.
- Manifest records Lode's compressor (`"noop"`), not Parquet's internal compression.

### Manifest Recording

- Lode's `Compressor` field records the external compressor (`"noop"` recommended).
- Internal Parquet compression is part of the file format, not recorded in manifest.

---

## Statistics and Metadata

Parquet files contain statistics (min/max, null count, row count).

### Manifest Integration

The following statistics MAY be extracted and recorded in manifests:

- `RowCount`: Total rows in the file (from Parquet metadata).
- `MinTimestamp` / `MaxTimestamp`: When a timestamp column is designated.

### Future: Extended Manifest Stats

Future versions MAY add:
- Column-level min/max statistics.
- Null counts per column.
- Byte size per column.

These extensions are additive and do not affect this contract.

---

## Error Semantics

### Error Sentinels

The following errors MUST be defined in `lode/api.go` for user matching:

```go
// ErrSchemaViolation indicates a record does not conform to the Parquet schema.
var ErrSchemaViolation = errSchemaViolation{}

// ErrInvalidFormat indicates the Parquet file is malformed or corrupted.
var ErrInvalidFormat = errInvalidFormat{}
```

### Encoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Missing required field       | `ErrSchemaViolation`            |
| Type mismatch after coercion | `ErrSchemaViolation`            |
| Nil value for non-nullable   | `ErrSchemaViolation`            |
| Invalid timestamp string     | `ErrSchemaViolation`            |
| Float64 with fractional part | `ErrSchemaViolation`            |
| Int32 overflow               | `ErrSchemaViolation`            |
| Int64 safe range exceeded    | `ErrSchemaViolation`            |
| Write failure                | Underlying io error             |

### Decoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Invalid Parquet file         | `ErrInvalidFormat`              |
| Corrupted data               | `ErrInvalidFormat`              |
| Empty file                   | `ErrInvalidFormat`              |
| Row read failure             | `ErrInvalidFormat` (wrapped)    |

Note: Row-level read errors are wrapped with `ErrInvalidFormat` because they
indicate format-level problems (corrupted row data, truncated file, etc.).

---

## Construction API

### Minimal API

```go
// NewParquetCodec creates a Parquet codec with the given schema.
// Returns an error if the schema is invalid (bad types, empty names, duplicates).
func NewParquetCodec(schema ParquetSchema, opts ...ParquetOption) (Codec, error)

// ParquetSchema defines the record structure.
type ParquetSchema struct {
    Fields []ParquetField
}

// ParquetField defines a single field.
type ParquetField struct {
    Name     string
    Type     ParquetType
    Nullable bool
}

// ParquetType enumerates supported Parquet logical types.
type ParquetType int

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
```

### Options

```go
// WithParquetCompression sets internal Parquet compression.
func WithParquetCompression(codec ParquetCompression) ParquetOption

type ParquetCompression int

const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy
    ParquetCompressionGzip
)
```

---

## External Reader Compatibility

### Scope

Parquet files produced by this codec are **tested with** and **expected to work with**:
- Apache Arrow / PyArrow
- DuckDB
- Apache Spark
- Polars

### Compatibility Constraints

- **Primitive types only**: Flat schemas with primitive types (no nested structs/lists/maps).
- **Standard logical types**: Uses standard Parquet logical types, no custom extensions.
- **No encryption**: Files are unencrypted.

### Not Guaranteed

- Complex nested schemas
- Custom logical type annotations
- Parquet encryption features
- Row-group-level statistics for partition pruning (reader-dependent)

### Standard Compliance

- Output MUST conform to Apache Parquet specification.
- Use standard logical types (not custom extensions).

---

## Implementation Notes

### Internal Invariants

The implementation validates all schema constraints in `NewParquetCodec`. Internal
functions that process validated schemas may panic on invalid types as a defense-in-depth
measure. Such panics indicate a programming error (validation bypass), not user input error.
This is acceptable because:

1. The panic is unreachable under normal usage (validation catches all invalid inputs).
2. It provides a clear failure mode if validation is inadvertently bypassed.
3. Returning errors from internal functions would add unnecessary complexity.

### Recommended Library

The `parquet-go` library (github.com/parquet-go/parquet-go) is recommended:
- Pure Go implementation.
- Supports schema-based encoding.
- Active maintenance.

### Alternative Libraries

- `github.com/xitongsys/parquet-go`: Older, less maintained.
- `github.com/apache/arrow-go`: Heavier dependency, more features.

Library choice is implementation detail, not contract-bound.

---

## Testing Requirements

### Unit Tests

- Encode/decode round-trip for all supported types.
- Schema construction validation (invalid types, empty names, duplicates).
- Record validation (missing fields, type mismatches, nullability).
- Type coercion (JSON numbers, timestamp strings).
- Numeric overflow protection (int32 range, int64 safe range, truncation).
- Extra fields ignored (forward compatibility).
- Empty record handling.
- Large record batches (verify row group behavior).

### Dataset-Level Tests

- `StreamWriteRecords` returns `ErrCodecNotStreamable` for Parquet codec.
- `Dataset.Write` produces readable Parquet files.
- Manifest records `codec: "parquet"`.

### External Reader Tests (Optional)

- Read files with DuckDB (if available in CI).
- Read files with PyArrow (if available in CI).

### Contract Compliance Tests

- `Name()` returns `"parquet"`.
- Manifests record codec as `"parquet"`.
- Error sentinels are returned for documented conditions.

---

## Open Questions

The following decisions are deferred to future versions:

1. **Nested types**: Should the codec support nested structs/lists/maps?
   - Decision: Not in initial implementation. Flat schemas only.

2. **Schema from struct tags**: Should schemas be derivable from Go struct tags?
   - Decision: Explicit schema only. Struct tags may be added later.

3. **Zstd compression**: Should Zstd be added as a compression option?
   - Decision: Deferred until library support is verified.

---

## References

- [Apache Parquet Specification](https://parquet.apache.org/docs/)
- [parquet-go Library](https://github.com/parquet-go/parquet-go)
- [CONTRACT_LAYOUT.md](CONTRACT_LAYOUT.md) — Codec recording in manifests
- [CONTRACT_CORE.md](CONTRACT_CORE.md) — Manifest requirements
- [CONTRACT_ERRORS.md](CONTRACT_ERRORS.md) — Error taxonomy
