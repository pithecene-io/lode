# StreamWriteRecords — Streaming Record Writes

This example demonstrates `StreamWriteRecords` for streaming large record sets
without loading all data into memory.

## What This Demonstrates

1. **Pull-based streaming**: Records are pulled from an iterator one at a time
2. **Memory efficiency**: Only one record in memory at a time (iterator controls pace)
3. **Atomic completion**: Either all records are written and manifest created, or nothing
4. **Metadata explicitness**: Metadata must be non-nil (empty `{}` is valid)

Note: Commit visibility (snapshot invisible before manifest) is documented behavior but
not explicitly demonstrated here. See `manifest_driven` example for visibility semantics.

## When to Use StreamWriteRecords

Use `StreamWriteRecords` when:
- You have large record streams that shouldn't be buffered in memory
- Records come from a database cursor, file reader, or channel
- You're using a streaming-capable codec (e.g., JSONL)

Do NOT use `StreamWriteRecords` when:
- You need partitioning (single-pass streaming cannot partition)
- Your codec doesn't support streaming
- Data is already in memory (use `Write` instead)

## RecordIterator Interface

```go
type RecordIterator interface {
    Next() bool  // Advances to next record. Returns false when exhausted.
    Record() any // Returns current record. Only valid after Next returns true.
    Err() error  // Returns any error encountered during iteration.
}
```

Implement this interface to stream from any data source.

## Run

```bash
go run ./examples/stream_write_records
```

## Expected Output

```
Storage root: /tmp/lode-stream-records-...

=== SETUP ===
Created dataset with JSONL codec (streaming-capable)

=== STREAM WRITE ===
Streaming 5 records...
Created snapshot: <id>
Row count: 5
Codec: jsonl
Files:
  - datasets/events/snapshots/<id>/data/data (...)

=== VERIFY ===
Read back 5 records:
  map[event:signup id:1 user:alice]
  ...

=== KEY POINTS ===
1. StreamWriteRecords requires a streaming-capable codec (e.g., JSONL)
2. Records are pulled from iterator one at a time (memory efficient)
3. Manifest is written only after successful completion
4. If iterator returns error, no manifest is written (no snapshot)
5. Metadata must be non-nil (use empty map {} if no metadata)
6. Partitioning is NOT supported (single-pass streaming cannot partition)

=== SUCCESS ===
StreamWriteRecords demonstration complete!
```

## Related

- [`blob_upload`](../blob_upload) — Raw blob streaming with `StreamWrite`
- [`default_layout`](../default_layout) — In-memory `Write` with codec
- [PUBLIC_API.md](../../PUBLIC_API.md) — Streaming API constraints
