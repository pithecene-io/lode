# PUBLIC_API.md — Lode Public API

This document is a developer-facing overview of Lode’s public API.
It describes the entrypoints, defaults, and configuration shape used at
the callsite.

---

## Scope

Lode focuses on persistence structure:
datasets, snapshots, manifests, metadata, and safe write semantics.

Execution, scheduling, and query planning are out of scope.

---

## Construction Overview

### Dataset

`NewDataset(id, storeFactory, opts...)` creates a dataset with a documented
default configuration. Options override parts of that default bundle.

Default bundle:
- Layout: DefaultLayout
- Partitioner: NoOp (via layout)
- Compressor: NoOp
- Codec: none (raw blob storage)
- Checksum: none (opt-in)

Example:
```go
import (
    "github.com/justapithecus/lode/lode"
)

ds, _ := lode.NewDataset(
    "mydata",
    lode.NewFSFactory("/data"),
    lode.WithCodec(lode.NewJSONLCodec()),  // Optional: for structured records
)
```

### Reader

`NewReader(storeFactory, opts...)` creates a read facade.

Default behavior:
- Layout: DefaultLayout

Example:
```go
reader, _ := lode.NewReader(
    lode.NewFSFactory("/data"),
)
// Or with Hive layout (preferred for partitioned data):
reader, _ := lode.NewReader(
    lode.NewFSFactory("/data"),
    lode.WithHiveLayout("day"),
)
```

---

## Configuration Options

Options are opt-in and composable. They override default components when
provided.

Options are validated at construction time. Passing an option that does
not apply to the target (dataset vs reader) returns an error.

**Layout-specific option:**
- `WithHiveLayout(keys...)` - Preferred for Hive layout (validates on apply)

**General options:**
- `WithLayout(layout)` - For any layout (DefaultLayout, FlatLayout, or advanced use)
- `WithCompressor(c)` - Dataset-only
- `WithCodec(c)` - Dataset-only
- `WithChecksum(c)` - Dataset-only

Dataset construction uses:
- StoreFactory
- Layout (via WithHiveLayout or WithLayout)
- Partitioning (via layout)
- Compressor
- Optional codec
- Optional checksum

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:

**Storage adapters:**
- `NewFSFactory(root)` - Filesystem storage
- `NewMemoryFactory()` - In-memory storage
- `s3.New(client, config)` - S3-compatible storage (see below)

**Layouts:**
- `NewDefaultLayout()` - Default novice-friendly layout (used automatically)
- `NewHiveLayout(keys...) (layout, error)` - Partition-first layout (prefer `WithHiveLayout` for fluent API)
- `NewFlatLayout()` - Minimal flat layout

**Compressors:**
- `NewNoOpCompressor()` - No compression (default)
- `NewGzipCompressor()` - Gzip compression

**Codecs:**
- `NewJSONLCodec()` - JSON Lines format

**Checksums:**
- `NewMD5Checksum()` - MD5 file checksums (opt-in)

Constructed components are intended to be passed into dataset or reader
construction.

**Record helpers:**
- `D` - Shorthand alias for `map[string]any` in callsites and examples
- `R(...)` - Convenience helper to build `[]any` from record literals

**Interfaces:**
- `Timestamped` - Optional interface for records with timestamps (see below)

**Range read support:**
- `Store.ReadRange(ctx, path, offset, length)` - Read byte range from object
- `Store.ReaderAt(ctx, path)` - Get `io.ReaderAt` for random access
- `Reader.ReaderAt(ctx, obj)` - Get `io.ReaderAt` for data object

Range reads enable efficient access to columnar formats (Parquet footers),
block-indexed logs, and partial artifact previews.

---

## Timestamped Records

Records can optionally implement the `Timestamped` interface to enable
automatic min/max timestamp tracking in manifests:

```go
type Timestamped interface {
    Timestamp() time.Time
}
```

When records implement this interface, `Dataset.Write` computes
`MinTimestamp` and `MaxTimestamp` from the data and populates the manifest.
This enables time-range pruning by query engines.

Example:
```go
type Event struct {
    ID        string
    EventTime time.Time
    Data      lode.D
}

func (e Event) Timestamp() time.Time { return e.EventTime }

// Write computes min/max from Event.Timestamp()
ds.Write(ctx, []any{Event{...}, Event{...}}, lode.Metadata{})
```

Records that do not implement `Timestamped` (including `lode.D`)
result in `nil` timestamp fields in the manifest—this is valid and indicates
timestamps are not applicable for that snapshot.

---

## Metadata

Writes always take explicit caller-supplied metadata. Empty metadata is
valid; nil metadata is not.

---

## Write APIs

`Dataset.Write(ctx, data, metadata)` creates a snapshot from in-memory data.

`Dataset.StreamWrite(ctx, metadata)` returns a `StreamWriter` for single-pass
streaming writes of a single binary payload. `StreamWriter.Write` streams bytes,
`Commit` finalizes and returns a snapshot, and `Abort` discards the write.
If `Close` is called before `Commit`, the stream is aborted and no snapshot is created.
Streamed writes are raw-blob only: codecs are not applied and row count is `1`.

`Dataset.StreamWriteRecords(ctx, metadata, records)` consumes a pull-based iterator
of records and streams them through a streaming-capable codec. If the configured
codec does not support streaming, `StreamWriteRecords` returns an error.

---

## Usage Gotchas (Important)

- `metadata` must be non-nil on every write (use `{}` for empty metadata).
- Raw blob mode (no codec) requires exactly one `[]byte` element in `Write`.
- Raw blob mode cannot use partitioning (no record fields to extract keys).
- `WithHiveLayout` requires at least one partition key (validated on apply).
- `ListDatasets` returns `ErrNoManifests` when storage has objects but no manifests.
- Layouts that do not model datasets (e.g., flat) return `ErrDatasetsNotModeled`.
- `ReaderAt` may return an `io.ReaderAt` that also implements `io.Closer`; close it when done.
- Checksums are computed and recorded in manifests only when configured.
- `StreamWrite` is only valid when no codec is configured; otherwise it returns an error.
- `StreamWriteRecords` requires a streaming-capable codec; otherwise it returns an error.
- `StreamWriteRecords` does not support partitioning (single-pass streaming cannot partition).
- Aborted streams leave no snapshot; partial objects may remain (see "Safety Guarantees").

---

## Choosing a Write API

- Use `Write` for in-memory data, partitioned data, or codecs that do not support streaming.
- Use `StreamWrite` for large binary payloads that should be streamed once (no codec).
- Use `StreamWriteRecords` for large record streams with streaming-capable codecs (no partitioning).

---

## Safety Guarantees

### Commit Semantics

**Manifest presence is the commit signal.**

A snapshot becomes visible only after its manifest is successfully written to storage.
Data files may exist before the manifest, but they are not discoverable until the
manifest references them.

- `StreamWriter.Commit()` writes the manifest; the snapshot is invisible until then.
- `StreamWriter.Abort()` or `Close()` without `Commit()` ensures no manifest is written.
- On error during streaming, no manifest is written (the write never "happened").

### Single-Writer Requirement

**Lode does not implement concurrent multi-writer conflict resolution.**

Callers MUST ensure at most one writer is active per dataset at any time.
Concurrent writes from multiple processes may produce inconsistent history
(e.g., two snapshots with the same parent).

External coordination (locks, queues, leader election) is the caller's responsibility.

### Large Upload Caveats (TOCTOU)

For uploads exceeding the storage adapter's atomic threshold (e.g., 5GB for S3),
the no-overwrite guarantee weakens:

1. Adapter performs preflight existence check → "not exists"
2. Another writer creates the same path
3. This writer's upload completes and may overwrite

This TOCTOU (time-of-check-to-time-of-use) window cannot be closed without
conditional multipart completion (which S3 does not support).

**To preserve the no-overwrite guarantee on large uploads:**
- Ensure single-writer semantics per object path, OR
- Use external coordination (distributed locks, etc.)

Small uploads (≤ threshold) use atomic conditional writes with no TOCTOU window.

### Cleanup Behavior

**Cleanup of partial objects is best-effort, not guaranteed.**

On abort or error before commit:
- No manifest is written (the snapshot does not exist).
- Partial data objects may remain in storage.
- Callers should not rely on automatic cleanup of partial objects.
- Cleanup uses an independent context to maximize success even if the caller's
  context was canceled.

Failure to delete a partial object does not create a snapshot.

---

## Errors

Errors are returned for invalid configuration, storage failures, or
missing objects. Error semantics are stable and documented.

`ListDatasets` returns `ErrNoManifests` when storage contains objects but
no valid manifests.

---

## S3 Storage Adapter

The `lode/s3` package provides an S3-compatible storage adapter.

**Public API:**
- `s3.New(client, config)` - Create store from AWS SDK client
- `s3.Config{Bucket, Prefix}` - Store configuration

Client construction uses the AWS SDK directly. This keeps Lode's API surface
minimal while giving you full control over credentials, endpoints, and options.

### AWS S3

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/justapithecus/lode/lode/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)

store, _ := s3.New(client, s3.Config{
    Bucket: "my-bucket",
    Prefix: "lode-data/",
})
```

### LocalStack

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/justapithecus/lode/lode/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider("test", "test", ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
    o.UsePathStyle = true
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

### MinIO

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:9000")
    o.UsePathStyle = true
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

### Cloudflare R2

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("auto"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("https://" + accountID + ".r2.cloudflarestorage.com")
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

**Bucket requirement:** The bucket must exist before use. Lode does not create buckets.

**Consistency:** AWS S3 provides strong read-after-write consistency.
Other backends may differ — consult their documentation.

---

## Examples

Examples under `examples/` use the public API only and demonstrate the
default bundle, explicit configuration, and required metadata.
