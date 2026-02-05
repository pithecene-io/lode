# Lode

**Database-like discipline for object storage.**

Lode is an embeddable Go persistence framework that brings snapshots,
metadata, and safe write semantics to object storage systems like
filesystems and S3—without running a database.

It is not a storage engine.
It is not a query engine.
It is a way to make object storage *reliable, inspectable, and hard to misuse*.

---

## Why Lode exists

Many systems already use object storage as their primary persistence layer:

- Events written to S3
- Parquet files on a filesystem
- Analytics data dumped “by convention”

What’s usually missing is discipline:
- No clear snapshots or versions
- No metadata guarantees
- Accidental overwrites
- Ad-hoc directory layouts
- Unclear read/write safety

Lode formalizes the parts that should be formalized and refuses to do the rest.

---

## Core ideas

Lode is built around a small set of invariants:

- **Datasets are immutable**
- **Writes produce snapshots**
- **Snapshots reference manifests**
- **Manifests describe data files and metadata**
- **Manifest presence is the commit signal** — a snapshot is visible only after its manifest is persisted
- **Metadata is explicit, persisted, and self-describing (never inferred)**
- **Storage, format, compression, and partitioning are orthogonal**
- **Single-writer semantics required** — Lode does not resolve concurrent writer conflicts

If you know the snapshot ID, you know exactly what data you are reading.

---

## Quick Start (5 minutes)

### Install

<!-- runnable -->
```bash
go get github.com/justapithecus/lode
```

### Write and read a blob

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
package main

import (
    "context"
    "fmt"
    "github.com/justapithecus/lode/lode"
)

func main() {
    ctx := context.Background()

    // Create dataset with filesystem storage
    ds, _ := lode.NewDataset("mydata", lode.NewFSFactory("/tmp/lode-demo"))

    // Write a blob (default: raw bytes, no codec)
    snap, _ := ds.Write(ctx, []any{[]byte("hello world")}, lode.Metadata{"source": "demo"})
    fmt.Println("Created snapshot:", snap.ID)

    // Read it back
    data, _ := ds.Read(ctx, snap.ID)
    fmt.Println("Data:", string(data[0].([]byte)))
}
```

### Write structured records

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// With a codec, Write accepts structured data
ds, _ := lode.NewDataset("events",
    lode.NewFSFactory("/tmp/lode-demo"),
    lode.WithCodec(lode.NewJSONLCodec()),
)

records := lode.R(
    lode.D{"id": 1, "event": "signup"},
    lode.D{"id": 2, "event": "login"},
)
snap, _ := ds.Write(ctx, records, lode.Metadata{"batch": "1"})
```

### Stream large files

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// StreamWrite is for large binary payloads (no codec)
ds, _ := lode.NewDataset("backups", lode.NewFSFactory("/tmp/lode-demo"))

sw, _ := ds.StreamWrite(ctx, lode.Metadata{"type": "backup"})
sw.Write([]byte("... large data ..."))
snap, _ := sw.Commit(ctx)
```

See [`examples/`](#examples) for complete runnable code.

---

## Which Write API?

| API | Use Case | Codec | Partitioning |
|-----|----------|-------|--------------|
| `Write` | In-memory data, small batches | ✅ Optional | ✅ Supported |
| `StreamWrite` | Large binary payloads (GB+) | ❌ Raw only | ❌ Not supported |
| `StreamWriteRecords` | Large record streams | ✅ Required (streaming) | ❌ Not supported |

**Decision flow:**
1. Is the data already in memory? → Use `Write`
2. Is it a large binary blob (no structure)? → Use `StreamWrite`
3. Is it a large stream of records? → Use `StreamWriteRecords` with a streaming codec

---

## What Lode is (and is not)

### Lode is:
- An embeddable Go library
- A persistence framework
- A way to organize object storage safely
- Explicit, boring, and predictable

### Lode is not:
- A database
- A storage engine
- A query planner
- A distributed system
- A background service

Lode owns *structure and life-cycle*, not execution.

---

## Guarantees

What Lode commits to:

| Guarantee | Detail |
|-----------|--------|
| **Immutable snapshots** | Once written, data files and manifests never change |
| **Atomic commits** | Manifest presence is the commit signal; no partial visibility |
| **Explicit metadata** | Every snapshot has caller-supplied metadata (never inferred) |
| **Safe writes** | Overwrites are prevented (atomic for small files; best-effort for large) |
| **Backend-agnostic** | Same semantics on filesystem, memory, or S3 |

What Lode explicitly does NOT provide:

| Non-goal | Why |
|----------|-----|
| Multi-writer conflict resolution | Requires distributed coordination; use external locks |
| Query execution | Lode structures data; query engines consume it |
| Background compaction | No implicit mutations; callers control lifecycle |
| Automatic cleanup | Partial objects from failed writes may remain |

For full contract details: [`docs/contracts/`](docs/contracts/)

---

## Gotchas

Common pitfalls when using Lode:

- **Metadata must be non-nil** — Pass `lode.Metadata{}` for empty metadata, not `nil`.
- **Raw mode expects `[]byte`** — Without a codec, `Write` expects exactly one `[]byte` element.
- **Single-writer only** — Concurrent writers to the same dataset may corrupt history.
- **Large uploads have TOCTOU risk** — Uploads >5GB (S3) use preflight checks; coordinate externally.
- **Cleanup is best-effort** — Failed streams may leave partial objects in storage.
- **StreamWriteRecords requires streaming codec** — Not all codecs support streaming.

See [`PUBLIC_API.md`](PUBLIC_API.md) for complete usage guidance.

---

## Example: event storage

A canonical Lode workflow looks like this:

- Dataset: `events`
- Rows: timestamped events
- Partitioning: Hive-style by day (`dt=YYYY-MM-DD`)
- Format: JSON Lines
- Compression: gzip
- Backend: file system or S3

Each write produces a new snapshot.
Reads always target a snapshot explicitly.

---

## Supported Backends

- **Filesystem** — Local storage via `NewFSFactory`
- **In-memory** — Testing via `NewMemoryFactory`
- **S3** — AWS S3, MinIO, LocalStack, R2 via `lode/s3`

---

## Storage Prerequisites

**Ensure storage exists before constructing a dataset or reader.**

Lode does not create storage infrastructure. This is intentional:
- No hidden side effects in constructors
- Explicit provisioning keeps control with the caller
- Same pattern across all backends

### Filesystem

```bash
# Create directory before use
mkdir -p /data/lode
```

<!-- illustrative -->
```go
// Then construct dataset
ds, err := lode.NewDataset("events", lode.NewFSFactory("/data/lode"))
```

### S3

```bash
# Create bucket before use (via AWS CLI, console, or IaC)
aws s3 mb s3://my-bucket
```

<!-- illustrative -->
```go
// Then construct dataset
store, err := s3.New(client, s3.Config{Bucket: "my-bucket"})
ds, err := lode.NewDataset("events", store)
```

### Bootstrap Helpers

If you need provisioning helpers, implement them outside core APIs:

<!-- illustrative -->
```go
// Example: ensure directory exists before constructing dataset
func EnsureFSDataset(id, root string, opts ...lode.Option) (*lode.Dataset, error) {
    if err := os.MkdirAll(root, 0755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.NewDataset(id, lode.NewFSFactory(root), opts...)
}
```

This keeps Lode's core APIs explicit and predictable.

---

## Examples

| Example | Purpose | Run |
|---------|---------|-----|
| [`default_layout`](examples/default_layout) | Write → list → read with default layout | `go run ./examples/default_layout` |
| [`hive_layout`](examples/hive_layout) | Partition-first layout with Hive partitioner | `go run ./examples/hive_layout` |
| [`blob_upload`](examples/blob_upload) | Raw blob write/read (no codec, default bundle) | `go run ./examples/blob_upload` |
| [`manifest_driven`](examples/manifest_driven) | Demonstrates manifest-as-commit-signal | `go run ./examples/manifest_driven` |
| [`s3_experimental`](examples/s3_experimental) | S3 adapter with LocalStack/MinIO | `go run ./examples/s3_experimental` |

Each example is self-contained and runnable. See the example source for detailed comments.

---

## Status

Lode is currently **pre-v0** and under active design.
APIs may change until invariants stabilize.

If you are evaluating Lode, focus on:
- snapshot semantics
- metadata visibility
- API clarity

Usage overview: `PUBLIC_API.md`
Concrete usage: `examples/`
Implementation milestones: `docs/IMPLEMENTATION_PLAN.md`

---

## License
Apache License 2.0. Copyright © 2026 Andrew Hu <me@andrewhu.nyc>.
