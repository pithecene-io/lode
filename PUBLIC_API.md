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
// Or with custom layout:
reader, _ := lode.NewReader(
    lode.NewFSFactory("/data"),
    lode.WithLayout(lode.NewHiveLayout("day")),
)
```

---

## Configuration Options

Options are opt-in and composable. They override default components when
provided.

Options are validated at construction time. Passing an option that does
not apply to the target (dataset vs reader) returns an error.

Dataset construction uses:
- StoreFactory
- Layout
- Partitioning (via layout)
- Compressor
- Optional codec

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:

**Storage adapters:**
- `NewFSFactory(root)` - Filesystem storage
- `NewMemoryFactory()` - In-memory storage

**Layouts:**
- `NewDefaultLayout()` - Default novice-friendly layout
- `NewHiveLayout(keys...)` - Partition-first layout for pruning
- `NewFlatLayout()` - Minimal flat layout

**Compressors:**
- `NewNoOpCompressor()` - No compression (default)
- `NewGzipCompressor()` - Gzip compression

**Codecs:**
- `NewJSONLCodec()` - JSON Lines format

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
    Data      map[string]any
}

func (e Event) Timestamp() time.Time { return e.EventTime }

// Write computes min/max from Event.Timestamp()
ds.Write(ctx, []any{Event{...}, Event{...}}, lode.Metadata{})
```

Records that do not implement `Timestamped` (including `map[string]any`)
result in `nil` timestamp fields in the manifest—this is valid and indicates
timestamps are not applicable for that snapshot.

---

## Metadata

Writes always take explicit caller-supplied metadata. Empty metadata is
valid; nil metadata is not.

---

## Errors

Errors are returned for invalid configuration, storage failures, or
missing objects. Error semantics are stable and documented.

`ListDatasets` returns `ErrNoManifests` when storage contains objects but
no valid manifests.

---

## Examples

Examples under `examples/` use the public API only and demonstrate the
default bundle, explicit configuration, and required metadata.
