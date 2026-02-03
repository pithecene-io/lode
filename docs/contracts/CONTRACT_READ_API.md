# Lode Read API — Design and Implementation

This document defines the **Lode Read API**, its goals, non-goals, adapter contracts, and implementation nuances.
It is intended to be authoritative for any storage adapter or higher-level tooling (Quarry CLI, UI, query engines)
that consumes data written by Lode.

---

## Goals

1. **Adapter-agnostic**: support S3-like object stores, local filesystems, etc.
2. **Manifest-driven**: planning decisions must be made from metadata before touching data.
3. **Range-read capable**: adapters must support efficient byte-range reads.
4. **Streaming-friendly**: sequential scans and tail-style reads must be natural.
5. **Stable semantics**: expose stored facts, not interpretations.

---

## Non-goals

- SQL execution engine
- Join planner or optimizer
- Arbitrary filtering beyond partition pruning and manifest statistics
- Mutable state or control-plane semantics

---

## Data Model

### Dataset → Partition → Segment → Object

- **Dataset**: logical collection (e.g. `nyc-rent/items`) **when the layout models datasets**.
- **Partition**: path-encoded organization, **as defined by the layout**.
- **Segment**: immutable append unit (often a run/attempt or time slice).
- **Object**: immutable blob (data file, index, manifest, artifact).

---

## Storage Adapter Interface

Adapters are the critical contract. Range reads must be *real*, not simulated.

```go
type Storage interface {
    Stat(ctx context.Context, key ObjectKey) (ObjectInfo, error)
    Open(ctx context.Context, key ObjectKey) (io.ReadCloser, error)
    ReadRange(ctx context.Context, key ObjectKey, offset int64, length int64) ([]byte, error)
    ReaderAt(ctx context.Context, key ObjectKey) (ReaderAt, error)
    List(ctx context.Context, prefix ObjectKey, opts ListOptions) (ListPage, error)
}
```

### Adapter obligations

- `ReadRange` must map to true range requests (e.g. HTTP Range on S3).
- `ReaderAt` should implement page-aligned caching and request coalescing.
- Adapters must document consistency guarantees and mitigations.

### Range read access paths

The `Reader` façade provides `ReaderAt(ctx, ObjectRef)` for random access reads.
Callers needing direct byte-range reads have two options:

1. **Via Reader.ReaderAt**: Returns `io.ReaderAt` for standard random access.
   Suitable for most use cases (Parquet footers, block indexes, etc.).

2. **Via Store.ReadRange**: Direct byte-range reads on the underlying store.
   Callers with access to the `Store` interface can use `ReadRange(ctx, path, offset, length)`.

The `Reader` interface intentionally omits a direct `ReadRange` method because
`io.ReaderAt` covers the majority of range-read use cases and provides a
standard Go interface for interoperability with existing libraries.

---

## ReaderAt Caching (Recommended)

- Page size: 256KiB–1MiB
- Cache size: 32–256 pages
- Optional sequential prefetch
- Metrics: range calls, bytes fetched

This is essential for Parquet footers, Arrow metadata, and block indexes.

---

## Lode Read API (Facade)

```go
type ReadAPI interface {
    ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error)
    ListPartitions(ctx context.Context, dataset DatasetID, opts PartitionListOptions) ([]PartitionRef, error)
    ListSegments(ctx context.Context, dataset DatasetID, partition PartitionPath, opts SegmentListOptions) ([]SegmentRef, error)
    GetManifest(ctx context.Context, dataset DatasetID, seg SegmentRef) (Manifest, error)
    OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)
    ReaderAt(ctx context.Context, obj ObjectRef) (ReaderAt, error)
}

### Read API Error Semantics

- `ListDatasets` MUST return a **layout-specific dataset error** when the layout
  does not model datasets (not a generic "not supported" error).
- `ListDatasets` MUST return an empty list only when storage is truly empty.
```

This layer understands Lode’s layout and semantics but performs **no interpretation**.

---

## Manifests

Manifests are immutable and authoritative.

### Minimum required fields

- schema name + version
- list of files with sizes and checksums
- row/event counts (total data units in snapshot)
- min/max timestamp (when data units are timestamped; omit if not applicable)
- references to sidecar indexes (optional)

When no codec is configured, reads MUST return raw bytes for the object
referenced by the manifest.

Manifests must be readable in a single small object fetch.

---

## Range Read Use Cases

### Columnar formats (Parquet / Arrow)

- Read footer via `ReadRange`
- Plan row groups and columns
- Fetch only required ranges

### Block-framed logs

- Fixed block headers with size + stats
- Optional sidecar index of block offsets
- Planner selects blocks, then issues range reads

### Artifacts

- Partial reads for previews
- Full reads only when explicitly requested

---

## Consistency and Commit Semantics

- Manifests (or explicit COMMIT markers) define segment visibility.
- Readers must treat manifest presence as the commit signal.
- Listing is discovery; manifests are truth.

---

## Layout

Layout is the **authoritative abstraction** for read/write path topology and
partition encoding. It governs:
- How manifests are discovered (commit visibility).
- How segment IDs are parsed from manifest paths.
- Whether datasets are modeled and how dataset IDs are discovered.
- How (and if) partitions are encoded in object paths.

**Layout is unified**: partition encoding is part of topology. Path-based partitioning
is not a separate concern from layout.

Implementations MAY compose Layout internally from subcomponents (e.g., topology +
partition encoding), but the **public surface MUST remain unified**. Internal
composition MUST validate compatibility and reject illogical combinations.

Directory topology is **not strictly enforced** by the library. Users may configure
layout strategies via an abstraction layer. The following is a **reference and
novice-friendly** layout (not canonical):

```
/datasets/<dataset>/
  /partition/<k=v>/<k=v>/
    /segments/<segment_id>/
      manifest.json
      /data/
      /index/
      /artifacts/
```

Alternative layouts are valid provided:
- Manifests remain discoverable via listing
- Object paths recorded in manifests are accurate and resolvable
- Commit semantics (manifest presence = visibility) are preserved

Layout abstraction enables efficient prefix listing and pruning for specific backends,
but layouts may also choose manifest-driven discovery when no path topology exists.

### Partition Semantics

Partitioning schemes (hive-style keys, ranges, hash buckets, spatial tiling, etc.)
are encoded by the layout. If partition information is **not** encoded in paths,
it MUST be recorded explicitly in manifests so consumers can interpret it.

### Reference Layouts (Curated)

The library SHOULD provide a **small, curated set** of layout implementations that
are known-good combinations, to reduce user friction and avoid illogical pairings.
Examples include:
- Default (novice-friendly) layout.
- Hive-style (path-encoded key/value partitions).
- Manifest-driven layout (minimal topology; discovery via manifests).

Additional layouts (spatial tiling, temporal hierarchies, range partitioning) are
valid future extensions, but MUST follow the same invariants.

#### Default Layout (Novice-Friendly)

The default layout MUST:
- Model datasets as the top-level discriminator.
- Be **segment-anchored**.
- Use **flat (unpartitioned)** data paths by default.
- Recognize manifests **only** at:
  `datasets/<dataset>/snapshots/<segment>/manifest.json`
- Treat datasets as existing **only** when at least one valid manifest path
  matches the strict default layout pattern.

Reference shape:
```
/datasets/<dataset>/
  /snapshots/<segment_id>/
    manifest.json
    /data/
      file.<ext>
```

### Future Extensions (Exploratory Matrix)

The following layout strategies and partition semantics are **non-normative**
examples intended to guide future exploration. They are not required for the
current implementation.

**Layout Strategy (topology / placement rules)**
- Segment-anchored: `.../<segment>/data/<partition...>/file`
- Partition-anchored: `.../<partition...>/<segment>/file`
- Manifest-only: only manifests have structure; data objects are opaque paths listed in manifest
- Flat: no partition dirs; everything under a segment is flat
- Keyed envelope: partitions encoded in filename, not path (e.g., `file__day=...__region=...`)

**Partition Semantics**
- Attribute K/V (Hive style: `day=`, `region=`)
- Range (`ts_range=2026-01-01_2026-01-07`)
- Hash/Bucket (`bucket=42`)
- Spatial tile (`h3=...`, `quadkey=...`)
- Temporal hierarchy (`2026/02/02` or `year=2026/month=02`)
- Prefix (`prefix=ab`)
- None (unpartitioned)

**Discouraged Pairings (Non-normative)**

Some combinations are intentionally discouraged because they degrade discovery
semantics or pruning behavior. This is a primary reason the public interface
exposes a **unified layout** rather than free mix-and-match components.

Examples of discouraged pairings:
- Partition-anchored + Hash/Bucket (high fan-out, poor locality for discovery).
- Flat + Attribute K/V (hides partitions from listing; forces manifest-only discovery).
- Keyed envelope + Spatial tile (opaque to prefix listing and typical spatial tooling).

---

## Implementation Phases

### R1 — Storage adapters
- Local FS
- S3 (HTTP range)
- ReaderAt cache

### R2 — Enumeration
- Dataset, partition, segment listing
- Canonical path parsing
- Commit rule enforcement

### R3 — Manifest handling
- Schema + parser + validator
- Object open + random access

### R4 — Reference helpers (optional)
- Manifest-based pruning helpers
- Parquet footer reader
- Block index reader

---

## Adapter Checklist

Any adapter must demonstrate:

1. True range reads
2. Efficient random access
3. Prefix listing with pagination
4. Cheap and accurate stat
5. Documented consistency semantics

---

## Design Invariant

> **Lode’s read API exposes stored facts, not interpretations.  
> Planning and meaning belong to consumers.**
