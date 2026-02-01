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

- **Dataset**: logical collection (e.g. `nyc-rent/items`)
- **Partition**: key-value path components (e.g. `day=2026-01-31/source=streeteasy`)
- **Segment**: immutable append unit (often a run/attempt or time slice)
- **Object**: immutable blob (data file, index, manifest, artifact)

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
```

This layer understands Lode’s layout and semantics but performs **no interpretation**.

---

## Manifests

Manifests are immutable and authoritative.

### Minimum required fields

- schema name + version
- list of files with sizes and checksums
- row/event counts
- min/max timestamp (when records are timestamped; omit if not applicable)
- references to sidecar indexes (optional)

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

Directory topology is **not strictly enforced** by the library. Users may configure
layout strategies via an abstraction layer. The following is a reference layout:

```
/datasets/<dataset>/
  /partition/<k=v>/<k=v>/
    /segments/<segment_id>/
      manifest.json
      /data/
      /index/
      /artifacts/
```

Alternative layouts (e.g., partitions nested inside segments) are valid provided:
- Manifests remain discoverable via listing
- Object paths recorded in manifests are accurate and resolvable
- Commit semantics (manifest presence = visibility) are preserved

Layout abstraction enables efficient prefix listing and pruning for specific backends.

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
