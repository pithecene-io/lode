# Lode Volume Model — Contract

This document defines the contract for the `Volume` persistence paradigm.
It is authoritative for all Volume implementations.

Status: **Active** (v0.6.0)

---

## Goals

1. Represent sparse, resumable byte-range persistence explicitly.
2. Preserve manifest-driven commit semantics for partial materialization workflows.
3. Keep adapter behavior backend-agnostic and lifecycle semantics explicit.

---

## Non-goals

- Torrent protocol behavior (pieces, peers, trackers).
- Networking, scheduling, runtime orchestration, or execution policy.
- Hidden background mutation or inference of missing ranges.

---

## Conceptual Model

### Volume

A logical byte address space `[0..N)` identified by `VolumeID` and `TotalLength`.

### VolumeSnapshot

An immutable commit that records the **cumulative** set of all verified
committed ranges across the volume's history. Each snapshot is self-contained:
its manifest is the sole authority for read visibility at that point in time.

### VolumeSnapshotID

A stable identifier for a committed Volume snapshot. Generated using Unix
nanoseconds (consistent with `DatasetSnapshotID`).

### SegmentRef

A reference to staged data eligible for commit (offset, length, object path).
It is not committed until included in a snapshot manifest.

Note: `SegmentRef` is a Volume concept. The Reader API uses `SnapshotRef`
(formerly `SegmentRef`) for dataset snapshot references.

### Volume Manifest

The authoritative description of committed ranges for a snapshot. Manifests
are **cumulative**: each manifest contains the full set of committed segments,
not just the segments added in that commit. This ensures every snapshot is
independently interpretable without chain traversal.

---

## Manifest Requirements

Volume manifests MUST include at minimum:
- schema name + version
- volume ID
- snapshot ID
- parent snapshot ID (when applicable)
- total length
- creation time
- explicit metadata object
- committed segments list (**cumulative** — all segments, not just new)

Each committed segment MUST record:
- offset
- length
- immutable object reference/path
- optional checksum metadata when configured

Manifests are cumulative:
- Each manifest contains the full set of committed segments across all
  prior commits, plus the new segments added in this commit.
- A single manifest is sufficient to determine complete read visibility.
- No chain traversal is required to interpret a snapshot.

Absence is meaningful:
- Ranges not listed are uncommitted and MUST NOT be inferred.

---

## Write Lifecycle

Volume write lifecycle is explicit:

1. **Stage**: Data is written to the final storage path. Staged data is not
   visible to readers (no manifest references it). Staged data files use the
   path `volumes/<volume_id>/data/<offset>-<length>.bin`.
2. **Verify**: Caller/runtime validates data externally (e.g., hash check).
3. **Commit**: Verified segments are recorded in a new immutable snapshot
   manifest alongside all previously committed segments (cumulative).

Rules:
- Unverified or uncommitted ranges MUST NOT appear in committed manifests.
- Commit visibility is manifest-driven, same as Dataset.
- Snapshots are immutable once committed.
- Overlapping committed ranges within a snapshot are invalid and MUST return
  `ErrOverlappingSegments`. Overlap validation applies to the full cumulative
  segment set (existing + new), not just new segments.
- Commit MUST include at least one new segment. Empty commits (no new segments)
  are invalid.

### Staged Data Lifecycle

Staged data is written directly to the final object path. Manifest-driven
visibility means uncommitted data files are invisible to readers — identical
to Dataset's partial-write behavior.

- Abandoned staged data (never committed) is harmless but may remain in storage.
- Cleanup of orphaned staged data is the caller's responsibility.
- No Volume-level Abort API is required.
- No implicit background cleanup is permitted.

### Resume Semantics

Resume is explicit and caller-driven.

- `Volume.Latest(ctx)` returns the most recent committed snapshot, which
  contains the cumulative set of all committed segments.
- The caller determines which ranges are missing and re-stages them.
- Volume does NOT discover or recover uncommitted staged data.
- No hidden inference or data directory scanning is permitted.

---

## Read Semantics

Volume reads are range-first:
- `ReadAt(snapshotID, offset, length)` succeeds only if the entire requested
  range is covered by committed segments.

Required behavior:
- If any sub-range is missing, return an explicit missing-range error.
- No zero-fill fallback.
- No partial-success ambiguity for committed read paths.
- Reads against a snapshot MUST reflect exactly the manifest state at commit time.

---

## Public API Surface (v0.6)

The public Volume API is explicit and minimal:

```go
// VolumeID uniquely identifies a volume.
type VolumeID string

// VolumeSnapshotID uniquely identifies an immutable volume snapshot.
type VolumeSnapshotID string

// SegmentRef references staged data eligible for commit.
type SegmentRef struct {
    Offset   int64
    Length   int64
    Path     string
    Checksum string // populated when WithVolumeChecksum is configured
}

// VolumeSnapshot represents an immutable point-in-time view of a volume.
type VolumeSnapshot struct {
    ID       VolumeSnapshotID
    Manifest *VolumeManifest
}

// NewVolume creates a volume with a fixed total length.
func NewVolume(id VolumeID, storeFactory StoreFactory, totalLength int64, opts ...VolumeOption) (*Volume, error)

// Volume represents a sparse, range-addressable byte space.
type Volume struct { /* opaque */ }

// StageWriteAt writes data at an offset and returns a segment handle.
// Staged data is not visible until Commit is called.
func (v *Volume) StageWriteAt(ctx context.Context, offset int64, r io.Reader) (SegmentRef, error)

// Commit records the provided segments into a new immutable snapshot.
// The resulting manifest is cumulative: it includes all previously committed
// segments plus the new segments. Commit MUST include at least one new segment.
func (v *Volume) Commit(ctx context.Context, segments []SegmentRef, metadata Metadata) (*VolumeSnapshot, error)

// ReadAt reads a fully committed range from a snapshot.
func (v *Volume) ReadAt(ctx context.Context, snapshotID VolumeSnapshotID, offset, length int64) ([]byte, error)

// Latest returns the most recently committed snapshot.
// Returns ErrNoSnapshots if no snapshots exist.
func (v *Volume) Latest(ctx context.Context) (*VolumeSnapshot, error)

// Snapshots lists all committed snapshots.
func (v *Volume) Snapshots(ctx context.Context) ([]*VolumeSnapshot, error)

// Snapshot retrieves a specific snapshot by ID.
// Returns ErrNotFound if the snapshot does not exist.
func (v *Volume) Snapshot(ctx context.Context, id VolumeSnapshotID) (*VolumeSnapshot, error)
```

### VolumeOption

Volume accepts a minimal set of options:

- `WithVolumeChecksum(c Checksum)` — opt-in integrity checksums on staged
  segments. Reuses the existing `Checksum` interface.

`VolumeOption` is extensible for future versions.

### API Constraints

- `StageWriteAt` MUST NOT create snapshot visibility.
- `Commit` MUST be manifest-driven and immutable.
- `Commit` MUST include at least one new segment.
- `Commit` MUST validate no overlaps in the full cumulative segment set.
- `ReadAt` MUST return `ErrRangeMissing` if any sub-range is uncommitted.
- `Latest` / `Snapshots` / `Snapshot` parallel Dataset's read surface.

---

## Error Semantics

- Missing committed range MUST return `ErrRangeMissing`.
- Overlapping segments at Commit MUST return `ErrOverlappingSegments`.
- Empty segment list at Commit MUST return an error.
- Nil metadata at Commit MUST return an error.
- Range reads MUST NOT return partial data without error.
- `Latest` on empty volume MUST return `ErrNoSnapshots`.
- `Snapshot` for unknown ID MUST return `ErrNotFound`.

---

## Concurrency

Snapshot history for a Volume is linear.

### Volume Write Concurrency Matrix

| Pattern | Writers | Segments | History | v0.6 Status | Future |
|---------|---------|----------|---------|-------------|--------|
| Single writer | 1 process | Non-overlapping ranges | Linear (guaranteed) | ✅ Supported | — |
| Multi-process, serialized | N processes, external coordination | Non-overlapping ranges | Linear (caller-enforced) | ✅ Supported (caller owns coordination) | CAS built into Lode |
| Multi-process, uncoordinated | N processes, no coordination | Non-overlapping ranges | **May fork** (undefined) | ⚠️ Unsafe | CAS + retry merges naturally |

**v0.6 behavior:**
- Single-writer semantics per volume apply unless callers provide external
  coordination.
- Concurrent uncoordinated writers MAY produce conflicting parent relationships
  or overlapping intent; Lode does not resolve these conflicts automatically.

**Future direction (not v0.6):**
- Optimistic concurrency (CAS): Commit detects stale parent and returns a
  conflict error. Callers retry by re-reading Latest(), merging their new
  segments with the updated cumulative manifest, and re-committing.
- Volume's non-overlapping byte ranges merge naturally on retry, making CAS
  particularly well-suited for this paradigm.

---

## Adapter Obligations

No new storage engine type is required.

Existing adapter capabilities remain sufficient:
- immutable object writes
- list/exists/get/delete behavior
- range reads

Adapters MUST preserve manifest-defined truth semantics.

## Storage Layout

Volume uses a **fixed internal layout**. This is not configurable.
The `Layout` abstraction is a Dataset-specific concept and does not apply
to Volume.

```
volumes/<volume_id>/snapshots/<snapshot_id>/manifest.json
volumes/<volume_id>/data/<offset>-<length>.bin
```

- Manifest paths follow the same commit-visibility pattern as Dataset.
- Data files are named by byte range for debuggability.
- This layout is an implementation detail, not part of the public API.

---

## Dataset Boundary

`Dataset` and `Volume` are coequal abstractions with distinct semantics:

- Dataset streaming: atomic construction of complete objects
- Volume streaming: incremental commitment of partial truth

If a workflow requires sparse, resumable, out-of-order, or range-verified truth,
it belongs to `Volume`, not `Dataset`.

---

## Prohibited Behaviors

- Inferring committed ranges not present in the manifest.
- Treating staged/unverified data as committed truth.
- Implicit background compaction or mutation of committed segments.

---

## Completeness

A Volume is considered complete when the union of committed ranges covers
the full address space `[0..N)`. Completeness is derived from committed ranges,
not stored as a separate flag.

---

## Invariants

The following invariants MUST hold:

1. A snapshot's manifest is the sole authority for read visibility.
2. Each manifest is self-contained (cumulative segments, no chain traversal needed).
3. No data may be readable unless referenced by a committed snapshot.
4. All committed segments are immutable.
5. Committed segments within a snapshot MUST NOT overlap.
6. Snapshot history for a Volume is linear.
7. Completeness is derived solely from committed ranges.
