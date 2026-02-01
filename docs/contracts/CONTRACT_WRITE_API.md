# Lode Write API — Contract

This document defines the required semantics for the write-facing API.
It is authoritative for any `Dataset` implementation.

---

## Goals

1. Deterministic snapshot creation.
2. Explicit metadata on every write.
3. Linear snapshot history.
4. Safe, immutable commits.

---

## Non-goals

- Query execution or filtering.
- Background compaction.
- Concurrent multi-writer conflict resolution.

---

## Dataset.Write Semantics

### Required behavior

- `Write(ctx, records, metadata)` MUST create a new snapshot on success.
- `metadata` MUST be non-nil; nil MUST return an error.
- Empty metadata is valid and MUST be persisted explicitly.
- The new snapshot MUST reference the previous snapshot as its parent (if any).
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in `CONTRACT_CORE.md`
  (including row/event count and min/max timestamp when applicable).

### Empty dataset behavior

- The first successful write creates the initial snapshot.
- `Latest()` on an empty dataset MUST return `ErrNoSnapshots`.
- `Snapshots()` on an empty dataset MUST return an empty list without error.
- `Snapshot(id)` on an empty dataset MUST return `ErrNotFound`.

---

## Read-after-write Visibility

- A snapshot is visible only after its manifest is persisted.
- Manifest presence is the commit signal.

---

## Error Semantics

Implementations MUST use the following error sentinels where applicable:

- `ErrNoSnapshots` for empty history
- `ErrNotFound` for missing snapshot ID
- `ErrPathExists` when attempting to write to an existing path

---

## Prohibited Behaviors

- In-place mutation of existing data files
- Metadata inference or defaulting
- Branching or multi-head snapshot history
