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

- `Write(ctx, data, metadata)` MUST create a new snapshot on success.
- `metadata` MUST be non-nil; nil MUST return an error.
- Empty metadata is valid and MUST be persisted explicitly.
- The new snapshot MUST reference the previous snapshot as its parent (if any).
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in `CONTRACT_CORE.md`
  (including row/event count and min/max timestamp when applicable).
- When no codec is configured, each write represents a single data unit and
  the row/event count MUST be `1`.

### Timestamp computation

- When records implement the `Timestamped` interface, `Write` MUST compute
  `MinTimestamp` and `MaxTimestamp` from the data.
- Timestamps are extracted by calling `Timestamp()` on each record that
  implements the interface.
- Records that do not implement `Timestamped` are ignored for timestamp
  computation.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Timestamp computation is explicit (via interface implementation), not inferred.

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

## Concurrency

Lode does not implement concurrent multi-writer conflict resolution.

**Single writer or external coordination required.**

- Concurrent writes from multiple processes to the same dataset may produce
  inconsistent history (e.g., two snapshots with the same parent).
- Callers MUST ensure at most one writer is active per dataset at any time.
- External coordination (locks, queues, leader election) is the caller's
  responsibility.

Lode guarantees safety within a single writer but does not detect or resolve
conflicts between multiple concurrent writers.

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
