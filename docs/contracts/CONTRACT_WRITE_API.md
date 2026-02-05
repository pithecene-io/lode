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

### StreamWrite Semantics

- `StreamWrite(ctx, metadata)` MUST return an error if metadata is nil.
- `StreamWrite` MUST return a `StreamWriter` for a single binary data unit.
- `StreamWriter.Commit(ctx)` MUST write the manifest and return the new snapshot.
- A snapshot MUST NOT be visible before `Commit` writes the manifest.
- `StreamWriter.Abort(ctx)` MUST ensure no manifest is written.
- `StreamWriter.Close()` without `Commit` MUST behave as `Abort`.
- Streamed writes MUST be single-pass writes to the final object path.
- Streamed writes MUST NOT mutate existing objects; all writes are new paths.
- Streamed writes MUST set row/event count to `1`.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- When a codec is configured, `StreamWrite` MUST return an error.

### StreamWriteRecords Semantics

- `StreamWriteRecords(ctx, records, metadata)` MUST return an error if metadata is nil.
- `StreamWriteRecords` MUST return an error if records iterator is nil.
- `StreamWriteRecords` MUST consume records via a pull-based iterator.
- `StreamWriteRecords` MUST return an error if the configured codec does not support
  streaming record encoding.
- `StreamWriteRecords` MUST return an error if partitioning is configured (non-noop
  partitioner), since single-pass streaming cannot partition without buffering.
- Streamed record writes MUST be single-pass writes to the final object path.
- Streamed record writes MUST NOT mutate existing objects; all writes are new paths.
- On success, the manifest is written and the new snapshot is returned.
- On error (iterator failure, codec error, storage error), no manifest is written
  and best-effort cleanup of partial objects is attempted.
- Row/event count MUST equal the total number of records consumed.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.

### Timestamp computation

- When records implement the `Timestamped` interface, `Write` MUST compute
  `MinTimestamp` and `MaxTimestamp` from the data.
- Timestamps are extracted by calling `Timestamp()` on each record that
  implements the interface.
- Records that do not implement `Timestamped` are ignored for timestamp
  computation.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Timestamp computation is explicit (via interface implementation), not inferred.
- The same timestamp rules apply to `StreamWriteRecords` as records are consumed.

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

### Storage-Level Concurrency for Streaming Writes

Streaming writes (`StreamWrite`, `StreamWriteRecords`) that produce large payloads
may use the storage adapter's multipart/chunked upload path.

**Backends with conditional completion support (e.g., S3 with `If-None-Match`):**
- Both atomic and multipart paths provide atomic no-overwrite guarantees.
- No additional coordination required beyond the adapter.

**Backends without conditional completion support:**
- The multipart path has a TOCTOU window between preflight check and completion.
- Callers MUST ensure single-writer semantics per object path, OR
- Callers MUST use external coordination (distributed locks, queues, etc.)

See `CONTRACT_STORAGE.md` for adapter-specific thresholds and guarantees.

---

## Error Semantics

Implementations MUST use the following error sentinels where applicable:

- `ErrNoSnapshots` for empty history
- `ErrNotFound` for missing snapshot ID
- `ErrPathExists` when attempting to write to an existing path
- `ErrCodecConfigured` when `StreamWrite` is called with a configured codec
- `ErrCodecNotStreamable` when `StreamWriteRecords` codec lacks streaming support
- `ErrNilIterator` when `StreamWriteRecords` is called with a nil iterator
- `ErrPartitioningNotSupported` when `StreamWriteRecords` is used with non-noop partitioning

Streaming-specific error taxonomy and handling guidance are defined in
`CONTRACT_ERRORS.md`.

---

## Prohibited Behaviors

- In-place mutation of existing data files
- Metadata inference or defaulting
- Branching or multi-head snapshot history
