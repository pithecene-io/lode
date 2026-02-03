# Lode Storage Adapter — Contract

This document defines the required semantics for storage adapters.
It is authoritative for any implementation of the `Store` interface.

---

## Goals

1. Safe, immutable writes.
2. Accurate existence checks and listings.
3. Backend-agnostic behavior.

---

## Adapter Obligations

### Put
- MUST write data to the given path.
- MUST NOT overwrite existing data.
- If the path already exists, MUST return `ErrPathExists` (or an equivalent error).

### Get
- MUST return a readable stream for an existing path.
- If the path does not exist, MUST return `ErrNotFound` (or an equivalent error).

### Exists
- MUST accurately report existence.
- MUST not create or mutate data.

### List
- MUST return all paths under the given prefix.
- Ordering is unspecified.
- Pagination behavior (if any) MUST be documented by the adapter.

### Delete
- MUST remove the path if it exists.
- MUST be safe to call on a missing path (idempotent or `ErrNotFound`).

### ReadRange
- MUST return bytes from `[offset, offset+length)` for the given path.
- If the path does not exist, MUST return `ErrNotFound`.
- If offset or length is negative, MUST return `ErrInvalidPath`.
- If length exceeds platform `int` capacity, MUST return `ErrInvalidPath`.
- If offset+length would overflow, MUST return `ErrInvalidPath`.
- If the range extends beyond EOF, MUST return available bytes (not an error).
- If offset is beyond EOF, MUST return an empty slice.
- MUST use true range reads (not whole-file read) where the backend supports it.

### ReaderAt
- MUST return an `io.ReaderAt` for random access reads.
- If the path does not exist, MUST return `ErrNotFound`.
- The returned `ReaderAt` MUST support concurrent reads at different offsets.
- Callers are responsible for closing the underlying resource if it implements `io.Closer`.

---

## Commit Semantics

- Manifests (or explicit commit markers) define visibility.
- Writers MUST write data objects before the manifest.
- Readers MUST treat manifest presence as the commit signal.

## Staged Writes

- Adapters MUST allow data objects to be written before manifest commit.
- Adapters MUST NOT provide any implicit commit signal outside manifest presence.
- Safe write semantics (no overwrite) apply to staged objects as well.
- Adapters MUST allow deletion of staged objects via `Delete` for cleanup.

---

## Consistency Notes

Adapters MUST document:
- Consistency guarantees for `List` and `Exists`
- Any required read-after-write delays or mitigations
