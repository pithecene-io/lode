# Lode Core Model — Contract

This document defines the **core data model** and invariants for Lode.
It is authoritative for all implementations and adapters.

Lode defines persistence structure only. It does not define execution,
planning, scheduling, or optimization.

---

## Goals

1. Immutable, self-describing snapshots.
2. Explicit, persisted metadata.
3. Linear history per dataset.
4. Backend-agnostic layout semantics.

---

## Non-goals

- Query planning or execution.
- Background compaction or mutation.
- Backend-specific behavioral flags.

---

## Core Entities

### Dataset
Logical collection of snapshots. A dataset has a stable `DatasetID`.

### Snapshot
Immutable point-in-time state of a dataset. Each snapshot has a stable
`SnapshotID` and (optionally) a parent snapshot ID.

### Manifest
The authoritative description of a snapshot and its data files.

---

## Manifest Requirements

Manifests MUST be **self-describing** and **persisted**.

Minimum required fields:
- schema name + version
- dataset ID
- snapshot ID
- creation time
- explicit metadata object (see below)
- list of files with sizes and checksums (if available)
- parent snapshot ID (when applicable)

Manifests are immutable once written.

---

## Metadata Rules

- Metadata MUST be explicit on every snapshot.
- `nil` metadata is invalid and MUST error.
- Empty metadata (`{}`) is valid and MUST be persisted as an explicit object.
- Metadata values MUST be JSON-serializable.
- Metadata MUST never be inferred or defaulted.

---

## History & Immutability

- Snapshot history is strictly linear (single head).
- Snapshots are immutable after commit.
- Data files are immutable after write.
- Commits create new state; they do not mutate existing state.

---

## Design Invariant

> **Lode stores facts, not interpretations.**
