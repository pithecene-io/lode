# Phase 0 — Architecture & Public API Implementation Plan (v0)

This document defines the **Phase 0 implementation plan** for Lode’s architecture
and public API.

It is intended for **agent-based implementation** (e.g. Claude Opus-class models),
not as end-user documentation.

The plan is explicitly iterative. Each section has a strict scope and a hard stop.

---

## Global Invariants (Apply to All Iterations)

- Dataset history is strictly linear
- Snapshots are immutable once written
- All reads and writes are streaming
- No query execution or scheduling logic
- Persistence plane and storage plane are strictly separated
- Public APIs must not leak backend-specific behavior

---

## Iteration 0.1 — Snapshot Metadata Semantics

### Scope

Define snapshot metadata semantics and validation behavior.

### Decisions

- Snapshot metadata is always explicit
- Nil metadata is **invalid** and MUST error
- Empty map represents explicitly empty metadata
- Metadata is persisted verbatim in manifests

### API Shape

```go
type SnapshotMetadata map[string]string
```

### Acceptance Criteria

- `Dataset.Write(ctx, records, nil)` returns an error
- `Dataset.Write(ctx, records, SnapshotMetadata{})` succeeds
- Persisted manifests always contain a metadata object

### Contract Assertions

- No snapshot may exist without explicit metadata
- Metadata must not be inferred or defaulted

### Stop Point

Do not introduce defaults, inference, or optional metadata behavior.

---

## Iteration 0.2 — Object Iteration Semantics

### Scope

Define minimal ObjectIterator lifecycle semantics.

### Decisions

- Ordering is unspecified
- Pagination is unspecified
- Lifecycle semantics are required

### API Shape

```go
type ObjectIterator interface {
    Next() bool
    Ref() ObjectRef
    Err() error
    Close() error
}
```

### Contract Assertions

- `Close()` is idempotent
- `Err()` may be called after exhaustion or close
- `Next()` returns false after exhaustion or close
- Implementations must release resources on `Close()` or exhaustion

### Stop Point

Do not define ordering, pagination, or filtering behavior.

---

## Iteration 0.3 — Snapshot History and Empty Dataset Behavior

### Scope

Define snapshot history rules and empty dataset behavior.

### Decisions

- Dataset history is strictly linear
- Empty datasets are permitted
- `Write` on empty dataset creates first snapshot

### Acceptance Criteria

- `Current()` on empty dataset returns `ErrNoSnapshots`
- `ListSnapshots()` on empty dataset returns empty iterator without error
- `Snapshot(id)` on empty dataset returns `ErrNotFound`

### Contract Assertions

- There is exactly one head snapshot at all times after first write
- Snapshot ordering derives from parent chain, not timestamps

### Stop Point

Do not introduce branching or multi-head semantics.

---

## Iteration 0.4 — NoOp Partitioning and Compression

### Scope

Eliminate nil handling for partitioning and compression.

### Decisions

- Canonical NoOp implementations must exist
- Nil values are not permitted in DatasetSpec
- Manifests always record component names

### API Shape

```go
var NoOpPartitioner Partitioner
var NoOpCompressionFormatter CompressionFormatter
```

### Acceptance Criteria

- DatasetSpec defaults to NoOp components when not provided
- Manifests record `partitioner = "noop"` and `compression = "noop"`
- Readers treat NoOp components as identity operations

### Contract Assertions

- No nil checks required during read/write
- Absence of behavior is still explicit

### Stop Point

Do not allow per-write overrides.

---

## Iteration 0.5 — Object Key Layout Responsibility

### Scope

Define object key layout ownership and adapter interaction.

### Decisions

- Logical key layout is defined by persistence plane
- Partitioners may generate Hive-style paths
- Adapters receive key hints and may rewrite them
- Returned keys are canonical and persisted

### Acceptance Criteria

- Same dataset produces identical logical keys regardless of adapter
- Manifests persist returned object keys only
- ObjectRef.Meta is never persisted

### Contract Assertions

- Storage adapters must not invent persistence structure
- Dataset layout is portable across backends

### Stop Point

Do not define debug hints or adapter-specific naming.

---

## Iteration 0.6 — Store Builder Determinism

### Scope

Define deterministic behavior for store composition.

### Decisions

- Exactly one base store is required
- Wrapper application order is user-defined
- Wrapper order must not affect correctness semantics
- Observability effects may vary by order

### Acceptance Criteria

- Same option list and order yields identical composition
- Reordering wrappers does not change read/write results
- Wrapper order may affect metrics or cache placement

### Contract Assertions

- Determinism is defined as option-order determinism
- Correctness excludes observability

### Stop Point

Do not introduce automatic sorting or implicit wrapper ordering.

---

## Out of Scope for Phase 0

- ObjectIterator ordering guarantees
- Debug or hint metadata
- Multi-writer concurrency
- Query execution

---

## Phase 0 Completion Checklist

- [ ] Snapshot metadata explicit and validated
- [ ] Object iteration lifecycle defined
- [ ] Empty dataset semantics pinned
- [ ] NoOp components implemented
- [ ] Key layout responsibility clear
- [ ] Store builder deterministic
- [ ] No abstraction boundary violations

End of Phase 0 plan.
