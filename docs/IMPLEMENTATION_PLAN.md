# Lode — Implementation Plan (Contract-First)

This plan reflects Lode’s clarified scope: **persistence structure only**.
It introduces an explicit **contract-freezing phase** before any implementation
details harden.

Lode’s core principle:

> **Lode defines structure, not execution.**  
> It stores facts, not interpretations.

---

## Conceptual Stack

```
Data Producer (any language / system)
        ↓
Lode Write API (structure + metadata)
        ↓
Storage Adapter (FS, S3, etc.)
        ↓
Immutable Objects + Manifests
        ↓
External Readers (query engines, tools)
```

---

## Phase 0 — Foundations & Guardrails

### Goal
Establish repo discipline and enforce boundary constraints before adding behavior.

### Deliverables
- Repo skeleton with `lode/`, `internal/`, `docs/`, `examples/`
- `AGENTS.md` constraints applied
- Taskfile scaffolding (even if minimal)

### Mini-milestones
- [x] Guardrails exist and are visible
- [ ] Task targets are present (build / test / lint)
- [ ] No execution logic introduced

---

## Phase 0.5 — Contract Definitions (NO CODE)

### Goal
Freeze **interfaces and invariants** required for parallel implementation,
without committing to internal details.

This phase produces **authoritative documents**, not code.

### Deliverables

#### 0.5.1 Core Model Contract (`docs/contracts/CONTRACT_CORE.md`)
Defines:
- Dataset and snapshot identity
- Manifest self-description requirements
- Metadata explicitness and persistence
- Immutability rules and linear history

#### 0.5.2 Write API Contract (`docs/contracts/CONTRACT_WRITE_API.md`)
Defines:
- `Dataset.Write` semantics and error behavior
- Snapshot creation and parent linkage
- Empty dataset behavior
- Prohibited behaviors (no in-place mutation)

#### 0.5.3 Storage Adapter Contract (`docs/contracts/CONTRACT_STORAGE.md`)
Defines:
- Adapter obligations for `Put/Get/List/Exists/Delete`
- Safe write semantics (no overwrite)
- Atomic commit expectations (manifest presence as commit signal)

#### 0.5.4 Layout & Component Contract (`docs/contracts/CONTRACT_LAYOUT.md`)
Defines:
- Logical key layout ownership
- Partitioner / compressor / codec roles
- NoOp components and explicit recording
- Optional codec semantics for raw-byte data
- Canonical object key persistence

#### 0.5.5 Iterator Contract (`docs/contracts/CONTRACT_ITERATION.md`)
Defines:
- Object iterator lifecycle semantics
- Close/Err/Next behavior guarantees

#### 0.5.6 Read API Contract (Stretch) (`docs/contracts/CONTRACT_READ_API.md`)
Defines:
- Adapter-agnostic read surface
- Range-read requirements
- Manifest-driven discovery

#### 0.5.7 Public API Spec (`PUBLIC_API.md`)
Defines:
- Public entrypoints and defaults
- Option-based configuration shape
- Default bundle behavior

### Exit criteria
- All contract documents exist
- Ambiguity is resolved by contract references
- Phase 0 plan assertions are captured in contracts
- Public API spec exists and documents defaults and options

---

## Phase 1 — Core Persistence Skeleton

### Goal
Implement the minimal, end-to-end write path with immutable snapshots.

### Deliverables
- Dataset abstraction
- Manifest schema and persistence
- Snapshot creation with explicit metadata
- Safe write semantics enforced

### Mini-milestones
- [ ] Write → manifest → snapshot works
- [ ] No metadata inference or defaults
- [ ] Empty dataset behavior matches contracts

---

## Phase 2 — Adapters & Components

### Goal
Make storage and format choices pluggable without changing semantics.

### Deliverables
- Filesystem and in-memory adapters
- JSONL codec
- Gzip compression
- Hive-style partitioner
- NoOp components for partitioning/compression
- **Unified Layout abstraction** (publicly single, internally composable)
- **Default layout** (dataset-modeled, segment-anchored, flat)
- Curated layout implementations (default + hive-style + manifest-driven)

### Mini-milestones
- [ ] Manifests record component names
- [ ] No nil component handling
- [ ] Layout is portable across adapters

---

## Phase 3 — Read Surface (Optional)

### Goal
Provide a minimal read facade for external consumers.

### Deliverables
- Dataset / partition / segment listing
- Manifest access
- Object open and random access via adapter
- Layout-aware discovery and path parsing
- Layout-specific dataset errors (when datasets are not modeled)

### Mini-milestones
- [ ] Range reads are true range reads
- [ ] Manifests are sufficient for discovery
- [ ] No planning or query logic added

---

## Phase 4 — Hardening

### Goal
Stabilize behavior and validate invariants with tests and examples.

### Deliverables
- Error taxonomy and test coverage
- Example: write → list → read
- Example: single-object blob upload (public API only)
- Explicit metadata visibility on disk
- Examples for default layout and hive-style layout

### Mini-milestones
- [ ] Tests enforce immutability
- [ ] Examples align with contracts
- [ ] Single-object blob example uses default bundle and explicit metadata
- [ ] No hidden state detected

---

## Phase 5 — Expansion Experiments (Optional)

### Goal
Explore new adapters or codecs without expanding the public API.

### Deliverables
- S3 adapter (if needed)
- Parquet codec
- Manifest stats (optional, additive)

### Mini-milestones
- [x] No API surface growth
- [x] No backend-specific conditionals
- [x] Explicitly marked experimental

### S3 Adapter (Experimental)

**Status**: Implemented (internal/s3)

The S3 adapter is available as an **experimental** internal implementation.
It supports AWS S3, MinIO, LocalStack, and other S3-compatible object stores.

**Location**: `internal/s3/`

**Consistency notes**:
- S3 provides strong read-after-write consistency (since December 2020)
- List operations are also strongly consistent
- Commit semantics rely on manifest presence: write data objects before manifest

**Integration testing**:
- Gated behind `LODE_S3_TESTS=1` environment variable
- Requires Docker Compose for LocalStack/MinIO services
- Run: `task s3:up && task s3:test && task s3:down`

**Usage**:
```go
import (
    "github.com/justapithecus/lode/internal/s3"
)

// For LocalStack
client, _ := s3.NewLocalStackClient(ctx)
store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})

// For MinIO
client, _ := s3.NewMinIOClient(ctx)
store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})

// For AWS S3 (use standard AWS SDK config)
cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)
store, _ := s3.New(client, s3.Config{Bucket: "my-bucket", Prefix: "datasets/"})
```

---

## Contract Change Protocol

Any change that affects contract behavior must:
1. Update the relevant `docs/contracts/CONTRACT_*.md` file.
2. Include a compatibility note (breaking vs additive).
3. Avoid expanding public API without explicit justification.
