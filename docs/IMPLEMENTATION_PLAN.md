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
- [x] Task targets are present (build / test / lint)
- [x] No execution logic introduced

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
- [x] Write → manifest → snapshot works
- [x] No metadata inference or defaults
- [x] Empty dataset behavior matches contracts

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
- Curated layout implementations (default + hive-style + flat)

### Mini-milestones
- [x] Manifests record component names
- [x] No nil component handling
- [x] Layout is portable across adapters

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
- [x] Range reads are true range reads
- [x] Manifests are sufficient for discovery
- [x] No planning or query logic added

---

## Phase 4 — Hardening

### Goal
Stabilize behavior and validate invariants with tests and examples.

### Deliverables
- Error taxonomy and test coverage
- Example: write → list → read
- Example: single-object blob upload (public API only)
- Example: manifest-driven discovery (pattern, not a layout)
- Explicit metadata visibility on disk
- Examples for default layout and hive-style layout

### Mini-milestones
- [x] Tests enforce immutability
- [x] Examples align with contracts
- [x] Single-object blob example uses default bundle and explicit metadata
- [x] No hidden state detected

---

## Phase 5 — Expansion Experiments (Optional)

### Goal
Explore new adapters or codecs without expanding the public API.

### Deliverables
- S3 adapter (if needed)
- Parquet codec
- Manifest stats (optional, additive)

### Mini-milestones
- [x] Minimal API surface (only `s3.New` and `s3.Config` exported)
- [x] No backend-specific conditionals
- [x] CONTRACT_STORAGE.md compliance verified
- [x] Zstd compressor added as an additive compression option
- [ ] Parquet codec implemented
- [ ] Manifest stats extensions finalized (additive)

### S3 Adapter

**Status**: Promoted to public API (`lode/s3`)

The S3 adapter is a stable public component supporting AWS S3, MinIO,
LocalStack, Cloudflare R2, and other S3-compatible object stores.
Client construction uses AWS SDK directly (see PUBLIC_API.md for examples).

**Location**: `lode/s3/`

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
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/justapithecus/lode/lode/s3"
)

// AWS S3 (use standard AWS SDK config)
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

---

## Post-v0.4 Roadmap (Planned)

### Priority Track A — Storage Safety Hardening

- [ ] Evaluate and adopt stronger S3 conditional-write guarantees for multipart paths
- [ ] Update storage contract/docs with backend-specific guarantee notes where needed
- [ ] Add integration coverage for large-upload overwrite prevention paths

### Priority Track B — Format and Ecosystem

- [ ] Prioritize Parquet codec delivery
- [ ] Define additive manifest stats needed for Parquet-oriented pruning workflows
- [ ] Add/refresh examples for columnar and streaming workflows

### Priority Track C — Zarr/Xarray Direction

- [ ] Design reference-based path for GRIB2/NetCDF-to-Zarr workflows within Lode boundaries
- [ ] Define docs/examples for chunk/region safety expectations
- [ ] Evaluate native Zarr encoding path after reference-based workflow is validated
