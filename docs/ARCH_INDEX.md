# ARCH_INDEX.md — Lode Architecture Index

This file is a **navigation map** of Lode’s subsystems.
It summarizes *what exists and where*, not how things are implemented.

Normative behavior is defined in `docs/contracts/CONTRACT_*.md`.
Public API shape and defaults are defined in `PUBLIC_API.md`.

Maintenance rule:
Update this file only when a new subsystem boundary becomes real.
Do not update for internal refactors.

---

## Root

- `AGENTS.md` — development guardrails and agent constraints
- `Taskfile.yaml` — task orchestration and developer workflows
- `go.mod` — Go module definition
- `mise.toml` — toolchain and task runner configuration
- `PUBLIC_API.md` — developer-facing public API spec (product contract)
- `README.md` — project overview and goals
- `CONTRIBUTING.md` — contribution guidelines

---

## docs/

Plans, contracts, and research notes. Contracts define **system behavior**.

- `IMPLEMENTATION_PLAN.md` — contract-first, phased implementation plan
- `contracts/CONTRACT_CORE.md` — core model invariants and immutability
- `contracts/CONTRACT_WRITE_API.md` — write API semantics and errors
- `contracts/CONTRACT_STORAGE.md` — storage adapter obligations
- `contracts/CONTRACT_LAYOUT.md` — layout, partitioning, and component rules
- `contracts/CONTRACT_ITERATION.md` — object iterator lifecycle semantics
- `contracts/CONTRACT_COMPOSITION.md` — store composition determinism
- `contracts/CONTRACT_READ_API.md` — read API contract and adapter obligations
- `contracts/CONTRACT_ERRORS.md` — error taxonomy and handling guidelines

Contracts are authoritative over code.

---

## lode/

Public API surface.

- `api.go` — core public interfaces, types, and error sentinels
- `s3/` — S3-compatible storage adapter

---

## internal/

Internal implementations.

- `testutil/` — internal test helpers

---

## examples/

Example usage and integration references.

- `default_layout/` — write → list → read with DefaultLayout
- `hive_layout/` — partition-first layout with Hive partitioner
- `manifest_driven/` — manifest-driven discovery demonstration
- `blob_upload/` — raw blob write/read with default bundle
- `s3_experimental/` — S3 adapter example (name retained for continuity)

---

## Architectural Notes

- Lode defines persistence structure: datasets, snapshots, manifests, metadata
- Execution concerns (planning, scheduling, optimization) are out of scope
- Metadata is explicit and persisted; snapshots and data files are immutable
- Layout and semantics must remain backend-agnostic

This index is intentionally stable and low-detail.
