# V1_READINESS.md — v1.0 Release Criteria

This document defines the **non-negotiable criteria** that must all pass
before Lode can be tagged v1.0.

Stability is proven by **usage and time**, not design review.

---

## Gate Rule

- Every criterion below must be checked off with recorded evidence.
- Evidence must reference a real downstream project (or redacted alias), real data, and a real observer.
- No criterion may be waived — if it cannot be met, it must be renegotiated and this document updated.
- The v1.0 tag is blocked until all criteria pass.

---

## Dogfooding Registry

Each downstream project that integrates Lode gets a subsection here.
Use the template to add new projects as they begin integration.

**Privacy rule:** Only public projects are listed by name. Private projects
use a redacted alias (e.g. `private-A`, `private-B`) and must not reveal
internal project names, repo URLs, domain-specific details, or
implementation specifics that could identify the project or its purpose.
Usage profiles and evidence summaries should describe Lode-level behavior
only (e.g. "Dataset round-trip over S3"), not the consumer's architecture
or data model. If a linked issue lives in a private tracker, reference it
as `(private)` instead of by number.

### quarry

- **Integration date:** TBD
- **Contact:** TBD
- **Usage profile:** Dataset / Volume / Both
- **Storage backend:** AWS S3
- **Status:** Not started

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | | | |
| Volume write round-trip | | | |
| Error sentinels observed | | | |
| API friction (none = pass) | | | |

<!--
### <project-name or redacted alias>

- **Integration date:** TBD
- **Contact:** TBD
- **Usage profile:** Dataset / Volume / Both
- **Storage backend:** <backend>
- **Status:** Not started
- **Visibility:** Public / Private (if private, use alias — see privacy rule above)

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | | | |
| Volume write round-trip | | | |
| Error sentinels observed | | | |
| API friction (none = pass) | | | |
-->

---

## Criteria

### 1. API Surface Freeze

- [ ] No public type, function, or method signature has changed for 4+ weeks after first downstream integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] No new sentinel errors added for 4+ weeks after first downstream integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] `PUBLIC_API.md` matches exported symbols in `lode/api.go` with zero discrepancies

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Option / VolumeOption surface has not expanded since dogfooding began

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 2. Manifest Format Freeze

- [ ] `Manifest` struct fields unchanged for 4+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] `VolumeManifest` struct fields unchanged for 4+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Owner sign-off on manifest JSON schema as v1.0 wire format

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 3. Dataset Dogfooding

- [ ] At least one consumer writing Dataset snapshots to S3 for 2+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least one consumer reading Dataset snapshots back (round-trip) for 2+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least 100 Dataset snapshots committed and read with no corruption

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] No Dataset API changes required to support integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 4. Volume Dogfooding

- [ ] At least one consumer using StageWriteAt + Commit with real data

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least one consumer using ReadAt on committed ranges

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least 3 Volume snapshots committed with cumulative manifests in a real workflow

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] No Volume API changes required to support integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 5. Error Path Validation

- [ ] ErrNoSnapshots observed and handled correctly by a downstream consumer

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] ErrPathExists observed in real storage confirming immutability enforcement

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] ErrNotFound observed and handled correctly for a missing snapshot lookup

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 6. S3 Backend Verification

- [ ] AWS S3 used for Dataset and Volume writes/reads during dogfooding with no adapter errors

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Non-AWS backend caveat documented in README and PUBLIC_API.md as known v1.0 limitation

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 7. Known Limitations Accepted

- [ ] Single-writer semantics documented as v1.0 limitation in README + PUBLIC_API.md + contracts

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Context cancellation cleanup nondeterminism documented as best-effort, no correctness impact

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Deferred test gaps (ITER-LIFECYCLE, COMP-WRAPPER-ORDER, ERR-RANGE-NOT-SUPPORTED) accepted as non-blocking

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 8. Documentation Completeness

- [ ] README "Status" section updated to v1.0 language

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] All 13 contract files reviewed — no "planned"/"draft"/"future" language for shipped features

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] All examples pass (`task examples`)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] CHANGELOG includes v1.0 entry summarizing stability commitment

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 9. Data Correctness

- [ ] Zero silent data corruption during dogfooding (Dataset or Volume)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Zero manifest deserialization failures on same-version data

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Zero snapshot history inconsistencies (forked history, missing parent, duplicate ID)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

---

## Out of Scope for v1.0

The following are explicitly **not blockers** for the v1.0 release:

- Content-addressable storage (CAS)
- Zarr codec support
- New codecs beyond Parquet
- Performance benchmarking or optimization
- New storage adapters beyond S3
- Compaction or garbage collection
- Multi-writer / distributed coordination
- Query planning or execution

These may become goals for v1.1+ but must not gate the initial stability release.

---

## References

- `PUBLIC_API.md` — public API shape and defaults
- `docs/contracts/CONTRACT_CORE.md` — core model invariants
- `docs/contracts/CONTRACT_WRITE_API.md` — write API semantics
- `docs/contracts/CONTRACT_READ_API.md` — read API contract
- `docs/contracts/CONTRACT_VOLUME.md` — Volume persistence model
- `docs/contracts/CONTRACT_ERRORS.md` — error taxonomy
- `docs/contracts/CONTRACT_TEST_MATRIX.md` — contract-to-test traceability
- `docs/contracts/CONTRACT_STORAGE.md` — storage adapter obligations
