# VOLUME_DIRECTION.md — Dataset + Volume Product Direction

This document records product intent for the dual-paradigm Lode model.
It is design framing, not an API contract.

---

## Purpose

Define a second first-class persistence paradigm (`Volume`) that complements
the existing `Dataset` abstraction without changing Lode's core scope.

Lode remains an embedded persistence library, not a runtime.

---

## Core Philosophy (Unchanged)

Lode continues to define:
- explicit persisted state
- immutable objects and snapshots
- manifest-driven commit visibility
- backend-agnostic storage semantics

Lode does not define:
- scheduling
- networking
- execution/runtime policy
- torrent protocol behavior

---

## Two Persistence Paradigms

### Dataset (existing)

Mental model: complete structured collections of named objects.

Key invariant:
- A committed Dataset snapshot is complete and authoritative.

### Volume (v0.6.0)

Mental model: a logical byte address space `[0..N)` that may be sparsely
materialized and committed over time.

Key invariant:
- A committed Volume snapshot explicitly lists which byte ranges exist.
- Missing ranges are meaningful and must never be inferred.

---

## Streaming Boundary (Critical)

Dataset streaming remains supported, but narrowly framed:

- Dataset streaming is an ingestion mechanic for building complete objects
- partial data is never committed truth in Dataset manifests
- commit remains atomic at snapshot visibility level

Volume streaming owns:
- incremental committed truth
- resumability
- sparse/out-of-order materialization
- range-verified reads

Anchor sentence:

> Dataset streaming builds complete objects before committing truth; Volume streaming commits truth incrementally.

---

## Phase-0 Objective for Volume

Prove that Lode can persist and serve sparse, verified byte ranges explicitly
while preserving manifest-driven commit semantics.

Expected phase-0 scope:
- manifest model for committed segments
- deterministic resume/restart behavior
- explicit range-read semantics
- filesystem and memory validation

Excluded from phase-0:
- torrent/runtime logic
- performance tuning
- deployable services

---

## Design Discipline

- `Dataset` and `Volume` are coequal primitives, not wrappers over each other
- adapters remain dumb; lifecycle semantics stay in Lode core
- runtime-aware policy belongs in consuming systems (for example, Quarry)
