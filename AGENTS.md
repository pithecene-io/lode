# AGENTS.md

This file defines constraints for AI agents operating in this repository.

It is not user documentation.
It is not a contribution guide for humans.

Agents must follow these rules.

---

## Core constraint

Lode abstracts **persistence structure**, not execution.

The system defines:
- datasets
- snapshots
- manifests
- metadata
- safe write semantics

The system does NOT define:
- query planning
- execution strategies
- scheduling
- optimization heuristics
- distributed coordination

Do not introduce features that cross this boundary.

---

## Public API boundaries

- Public APIs live in `/lode`
- The public surface should remain intentionally scoped and coherent
- Adding a public method or type is allowed when it clearly improves usability or expressiveness
- Prefer internal implementations over public extensibility

If functionality can live in `/internal`, it must live there.

---

## Abstraction discipline

Agents must not expand abstractions to accommodate:
- backend-specific quirks
- performance optimizations
- convenience behaviors
- conditional logic for edge cases

Differences between backends must be handled by composition, not flags.

---

## Metadata rules

- Metadata must be explicit
- Metadata must be persisted
- Metadata must never be inferred implicitly
- Snapshots and manifests must be self-describing

---

## Immutability rules

- Data files are immutable once written
- Snapshots are immutable
- Commits create new state; they do not mutate existing state

Any design that violates immutability is invalid.

---

## Allowed additions

Agents may add:
- storage adapters
- codecs
- compression implementations
- partitioning strategies
- examples

Provided they respect existing invariants.

---

## Disallowed additions

Agents must not add:
- background workers
- automatic compaction
- query execution logic
- hidden global state
- convenience APIs that bypass snapshots

---

## When uncertain

If a change increases:
- API surface area
- configuration options
- conditional behavior

Assume it is invalid and stop.

---

## Priority order

1. Correctness
2. Clarity
3. Explicitness
4. Convenience
5. Extensibility
