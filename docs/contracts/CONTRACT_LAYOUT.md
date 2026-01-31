# Lode Layout & Components — Contract

This document defines layout ownership and component behavior:
partitioning, compression, and codec roles.

---

## Goals

1. Portable, backend-agnostic layout.
2. Explicit component choices in manifests.
3. No implicit defaults or inference.

---

## Layout Ownership

- Logical key layout is defined by the persistence plane.
- Storage adapters MUST NOT invent persistence structure.
- Adapters MAY rewrite keys, but MUST return canonical keys that are persisted.

---

## Partitioner

- Determines logical partition paths (e.g. hive-style `k=v`).
- MUST be applied before storage adapter rewriting.
- MUST be recorded in manifests by name.

---

## Compressor

- Defines compression format and file extension.
- MUST be recorded in manifests by name.
- No-op compression is explicit, not implicit.

---

## Codec

- Defines record serialization format.
- MUST be recorded in manifests by name.

---

## NoOp Components

- Canonical NoOp implementations MUST exist.
- Dataset configuration MUST never use nil components.
- Manifests MUST record `partitioner = "noop"` and `compression = "noop"` when applicable.

---

## Key Persistence

- Only canonical object keys returned by adapters are persisted.
- Ephemeral metadata or hints MUST NOT be persisted.
