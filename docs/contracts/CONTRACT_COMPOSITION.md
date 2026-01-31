# Lode Store Composition — Contract

This document defines deterministic store composition behavior.

---

## Goals

1. Deterministic behavior from explicit configuration.
2. No hidden ordering or backend-specific flags.

---

## Required Semantics

- Exactly one base store is required.
- Wrapper application order is **user-defined** and must be respected.
- Reordering wrappers MUST NOT change read/write correctness.
- Wrapper order MAY affect observability (metrics, caching) but not results.

---

## Prohibited Behaviors

- Implicit sorting or reordering of wrappers
- Backend-specific conditional behavior
