# Lode Object Iteration — Contract

This document defines the lifecycle semantics for object iteration.

---

## Goals

1. Predictable iterator lifecycle behavior.
2. Resource safety and idempotent closure.

---

## Interface Shape (Conceptual)

```
type ObjectIterator interface {
    Next() bool
    Ref() ObjectRef
    Err() error
    Close() error
}
```

---

## Required Semantics

- Ordering is unspecified.
- Pagination is unspecified.
- `Next()` MUST return false after exhaustion or after `Close()` is called.
- `Close()` MUST be idempotent.
- `Err()` MAY be called after exhaustion or close.
- Implementations MUST release resources on `Close()` or exhaustion.

---

## Prohibited Behaviors

- Implicit ordering guarantees
- Hidden buffering that changes visibility semantics
