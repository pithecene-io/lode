# CONTRACT_EXAMPLES.md — Example and Callsite Conventions

This contract defines normative conventions for examples and API callsites in Lode.

---

## Authority

- `PUBLIC_API.md` is canonical for callsite conventions and API signatures.
- Example code must not drift from `PUBLIC_API.md`.
- If examples and `PUBLIC_API.md` conflict, `PUBLIC_API.md` is authoritative.

---

## Record Construction Conventions

### Preferred: `lode.R(...)`

Use `lode.R(...)` for Write examples with inline record literals:

```go
records := lode.R(
    lode.D{"id": "1", "name": "alice"},
    lode.D{"id": "2", "name": "bob"},
)
snap, err := ds.Write(ctx, records, lode.Metadata{})
```

**Why:** `R(...)` is ergonomic, reads naturally, and builds `[]any` directly.

### Allowed: `[]lode.D` for Iterator Backing

Use `[]lode.D` only when the slice backs a `RecordIterator`:

```go
// []lode.D is appropriate here: it backs the iterator
records := []lode.D{
    {"id": "1", "name": "alice"},
    {"id": "2", "name": "bob"},
}
iter := NewSliceIterator(records)
snap, err := ds.StreamWriteRecords(ctx, lode.Metadata{}, iter)
```

When using `[]lode.D`, add a brief comment explaining the iterator backing relationship.

### Raw Blobs

For raw blob writes (no codec), use `[]any{blobData}`:

```go
snap, err := ds.Write(ctx, []any{blobData}, lode.Metadata{})
```

---

## Variable Naming Conventions

| Variable | Purpose |
|----------|---------|
| `records` | Slice of records for Write or iterator backing |
| `iter` | RecordIterator instance |
| `metadata` | Metadata map (`lode.Metadata{}`) |
| `snap` | Snapshot result from write operations |
| `ds` | Dataset instance |
| `ctx` | Context |

---

## Pre-v1 Ergonomics Changes

Pre-v1 ergonomics changes (e.g., parameter reordering) are allowed only when:

1. Contract is updated in the same PR as the behavior change
2. `PUBLIC_API.md` is updated
3. All tests are updated
4. All examples are updated
5. CHANGELOG includes migration note

No mixed old/new callsites may exist after the change.

---

## Agent Behavior

Agents must:
- Stop and ask if an example diverges from `PUBLIC_API.md` conventions
- Stop and ask before introducing new callsite patterns not covered here
- Prefer editing existing examples over creating new ones

---

## References

- [`PUBLIC_API.md`](../../PUBLIC_API.md) — Canonical API documentation
- [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) — Write API semantics
