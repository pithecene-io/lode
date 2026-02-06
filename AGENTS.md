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

## Testing conventions

- Use `t.Context()` instead of `context.Background()` in tests
- Use `errors.Is()` for error comparisons, not `==`

---

## Go code style

- Prefer `any` over `interface{}`
- Exceptions: When implementing interfaces from external packages that use `interface{}`
- Use `//nolint:` comments with explanation when suppressing linters

---

## Declarative style (critical)

Code in this repository should prefer declarative patterns over imperative control flow.

### Preferred patterns

- **Data-first shapes**: Define behavior via data structures (tables, maps, slices) rather than branching logic.
- **Table-driven logic**: Use lookup tables for type dispatch, validation rules, and error mapping.
- **Iterators and helpers**: Extract iteration patterns into reusable helpers; avoid inline loops with complex bodies.
- **Named helpers**: Extract small, single-purpose functions with clear names instead of inline blocks.
- **Explicit validation passes**: Validate inputs in a dedicated pass before processing, not scattered inline.

### Anti-patterns to avoid

- Long switch/if chains that could be a map lookup
- Deep nesting (prefer ≤2 levels unless it improves clarity)
- Inline anonymous functions with complex logic
- Mixing validation, transformation, and I/O in a single function
- Repeated patterns that should be a helper

### Refactoring rules

Style refactors are allowed when:
- Behavior is unchanged (tests remain green without modification)
- Changes are localized (one file or small group of related files)
- The PR is clearly marked "refactor only"

Style refactors must NOT:
- Change API surface
- Modify error messages (may break error matching)
- Introduce new abstractions or types
- Combine with feature or bugfix changes

If a style change starts to feel like a design change, **stop and ask**.

---

## API and example conventions

- `PUBLIC_API.md` is canonical for callsite conventions
- Example code must not drift from `PUBLIC_API.md`
- Write examples prefer `lode.R(...)` for inline record construction
- `[]lode.D` is allowed only when backing an iterator (add comment explaining why)
- Pre-v1 ergonomics changes require contract+docs+tests+examples updated together

If examples diverge from these conventions, stop and ask before editing.

See `docs/contracts/CONTRACT_EXAMPLES.md` for full normative specification.

---

## Priority order

1. Correctness
2. Clarity
3. Explicitness
4. Convenience
5. Extensibility
