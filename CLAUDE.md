# Repository Constitution

## 1. Constitutional Order of Authority

1. Global dotfiles CLAUDE.md (sovereign)
2. This repository CLAUDE.md
3. AGENTS.md (behavioral expectations and guardrails)
4. docs/ARCH_INDEX.md (structural ontology)
5. docs/contracts/CONTRACT_*.md (normative behavior)
6. PUBLIC_API.md (public surface contract)

## 2. Role of AGENTS.md

AGENTS.md provides:
- Development guardrails and agent constraints
- Scope discipline and abstraction rules
- Go-specific coding conventions
- Change and refactoring discipline

AGENTS.md does NOT:
- Override constitutional structural rules
- Define enforcement policy
- Weaken global prohibitions

## 3. Role of docs/ARCH_INDEX.md

ARCH_INDEX.md defines:
- Repository subsystem map
- Module responsibilities
- Directory semantics

ARCH_INDEX.md is authoritative for:
- Top-level module declarations
- Contract-to-directory mapping
- Architectural boundaries

## 4. Role of Contracts

docs/contracts/CONTRACT_*.md files define normative system behavior.
Contracts are authoritative over code.

## 5. Structural Invariants

Required top-level directories:
- `lode/` — public API surface
- `internal/` — internal implementations
- `examples/` — usage and integration references
- `docs/` — plans, contracts, and research
- `scripts/` — build and maintenance scripts

Required docs structure:
- `docs/ARCH_INDEX.md` — subsystem navigation
- `docs/contracts/` — normative behavior contracts

Required root files:
- `AGENTS.md` — agent guardrails
- `PUBLIC_API.md` — public API contract
- `go.mod` — Go module definition

Forbidden patterns:
- No orphan top-level directories without ARCH_INDEX entry
- No duplicate module responsibilities across directories
- No public API types or functions outside `lode/`
- No contracts outside `docs/contracts/`
