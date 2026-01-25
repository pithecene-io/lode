# Roadmap

## Phase 0 — Scope lock

### Goals

- Embeddable Go library
- Object storage persistence discipline
- Snapshots + metadata + immutability

### Non-goals

- Query execution engine
- Background jobs
- Distributed coordination
- Storage engine replacement

## Phase 1 — Walking skeleton (must ship first)

### Core

- Store interface
- FS + in-memory adapters
- Codec (JSONL)
- Compressor (gzip)
- Partitioner (hive dt)
- Dataset abstraction
- Snapshot + manifest model
- Atomic commit (FS)

### Examples

- examples/events_fs_jsonl_gzip/
    - write events
    - list snapshots
    - read a snapshot

Success criteria:
- End-to-end write → snapshot → read
- No hidden state
- Metadata visible on disk

## Phase 2 — Hardening

- Parquet codec
- Stats in manifest
- Snapshot listing + time travel
- Cache decorator
- Error taxonomy

## Phase 3 — Expansion experiments (optional)

- S3 adapter
- External integration example (e.g., DuckDB) that consumes snapshots/manifests outside Lode
- Encryption transform
- Explicitly marked experimental

## Phase 4+ — Research experiments (optional)

- Commit strategy exploration (if needed), without expanding public API or introducing backend-specific conditionals
