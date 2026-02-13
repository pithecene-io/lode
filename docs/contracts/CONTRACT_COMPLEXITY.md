# Lode Complexity Bounds — Contract

Status: Draft (v0.7). Enforced when all known violations are resolved.

---

## Principle

Every public method MUST have documented, bounded cost.
Cost that exceeds the documented bound is a bug.
Hot-path operations MUST NOT scale with dataset/volume history size.

---

## Definitions

### Cost dimensions

- Store calls: network round-trips (Get, Put, List, Exists, Delete, ReadRange)
- Memory: peak allocation relative to input/output size
- CPU: algorithmic complexity class

### Variables

| Variable | Meaning |
|----------|---------|
| S | Committed snapshots in dataset or volume |
| B | Committed blocks in a volume snapshot (cumulative) |
| K | New blocks in a single commit |
| F | Files in a dataset snapshot |
| R | Records in a write or read |
| P | Partitions in a dataset write |
| M | Manifests matching a listing query |
| N | Keys returned by Store.List |
| L | Requested byte range length |

### Path classification

- Hot path: per-read, per-write. O(S), O(B), O(M) is a bug.
- Cold path: explicit listing (Snapshots(), ListDatasets). Documented cost; acknowledged scaling.
- Degraded path: backward-compat fallback (scan when pointer missing). Documented, self-healing.

---

## Store Interface

| Method | Calls | Memory | Notes |
|--------|-------|--------|-------|
| Put | 1 | O(payload) | Atomic; multipart adds preflight |
| Get | 1 | O(1) stream | Returns io.ReadCloser |
| Exists | 1 | O(1) | |
| Delete | 1 | O(1) | |
| ReadRange | 1 | O(L) | |
| ReaderAt | 1 setup + 1/call | O(L) per call | |
| List | ⌈N/page⌉ | O(N) | Full materialization (interface constraint) |

---

## Dataset — Write Operations

| Operation | Store Calls (warm) | Store Calls (cold) | Memory |
|-----------|-------------------|--------------------|--------|
| Write (no partitions) | 4 fixed | +1 List (scan) | O(R + encoded) |
| Write (P partitions) | 2P + 3 | +1 List (scan) | O(R + encoded) |
| StreamWrite | 4 fixed | +1 List (scan) | O(1) streaming |
| StreamWriteRecords | 4 fixed | +1 List (scan) | O(1) streaming |

Parent resolution:
- In-memory cache: 0 calls
- Pointer + Exists: 2 calls
- Scan fallback (degraded): O(N) via List. Self-heals.

Parent resolution MUST NOT use List when a valid pointer exists.

---

## Dataset — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| Latest (warm) | Hot | 2 (pointer + manifest) | O(manifest) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get | O(N) | O(N) |
| Snapshot(id) | Hot | 1 Get | O(manifest) | O(1) |
| Snapshot(id) fallback | Degraded | 1 List + scan | O(N) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × manifest) | O(S log S) |
| Read(id) | Hot | 1 + F Gets | O(R_total) | O(R_total) |

---

## DatasetReader — Enumeration

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| ListDatasets | Cold | 1 List | O(N) |
| ListManifests | Cold | 1 List + M Gets (validation) | O(N + M × manifest) |
| ListPartitions | Cold | 1 List + 2M Gets | O(N + M × manifest) |
| GetManifest | Hot | 1 Get | O(manifest) |
| OpenObject | Hot | 1 Get | O(1) stream |

ListManifests MUST extract snapshot IDs from paths. Manifest validation is required per CONTRACT_ERRORS.md.
ListPartitions MUST NOT double-deserialize manifests.

---

## Volume — Write Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| StageWriteAt | Hot | 1 Put | O(block) | O(block) |
| Commit (warm) | Hot | 4 fixed | O(B) manifest | O(B + K log K) |
| Commit (cold) | Degraded | +1 List | +O(N) | +O(N) |

Cumulative manifest size grows O(B) per commit. This is inherent in the cumulative design.

---

## Volume — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| ReadAt | Hot | 1 Get + R ReadRange | O(L) | O(log B + R) |
| Latest (warm) | Hot | 2 | O(B) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get | O(N + B) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × B_avg) | O(S log S) |
| Snapshot(id) | Hot | 1 Get | O(B) | O(B) |

ReadAt block lookup MUST be O(log B + R). Per-read sort checks MUST NOT exist.

---

## Known Violations

| ID | Operation | Required | Current | Severity |
|----|-----------|----------|---------|----------|
| CX-1 | ReadAt sort check | O(log B + R) | O(B) per read | Critical |
| CX-2 | ListManifests deser. | path-only IDs | O(M) Gets | High |
| CX-3 | ListPartitions | single pass | O(2M) Gets | High |
| CX-4 | findSnapshotByID | O(1) Get | O(N) scan fallback | Medium |
| CX-5 | Write memory (partitioned) | O(R + encoded) | O(3R) | Medium |
| CX-6 | Volume.Snapshots validation | O(S × B_avg) | O(S × B_avg) + redundant sort checks | Medium |
| CX-7 | fsStore.List | O(N) readdir | O(N) stat via Walk | Low |

---

## Design Invariant

> Every public method has a documented cost.
> Cost that exceeds the documented bound is a bug, not a tradeoff.
