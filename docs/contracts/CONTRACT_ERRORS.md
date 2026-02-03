# Lode Error Taxonomy — Contract

This document defines the error taxonomy for Lode's read and write APIs.
Errors are categorized by source and intent to enable precise error handling.

---

## Goals

1. **Predictable**: Callers know which errors to expect from each operation.
2. **Actionable**: Error types indicate whether retry, reconfiguration, or abort is appropriate.
3. **Immutability-safe**: No error implies mutable state or background recovery.

---

## Error Categories

### 1. Not Found Errors

These indicate a requested resource does not exist.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNotFound` | Storage | Object path does not exist |
| `lode.ErrNotFound` | Read API | Dataset or segment not found (no manifests) |
| `lode.ErrNoSnapshots` | Dataset | Dataset exists but has no committed snapshots |
| `lode.ErrNoManifests` | Read API | Storage contains objects but no valid manifests |

**Behavior**:
- `ListSegments` returns `ErrNotFound` when dataset has no committed manifests.
- `ListPartitions` returns `ErrNotFound` when dataset has no committed manifests.
- `GetManifest` returns `ErrNotFound` when manifest path doesn't exist.
- `Snapshot` returns `ErrNotFound` when snapshot ID doesn't exist.
 - `ListDatasets` returns `ErrNoManifests` when storage contains objects but no valid manifests.

---

### 2. Layout Errors

These indicate semantic incompatibility between the layout and the requested operation.

| Error | Source | Meaning |
|-------|--------|---------|
| `read.ErrDatasetsNotModeled` | Read API | Layout doesn't support dataset enumeration |

**Behavior**:
- `ListDatasets` returns `ErrDatasetsNotModeled` when `Layout.SupportsDatasetEnumeration()` is false.
- An empty list is returned **only** when storage is truly empty (not as a fallback for unsupported operations).

**Layout-Specific Notes**:
- `DefaultLayout`: Supports dataset enumeration (prefix: `datasets/`).
- `HiveLayout`: Supports dataset enumeration (prefix: `datasets/`).
- `FlatLayout`: Does NOT support dataset enumeration (no common prefix).

---

### 3. Manifest Validation Errors

These indicate a manifest fails structural or semantic validation.

| Error | Source | Meaning |
|-------|--------|---------|
| `read.ManifestValidationError` | Manifest loader | Manifest missing required fields or has invalid values |

**Required Manifest Fields** (per CONTRACT_CORE.md):
- `SchemaName`: Non-empty string identifying schema
- `FormatVersion`: Non-empty version string
- `DatasetID`: Non-empty dataset identifier
- `SnapshotID`: Non-empty snapshot identifier
- `CreatedAt`: Non-zero timestamp
- `Metadata`: Non-nil map (empty `{}` is valid)
- `Files`: Non-nil slice (empty `[]` is valid)
- `RowCount`: Non-negative integer
- `Compressor`: Non-empty compressor name
- `Partitioner`: Non-empty partitioner name

**Optional Fields**:
- `Codec`: May be empty when no codec configured
- `ParentSnapshotID`: May be empty for first snapshot
- `MinTimestamp`, `MaxTimestamp`: May be nil when not applicable
- `Checksum` in `FileRef`: May be empty

**File Validation**:
- Each `FileRef.Path` must be non-empty
- Each `FileRef.SizeBytes` must be non-negative

**Behavior**:
- `GetManifest` returns wrapped `ManifestValidationError` for invalid manifests.
- `ListSegments` returns error (not skip) when manifest validation fails.
- `ListPartitions` returns error (not skip) when manifest validation fails.

---

### 4. Storage Errors

These indicate storage-level failures.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrPathExists` | Storage | Attempt to write to existing path (immutability violation) |
| `lode.ErrInvalidPath` | Storage | Path escapes storage root or is empty |
| `read.ErrRangeReadNotSupported` | Read API | Store doesn't support range reads |

**Behavior**:
- `Put` returns `ErrPathExists` if path already exists (enforces immutability).
- `Put` returns `ErrInvalidPath` for paths that escape root or are empty.
- `Get` returns `ErrInvalidPath` for invalid paths.
- `ObjectReaderAt` returns `ErrRangeReadNotSupported` for stores without range capability.

---

### 5. Configuration Errors

These indicate invalid configuration at setup time.

| Error | Source | Meaning |
|-------|--------|---------|
| Error | Reader/Dataset | Nil store provided |

**Behavior**:
- `NewReader(nil)` returns error.
- `NewDataset(id, nil)` returns error.

---

### 6. Codec/Component Mismatch Errors

These indicate a mismatch between stored data and current configuration.

| Error | Source | Meaning |
|-------|--------|---------|
| Error | Dataset.Read | Snapshot codec doesn't match dataset codec |
| Error | Dataset.Read | Snapshot compressor doesn't match dataset compressor |

**Behavior**:
- `Read` validates manifest components against dataset config before reading.
- Mismatch returns descriptive error (not silent corruption).

---

## Error Handling Guidelines

### Retry-Safe Errors
- Storage I/O errors (network, timeout) — may retry.
- `ErrNotFound` during race — may retry if expecting eventual consistency.

### Non-Retry Errors
- `ErrDatasetsNotModeled` — reconfigure with different layout.
- `ManifestValidationError` — data corruption, investigate source.
- `ErrPathExists` — logic error in caller (double-write attempt).
- Component mismatch — reconfigure dataset or use matching snapshot.

### Fatal Errors
- Nil store/layout panics — programming error.

---

## Immutability Invariants

Errors MUST NOT imply:
- Mutable state (no "update failed" semantics).
- Background recovery (no "will retry later" semantics).
- Partial success (writes are atomic via manifest).

All operations are:
- **Idempotent reads**: Same input, same output (modulo storage consistency).
- **Atomic writes**: Manifest presence is the sole commit signal.

---

## Error Flow Examples

### ListDatasets with FlatLayout
```
ListDatasets(ctx, opts)
  → Layout.SupportsDatasetEnumeration() == false
  → return nil, ErrDatasetsNotModeled
```

### GetManifest with Invalid Schema
```
GetManifest(ctx, dataset, segment)
  → store.Get(manifestPath)
  → json.Decode() succeeds
  → ValidateManifest() fails (missing SchemaName)
  → return nil, ManifestValidationError{Field: "SchemaName", ...}
```

### ListSegments with Corrupt Manifest
```
ListSegments(ctx, dataset, partition, opts)
  → for each manifest path:
      → loadManifest(path)
      → ValidateManifest() fails
      → return nil, fmt.Errorf("failed to load manifest %s: %w", path, err)
```

---

## Design Invariant

> **Errors expose facts about storage state and configuration.
> They never imply mutable operations or deferred recovery.**
