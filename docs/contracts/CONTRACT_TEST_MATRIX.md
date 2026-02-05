# Contract-to-Test Traceability Matrix

This document maps contract requirements to tests and tracks coverage gaps.

---

## Legend

### Coverage Status

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully covered by test(s) |
| ⚠️ | Partial coverage or documented limitation |
| ❌ | Gap — test needed |

### Gap Codes

Gaps are tracked with codes indicating category and priority:

| Prefix | Category | Description |
|--------|----------|-------------|
| `CORE-` | Core Model | Manifest structure, immutability, metadata rules |
| `WRITE-` | Write API | Dataset.Write, StreamWrite, StreamWriteRecords |
| `STORE-` | Storage | Adapter operations (Put, Get, List, Delete, Range) |
| `LAYOUT-` | Layout | Path topology, partitioning, component rules |
| `READ-` | Read API | Reader operations, manifest loading |
| `ERR-` | Errors | Error sentinels and taxonomy |
| `ITER-` | Iteration | ObjectIterator lifecycle |
| `COMP-` | Composition | Store wrapper ordering |

---

## Coverage by Contract

### CONTRACT_CORE.md — Core Model

**Manifest Required Fields**: All covered ✅

| Field | Test |
|-------|------|
| SchemaName | `TestReader_GetManifest_InvalidManifest_MissingSchemaName` |
| FormatVersion | `TestReader_GetManifest_InvalidManifest_MissingFormatVersion` |
| DatasetID | `TestReader_GetManifest_InvalidManifest_MissingDatasetID` |
| SnapshotID | `TestReader_GetManifest_InvalidManifest_MissingSnapshotID` |
| CreatedAt | `TestReader_GetManifest_InvalidManifest_ZeroCreatedAt` |
| Metadata (non-nil) | `TestReader_GetManifest_InvalidManifest_NilMetadata` |
| Files (non-nil) | `TestReader_GetManifest_InvalidManifest_NilFiles` |
| RowCount (≥0) | `TestReader_GetManifest_InvalidManifest_NegativeRowCount` |
| Compressor | `TestReader_GetManifest_InvalidManifest_MissingCompressor` |
| Partitioner | `TestReader_GetManifest_InvalidManifest_MissingPartitioner` |

**Manifest Optional Fields**: All covered ✅

| Field | Test |
|-------|------|
| Codec | Raw blob tests (codec omitted) |
| ParentSnapshotID | First snapshot tests (no parent) |
| MinTimestamp/MaxTimestamp | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax` |
| Checksum | `TestDataset_Write_WithoutChecksum_OmitsChecksum` |

**Metadata Rules**: All covered ✅

- nil metadata rejected: `TestDataset_Write_NilMetadata_ReturnsError`, `TestDataset_StreamWrite_NilMetadata_ReturnsError`, `TestDataset_StreamWriteRecords_NilMetadata_ReturnsError`
- Empty metadata valid: `TestDataset_Write_EmptyMetadata_ValidAndPersisted`, etc.

**Immutability**: All covered ✅

- Data files immutable: `TestFSStore_Put_ErrPathExists`, `TestMemoryStore_Put_ErrPathExists`, `TestStore_Put_ErrPathExists`
- Linear history: `TestDataset_StreamWrite_ParentSnapshotLinked`, `TestDataset_StreamWriteRecords_ParentSnapshotLinked`

---

### CONTRACT_WRITE_API.md — Write API

**Dataset.Write**: All covered ✅

| Requirement | Test |
|-------------|------|
| Creates snapshot | Multiple write tests |
| nil metadata error | `TestDataset_Write_NilMetadata_ReturnsError` |
| Parent snapshot linked | `TestDataset_StreamWrite_ParentSnapshotLinked` |
| Raw blob RowCount=1 | `TestDataset_StreamWrite_Success` |

**StreamWrite**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata error | `TestDataset_StreamWrite_NilMetadata_ReturnsError` |
| Commit writes manifest | `TestDataset_StreamWrite_Success` |
| Snapshot invisible before Commit | `TestDataset_StreamWrite_NotVisibleBeforeCommit` |
| Abort → no manifest | `TestDataset_StreamWrite_Abort_NoManifest` |
| Close without Commit → abort | `TestDataset_StreamWrite_CloseWithoutCommit_BehavesAsAbort` |
| Codec configured → error | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| Checksum computed | `TestDataset_StreamWrite_WithChecksum_RecordsChecksum` |
| Manifest Put error → no manifest + cleanup | `TestStreamWrite_ManifestPutError_NoManifest_CleanupAttempted` |
| Abort → cleanup attempted | `TestStreamWrite_Abort_NoManifest_CleanupAttempted` |
| Close → cleanup attempted | `TestStreamWrite_CloseWithoutCommit_NoManifest_CleanupAttempted` |
| Cleanup errors ignored | `TestStreamWrite_CleanupErrorIgnored` |
| Blocked Put + cancel → no manifest | `TestStreamWrite_BlockedPut_ContextCancel_NoManifest` |

**StreamWriteRecords**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata error | `TestDataset_StreamWriteRecords_NilMetadata_ReturnsError` |
| nil iterator error | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| Non-streaming codec error | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| Partitioning error | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |
| Iterator error → no manifest | `TestDataset_StreamWriteRecords_IteratorError` |
| RowCount = records consumed | `TestDataset_StreamWriteRecords_Success` |
| Iterator error → cleanup attempted | `TestStreamWriteRecords_IteratorError_NoManifest_CleanupAttempted` |
| Manifest Put error → no manifest + cleanup | `TestStreamWriteRecords_ManifestPutError_NoManifest` |

**Timestamp Computation**: All covered ✅

| Requirement | Test |
|-------------|------|
| Timestamped interface | `TestDataset_Write_TimestampedRecords_ComputesMinMax` |
| Non-timestamped omits | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax` |
| Raw blob omits | `TestDataset_Write_RawBlob_OmitsTimestamps` |

**Empty Dataset**: All covered ✅

| Requirement | Test |
|-------------|------|
| Latest() → ErrNoSnapshots | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| Snapshots() → empty | `TestDataset_Snapshots_EmptyDataset_ReturnsEmptyList` |
| Snapshot(id) → ErrNotFound | `TestDataset_Snapshot_EmptyDataset_ReturnsErrNotFound` |

---

### CONTRACT_STORAGE.md — Storage Adapter

**Put**: All covered ✅

| Requirement | Test |
|-------------|------|
| ErrPathExists on duplicate (atomic) | `TestStore_Put_Atomic_Duplicate_ReturnsErrPathExists` |
| Multipart preflight check | `TestStore_PutMultipartFromFile_PreExisting_ReturnsErrPathExists` |
| Multipart conditional completion | `TestStore_PutMultipartFromFile_ConditionalCompletion_ReturnsErrPathExists` |
| Conditional completion documented | `TestStore_Multipart_ConditionalCompletion` |
| Temp file cleanup | `TestStore_Put_TempFileCleanup_OnSuccess`, `TestStore_Put_TempFileCleanup_OnFailure` |

**Get/Exists/List/Delete**: All covered ✅

| Operation | Tests |
|-----------|-------|
| Get | `TestStore_Get_Success`, `TestStore_Get_ErrNotFound` |
| Exists | `TestStore_Exists_True`, `TestStore_Exists_False` |
| List | `TestStore_List_WithPrefix`, `TestStore_List_Empty` |
| Delete | `TestStore_Delete_Exists`, `TestStore_Delete_NotExists_Idempotent` |

**ReadRange**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic range read | `TestStore_ReadRange_Basic` (FS, Memory, S3) |
| ErrNotFound | `TestStore_ReadRange_NotFound` |
| Invalid params | `TestStore_ReadRange_NegativeOffset`, `TestStore_ReadRange_NegativeLength`, etc. |
| Beyond EOF | `TestStore_ReadRange_BeyondEOF`, `TestStore_ReadRange_OffsetBeyondEOF` |

**ReaderAt**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic | `TestStore_ReaderAt_Basic` |
| Concurrent reads | `TestStore_ReaderAt_ConcurrentReads` |
| ErrNotFound | `TestStore_ReaderAt_NotFound` |

---

### CONTRACT_LAYOUT.md — Layout & Components

All covered ✅

| Requirement | Test |
|-------------|------|
| nil layout rejected | `TestNewDataset_NilLayout_ReturnsError`, `TestNewReader_NilLayout_ReturnsError` |
| nil compressor rejected | `TestNewDataset_NilCompressor_ReturnsError` |
| Raw blob + partitioner rejected | `TestNewDataset_RawBlobWithPartitioner_ReturnsError` |
| Hive layout keys | `TestNewHiveLayout_WithKeys_Success` |

**Compression**: All covered ✅

| Requirement | Test |
|-------------|------|
| Gzip round-trip | `TestDataset_StreamWrite_WithGzipCompression` |
| Zstd round-trip (Write) | `TestDataset_Write_WithZstdCompression` |
| Zstd round-trip (StreamWrite) | `TestDataset_StreamWrite_WithZstdCompression` |
| Zstd round-trip (StreamWriteRecords) | `TestDataset_StreamWriteRecords_WithZstdCompression` |
| Compressor mismatch error | `TestDataset_Read_CompressorMismatch_ReturnsError` |

---

### CONTRACT_READ_API.md — Read API

All covered ✅

| Requirement | Test |
|-------------|------|
| ListDatasets layout error | `TestReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |
| ListDatasets empty storage | `TestReader_ListDatasets_EmptyStorage` |
| ErrNoManifests | `TestReader_ListDatasets_ErrNoManifests` |
| Manifest validation errors | Multiple `TestReader_GetManifest_InvalidManifest_*` tests |

---

### CONTRACT_ERRORS.md — Error Taxonomy

All error sentinels covered ✅

| Sentinel | Test |
|----------|------|
| ErrNotFound | Multiple tests |
| ErrNoSnapshots | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| ErrNoManifests | `TestReader_ListDatasets_ErrNoManifests` |
| ErrPathExists | Multiple store tests |
| ErrInvalidPath | Multiple store tests |
| ErrDatasetsNotModeled | `TestReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |
| ErrManifestInvalid | Multiple validation tests |
| ErrCodecConfigured | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| ErrCodecNotStreamable | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| ErrNilIterator | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| ErrPartitioningNotSupported | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |

---

## Open Gaps

### G3-4: Streaming Failure Semantics

The following invariants are now **deterministically tested** via fault injection:

| Invariant | Coverage |
|-----------|----------|
| No manifest on failed stream | ✅ All failure paths |
| Cleanup (Delete) attempted on failure | ✅ All failure paths |
| Cleanup errors ignored (best-effort) | ✅ Explicit test |
| Snapshot invisibility preserved | ✅ All failure paths |

**Deterministic test infrastructure**: `lode/store_fault_test.go` provides a fault-injection
store wrapper with error injection, call observation, and blocking for synchronization.

### Residual Risk: STORE-CTX-CANCEL (Addressed)

**Context cancellation cleanup timing is adapter-dependent.**

| Aspect | Status |
|--------|--------|
| Core invariant: no manifest on cancel | ✅ Deterministically tested with blocked Put |
| Cleanup attempt on cancel | ✅ Deterministically tested with blocked Put |
| Actual cleanup completion | ⚠️ Timing-dependent (adapter-specific) |
| FS adapter timing | ✅ Integration tests in `lode/adapter_timing_test.go` |
| S3 adapter timing | ✅ Integration tests in `lode/s3/integration_test.go` (gated) |

**Why timing matters:**
- In-memory stores complete synchronously before cancellation takes effect
- Real adapters (S3, FS) have varying timing windows
- Whether cleanup Delete runs before or after context deadline is nondeterministic

**Mitigation:**
- Core invariants are tested deterministically via `TestStreamWrite_BlockedPut_ContextCancel_NoManifest`
- FS adapter timing verified via `TestFSAdapter_StreamWrite_ContextCancel_NoSnapshot`
- S3 adapter timing verified via `TestLocalStack_StreamWrite_ContextCancel_NoSnapshot` (requires `LODE_S3_TESTS=1`)
- Cleanup is documented as best-effort per CONTRACT_ERRORS.md

**Status**: Core invariants covered. Adapter-specific timing tests exercise real behavior without asserting on inherently nondeterministic outcomes.

---

### Deferred: Requires API Exposure

| Gap | Contract | Blocked By |
|-----|----------|------------|
| ITER-LIFECYCLE | ITERATION | ObjectIterator not yet public |
| COMP-WRAPPER-ORDER | COMPOSITION | Composition API not yet public |
| ERR-RANGE-NOT-SUPPORTED | ERRORS | No non-range adapter exists |

These gaps will be addressed when the relevant APIs are exposed.

---

## Test Baseline

```
$ go test ./...
ok  	github.com/justapithecus/lode/lode
ok  	github.com/justapithecus/lode/lode/s3
```

All contracts have test coverage except for deferred gaps and documented residual risk.
