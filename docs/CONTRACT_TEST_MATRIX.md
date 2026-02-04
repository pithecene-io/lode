# Contract-to-Test Traceability Matrix

This document maps requirements from `docs/contracts/CONTRACT_*.md` to existing tests
and identifies gaps requiring additional test coverage for v0.3.0 release.

**Baseline captured**: `go test ./...` passes (2024-01-XX)

---

## Legend

| Status | Meaning |
|--------|---------|
| ✅ | Covered by existing test(s) |
| ⚠️ | Partially covered (implicit or incomplete) |
| ❌ | Gap - explicit test needed |

---

## CONTRACT_CORE.md — Core Model

### Goals

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Immutable, self-describing snapshots | ✅ | Multiple write/read tests |
| Explicit, persisted metadata | ✅ | Manifest validation tests |
| Linear history per dataset | ✅ | `TestDataset_StreamWrite_ParentSnapshotLinked`, `TestDataset_StreamWriteRecords_ParentSnapshotLinked` |
| Backend-agnostic layout semantics | ✅ | Layout tests across FSStore, MemoryStore, S3Store |

### Manifest Requirements

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| SchemaName required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingSchemaName` |
| FormatVersion required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingFormatVersion` |
| DatasetID required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingDatasetID` |
| SnapshotID required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingSnapshotID` |
| CreatedAt required | ✅ | `TestReader_GetManifest_InvalidManifest_ZeroCreatedAt` |
| Metadata non-nil | ✅ | `TestReader_GetManifest_InvalidManifest_NilMetadata` |
| Files non-nil | ✅ | `TestReader_GetManifest_InvalidManifest_NilFiles` |
| RowCount non-negative | ✅ | `TestReader_GetManifest_InvalidManifest_NegativeRowCount` |
| Compressor required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingCompressor` |
| Partitioner required | ✅ | `TestReader_GetManifest_InvalidManifest_MissingPartitioner` |
| Codec optional | ✅ | `TestDataset_Write_WithoutChecksum_OmitsChecksum` shows codec-less writes work |
| ParentSnapshotID optional | ✅ | First snapshot tests have no parent |
| MinTimestamp/MaxTimestamp optional | ✅ | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax` |
| Checksum optional | ✅ | `TestDataset_Write_WithoutChecksum_OmitsChecksum` |

### Checksum Rules

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Checksum opt-in and explicit | ✅ | `TestDataset_Write_WithChecksum_RecordsChecksum` |
| Checksum algorithm recorded when configured | ✅ | `TestDataset_Write_WithChecksum_RecordsChecksum` |
| Checksum per file when configured | ✅ | `TestDataset_Write_WithChecksum_RecordsChecksum` |
| Checksum fields omitted when not configured | ✅ | `TestDataset_Write_WithoutChecksum_OmitsChecksum` |

### Metadata Rules

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| nil metadata is invalid and MUST error | ✅ | `TestDataset_Write_NilMetadata_ReturnsError`, `TestDataset_StreamWrite_NilMetadata_ReturnsError`, `TestDataset_StreamWriteRecords_NilMetadata_ReturnsError` |
| Empty metadata (`{}`) is valid | ✅ | `TestDataset_Write_EmptyMetadata_ValidAndPersisted`, `TestDataset_StreamWrite_EmptyMetadata_ValidAndPersisted`, `TestDataset_StreamWriteRecords_EmptyMetadata_ValidAndPersisted` |
| Metadata never inferred | ✅ | Design constraint (metadata must be explicitly passed) |

### History & Immutability

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Linear snapshot history | ✅ | Parent snapshot tests |
| Snapshots immutable after commit | ✅ | `ErrPathExists` tests |
| Data files immutable after write | ✅ | `TestFSStore_Put_ErrPathExists`, `TestMemoryStore_Put_ErrPathExists`, `TestStore_Put_ErrPathExists` |
| Commits create new state | ✅ | All write tests create new snapshots |

---

## CONTRACT_WRITE_API.md — Write API

### Dataset.Write Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Write creates new snapshot on success | ✅ | Multiple write tests |
| nil metadata MUST return error | ✅ | `TestDataset_Write_NilMetadata_ReturnsError` |
| Empty metadata valid and persisted | ✅ | `TestDataset_Write_EmptyMetadata_ValidAndPersisted` |
| New snapshot references parent | ✅ | `TestDataset_StreamWrite_ParentSnapshotLinked` |
| Writes MUST NOT mutate existing | ✅ | `ErrPathExists` tests |
| Manifest includes required fields | ✅ | Manifest validation tests |
| Raw blob RowCount = 1 | ✅ | `TestDataset_StreamWrite_Success` checks RowCount |

### StreamWrite Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| nil metadata returns error | ✅ | `TestDataset_StreamWrite_NilMetadata_ReturnsError` |
| Returns StreamWriter for single binary unit | ✅ | `TestDataset_StreamWrite_Success` |
| Commit writes manifest and returns snapshot | ✅ | `TestDataset_StreamWrite_Success` |
| Snapshot NOT visible before Commit | ❌ | **GAP: Need explicit visibility test** |
| Abort ensures no manifest written | ✅ | `TestDataset_StreamWrite_Abort_NoManifest` |
| Close without Commit behaves as Abort | ✅ | `TestDataset_StreamWrite_CloseWithoutCommit_BehavesAsAbort` |
| Single-pass writes to final path | ✅ | Design verified |
| RowCount = 1 for streamed blob | ✅ | `TestDataset_StreamWrite_Success` |
| Checksum computed during streaming | ✅ | `TestDataset_StreamWrite_WithChecksum_RecordsChecksum` |
| Codec configured returns ErrCodecConfigured | ✅ | `TestDataset_StreamWrite_WithCodec_ReturnsError` |

### StreamWriteRecords Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| nil metadata returns error | ✅ | `TestDataset_StreamWriteRecords_NilMetadata_ReturnsError` |
| nil iterator returns ErrNilIterator | ✅ | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| Pull-based iterator consumption | ✅ | `TestDataset_StreamWriteRecords_Success` |
| Non-streaming codec returns ErrCodecNotStreamable | ✅ | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| Partitioning returns ErrPartitioningNotSupported | ✅ | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |
| Single-pass writes | ✅ | Design verified |
| On success, manifest written | ✅ | `TestDataset_StreamWriteRecords_Success` |
| On error, no manifest, best-effort cleanup | ✅ | `TestDataset_StreamWriteRecords_IteratorError` |
| RowCount = total records consumed | ✅ | `TestDataset_StreamWriteRecords_Success` |
| Checksum computed during streaming | ✅ | `TestDataset_StreamWriteRecords_WithChecksum_RecordsChecksum` |

### Timestamp Computation

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Timestamped interface extracts min/max | ✅ | `TestDataset_Write_TimestampedRecords_ComputesMinMax`, `TestDataset_StreamWriteRecords_TimestampedRecords_ComputesMinMax` |
| Non-timestamped records omit timestamps | ✅ | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax`, `TestDataset_StreamWriteRecords_NonTimestampedRecords_OmitsMinMax` |
| Raw blob omits timestamps | ✅ | `TestDataset_Write_RawBlob_OmitsTimestamps` |
| Single record has same min/max | ✅ | `TestDataset_Write_SingleTimestampedRecord_SameMinMax` |

### Empty Dataset Behavior

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| First write creates initial snapshot | ✅ | All write tests |
| Latest() returns ErrNoSnapshots | ✅ | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| Snapshots() returns empty list | ✅ | `TestDataset_Snapshots_EmptyDataset_ReturnsEmptyList` |
| Snapshot(id) returns ErrNotFound | ✅ | `TestDataset_Snapshot_EmptyDataset_ReturnsErrNotFound` |

### Error Sentinels

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrNoSnapshots | ✅ | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| ErrNotFound | ✅ | `TestDataset_Snapshot_EmptyDataset_ReturnsErrNotFound` |
| ErrPathExists | ✅ | Multiple store tests |
| ErrCodecConfigured | ✅ | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| ErrCodecNotStreamable | ✅ | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| ErrNilIterator | ✅ | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| ErrPartitioningNotSupported | ✅ | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |

### Concurrency / TOCTOU

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Single writer requirement documented | ✅ | `TestStore_Multipart_TOCTOU_Documentation` |
| Multipart TOCTOU window documented | ✅ | `TestStore_Multipart_TOCTOU_Documentation` |

---

## CONTRACT_STORAGE.md — Storage Adapter

### Put

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Write data to path | ✅ | Multiple Put tests |
| ErrPathExists on duplicate | ✅ | `TestFSStore_Put_ErrPathExists`, `TestMemoryStore_Put_ErrPathExists`, `TestStore_Put_ErrPathExists` |
| Atomic path uses conditional write | ✅ | `TestStore_Put_Atomic_Duplicate_ReturnsErrPathExists` |
| Multipart path uses preflight check | ✅ | `TestStore_PutMultipartFromFile_PreExisting_ReturnsErrPathExists` |
| TOCTOU window exists for multipart | ✅ | `TestStore_Multipart_TOCTOU_Documentation` |
| Temp file cleanup on success | ✅ | `TestStore_Put_TempFileCleanup_OnSuccess` |
| Temp file cleanup on failure | ✅ | `TestStore_Put_TempFileCleanup_OnFailure` |

### Get

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Returns readable stream | ✅ | `TestStore_Get_Success` and similar |
| ErrNotFound for missing path | ✅ | `TestFSStore_ReadRange_NotFound`, `TestStore_Get_ErrNotFound` |

### Exists

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Accurate existence report | ✅ | `TestStore_Exists_True`, `TestStore_Exists_False` |
| Does not mutate data | ✅ | Design constraint |

### List

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Returns all paths under prefix | ✅ | `TestStore_List_WithPrefix` |
| Empty prefix on empty storage | ✅ | `TestFSStore_List_EmptyPrefix_ReturnsEmpty`, `TestMemoryStore_List_NonExistentPrefix_ReturnsEmpty`, `TestStore_List_Empty` |

### Delete

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Removes path if exists | ✅ | `TestStore_Delete_Exists` |
| Idempotent on missing path | ✅ | `TestStore_Delete_NotExists_Idempotent` |

### ReadRange

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Returns bytes from range | ✅ | `TestFSStore_ReadRange_Basic`, `TestMemoryStore_ReadRange_Basic`, `TestStore_ReadRange_Basic` |
| ErrNotFound for missing | ✅ | `TestFSStore_ReadRange_NotFound`, `TestMemoryStore_ReadRange_NotFound`, `TestStore_ReadRange_NotFound` |
| ErrInvalidPath for negative offset | ✅ | `TestFSStore_ReadRange_NegativeOffset`, etc. |
| ErrInvalidPath for negative length | ✅ | `TestFSStore_ReadRange_NegativeLength`, etc. |
| ErrInvalidPath for length overflow | ✅ | `TestFSStore_ReadRange_LengthOverflow`, etc. |
| ErrInvalidPath for offset+length overflow | ✅ | `TestFSStore_ReadRange_OffsetPlusLengthOverflow`, etc. |
| Range beyond EOF returns available | ✅ | `TestFSStore_ReadRange_BeyondEOF`, etc. |
| Offset beyond EOF returns empty | ✅ | `TestFSStore_ReadRange_OffsetBeyondEOF`, etc. |

### ReaderAt

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Returns io.ReaderAt | ✅ | `TestFSStore_ReaderAt_Basic`, `TestMemoryStore_ReaderAt_Basic`, `TestStore_ReaderAt_Basic` |
| ErrNotFound for missing | ✅ | `TestFSStore_ReaderAt_NotFound`, etc. |
| Concurrent reads supported | ✅ | `TestStore_ReaderAt_ConcurrentReads` |

### Commit Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Manifest presence = commit signal | ✅ | Integration test `manifest_last_visibility` |
| Writers write data before manifest | ✅ | Integration test `manifest_last_visibility` |

### Streaming Write Atomicity

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Failure triggers abort/cleanup | ✅ | `TestStore_PutMultipartFromFile_FailureTriggersAbort` |
| Cleanup uses independent context | ⚠️ | Implementation detail, needs verification |

---

## CONTRACT_LAYOUT.md — Layout & Components

### Layout Ownership

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Path topology defined by layout | ✅ | Layout integration tests |
| Flat layouts require noop partitioner | ✅ | `TestNewDataset_RawBlobWithPartitioner_ReturnsError` |
| Hive layouts partition-anchored | ✅ | `TestNewHiveLayout_WithKeys_Success` |

### Partitioner

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Recorded in manifest by name | ✅ | Manifest tests |
| Noop accepted for flat layouts | ✅ | `TestNewDataset_RawBlobWithDefaultLayout_Success` |
| Non-noop rejected for flat layouts | ✅ | `TestNewDataset_RawBlobWithPartitioner_ReturnsError` |

### Compressor

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Recorded in manifest by name | ✅ | `TestDataset_StreamWrite_WithGzipCompression` |
| Noop explicit | ✅ | Default layout tests |

### Codec

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Optional | ✅ | Raw blob tests |
| Recorded when configured | ✅ | `TestDataset_StreamWriteRecords_Success` |
| Omitted when not configured | ✅ | Raw blob manifest tests |

### NoOp Components

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Canonical NoOp implementations exist | ✅ | Implicit |
| nil layout rejected | ✅ | `TestNewDataset_NilLayout_ReturnsError`, `TestNewReader_NilLayout_ReturnsError` |
| nil compressor rejected | ✅ | `TestNewDataset_NilCompressor_ReturnsError` |
| Manifest records "noop" explicitly | ✅ | Manifest tests |

---

## CONTRACT_ITERATION.md — Object Iteration

### Required Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Next() returns false after exhaustion | ❌ | **GAP: Need ObjectIterator lifecycle tests** |
| Next() returns false after Close() | ❌ | **GAP: Need ObjectIterator lifecycle tests** |
| Close() idempotent | ❌ | **GAP: Need ObjectIterator lifecycle tests** |
| Err() callable after exhaustion/close | ❌ | **GAP: Need ObjectIterator lifecycle tests** |
| Resources released on Close/exhaustion | ❌ | **GAP: Need ObjectIterator lifecycle tests** |

---

## CONTRACT_COMPOSITION.md — Store Composition

### Required Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Exactly one base store required | ⚠️ | Implicit in NewDataset/NewReader |
| Wrapper order respected | ❌ | **GAP: Need wrapper ordering tests** |
| Reordering doesn't change correctness | ❌ | **GAP: Need wrapper behavior tests** |

---

## CONTRACT_READ_API.md — Read API

### Storage Adapter

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ReadRange true range reads | ✅ | S3 integration tests |
| ReaderAt random access | ✅ | ReaderAt tests |

### Read API Error Semantics

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ListDatasets layout-specific error | ✅ | `TestReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |
| ListDatasets empty only when truly empty | ✅ | `TestReader_ListDatasets_EmptyStorage` |

### Manifest Validation

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrManifestInvalid for missing SchemaName | ✅ | `TestReader_GetManifest_InvalidManifest_MissingSchemaName` |
| ErrManifestInvalid for missing FormatVersion | ✅ | `TestReader_GetManifest_InvalidManifest_MissingFormatVersion` |
| ErrManifestInvalid for nil Metadata | ✅ | `TestReader_GetManifest_InvalidManifest_NilMetadata` |
| ErrManifestInvalid for missing DatasetID | ✅ | `TestReader_GetManifest_InvalidManifest_MissingDatasetID` |
| ErrManifestInvalid for missing SnapshotID | ✅ | `TestReader_GetManifest_InvalidManifest_MissingSnapshotID` |
| ErrManifestInvalid for zero CreatedAt | ✅ | `TestReader_GetManifest_InvalidManifest_ZeroCreatedAt` |
| ErrManifestInvalid for nil Files | ✅ | `TestReader_GetManifest_InvalidManifest_NilFiles` |
| ErrManifestInvalid for negative RowCount | ✅ | `TestReader_GetManifest_InvalidManifest_NegativeRowCount` |
| ErrManifestInvalid for missing Compressor | ✅ | `TestReader_GetManifest_InvalidManifest_MissingCompressor` |
| ErrManifestInvalid for missing Partitioner | ✅ | `TestReader_GetManifest_InvalidManifest_MissingPartitioner` |
| ErrManifestInvalid for empty FileRef.Path | ✅ | `TestReader_GetManifest_InvalidManifest_EmptyFilePath` |
| ErrManifestInvalid for negative FileRef.SizeBytes | ✅ | `TestReader_GetManifest_InvalidManifest_NegativeFileSize` |
| ListSegments returns error on invalid manifest | ✅ | `TestReader_ListSegments_InvalidManifest_*` |

---

## CONTRACT_ERRORS.md — Error Taxonomy

### Not Found Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrNotFound for storage path | ✅ | Multiple tests |
| ErrNoSnapshots for empty dataset | ✅ | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| ErrNoManifests | ✅ | `TestReader_ListDatasets_ErrNoManifests` |

### Layout Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrDatasetsNotModeled | ✅ | `TestReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |

### Manifest Validation Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrManifestInvalid | ✅ | Multiple tests |

### Storage Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrPathExists | ✅ | Multiple tests |
| ErrInvalidPath | ✅ | Multiple tests |
| ErrRangeReadNotSupported | ❌ | **GAP: Need test for stores without range capability** |

### Configuration Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| NewReader(nil factory) returns error | ✅ | `TestNewReader_NilFactory_ReturnsError` |
| NewDataset(id, nil) returns error | ✅ | `TestNewDataset_NilFactory_ReturnsError` |

### Codec/Component Mismatch Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| Codec mismatch on read | ✅ | `TestDataset_Read_CodecMismatch_ReturnsError` |
| Compressor mismatch on read | ✅ | `TestDataset_Read_CompressorMismatch_ReturnsError` |
| ErrCodecNotStreamable | ✅ | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |

### Streaming API Errors

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| ErrCodecConfigured | ✅ | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| ErrNilIterator | ✅ | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| ErrPartitioningNotSupported | ✅ | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |

### Cleanup on Error

| Requirement | Status | Test(s) |
|-------------|--------|---------|
| No manifest on error | ✅ | `TestDataset_StreamWriteRecords_IteratorError`, `TestDataset_StreamWrite_Abort_NoManifest` |
| Best-effort cleanup documented | ✅ | Documentation only (non-deterministic) |

---

## Gap Summary — Actionable Test Tasks

### PR 2: Error-path and Invariant Tests ✅ COMPLETED

| Gap ID | Contract | Requirement | Test(s) Added |
|--------|----------|-------------|---------------|
| G2-1 | CORE | ErrManifestInvalid for missing DatasetID | ✅ `TestReader_GetManifest_InvalidManifest_MissingDatasetID` |
| G2-2 | CORE | ErrManifestInvalid for missing SnapshotID | ✅ `TestReader_GetManifest_InvalidManifest_MissingSnapshotID` |
| G2-3 | CORE | ErrManifestInvalid for zero CreatedAt | ✅ `TestReader_GetManifest_InvalidManifest_ZeroCreatedAt` |
| G2-4 | CORE | ErrManifestInvalid for nil Files | ✅ `TestReader_GetManifest_InvalidManifest_NilFiles` |
| G2-5 | CORE | ErrManifestInvalid for negative RowCount | ✅ `TestReader_GetManifest_InvalidManifest_NegativeRowCount` |
| G2-6 | CORE | ErrManifestInvalid for missing Compressor | ✅ `TestReader_GetManifest_InvalidManifest_MissingCompressor` |
| G2-7 | CORE | ErrManifestInvalid for missing Partitioner | ✅ `TestReader_GetManifest_InvalidManifest_MissingPartitioner` |
| G2-8 | CORE | ErrManifestInvalid for empty FileRef.Path | ✅ `TestReader_GetManifest_InvalidManifest_EmptyFilePath` |
| G2-9 | CORE | ErrManifestInvalid for negative FileRef.SizeBytes | ✅ `TestReader_GetManifest_InvalidManifest_NegativeFileSize` |
| G2-10 | LAYOUT | nil layout rejected | ✅ `TestNewDataset_NilLayout_ReturnsError`, `TestNewReader_NilLayout_ReturnsError` |
| G2-11 | LAYOUT | nil compressor rejected | ✅ `TestNewDataset_NilCompressor_ReturnsError` |
| G2-12 | ERRORS | NewReader(nil factory) error | ✅ `TestNewReader_NilFactory_ReturnsError`, `TestNewReader_FactoryReturnsNil_ReturnsError` |
| G2-13 | ERRORS | Codec mismatch on read | ✅ `TestDataset_Read_CodecMismatch_ReturnsError` |
| G2-14 | ERRORS | Compressor mismatch on read | ✅ `TestDataset_Read_CompressorMismatch_ReturnsError` |
| G2-15 | WRITE | Empty metadata explicitly valid | ✅ `TestDataset_Write_EmptyMetadata_ValidAndPersisted`, `TestDataset_StreamWrite_EmptyMetadata_ValidAndPersisted`, `TestDataset_StreamWriteRecords_EmptyMetadata_ValidAndPersisted` |

### PR 3: Streaming Failure Semantics Tests

| Gap ID | Contract | Requirement | Test Task |
|--------|----------|-------------|-----------|
| G3-1 | WRITE | Snapshot NOT visible before Commit | Add `TestDataset_StreamWrite_NotVisibleBeforeCommit` |
| G3-2 | WRITE | StreamWriter context cancellation cleanup | Add `TestDataset_StreamWrite_ContextCancellation_NoManifest` |
| G3-3 | WRITE | StreamWriteRecords context cancellation | Add `TestDataset_StreamWriteRecords_ContextCancellation_NoManifest` |
| G3-4 | STORAGE | Cleanup uses independent context | Verify implementation + document |

### Future (Lower Priority)

| Gap ID | Contract | Requirement | Test Task |
|--------|----------|-------------|-----------|
| GF-1 | ITERATION | ObjectIterator lifecycle | Add iterator lifecycle tests when ObjectIterator is exposed |
| GF-2 | COMPOSITION | Wrapper ordering | Add wrapper ordering tests when composition API is exposed |
| GF-3 | ERRORS | ErrRangeReadNotSupported | Add when non-range store adapter exists |

---

## Test Baseline Evidence

```
$ go test ./...
?   	github.com/justapithecus/lode/examples/blob_upload	[no test files]
?   	github.com/justapithecus/lode/examples/default_layout	[no test files]
?   	github.com/justapithecus/lode/examples/hive_layout	[no test files]
?   	github.com/justapithecus/lode/examples/manifest_driven	[no test files]
?   	github.com/justapithecus/lode/examples/s3_experimental	[no test files]
?   	github.com/justapithecus/lode/internal/testutil	[no test files]
ok  	github.com/justapithecus/lode/lode
ok  	github.com/justapithecus/lode/lode/s3
```

---

## Document History

| Date | PR | Changes |
|------|----|---------|
| 2024-XX-XX | PR 1 | Initial matrix creation |
| 2024-XX-XX | PR 2 | G2-1..G2-15 covered: manifest validation, constructor rejection, read mismatch, empty metadata |
