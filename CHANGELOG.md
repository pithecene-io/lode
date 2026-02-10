# Changelog

All notable changes to Lode will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.7.1] - 2026-02-09

### Fixed

- **O(n²) write degradation on remote stores**: `Write()`, `StreamWrite()`, and `StreamWriteRecords()` called `Latest()` on every invocation, scanning all manifests via `store.List` + N×`store.Get`. On remote stores (S3, R2), this caused sequential writes to degrade quadratically. A 40-write burst that should complete in <1s took ~40 minutes. Parent snapshot ID is now cached after each successful write; only the first write (cold start) falls back to `Latest()`. ([#109](https://github.com/pithecene-io/lode/pull/109), closes [#108](https://github.com/pithecene-io/lode/issues/108))

### Added

- **Sequential write benchmarks**: `BenchmarkDataset_SequentialWrites` (wall-clock with simulated latency) and `BenchmarkDataset_SequentialWrites_StoreCallCount` (correctness assertion) guard against parent-resolution regressions

### Upgrade Notes

- No API changes; transparent internal optimization
- All write paths (`Write`, `StreamWrite`, `StreamWriteRecords`) benefit automatically
- Safe to upgrade from v0.7.0

---

## [0.7.0] - 2026-02-07

### Added

- **Per-File Column Statistics**: New `StatisticalCodec` and `StatisticalStreamEncoder` interfaces allow any codec to report per-file column stats (min, max, null count, distinct count) persisted on `FileRef`
- **Parquet Statistics**: Parquet codec implements `StatisticalCodec`, reporting column-level min/max/null count for all orderable types (int32, int64, float32, float64, string, timestamp)
- **New Public Types**: `FileStats`, `ColumnStats` on the public API surface

### Changed

- **Nil Metadata Coalescing**: `Write`, `StreamWrite`, `StreamWriteRecords`, and `Volume.Commit` now coalesce nil metadata to `Metadata{}` instead of returning an error

### Upgrade Notes

- Callers that previously passed `Metadata{}` solely to avoid nil errors can now pass `nil` safely
- Callers that relied on nil metadata returning an error should remove that expectation
- Per-file stats are opt-in: only codecs implementing `StatisticalCodec` produce them; manifests without stats remain valid

### References

- [CONTRACT_PARQUET.md](docs/contracts/CONTRACT_PARQUET.md) — Updated with per-file statistics spec
- [CONTRACT_WRITE_API.md](docs/contracts/CONTRACT_WRITE_API.md) — Updated with stats collection and nil coalescing semantics
- [CONTRACT_CORE.md](docs/contracts/CONTRACT_CORE.md) — Updated metadata rules

---

## [0.6.0] - 2026-02-07

### Added

- **Volume Persistence Paradigm**: `NewVolume`, `StageWriteAt`, `Commit`, `ReadAt`, `Latest`, `Snapshots`, `Snapshot` — sparse, resumable byte-range persistence as a coequal paradigm alongside Dataset
- **Volume Type Definitions**: `VolumeID`, `VolumeSnapshotID`, `BlockRef`, `VolumeManifest`, `VolumeSnapshot`, `VolumeOption`, `WithVolumeChecksum`
- **Volume Error Sentinels**: `ErrRangeMissing`, `ErrOverlappingBlocks`
- **Volume Example**: `examples/volume_sparse/` demonstrating stage → commit → read, ErrRangeMissing, incremental commits, resume pattern
- **Volume Test Suite**: Comprehensive contract-driven tests covering construction, staging, commit, read, validation, overlap detection, resume, cumulative manifests, and fault injection
- **Volume Contract**: `CONTRACT_VOLUME.md` — normative contract for Volume persistence model
- **Overflow-Safe Arithmetic**: Volume bounds checks use overflow-safe arithmetic to prevent silent wrapping on extreme int64 values

### Changed

- **Dataset Rename Pass (DD-1/DD-7)**: Public API types renamed for clarity now that Volume types coexist:
  - `SnapshotID` → `DatasetSnapshotID`
  - `Snapshot` → `DatasetSnapshot`
  - `Reader` → `DatasetReader`
  - `NewReader` → `NewDatasetReader`
  - `SegmentRef` → `ManifestRef`
  - `SegmentListOptions` → `ManifestListOptions`
  - `ListSegments` → `ListManifests`
  - `ErrOptionNotValidForReader` → `ErrOptionNotValidForDatasetReader`

### Breaking Changes

- **Public API renames**: All types and functions listed above are renamed without compatibility aliases. This is intentional — the renames disambiguate Dataset-specific types from the new Volume type definitions.

### Upgrade Notes

- **Search-and-replace migration**: The renames are mechanical. Apply these replacements across your codebase:
  - `lode.NewReader(` → `lode.NewDatasetReader(`
  - `lode.Reader` → `lode.DatasetReader`
  - `lode.SnapshotID` → `lode.DatasetSnapshotID`
  - `lode.Snapshot` → `lode.DatasetSnapshot`
  - `lode.SegmentRef` → `lode.ManifestRef`
  - `lode.SegmentListOptions` → `lode.ManifestListOptions`
  - `.ListSegments(` → `.ListManifests(`
  - `lode.ErrOptionNotValidForReader` → `lode.ErrOptionNotValidForDatasetReader`
- **No behavior changes intended**: All existing functionality is preserved under the new names.

### References

- [CONTRACT_VOLUME.md](docs/contracts/CONTRACT_VOLUME.md) — Volume persistence contract

---

## [0.5.0] - 2026-02-06

### Added

- **Parquet Codec**: `NewParquetCodec(schema, opts...)` for columnar storage with schema-explicit encoding
- **Parquet Example**: `examples/parquet/` demonstrating schema-typed columnar storage
- **Parquet Types**: `ParquetSchema`, `ParquetField`, and `ParquetType` constants for all primitive types
- **Parquet Compression**: `WithParquetCompression()` option for Snappy/Gzip internal compression
- **Parquet Error Sentinels**: `ErrSchemaViolation` and `ErrInvalidFormat` for precise error handling

### Breaking Changes

- **NewParquetCodec Signature**: Returns `(Codec, error)` instead of `Codec` to enable schema validation at construction time. All callsites must handle the error return.

### Upgrade Notes

- **Parquet codec is non-streaming**: Parquet requires a footer referencing all row groups, so `StreamWriteRecords` returns `ErrCodecNotStreamable`. Use `Dataset.Write` for Parquet encoding.
- **Compression layering**: When using Parquet codec, set Lode's compressor to `NewNoOpCompressor()`. Parquet has internal compression; double compression wastes CPU.
- **Schema validation**: Invalid schemas (bad types, empty names, duplicates) now return errors at construction time rather than encoding time.

### References

- [CONTRACT_PARQUET.md](docs/contracts/CONTRACT_PARQUET.md) — Parquet codec contract
- [CONTRACT_ERRORS.md](docs/contracts/CONTRACT_ERRORS.md) — Updated with Parquet error semantics

---

## [0.4.1] - 2026-02-05

### Changed

- **S3 Backend Compatibility Matrix**: Added documentation for conditional multipart completion support across S3-compatible backends; verified on AWS S3, untested on MinIO/LocalStack/R2

### Fixed

- **S3 Multipart Atomic No-Overwrite**: Large uploads (>5GB) now use conditional completion (`If-None-Match` on `CompleteMultipartUpload`) for atomic no-overwrite guarantee, closing the TOCTOU window that existed in v0.2.0

### Known Limitations

- **S3-compatible backend caveat**: Atomic no-overwrite for large uploads (>5GB) is verified on AWS S3; assumed but untested for other S3-compatible backends (MinIO, LocalStack, R2). If your backend does not support `If-None-Match` on `CompleteMultipartUpload`, large uploads may fail or lose atomicity. See `lode/s3` package docs for the backend support matrix.

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.4.0] - 2026-02-05

### Added

- **Zstd Compressor**: `NewZstdCompressor()` for higher compression ratio and faster decompression than gzip
- **Example Convention Contract**: `CONTRACT_EXAMPLES.md` defines normative callsite conventions
- **Agent Convention Rules**: `AGENTS.md` now includes API/example convention rules with stop triggers

### Changed

- **StreamWriteRecords Parameter Order**: Reordered from `(ctx, metadata, records)` to `(ctx, records, metadata)` for ergonomic consistency with iterator-first patterns
- **Example Variable Naming**: Normalized `snapshot` variable naming across all examples

### Breaking Changes

- **StreamWriteRecords Signature**: Parameter order changed from `StreamWriteRecords(ctx, metadata, records)` to `StreamWriteRecords(ctx, records, metadata)`. Update callsites accordingly.

### Known Limitations

- **G3-4 (Residual Risk)**: Context cancellation cleanup behavior remains nondeterministic due to
  timing characteristics of storage adapters. Deterministic abort paths are tested; context
  cancellation semantics documented as best-effort. See `docs/contracts/CONTRACT_TEST_MATRIX.md` for details.

### Upgrade Notes

- **StreamWriteRecords migration**: Update all `StreamWriteRecords` callsites to use the new parameter order: `ds.StreamWriteRecords(ctx, iter, metadata)` instead of `ds.StreamWriteRecords(ctx, metadata, iter)`

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
- [docs/contracts/CONTRACT_EXAMPLES.md](docs/contracts/CONTRACT_EXAMPLES.md) — Example convention contract
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.3.0] - 2026-02-04

### Added

- **README Quick Start**: 5-minute guide with blob, records, and streaming examples
- **Write API Decision Table**: Clear guidance for choosing Write/StreamWrite/StreamWriteRecords
- **Guarantees Table**: Explicit commitments vs non-goals in README
- **Gotchas Section**: Common pitfalls documented in README
- **Examples Index**: One-line purpose + run commands for all examples
- **Option Applicability Matrix**: Dataset vs Reader option compatibility in PUBLIC_API.md
- **Sentinel Error Table**: Complete error reference with `errors.Is()` guidance
- **Streaming Constraints Table**: Clear codec/partitioning rules for streaming APIs
- **Contract Cross-Links**: All PUBLIC_API.md claims now traceable to contracts
- **Streaming Failure Tests**: G3-1..G3-3 hardening tests for commit/abort/error semantics

### Changed

- **CHANGELOG Format**: Adopted Keep a Changelog structured format
- **PUBLIC_API.md**: Reorganized with Safety Guarantees section and error handling guidelines

### Fixed

- N/A

### Breaking Changes

- None

### Known Limitations

- **G3-4 (Residual Risk)**: Context cancellation cleanup behavior is nondeterministic due to
  timing characteristics of storage adapters. Deterministic abort paths are tested; context
  cancellation semantics documented as best-effort. See `docs/contracts/CONTRACT_TEST_MATRIX.md` for details.

### Next

Post-v0.3.0 improvements planned:

- **Bootstrap pattern documentation**: "Ensure storage exists before constructing dataset/reader"
  as an explicit callsite pattern in examples and docs.
- **Optional helper utilities**: Bootstrap helpers (e.g., `EnsureStorageExists`) may be added
  outside core write/read semantics, in examples, docs, or a separate helpers package.
- **Markdown snippet CI**: Lint extracted code snippets from README and PUBLIC_API.md.

### Upgrade Notes

- No API changes; documentation and test coverage improvements only
- Safe to upgrade from v0.2.0

### References

- [PUBLIC_API.md](PUBLIC_API.md) — Enhanced with option matrix and error guidance
- [docs/contracts/CONTRACT_TEST_MATRIX.md](docs/contracts/CONTRACT_TEST_MATRIX.md) — Test coverage traceability
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.2.0] - 2026-02-03

### Added

- **S3 Adapter**: Now part of public API under `lode/s3`
- **S3 Documentation**: Examples for AWS S3, MinIO, LocalStack, and Cloudflare R2
- **CHANGELOG.md**: Added for release tracking

### Changed

- **License**: Project is now licensed under Apache 2.0
- **README**: Now lists supported backends and license

### Fixed

- N/A

### Breaking Changes

- None

### Known Limitations

- Single-writer semantics required (no concurrent writer conflict resolution)
- Large uploads (>5GB on S3) have TOCTOU window for no-overwrite guarantee
- Cleanup of partial objects is best-effort, not guaranteed

### Upgrade Notes

- S3 adapter moved from `internal/` to `lode/s3`; update import paths if using experimental adapter

### References

- [PUBLIC_API.md](PUBLIC_API.md) — S3 adapter documentation
- [examples/s3_experimental/](examples/s3_experimental/) — S3 usage examples

---

## [0.1.0] - 2026-02-03

### Added

- **Public API**: Datasets and readers with immutable snapshots, manifests, and explicit metadata
- **Layout System**: Default, Hive, and Flat layouts with dataset enumeration and partition-pruning
- **Storage Adapters**: Filesystem (`NewFSFactory`) and in-memory (`NewMemoryFactory`)
- **S3 Adapter**: Experimental adapter under `internal/` (promoted to public in v0.2.0)
- **Codecs**: JSONL codec for structured records
- **Compression**: Gzip compressor and no-op default
- **Range Reads**: `ReadRange` and `ReaderAt` for partial object access
- **Examples**: Default layout, Hive layout, manifest-driven discovery, blob upload, S3 experimental

### Changed

- N/A (initial release)

### Fixed

- N/A (initial release)

### Breaking Changes

- None (initial release)

### Known Limitations

1. **Single-writer only**: Concurrent writes to the same dataset may corrupt history
2. **No query execution**: Lode structures data; query engines consume it
3. **No background compaction**: Callers control snapshot lifecycle
4. **No automatic cleanup**: Partial objects from failed writes may remain
5. **Manifest-driven only**: Data files without manifests are not discovered

### Upgrade Notes

**Runtime Requirements:**
- Go 1.25 or later

**Storage:**
- Filesystem paths must exist before use; Lode does not create directories
- S3 buckets must exist before use; Lode does not create buckets

### References

- [PUBLIC_API.md](PUBLIC_API.md) — Public API documentation
- [docs/contracts/](docs/contracts/) — Normative contract specifications
- [examples/](examples/) — Runnable usage examples

---

[Unreleased]: https://github.com/pithecene-io/lode/compare/v0.7.1...HEAD
[0.7.1]: https://github.com/pithecene-io/lode/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/pithecene-io/lode/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/pithecene-io/lode/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/pithecene-io/lode/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/pithecene-io/lode/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/pithecene-io/lode/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/pithecene-io/lode/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pithecene-io/lode/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pithecene-io/lode/releases/tag/v0.1.0
