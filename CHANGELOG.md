# Changelog

All notable changes to Lode will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- N/A

### Changed

- N/A

### Fixed

- N/A

### Breaking Changes

- None

### Known Limitations

- N/A

### Upgrade Notes

- N/A

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
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

[Unreleased]: https://github.com/justapithecus/lode/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/justapithecus/lode/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/justapithecus/lode/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/justapithecus/lode/releases/tag/v0.1.0
