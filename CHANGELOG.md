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

- G3-4: Context cancellation cleanup behavior is nondeterministic; documented as residual risk

### Upgrade Notes

- N/A

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
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

[Unreleased]: https://github.com/justapithecus/lode/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/justapithecus/lode/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/justapithecus/lode/releases/tag/v0.1.0
