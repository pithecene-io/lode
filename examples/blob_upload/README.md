# Blob Upload — Large Binary Guidance

This example focuses on raw blob storage using the default bundle. For large
binary artifacts, prefer staged streaming writes rather than modeling chunks
as logical records.

## Recommended Pattern

- Use `StreamWrite` to write the binary payload as data objects.
- The snapshot becomes visible only after `Commit` writes the manifest.
- If a stream is aborted or fails, no snapshot is created.

## Range-Read Access

For efficient partial reads:
- Store a single object and rely on `ReadRange`/`ReaderAt` for offsets.
- If chunking is required, write multiple objects and record an explicit
  chunk index (object keys, byte ranges, order) in metadata.

## Cleanup Guidance

Staged objects may exist if a stream is aborted. Callers should:
- Use a predictable staging prefix for cleanup.
- Track staged object keys and delete them explicitly when abandoning a stream.

## Metadata Expectations

Record artifact metadata explicitly (size, media type, chunk index, etc.).
Checksum fields are recorded only when a checksum component is configured.
