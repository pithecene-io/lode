package read

import (
	"context"
	"io"
)

// ReadStorage defines read-only storage operations required by the read API.
//
// This interface is internal and separate from lode.Store per CONTRACT_READ_API.md.
// Adapters must support true range reads, not simulated full downloads.
//
// Consistency guarantees and mitigations must be documented by implementations.
type ReadStorage interface {
	// Stat returns metadata about an object.
	// Returns ErrNotFound if the object does not exist.
	Stat(ctx context.Context, key ObjectKey) (ObjectInfo, error)

	// Open returns a reader for the entire object.
	// The caller must close the reader when done.
	// Returns ErrNotFound if the object does not exist.
	Open(ctx context.Context, key ObjectKey) (io.ReadCloser, error)

	// ReadRange reads a byte range from an object.
	// Per CONTRACT_READ_API.md, this must be a true range read
	// (e.g., HTTP Range on S3), not a simulated full download.
	// Returns ErrNotFound if the object does not exist.
	ReadRange(ctx context.Context, key ObjectKey, offset int64, length int64) ([]byte, error)

	// ReaderAt returns a random-access reader for an object.
	// Supports repeated access without re-reading the full object.
	// The caller must close the reader when done.
	// Returns ErrNotFound if the object does not exist.
	ReaderAt(ctx context.Context, key ObjectKey) (ReaderAt, error)

	// List returns objects matching the given prefix.
	// Per CONTRACT_STORAGE.md, ordering is unspecified.
	List(ctx context.Context, prefix ObjectKey, opts ListOptions) (ListPage, error)
}

// ReaderAt provides random access to an object.
//
// Per CONTRACT_READ_API.md, implementations should consider:
// - Page size: 256KiB-1MiB
// - Cache size: 32-256 pages
// - Optional sequential prefetch
type ReaderAt interface {
	io.ReaderAt
	io.Closer

	// Size returns the total size of the object in bytes.
	Size() int64
}
