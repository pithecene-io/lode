package lode

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"testing"

	"github.com/pithecene-io/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// G1: Immutability tests - Put returns ErrPathExists on overwrite
// -----------------------------------------------------------------------------

func TestFSStore_Put_ErrPathExists(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// First write should succeed
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second write to same path should return ErrPathExists
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("world")))
	if !errors.Is(err, ErrPathExists) {
		t.Errorf("expected ErrPathExists, got: %v", err)
	}
}

func TestMemoryStore_Put_ErrPathExists(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// First write should succeed
	err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second write to same path should return ErrPathExists
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("world")))
	if !errors.Is(err, ErrPathExists) {
		t.Errorf("expected ErrPathExists, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// G5: Range read tests
// -----------------------------------------------------------------------------

func TestFSStore_ReadRange_Basic(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("hello world")
	err = store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Read middle portion
	data, err := store.ReadRange(ctx, "test.txt", 6, 5)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("expected 'world', got %q", string(data))
	}
}

func TestFSStore_ReadRange_BeyondEOF(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("hello")
	err = store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Read beyond EOF - should return available bytes
	data, err := store.ReadRange(ctx, "test.txt", 3, 100)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "lo" {
		t.Errorf("expected 'lo', got %q", string(data))
	}
}

func TestFSStore_ReadRange_OffsetBeyondEOF(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("hello")
	err = store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Offset beyond EOF - should return empty slice (EOF behavior from ReadAt)
	data, err := store.ReadRange(ctx, "test.txt", 100, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty slice, got %d bytes", len(data))
	}
}

func TestFSStore_ReadRange_NotFound(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.ReadRange(ctx, "nonexistent.txt", 0, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestFSStore_ReadRange_NegativeOffset(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.ReadRange(ctx, "test.txt", -1, 10)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
	}
}

func TestFSStore_ReadRange_NegativeLength(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.ReadRange(ctx, "test.txt", 0, -1)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative length, got: %v", err)
	}
}

func TestFSStore_ReadRange_LengthOverflow(t *testing.T) {
	// This test validates 32-bit platform protection.
	// On 64-bit systems, math.MaxInt == math.MaxInt64, so the overflow check
	// cannot trigger. Skip on 64-bit.
	if math.MaxInt == math.MaxInt64 {
		t.Skip("length overflow check only applies to 32-bit platforms")
	}

	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Length exceeding maxInt (only relevant on 32-bit)
	_, err = store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
	}
}

func TestFSStore_ReadRange_OffsetPlusLengthOverflow(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// offset + length would overflow int64
	_, err = store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
	}
}

func TestFSStore_ReaderAt_Basic(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("hello world")
	err = store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	ra, err := store.ReaderAt(ctx, "test.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}
	// fsStore.ReaderAt returns *os.File which implements io.Closer
	if closer, ok := ra.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}

	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 5 || string(buf) != "world" {
		t.Errorf("expected 'world', got %q", string(buf[:n]))
	}
}

func TestFSStore_ReaderAt_NotFound(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.ReaderAt(ctx, "nonexistent.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// Memory store tests

func TestMemoryStore_ReadRange_Basic(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	content := []byte("hello world")
	err := store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	data, err := store.ReadRange(ctx, "test.txt", 6, 5)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("expected 'world', got %q", string(data))
	}
}

func TestMemoryStore_ReadRange_BeyondEOF(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	content := []byte("hello")
	err := store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	data, err := store.ReadRange(ctx, "test.txt", 3, 100)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "lo" {
		t.Errorf("expected 'lo', got %q", string(data))
	}
}

func TestMemoryStore_ReadRange_OffsetBeyondEOF(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	content := []byte("hello")
	err := store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	data, err := store.ReadRange(ctx, "test.txt", 100, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty slice, got %d bytes", len(data))
	}
}

func TestMemoryStore_ReadRange_NotFound(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_NegativeOffset(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", -1, 10)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_NegativeLength(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", 0, -1)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative length, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_LengthOverflow(t *testing.T) {
	// This test validates 32-bit platform protection.
	// On 64-bit systems, math.MaxInt == math.MaxInt64, so the overflow check
	// cannot trigger. Skip on 64-bit.
	if math.MaxInt == math.MaxInt64 {
		t.Skip("length overflow check only applies to 32-bit platforms")
	}

	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_OffsetPlusLengthOverflow(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
	}
}

func TestMemoryStore_ReaderAt_Basic(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	content := []byte("hello world")
	err := store.Put(ctx, "test.txt", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	ra, err := store.ReaderAt(ctx, "test.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}
	// Close if the ReaderAt implements io.Closer (consistency with fsStore pattern)
	if closer, ok := ra.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}

	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 5 || string(buf) != "world" {
		t.Errorf("expected 'world', got %q", string(buf[:n]))
	}
}

func TestMemoryStore_ReaderAt_NotFound(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.ReaderAt(ctx, "nonexistent.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// List empty prefix tests (empty dataset/storage semantics)
// -----------------------------------------------------------------------------

func TestFSStore_List_NonExistentPrefix_ReturnsEmpty(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// List a prefix that doesn't exist - should return empty list, not error
	paths, err := store.List(ctx, "nonexistent/prefix")
	if err != nil {
		t.Fatalf("expected no error for non-existent prefix, got: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected empty list, got: %v", paths)
	}
}

func TestFSStore_List_EmptyPrefix_ReturnsEmpty(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// List empty prefix on empty storage - should return empty list
	paths, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("expected no error for empty prefix, got: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected empty list, got: %v", paths)
	}
}

func TestMemoryStore_List_NonExistentPrefix_ReturnsEmpty(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	// List a prefix that doesn't exist - should return empty list, not error
	paths, err := store.List(ctx, "nonexistent/prefix")
	if err != nil {
		t.Fatalf("expected no error for non-existent prefix, got: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected empty list, got: %v", paths)
	}
}

// -----------------------------------------------------------------------------
// CompareAndSwap tests — FS store
// -----------------------------------------------------------------------------

func TestFSStore_CompareAndSwap_CreateWhenEmpty(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	cw := store.(ConditionalWriter)

	// Empty expected on new path should create.
	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("CompareAndSwap create failed: %v", err)
	}

	// Verify content was written.
	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after CAS create failed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-1" {
		t.Errorf("expected 'snap-1', got %q", string(data))
	}
}

func TestFSStore_CompareAndSwap_UpdateMatch(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	cw := store.(ConditionalWriter)

	// Create initial content.
	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("initial create failed: %v", err)
	}

	// Update with matching expected.
	if err := cw.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2"); err != nil {
		t.Fatalf("CompareAndSwap update failed: %v", err)
	}

	// Verify content was updated.
	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after CAS update failed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-2" {
		t.Errorf("expected 'snap-2', got %q", string(data))
	}
}

func TestFSStore_CompareAndSwap_ConflictMismatch(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	cw := store.(ConditionalWriter)

	// Create initial content.
	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("initial create failed: %v", err)
	}

	// Attempt update with wrong expected.
	err = cw.CompareAndSwap(ctx, "ptr/latest", "stale", "snap-2")
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Errorf("expected ErrSnapshotConflict, got: %v", err)
	}

	// Verify content unchanged.
	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after conflict: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-1" {
		t.Errorf("expected 'snap-1' (unchanged), got %q", string(data))
	}
}

func TestFSStore_CompareAndSwap_ConflictNotExist(t *testing.T) {
	ctx := t.Context()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	cw := store.(ConditionalWriter)

	// Non-empty expected on missing path should return conflict.
	err = cw.CompareAndSwap(ctx, "ptr/latest", "snap-0", "snap-1")
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Errorf("expected ErrSnapshotConflict, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// CompareAndSwap tests — Memory store
// -----------------------------------------------------------------------------

func TestMemoryStore_CompareAndSwap_CreateWhenEmpty(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()
	cw := store.(ConditionalWriter)

	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("CompareAndSwap create failed: %v", err)
	}

	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after CAS create failed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-1" {
		t.Errorf("expected 'snap-1', got %q", string(data))
	}
}

func TestMemoryStore_CompareAndSwap_UpdateMatch(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()
	cw := store.(ConditionalWriter)

	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("initial create failed: %v", err)
	}

	if err := cw.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2"); err != nil {
		t.Fatalf("CompareAndSwap update failed: %v", err)
	}

	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after CAS update failed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-2" {
		t.Errorf("expected 'snap-2', got %q", string(data))
	}
}

func TestMemoryStore_CompareAndSwap_ConflictMismatch(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()
	cw := store.(ConditionalWriter)

	if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatalf("initial create failed: %v", err)
	}

	err := cw.CompareAndSwap(ctx, "ptr/latest", "stale", "snap-2")
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Errorf("expected ErrSnapshotConflict, got: %v", err)
	}

	rc, err := store.Get(ctx, "ptr/latest")
	if err != nil {
		t.Fatalf("Get after conflict: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "snap-1" {
		t.Errorf("expected 'snap-1' (unchanged), got %q", string(data))
	}
}

func TestMemoryStore_CompareAndSwap_ConflictNotExist(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()
	cw := store.(ConditionalWriter)

	err := cw.CompareAndSwap(ctx, "ptr/latest", "snap-0", "snap-1")
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Errorf("expected ErrSnapshotConflict, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Compile-time interface assertions
// -----------------------------------------------------------------------------

var (
	_ ConditionalWriter = (*fsStore)(nil)
	_ ConditionalWriter = (*memoryStore)(nil)
)
