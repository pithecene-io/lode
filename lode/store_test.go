package lode

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"testing"
)

// -----------------------------------------------------------------------------
// G1: Immutability tests - Put returns ErrPathExists on overwrite
// -----------------------------------------------------------------------------

func TestFSStore_Put_ErrPathExists(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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

	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_NegativeOffset(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", -1, 10)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_NegativeLength(t *testing.T) {
	ctx := context.Background()
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

	ctx := context.Background()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
	}
}

func TestMemoryStore_ReadRange_OffsetPlusLengthOverflow(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()

	_, err := store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
	}
}

func TestMemoryStore_ReaderAt_Basic(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
	store := NewMemory()

	_, err := store.ReaderAt(ctx, "nonexistent.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}
