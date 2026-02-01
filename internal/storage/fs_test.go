package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

func TestFS_Put_Success(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	data := []byte("test data")
	err = store.Put(ctx, "test/file.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify file exists
	fullPath := filepath.Join(root, "test", "file.txt")
	content, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("content mismatch: got %q, want %q", content, data)
	}
}

func TestFS_Put_ErrPathExists(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	data := []byte("test data")
	err = store.Put(ctx, "test/file.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second put should fail with ErrPathExists
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("new data")))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected ErrPathExists, got %v", err)
	}
}

func TestFS_Get_Success(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	data := []byte("test data")
	_ = store.Put(ctx, "test/file.txt", bytes.NewReader(data))

	rc, err := store.Get(ctx, "test/file.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	content, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("content mismatch: got %q, want %q", content, data)
	}
}

func TestFS_Get_ErrNotFound(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	_, err = store.Get(ctx, "nonexistent.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestFS_Exists(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Non-existent path
	exists, err := store.Exists(ctx, "nonexistent.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected exists=false for nonexistent path")
	}

	// Create file
	_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("data")))

	// Existing path
	exists, err = store.Exists(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected exists=true for existing path")
	}
}

func TestFS_List(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Create some files
	_ = store.Put(ctx, "prefix/a.txt", bytes.NewReader([]byte("a")))
	_ = store.Put(ctx, "prefix/b.txt", bytes.NewReader([]byte("b")))
	_ = store.Put(ctx, "prefix/sub/c.txt", bytes.NewReader([]byte("c")))
	_ = store.Put(ctx, "other/d.txt", bytes.NewReader([]byte("d")))

	paths, err := store.List(ctx, "prefix")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(paths) != 3 {
		t.Errorf("expected 3 paths, got %d: %v", len(paths), paths)
	}

	// Check all prefix paths are present
	pathSet := make(map[string]bool)
	for _, p := range paths {
		pathSet[p] = true
	}

	expected := []string{"prefix/a.txt", "prefix/b.txt", "prefix/sub/c.txt"}
	for _, exp := range expected {
		if !pathSet[exp] {
			t.Errorf("missing expected path: %s", exp)
		}
	}
}

func TestFS_Delete_Idempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Delete non-existent (should be idempotent)
	err = store.Delete(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("Delete non-existent should be idempotent, got: %v", err)
	}

	// Create and delete
	_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("data")))
	err = store.Delete(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	exists, _ := store.Exists(ctx, "test.txt")
	if exists {
		t.Error("file should be deleted")
	}
}

// -----------------------------------------------------------------------------
// Path traversal security tests
// -----------------------------------------------------------------------------

func TestFS_Put_PathTraversal_Rejected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	testCases := []struct {
		name string
		path string
	}{
		{"parent dir", "../escape.txt"},
		{"nested parent", "foo/../../escape.txt"},
		{"absolute path", "/etc/passwd"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := store.Put(ctx, tc.path, bytes.NewReader([]byte("data")))
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Put(%q) = %v, want ErrInvalidPath", tc.path, err)
			}
		})
	}
}

func TestFS_Get_PathTraversal_Rejected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	testCases := []string{"../escape.txt", "foo/../../escape.txt", "/etc/passwd"}

	for _, path := range testCases {
		t.Run(path, func(t *testing.T) {
			_, err := store.Get(ctx, path)
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Get(%q) = %v, want ErrInvalidPath", path, err)
			}
		})
	}
}

func TestFS_List_PathTraversal_Rejected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	testCases := []string{"../", "foo/../..", "/etc"}

	for _, prefix := range testCases {
		t.Run(prefix, func(t *testing.T) {
			_, err := store.List(ctx, prefix)
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("List(%q) = %v, want ErrInvalidPath", prefix, err)
			}
		})
	}
}

func TestFS_EmptyPath_Rejected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Empty path and "." should be rejected for file operations
	testCases := []string{"", "."}

	for _, path := range testCases {
		t.Run("Put_"+path, func(t *testing.T) {
			err := store.Put(ctx, path, bytes.NewReader([]byte("data")))
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Put(%q) = %v, want ErrInvalidPath", path, err)
			}
		})

		t.Run("Get_"+path, func(t *testing.T) {
			_, err := store.Get(ctx, path)
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Get(%q) = %v, want ErrInvalidPath", path, err)
			}
		})

		t.Run("Exists_"+path, func(t *testing.T) {
			_, err := store.Exists(ctx, path)
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Exists(%q) = %v, want ErrInvalidPath", path, err)
			}
		})

		t.Run("Delete_"+path, func(t *testing.T) {
			err := store.Delete(ctx, path)
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Errorf("Delete(%q) = %v, want ErrInvalidPath", path, err)
			}
		})
	}
}

func TestFS_List_EmptyPrefix_Allowed(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, err := storage.NewFS(root)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Create a file
	_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("data")))

	// Empty prefix and "." should work for List (list all)
	for _, prefix := range []string{"", "."} {
		t.Run(prefix, func(t *testing.T) {
			paths, err := store.List(ctx, prefix)
			if err != nil {
				t.Errorf("List(%q) failed: %v", prefix, err)
			}
			if len(paths) != 1 {
				t.Errorf("List(%q) returned %d paths, want 1", prefix, len(paths))
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Range read tests (FS)
// -----------------------------------------------------------------------------

func TestFS_Stat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("hello world")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	size, err := store.Stat(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Stat returned size %d, want %d", size, len(data))
	}
}

func TestFS_Stat_NotFound(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)

	_, err := store.Stat(ctx, "missing.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestFS_ReadRange(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("0123456789abcdef")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	// Read middle portion
	result, err := store.ReadRange(ctx, "file.txt", 4, 6)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	expected := []byte("456789")
	if !bytes.Equal(result, expected) {
		t.Errorf("ReadRange returned %q, want %q", result, expected)
	}
}

func TestFS_ReadRange_Start(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("0123456789")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	result, err := store.ReadRange(ctx, "file.txt", 0, 5)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if !bytes.Equal(result, []byte("01234")) {
		t.Errorf("ReadRange returned %q, want %q", result, "01234")
	}
}

func TestFS_ReadRange_End(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("0123456789")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	// Read past end - should return available bytes
	result, err := store.ReadRange(ctx, "file.txt", 7, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if !bytes.Equal(result, []byte("789")) {
		t.Errorf("ReadRange returned %q, want %q", result, "789")
	}
}

func TestFS_ReadRange_NotFound(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)

	_, err := store.ReadRange(ctx, "missing.txt", 0, 10)
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestFS_ReaderAt(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("0123456789abcdef")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	ra, err := store.ReaderAt(ctx, "file.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}
	defer ra.Close()

	// Verify size
	if ra.Size() != int64(len(data)) {
		t.Errorf("Size() = %d, want %d", ra.Size(), len(data))
	}

	// Read at various offsets
	buf := make([]byte, 4)

	n, err := ra.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt(0) failed: %v", err)
	}
	if n != 4 || !bytes.Equal(buf, []byte("0123")) {
		t.Errorf("ReadAt(0) = %d, %q; want 4, %q", n, buf, "0123")
	}

	n, err = ra.ReadAt(buf, 8)
	if err != nil {
		t.Fatalf("ReadAt(8) failed: %v", err)
	}
	if n != 4 || !bytes.Equal(buf, []byte("89ab")) {
		t.Errorf("ReadAt(8) = %d, %q; want 4, %q", n, buf, "89ab")
	}
}

func TestFS_ReaderAt_RepeatedAccess(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)
	data := []byte("0123456789")
	_ = store.Put(ctx, "file.txt", bytes.NewReader(data))

	ra, err := store.ReaderAt(ctx, "file.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}
	defer ra.Close()

	buf := make([]byte, 2)

	// Read same offset multiple times - should work without re-reading full object
	for i := 0; i < 3; i++ {
		n, err := ra.ReadAt(buf, 4)
		if err != nil {
			t.Fatalf("ReadAt iteration %d failed: %v", i, err)
		}
		if n != 2 || !bytes.Equal(buf, []byte("45")) {
			t.Errorf("ReadAt iteration %d = %d, %q; want 2, %q", i, n, buf, "45")
		}
	}

	// Read at different offset
	n, err := ra.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt(0) failed: %v", err)
	}
	if n != 2 || !bytes.Equal(buf, []byte("01")) {
		t.Errorf("ReadAt(0) = %d, %q; want 2, %q", n, buf, "01")
	}
}

func TestFS_ReaderAt_NotFound(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	store, _ := storage.NewFS(root)

	_, err := store.ReaderAt(ctx, "missing.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
