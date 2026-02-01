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
