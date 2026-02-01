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
