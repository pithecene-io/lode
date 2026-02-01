package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

// TestMemory_MatchesFS verifies Memory adapter has identical semantics to FS.

func TestMemory_Put_Success(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	data := []byte("test data")
	err := store.Put(ctx, "test/file.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify via Get
	rc, err := store.Get(ctx, "test/file.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	content, _ := io.ReadAll(rc)
	if !bytes.Equal(content, data) {
		t.Errorf("content mismatch: got %q, want %q", content, data)
	}
}

func TestMemory_Put_ErrPathExists(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	data := []byte("test data")
	err := store.Put(ctx, "test/file.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second put should fail with ErrPathExists
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("new data")))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected ErrPathExists, got %v", err)
	}
}

func TestMemory_Get_ErrNotFound(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	_, err := store.Get(ctx, "nonexistent.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_Exists(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

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

func TestMemory_List(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

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

func TestMemory_Delete_Idempotent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Delete non-existent (should be idempotent)
	err := store.Delete(ctx, "nonexistent.txt")
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
