// Package storage provides storage adapter implementations.
package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/justapithecus/lode/lode"
)

// FS implements lode.Store using the local filesystem.
//
// Consistency: Immediate read-after-write on local filesystems.
// Pagination: Not applicable; List returns all matching paths.
type FS struct {
	root string
}

// NewFS creates a filesystem-backed Store rooted at the given directory.
// The directory must exist.
func NewFS(root string) (*FS, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, os.ErrNotExist
	}
	return &FS{root: root}, nil
}

// Put writes data to the given path.
// Returns ErrPathExists if the path already exists.
func (f *FS) Put(_ context.Context, path string, r io.Reader) error {
	fullPath := filepath.Join(f.root, path)

	// Check for existing file first
	if _, err := os.Stat(fullPath); err == nil {
		return lode.ErrPathExists
	}

	// Ensure parent directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	// Use O_EXCL to prevent race conditions
	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return lode.ErrPathExists
		}
		return err
	}
	defer func() { _ = file.Close() }()

	_, err = io.Copy(file, r)
	return err
}

// Get retrieves data from the given path.
// Returns ErrNotFound if the path does not exist.
func (f *FS) Get(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(f.root, path)
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, lode.ErrNotFound
		}
		return nil, err
	}
	return file, nil
}

// Exists checks whether a path exists.
func (f *FS) Exists(_ context.Context, path string) (bool, error) {
	fullPath := filepath.Join(f.root, path)
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// List returns all paths under the given prefix.
// Paths are returned relative to the store root.
func (f *FS) List(_ context.Context, prefix string) ([]string, error) {
	searchPath := filepath.Join(f.root, prefix)
	var paths []string

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // prefix doesn't exist, return empty list
			}
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(f.root, path)
			if err != nil {
				return err
			}
			// Normalize to forward slashes for consistency
			paths = append(paths, filepath.ToSlash(relPath))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

// Delete removes the path if it exists.
// Safe to call on a missing path (idempotent).
func (f *FS) Delete(_ context.Context, path string) error {
	fullPath := filepath.Join(f.root, path)
	err := os.Remove(fullPath)
	if err != nil && os.IsNotExist(err) {
		return nil // idempotent
	}
	return err
}

// Root returns the root directory of this store.
func (f *FS) Root() string {
	return f.root
}

// Ensure FS implements lode.Store
var _ lode.Store = (*FS)(nil)

// Memory implements lode.Store using an in-memory map.
//
// Consistency: Immediate.
// Pagination: Not applicable; List returns all matching paths.
//
// Memory is safe for concurrent use.
type Memory struct {
	data map[string][]byte
}

// NewMemory creates an in-memory Store.
func NewMemory() *Memory {
	return &Memory{
		data: make(map[string][]byte),
	}
}

// Put writes data to the given path.
// Returns ErrPathExists if the path already exists.
func (m *Memory) Put(_ context.Context, path string, r io.Reader) error {
	// Normalize path
	path = normalizePath(path)

	if _, exists := m.data[path]; exists {
		return lode.ErrPathExists
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.data[path] = data
	return nil
}

// Get retrieves data from the given path.
// Returns ErrNotFound if the path does not exist.
func (m *Memory) Get(_ context.Context, path string) (io.ReadCloser, error) {
	path = normalizePath(path)

	data, exists := m.data[path]
	if !exists {
		return nil, lode.ErrNotFound
	}

	return io.NopCloser(strings.NewReader(string(data))), nil
}

// Exists checks whether a path exists.
func (m *Memory) Exists(_ context.Context, path string) (bool, error) {
	path = normalizePath(path)
	_, exists := m.data[path]
	return exists, nil
}

// List returns all paths under the given prefix.
func (m *Memory) List(_ context.Context, prefix string) ([]string, error) {
	prefix = normalizePath(prefix)
	var paths []string

	for path := range m.data {
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}

	return paths, nil
}

// Delete removes the path if it exists.
// Safe to call on a missing path (idempotent).
func (m *Memory) Delete(_ context.Context, path string) error {
	path = normalizePath(path)
	delete(m.data, path)
	return nil
}

// normalizePath ensures consistent path formatting.
func normalizePath(path string) string {
	// Remove leading slash for consistency
	return strings.TrimPrefix(path, "/")
}

// Ensure Memory implements lode.Store
var _ lode.Store = (*Memory)(nil)
