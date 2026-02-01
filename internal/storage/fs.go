// Package storage provides storage adapter implementations.
package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/justapithecus/lode/lode"
)

// ErrInvalidPath indicates a path that would escape the storage root.
var ErrInvalidPath = errors.New("invalid path: escapes storage root")

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
// Returns ErrInvalidPath if the path would escape the storage root or is empty.
func (f *FS) Put(_ context.Context, path string, r io.Reader) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}

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
// Returns ErrInvalidPath if the path would escape the storage root or is empty.
func (f *FS) Get(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}
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
// Returns ErrInvalidPath if the path would escape the storage root or is empty.
func (f *FS) Exists(_ context.Context, path string) (bool, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(fullPath)
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
// Empty prefix lists all files. Returns ErrInvalidPath if the prefix would escape the root.
func (f *FS) List(_ context.Context, prefix string) ([]string, error) {
	searchPath, err := f.safePathForPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var paths []string

	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
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
// Returns ErrInvalidPath if the path would escape the storage root or is empty.
func (f *FS) Delete(_ context.Context, path string) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}
	err = os.Remove(fullPath)
	if err != nil && os.IsNotExist(err) {
		return nil // idempotent
	}
	return err
}

// Root returns the root directory of this store.
func (f *FS) Root() string {
	return f.root
}

// safePathForFile validates and resolves a file path, ensuring it stays within the root.
// Rejects empty path and "." since those would target the root directory itself.
// Returns ErrInvalidPath if the path is invalid or would escape the root.
//
// Note: This does not prevent symlink escapes. A symlink inside the root pointing
// outside can still be accessed. Symlink hardening is out of scope for this adapter.
func (f *FS) safePathForFile(path string) (string, error) {
	// Normalize the path: clean it and ensure no leading slash
	cleaned := filepath.Clean(path)

	// Reject empty path or "." (would target root directory)
	if cleaned == "." || path == "" {
		return "", ErrInvalidPath
	}

	// Reject absolute paths
	if filepath.IsAbs(cleaned) {
		return "", ErrInvalidPath
	}

	// Reject paths that start with ..
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	// Join with root
	fullPath := filepath.Join(f.root, cleaned)

	// Verify the resolved path is still under root
	absRoot, err := filepath.Abs(f.root)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}

	// Path must be strictly under root (not equal to root)
	if !strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	return fullPath, nil
}

// safePathForPrefix validates and resolves a prefix path for listing.
// Allows empty path (list all) but rejects traversal attempts.
// Returns ErrInvalidPath if the path would escape the root.
func (f *FS) safePathForPrefix(path string) (string, error) {
	// Empty prefix is valid for listing all files
	if path == "" {
		return f.root, nil
	}

	// Normalize the path
	cleaned := filepath.Clean(path)

	// "." is equivalent to empty prefix (list all)
	if cleaned == "." {
		return f.root, nil
	}

	// Reject absolute paths
	if filepath.IsAbs(cleaned) {
		return "", ErrInvalidPath
	}

	// Reject paths that start with ..
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	return filepath.Join(f.root, cleaned), nil
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
	mu   sync.RWMutex
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
// Returns ErrInvalidPath if the path is empty or contains traversal sequences.
func (m *Memory) Put(_ context.Context, path string, r io.Reader) error {
	// Normalize and validate path
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return ErrInvalidPath
	}

	// Read data before acquiring lock to minimize lock duration
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[normalized]; exists {
		return lode.ErrPathExists
	}

	m.data[normalized] = data
	return nil
}

// Get retrieves data from the given path.
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath if the path is empty or contains traversal sequences.
func (m *Memory) Get(_ context.Context, path string) (io.ReadCloser, error) {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	data, exists := m.data[normalized]
	m.mu.RUnlock()

	if !exists {
		return nil, lode.ErrNotFound
	}

	// Return a copy to avoid races if caller reads while another goroutine writes
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return io.NopCloser(strings.NewReader(string(dataCopy))), nil
}

// Exists checks whether a path exists.
// Returns ErrInvalidPath if the path is empty or contains traversal sequences.
func (m *Memory) Exists(_ context.Context, path string) (bool, error) {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return false, ErrInvalidPath
	}

	m.mu.RLock()
	_, exists := m.data[normalized]
	m.mu.RUnlock()

	return exists, nil
}

// List returns all paths under the given prefix.
// Empty prefix lists all files. Returns ErrInvalidPath if the prefix would escape.
func (m *Memory) List(_ context.Context, prefix string) ([]string, error) {
	normalized, valid := normalizePathForPrefix(prefix)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var paths []string
	for path := range m.data {
		if strings.HasPrefix(path, normalized) {
			paths = append(paths, path)
		}
	}

	return paths, nil
}

// Delete removes the path if it exists.
// Safe to call on a missing path (idempotent).
// Returns ErrInvalidPath if the path is empty or contains traversal sequences.
func (m *Memory) Delete(_ context.Context, path string) error {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return ErrInvalidPath
	}

	m.mu.Lock()
	delete(m.data, normalized)
	m.mu.Unlock()

	return nil
}

// normalizePathForFile ensures consistent path formatting for file operations.
// Rejects empty path and "." since those are not valid file paths.
// Returns the normalized path and whether it's valid.
func normalizePathForFile(path string) (string, bool) {
	// Empty path is invalid for file operations
	if path == "" {
		return "", false
	}

	// Clean the path to resolve . and .. components
	cleaned := filepath.Clean(path)

	// Convert to forward slashes for consistency
	cleaned = filepath.ToSlash(cleaned)

	// Remove leading slash for consistency
	cleaned = strings.TrimPrefix(cleaned, "/")

	// Reject paths that escape via .. or target root via .
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned == "." {
		return "", false
	}

	return cleaned, true
}

// normalizePathForPrefix ensures consistent path formatting for prefix listing.
// Allows empty path and "." for listing all files.
// Returns the normalized path and whether it's valid.
func normalizePathForPrefix(path string) (string, bool) {
	// Empty path is valid for listing all
	if path == "" {
		return "", true
	}

	// Clean the path to resolve . and .. components
	cleaned := filepath.Clean(path)

	// Convert to forward slashes for consistency
	cleaned = filepath.ToSlash(cleaned)

	// Remove leading slash for consistency
	cleaned = strings.TrimPrefix(cleaned, "/")

	// "." is equivalent to empty prefix (list all)
	if cleaned == "." {
		return "", true
	}

	// Reject paths that escape via ..
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", false
	}

	return cleaned, true
}

// Ensure Memory implements lode.Store
var _ lode.Store = (*Memory)(nil)
