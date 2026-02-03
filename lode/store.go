package lode

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrInvalidPath indicates a path that would escape the storage root.
var ErrInvalidPath = errors.New("invalid path: escapes storage root")

// -----------------------------------------------------------------------------
// Filesystem Store
// -----------------------------------------------------------------------------

// fsStore implements Store using the local filesystem.
type fsStore struct {
	root string
}

// NewFS creates a filesystem-backed Store rooted at the given directory.
// The directory must exist.
//
// Consistency: Immediate read-after-write on local filesystems.
func NewFS(root string) (Store, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, os.ErrNotExist
	}
	return &fsStore{root: root}, nil
}

func (f *fsStore) Put(_ context.Context, path string, r io.Reader) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}

	if _, err := os.Stat(fullPath); err == nil {
		return ErrPathExists
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return ErrPathExists
		}
		return err
	}
	defer func() { _ = file.Close() }()

	_, err = io.Copy(file, r)
	return err
}

func (f *fsStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return file, nil
}

func (f *fsStore) Exists(_ context.Context, path string) (bool, error) {
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

func (f *fsStore) List(_ context.Context, prefix string) ([]string, error) {
	searchPath, err := f.safePathForPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var paths []string

	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(f.root, path)
			if err != nil {
				return err
			}
			paths = append(paths, filepath.ToSlash(relPath))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func (f *fsStore) Delete(_ context.Context, path string) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}
	err = os.Remove(fullPath)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (f *fsStore) safePathForFile(path string) (string, error) {
	cleaned := filepath.Clean(path)
	if cleaned == "." || path == "" {
		return "", ErrInvalidPath
	}
	if filepath.IsAbs(cleaned) {
		return "", ErrInvalidPath
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	fullPath := filepath.Join(f.root, cleaned)

	absRoot, err := filepath.Abs(f.root)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	return fullPath, nil
}

func (f *fsStore) safePathForPrefix(path string) (string, error) {
	if path == "" {
		return f.root, nil
	}

	cleaned := filepath.Clean(path)
	if cleaned == "." {
		return f.root, nil
	}
	if filepath.IsAbs(cleaned) {
		return "", ErrInvalidPath
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}

	return filepath.Join(f.root, cleaned), nil
}

// -----------------------------------------------------------------------------
// Memory Store
// -----------------------------------------------------------------------------

// memoryStore implements Store using an in-memory map.
type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemory creates an in-memory Store.
//
// Consistency: Immediate.
// Memory is safe for concurrent use.
func NewMemory() Store {
	return &memoryStore{
		data: make(map[string][]byte),
	}
}

func (m *memoryStore) Put(_ context.Context, path string, r io.Reader) error {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return ErrInvalidPath
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[normalized]; exists {
		return ErrPathExists
	}

	m.data[normalized] = data
	return nil
}

func (m *memoryStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	data, exists := m.data[normalized]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return io.NopCloser(strings.NewReader(string(dataCopy))), nil
}

func (m *memoryStore) Exists(_ context.Context, path string) (bool, error) {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return false, ErrInvalidPath
	}

	m.mu.RLock()
	_, exists := m.data[normalized]
	m.mu.RUnlock()

	return exists, nil
}

func (m *memoryStore) List(_ context.Context, prefix string) ([]string, error) {
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

func (m *memoryStore) Delete(_ context.Context, path string) error {
	normalized, valid := normalizePathForFile(path)
	if !valid {
		return ErrInvalidPath
	}

	m.mu.Lock()
	delete(m.data, normalized)
	m.mu.Unlock()

	return nil
}

func normalizePathForFile(path string) (string, bool) {
	if path == "" {
		return "", false
	}

	cleaned := filepath.Clean(path)
	cleaned = filepath.ToSlash(cleaned)
	cleaned = strings.TrimPrefix(cleaned, "/")

	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned == "." {
		return "", false
	}

	return cleaned, true
}

func normalizePathForPrefix(path string) (string, bool) {
	if path == "" {
		return "", true
	}

	cleaned := filepath.Clean(path)
	cleaned = filepath.ToSlash(cleaned)
	cleaned = strings.TrimPrefix(cleaned, "/")

	if cleaned == "." {
		return "", true
	}

	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", false
	}

	return cleaned, true
}
