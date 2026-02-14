package lode

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
)

// -----------------------------------------------------------------------------
// Fault-Injection Store Wrapper (test-only)
// -----------------------------------------------------------------------------
//
// faultStore wraps a Store and enables deterministic fault injection for testing
// streaming failure paths. It provides:
//   - Error injection on specific operations
//   - Call observation/recording
//   - Blocking/synchronization points
//
// This is NOT production code. It exists solely to verify contract guarantees
// under controlled failure conditions without relying on timing.

// faultStore wraps a Store with fault injection capabilities.
type faultStore struct {
	inner Store

	mu sync.Mutex

	// Error injection: set these to inject errors on specific operations
	putErr      error
	putErrMatch string // if set, only inject putErr on paths containing this substring
	getErr      error
	deleteErr   error
	existsErr   error
	listErr     error

	// Call observation: tracks which methods were called
	putCalls    []string
	getCalls    []string
	deleteCalls []string
	existsCalls []string
	listCalls   []string

	// Blocking: channels to synchronize test assertions with operations
	putBlock    chan struct{} // if non-nil, Put blocks until closed
	deleteBlock chan struct{} // if non-nil, Delete blocks until closed

	// Pre-call hooks: called after recording but before blocking
	beforePut func(path string)

	// Post-call hooks: called after operation completes (before returning)
	afterPut    func(path string, err error)
	afterDelete func(path string, err error)

	// CAS fault injection (CompareAndSwap)
	casErr      error
	casErrMatch string
	casCalls    []string
	casBlock    chan struct{}
	beforeCAS   func(path string)
}

// newFaultStore creates a fault-injection wrapper around the given store.
func newFaultStore(inner Store) *faultStore {
	return &faultStore{inner: inner}
}

// --- Fault injection setters ---

// SetPutError sets an error to be returned by Put calls.
// If match is non-empty, error is only returned for paths containing match.
func (f *faultStore) SetPutError(err error, match ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putErr = err
	if len(match) > 0 {
		f.putErrMatch = match[0]
	} else {
		f.putErrMatch = ""
	}
}

// SetDeleteError sets an error to be returned by all Delete calls.
func (f *faultStore) SetDeleteError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteErr = err
}

// SetPutBlock sets a channel that Put will block on before proceeding.
// Close the channel to unblock.
func (f *faultStore) SetPutBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putBlock = ch
}

// SetBeforePut sets a hook called after recording but before blocking.
// Use this for deterministic synchronization with blocking operations.
func (f *faultStore) SetBeforePut(hook func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforePut = hook
}

// SetAfterPut sets a hook called after each Put completes.
func (f *faultStore) SetAfterPut(hook func(path string, err error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterPut = hook
}

// SetAfterDelete sets a hook called after each Delete completes.
func (f *faultStore) SetAfterDelete(hook func(path string, err error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterDelete = hook
}

// SetCASError sets an error to be returned by CompareAndSwap calls.
// If match is non-empty, error is only returned for paths containing match.
func (f *faultStore) SetCASError(err error, match ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casErr = err
	if len(match) > 0 {
		f.casErrMatch = match[0]
	} else {
		f.casErrMatch = ""
	}
}

// SetCASBlock sets a channel that CompareAndSwap will block on before proceeding.
// Close the channel to unblock.
func (f *faultStore) SetCASBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casBlock = ch
}

// SetBeforeCAS sets a hook called after recording but before blocking.
func (f *faultStore) SetBeforeCAS(hook func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeCAS = hook
}

// --- Call observation ---

// PutCalls returns paths passed to Put.
func (f *faultStore) PutCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.putCalls...)
}

// DeleteCalls returns paths passed to Delete.
func (f *faultStore) DeleteCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleteCalls...)
}

// ListCalls returns prefixes passed to List.
func (f *faultStore) ListCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listCalls...)
}

// GetCalls returns paths passed to Get.
func (f *faultStore) GetCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.getCalls...)
}

// CASCalls returns paths passed to CompareAndSwap.
func (f *faultStore) CASCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.casCalls...)
}

// Reset clears all recorded calls.
func (f *faultStore) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls = nil
	f.getCalls = nil
	f.deleteCalls = nil
	f.existsCalls = nil
	f.listCalls = nil
	f.casCalls = nil
}

// --- Store interface implementation ---

func (f *faultStore) Put(ctx context.Context, path string, r io.Reader) error {
	f.mu.Lock()
	injectedErr := f.putErr
	errMatch := f.putErrMatch
	block := f.putBlock
	beforeHook := f.beforePut
	hook := f.afterPut
	f.putCalls = append(f.putCalls, path)
	f.mu.Unlock()

	// Call beforePut hook (for deterministic sync before blocking)
	if beforeHook != nil {
		beforeHook(path)
	}

	// Block if configured (useful for testing cancellation windows)
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Return injected error if set (with optional path matching)
	if injectedErr != nil && (errMatch == "" || strings.Contains(path, errMatch)) {
		if hook != nil {
			hook(path, injectedErr)
		}
		return injectedErr
	}

	// Delegate to inner store
	err := f.inner.Put(ctx, path, r)
	if hook != nil {
		hook(path, err)
	}
	return err
}

func (f *faultStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	f.mu.Lock()
	injectedErr := f.getErr
	f.getCalls = append(f.getCalls, path)
	f.mu.Unlock()

	if injectedErr != nil {
		return nil, injectedErr
	}
	return f.inner.Get(ctx, path)
}

func (f *faultStore) Exists(ctx context.Context, path string) (bool, error) {
	f.mu.Lock()
	injectedErr := f.existsErr
	f.existsCalls = append(f.existsCalls, path)
	f.mu.Unlock()

	if injectedErr != nil {
		return false, injectedErr
	}
	return f.inner.Exists(ctx, path)
}

func (f *faultStore) List(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	injectedErr := f.listErr
	f.listCalls = append(f.listCalls, prefix)
	f.mu.Unlock()

	if injectedErr != nil {
		return nil, injectedErr
	}
	return f.inner.List(ctx, prefix)
}

func (f *faultStore) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	injectedErr := f.deleteErr
	block := f.deleteBlock
	hook := f.afterDelete
	f.deleteCalls = append(f.deleteCalls, path)
	f.mu.Unlock()

	// Block if configured
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if injectedErr != nil {
		if hook != nil {
			hook(path, injectedErr)
		}
		return injectedErr
	}

	err := f.inner.Delete(ctx, path)
	if hook != nil {
		hook(path, err)
	}
	return err
}

func (f *faultStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return f.inner.ReadRange(ctx, path, offset, length)
}

func (f *faultStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	return f.inner.ReaderAt(ctx, path)
}

// CompareAndSwap forwards to the inner store's ConditionalWriter implementation
// with full fault injection support (error injection, blocking, hooks).
func (f *faultStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	f.mu.Lock()
	injectedErr := f.casErr
	errMatch := f.casErrMatch
	block := f.casBlock
	beforeHook := f.beforeCAS
	f.casCalls = append(f.casCalls, path)
	f.mu.Unlock()

	if beforeHook != nil {
		beforeHook(path)
	}

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if injectedErr != nil && (errMatch == "" || strings.Contains(path, errMatch)) {
		return injectedErr
	}

	return f.inner.(ConditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}

// --- Factory helper ---

// newFaultStoreFactory creates a StoreFactory that returns the given faultStore.
func newFaultStoreFactory(fs *faultStore) StoreFactory {
	return func() (Store, error) {
		return fs, nil
	}
}

// --- Sentinel errors for injection ---

var (
	errInjectedPut    = errors.New("injected: put error")
	errInjectedDelete = errors.New("injected: delete error")
)
