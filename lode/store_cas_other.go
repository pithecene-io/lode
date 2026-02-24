//go:build !unix

package lode

import (
	"context"
	"errors"
)

// CompareAndSwap is not supported on non-Unix platforms.
// The filesystem CAS implementation requires advisory file locking (flock),
// which is only available on Unix-like operating systems.
func (f *fsStore) CompareAndSwap(_ context.Context, _, _, _ string) error {
	return errors.New("lode: filesystem CompareAndSwap is not supported on this platform (requires Unix flock)")
}
