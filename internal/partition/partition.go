// Package partition provides partitioning implementations.
package partition

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/justapithecus/lode/lode"
)

// Hive implements lode.Partitioner using Hive-style k=v paths.
// It extracts partition keys from record fields.
type Hive struct {
	keys []string
}

// NewHive creates a Hive-style partitioner with the given partition keys.
// Keys are extracted from records in order.
func NewHive(keys ...string) *Hive {
	return &Hive{keys: keys}
}

// Name returns the partitioner identifier.
func (h *Hive) Name() string {
	return "hive"
}

// PartitionKey returns the partition path component for a record.
// The record must be a map with string keys.
func (h *Hive) PartitionKey(record any) (string, error) {
	m, ok := record.(map[string]any)
	if !ok {
		return "", fmt.Errorf("hive partitioner: record must be map[string]any, got %T", record)
	}

	var parts []string
	for _, key := range h.keys {
		val, exists := m[key]
		if !exists {
			return "", fmt.Errorf("hive partitioner: missing key %q", key)
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, escapeValue(val)))
	}

	return strings.Join(parts, "/"), nil
}

// escapeValue safely converts a value to a path-safe string.
func escapeValue(v any) string {
	var s string
	switch val := v.(type) {
	case string:
		s = val
	case time.Time:
		s = val.Format("2006-01-02")
	default:
		s = fmt.Sprintf("%v", val)
	}
	// URL-encode to ensure path safety
	return url.PathEscape(s)
}

// Ensure Hive implements lode.Partitioner
var _ lode.Partitioner = (*Hive)(nil)

// Noop implements lode.Partitioner with no partitioning.
// All records map to an empty partition key.
// This is the explicit "noop" partitioner per CONTRACT_LAYOUT.md.
type Noop struct{}

// NewNoop creates a noop partitioner.
func NewNoop() *Noop {
	return &Noop{}
}

// Name returns the partitioner identifier.
func (n *Noop) Name() string {
	return "noop"
}

// PartitionKey returns an empty string (no partitioning).
func (n *Noop) PartitionKey(_ any) (string, error) {
	return "", nil
}

// IsNoop returns true, indicating this partitioner produces no partitions.
// This implements the dataset.NoopPartitioner marker interface.
func (n *Noop) IsNoop() bool {
	return true
}

// Ensure Noop implements lode.Partitioner
var _ lode.Partitioner = (*Noop)(nil)
