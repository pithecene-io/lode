package lode

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// partitioner is the internal interface for partition key extraction.
// This is NOT part of the public API - partitioning is configured through Layout.
type partitioner interface {
	name() string
	partitionKey(record any) (string, error)
	isNoop() bool
}

// -----------------------------------------------------------------------------
// Hive Partitioner (internal)
// -----------------------------------------------------------------------------

// hivePartitioner extracts partition keys from record fields.
type hivePartitioner struct {
	keys []string
}

func newHivePartitioner(keys ...string) partitioner {
	return &hivePartitioner{keys: keys}
}

func (h *hivePartitioner) name() string {
	return "hive"
}

func (h *hivePartitioner) partitionKey(record any) (string, error) {
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

func (h *hivePartitioner) isNoop() bool {
	return false
}

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
	return url.PathEscape(s)
}

// -----------------------------------------------------------------------------
// NoOp Partitioner (internal)
// -----------------------------------------------------------------------------

// noopPartitioner returns empty partition keys for all records.
type noopPartitioner struct{}

func newNoopPartitioner() partitioner {
	return &noopPartitioner{}
}

func (n *noopPartitioner) name() string {
	return "noop"
}

func (n *noopPartitioner) partitionKey(_ any) (string, error) {
	return "", nil
}

func (n *noopPartitioner) isNoop() bool {
	return true
}
