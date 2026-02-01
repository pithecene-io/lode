package partition_test

import (
	"testing"

	"github.com/justapithecus/lode/internal/partition"
)

func TestHive_Name(t *testing.T) {
	p := partition.NewHive("date")
	if p.Name() != "hive" {
		t.Errorf("Name() = %q, want hive", p.Name())
	}
}

func TestHive_SingleKey(t *testing.T) {
	p := partition.NewHive("region")
	record := map[string]any{"region": "us-east", "id": 1}

	key, err := p.PartitionKey(record)
	if err != nil {
		t.Fatalf("PartitionKey failed: %v", err)
	}
	if key != "region=us-east" {
		t.Errorf("key = %q, want region=us-east", key)
	}
}

func TestHive_MultipleKeys(t *testing.T) {
	p := partition.NewHive("year", "month")
	record := map[string]any{"year": "2024", "month": "01", "data": "value"}

	key, err := p.PartitionKey(record)
	if err != nil {
		t.Fatalf("PartitionKey failed: %v", err)
	}
	if key != "year=2024/month=01" {
		t.Errorf("key = %q, want year=2024/month=01", key)
	}
}

func TestHive_MissingKey_Error(t *testing.T) {
	p := partition.NewHive("region")
	record := map[string]any{"id": 1} // missing "region"

	_, err := p.PartitionKey(record)
	if err == nil {
		t.Error("expected error for missing partition key")
	}
}

func TestHive_WrongType_Error(t *testing.T) {
	p := partition.NewHive("region")
	record := "not a map"

	_, err := p.PartitionKey(record)
	if err == nil {
		t.Error("expected error for non-map record")
	}
}

func TestHive_SpecialCharacters_Escaped(t *testing.T) {
	p := partition.NewHive("path")
	record := map[string]any{"path": "a/b c"}

	key, err := p.PartitionKey(record)
	if err != nil {
		t.Fatalf("PartitionKey failed: %v", err)
	}
	// Should URL-encode special characters
	if key != "path=a%2Fb%20c" {
		t.Errorf("key = %q, expected URL-encoded value", key)
	}
}

func TestNoop_Name(t *testing.T) {
	p := partition.NewNoop()
	if p.Name() != "noop" {
		t.Errorf("Name() = %q, want noop", p.Name())
	}
}

func TestNoop_AlwaysEmpty(t *testing.T) {
	p := partition.NewNoop()

	// Any record type should work
	records := []any{
		map[string]any{"id": 1},
		"string",
		123,
		nil,
	}

	for _, record := range records {
		key, err := p.PartitionKey(record)
		if err != nil {
			t.Errorf("PartitionKey(%v) failed: %v", record, err)
		}
		if key != "" {
			t.Errorf("PartitionKey(%v) = %q, want empty", record, key)
		}
	}
}
