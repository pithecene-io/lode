package codec_test

import (
	"bytes"
	"testing"

	"github.com/justapithecus/lode/internal/codec"
)

func TestJSONL_Name(t *testing.T) {
	c := codec.NewJSONL()
	if c.Name() != "jsonl" {
		t.Errorf("Name() = %q, want jsonl", c.Name())
	}
}

func TestJSONL_EncodeDecode(t *testing.T) {
	c := codec.NewJSONL()

	records := []any{
		map[string]any{"id": float64(1), "name": "Alice"},
		map[string]any{"id": float64(2), "name": "Bob"},
	}

	var buf bytes.Buffer
	err := c.Encode(&buf, records)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := c.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(decoded), len(records))
	}

	// Verify content
	rec0 := decoded[0].(map[string]any)
	if rec0["name"] != "Alice" {
		t.Errorf("decoded record 0 name = %v, want Alice", rec0["name"])
	}
}

func TestJSONL_EmptyRecords(t *testing.T) {
	c := codec.NewJSONL()

	var buf bytes.Buffer
	err := c.Encode(&buf, []any{})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := c.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("decoded %d records, want 0", len(decoded))
	}
}
