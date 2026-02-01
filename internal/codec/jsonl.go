// Package codec provides record serialization implementations.
package codec

import (
	"bufio"
	"io"

	jsoniter "github.com/json-iterator/go"

	"github.com/justapithecus/lode/lode"
)

// json is a drop-in replacement for encoding/json with better performance.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// JSONL implements lode.Codec using JSON Lines format.
// Each record is serialized as a single line of JSON.
type JSONL struct{}

// NewJSONL creates a JSONL codec.
func NewJSONL() *JSONL {
	return &JSONL{}
}

// Name returns the codec identifier.
func (j *JSONL) Name() string {
	return "jsonl"
}

// Encode writes records as JSON Lines to the given writer.
func (j *JSONL) Encode(w io.Writer, records []any) error {
	enc := json.NewEncoder(w)
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads JSON Lines from the given reader.
func (j *JSONL) Decode(r io.Reader) ([]any, error) {
	var records []any
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record any
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// Ensure JSONL implements lode.Codec
var _ lode.Codec = (*JSONL)(nil)
