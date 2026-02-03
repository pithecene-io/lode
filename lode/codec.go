package lode

import (
	"bufio"
	"io"

	jsoniter "github.com/json-iterator/go"
)

var jsonCodec = jsoniter.ConfigCompatibleWithStandardLibrary

const maxScanTokenSize = 10 * 1024 * 1024 // 10MB

// -----------------------------------------------------------------------------
// JSONL Codec
// -----------------------------------------------------------------------------

// jsonlCodec implements Codec using JSON Lines format.
type jsonlCodec struct{}

// NewJSONLCodec creates a JSONL (JSON Lines) codec.
//
// Each record is serialized as a single line of JSON.
// Records can be any JSON-serializable value.
func NewJSONLCodec() Codec {
	return &jsonlCodec{}
}

func (j *jsonlCodec) Name() string {
	return "jsonl"
}

func (j *jsonlCodec) Encode(w io.Writer, records []any) error {
	enc := jsonCodec.NewEncoder(w)
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func (j *jsonlCodec) Decode(r io.Reader) ([]any, error) {
	var records []any
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanTokenSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record any
		if err := jsonCodec.Unmarshal(line, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
