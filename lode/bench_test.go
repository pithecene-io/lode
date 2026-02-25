package lode

import (
	"bytes"
	"strings"
	"testing"
)

// -----------------------------------------------------------------------------
// BenchmarkDataset_Read measures read throughput for in-memory JSONL records.
// -----------------------------------------------------------------------------

func BenchmarkDataset_Read(b *testing.B) {
	ds, err := NewDataset("bench-read", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		b.Fatal(err)
	}

	records := R(
		D{"id": "1", "value": "hello"},
		D{"id": "2", "value": "world"},
		D{"id": "3", "value": "bench"},
	)
	snap, err := ds.Write(b.Context(), records, Metadata{})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ds.Read(b.Context(), snap.ID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkDataset_StreamWrite measures streaming write throughput with ~1KB payload.
// -----------------------------------------------------------------------------

func BenchmarkDataset_StreamWrite(b *testing.B) {
	payload := []byte(strings.Repeat("x", 1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ds, err := NewDataset("bench-stream", NewMemoryFactory())
		if err != nil {
			b.Fatal(err)
		}

		sw, err := ds.StreamWrite(b.Context(), Metadata{})
		if err != nil {
			b.Fatal(err)
		}

		if _, err := sw.Write(payload); err != nil {
			b.Fatal(err)
		}

		if _, err := sw.Commit(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkParquetCodec_Encode measures encoding 100 records through Parquet.
// -----------------------------------------------------------------------------

func BenchmarkParquetCodec_Encode(b *testing.B) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
			{Name: "score", Type: ParquetFloat64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		b.Fatal(err)
	}

	records := make([]any, 100)
	for i := range records {
		records[i] = D{"id": int64(i), "name": "user", "score": 99.5}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkParquetCodec_Decode measures decoding 100 Parquet records.
// -----------------------------------------------------------------------------

func BenchmarkParquetCodec_Decode(b *testing.B) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
			{Name: "score", Type: ParquetFloat64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		b.Fatal(err)
	}

	records := make([]any, 100)
	for i := range records {
		records[i] = D{"id": int64(i), "name": "user", "score": 99.5}
	}

	var encoded bytes.Buffer
	if err := codec.Encode(&encoded, records); err != nil {
		b.Fatal(err)
	}
	data := encoded.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Decode(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkVolume_ReadAt measures random-access reads across 10 committed blocks.
// -----------------------------------------------------------------------------

func BenchmarkVolume_ReadAt(b *testing.B) {
	const blockSize = 1024
	const blockCount = 10
	const totalLength = blockSize * blockCount

	vol, err := NewVolume("bench-vol", NewMemoryFactory(), totalLength)
	if err != nil {
		b.Fatal(err)
	}

	// Stage and commit 10 blocks
	blocks := make([]BlockRef, blockCount)
	for i := range blocks {
		data := bytes.Repeat([]byte{byte(i)}, blockSize)
		blk, err := vol.StageWriteAt(b.Context(), int64(i*blockSize), bytes.NewReader(data))
		if err != nil {
			b.Fatalf("StageWriteAt block %d: %v", i, err)
		}
		blocks[i] = blk
	}

	snap, err := vol.Commit(b.Context(), blocks, Metadata{})
	if err != nil {
		b.Fatalf("Commit: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read 512 bytes from the middle of block 5
		_, err := vol.ReadAt(b.Context(), snap.ID, 5*blockSize+256, 512)
		if err != nil {
			b.Fatal(err)
		}
	}
}
