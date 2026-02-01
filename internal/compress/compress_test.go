package compress_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/justapithecus/lode/internal/compress"
)

func TestGzip_Name(t *testing.T) {
	c := compress.NewGzip()
	if c.Name() != "gzip" {
		t.Errorf("Name() = %q, want gzip", c.Name())
	}
}

func TestGzip_Extension(t *testing.T) {
	c := compress.NewGzip()
	if c.Extension() != ".gz" {
		t.Errorf("Extension() = %q, want .gz", c.Extension())
	}
}

func TestGzip_CompressDecompress(t *testing.T) {
	c := compress.NewGzip()
	data := []byte("test data for compression")

	// Compress
	var compressed bytes.Buffer
	w, err := c.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	_, _ = w.Write(data)
	_ = w.Close()

	// Decompress
	r, err := c.Decompress(&compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Errorf("decompressed data mismatch: got %q, want %q", decompressed, data)
	}
}

func TestNoop_Name(t *testing.T) {
	c := compress.NewNoop()
	if c.Name() != "noop" {
		t.Errorf("Name() = %q, want noop", c.Name())
	}
}

func TestNoop_Extension(t *testing.T) {
	c := compress.NewNoop()
	if c.Extension() != "" {
		t.Errorf("Extension() = %q, want empty", c.Extension())
	}
}

func TestNoop_Passthrough(t *testing.T) {
	c := compress.NewNoop()
	data := []byte("test data unchanged")

	// "Compress"
	var buf bytes.Buffer
	w, err := c.Compress(&buf)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	_, _ = w.Write(data)
	_ = w.Close()

	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("noop compress should not modify data")
	}

	// "Decompress"
	r, err := c.Decompress(&buf)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	decompressed, _ := io.ReadAll(r)
	if !bytes.Equal(decompressed, data) {
		t.Error("noop decompress should not modify data")
	}
}
