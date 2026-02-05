package lode

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

// -----------------------------------------------------------------------------
// Gzip Compressor
// -----------------------------------------------------------------------------

// gzipCompressor implements Compressor using gzip compression.
type gzipCompressor struct{}

// NewGzipCompressor creates a gzip compressor.
//
// Files are compressed using standard gzip format with .gz extension.
func NewGzipCompressor() Compressor {
	return &gzipCompressor{}
}

func (g *gzipCompressor) Name() string {
	return "gzip"
}

func (g *gzipCompressor) Extension() string {
	return ".gz"
}

func (g *gzipCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (g *gzipCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

// -----------------------------------------------------------------------------
// Zstd Compressor
// -----------------------------------------------------------------------------

// zstdCompressor implements Compressor using zstd compression.
type zstdCompressor struct{}

// NewZstdCompressor creates a zstd compressor.
//
// Files are compressed using Zstandard format with .zst extension.
// Zstd provides higher compression ratios and faster decompression than gzip.
func NewZstdCompressor() Compressor {
	return &zstdCompressor{}
}

func (z *zstdCompressor) Name() string {
	return "zstd"
}

func (z *zstdCompressor) Extension() string {
	return ".zst"
}

func (z *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w)
}

func (z *zstdCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return decoder.IOReadCloser(), nil
}

// -----------------------------------------------------------------------------
// NoOp Compressor
// -----------------------------------------------------------------------------

// noopCompressor implements Compressor with no compression.
type noopCompressor struct{}

// NewNoOpCompressor creates a noop compressor.
//
// Data passes through unchanged. This is the explicit "noop" compressor
// per CONTRACT_LAYOUT.md.
func NewNoOpCompressor() Compressor {
	return &noopCompressor{}
}

func (n *noopCompressor) Name() string {
	return "noop"
}

func (n *noopCompressor) Extension() string {
	return ""
}

func (n *noopCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &noopWriteCloser{w}, nil
}

func (n *noopCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

type noopWriteCloser struct {
	io.Writer
}

func (n *noopWriteCloser) Close() error {
	return nil
}
