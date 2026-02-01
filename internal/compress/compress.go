// Package compress provides compression implementations.
package compress

import (
	"compress/gzip"
	"io"

	"github.com/justapithecus/lode/lode"
)

// Gzip implements lode.Compressor using gzip compression.
type Gzip struct{}

// NewGzip creates a gzip compressor.
func NewGzip() *Gzip {
	return &Gzip{}
}

// Name returns the compressor identifier.
func (g *Gzip) Name() string {
	return "gzip"
}

// Extension returns the file extension for gzip.
func (g *Gzip) Extension() string {
	return ".gz"
}

// Compress wraps a writer with gzip compression.
func (g *Gzip) Compress(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

// Decompress wraps a reader with gzip decompression.
func (g *Gzip) Decompress(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

// Ensure Gzip implements lode.Compressor
var _ lode.Compressor = (*Gzip)(nil)

// Noop implements lode.Compressor with no compression.
// This is the explicit "noop" compressor per CONTRACT_LAYOUT.md.
type Noop struct{}

// NewNoop creates a noop compressor.
func NewNoop() *Noop {
	return &Noop{}
}

// Name returns the compressor identifier.
func (n *Noop) Name() string {
	return "noop"
}

// Extension returns an empty extension (no compression).
func (n *Noop) Extension() string {
	return ""
}

// Compress returns a writer that passes through unchanged.
func (n *Noop) Compress(w io.Writer) (io.WriteCloser, error) {
	return &noopWriteCloser{w}, nil
}

// Decompress returns a reader that passes through unchanged.
func (n *Noop) Decompress(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

// noopWriteCloser wraps a writer to implement WriteCloser.
type noopWriteCloser struct {
	io.Writer
}

func (n *noopWriteCloser) Close() error {
	return nil
}

// Ensure Noop implements lode.Compressor
var _ lode.Compressor = (*Noop)(nil)
