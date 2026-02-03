// Example: Blob Upload (Raw Bytes)
//
// This example demonstrates the default bundle configuration for raw blob storage:
//   - No codec (raw bytes)
//   - No compression (noop compressor)
//   - No partitioning (noop partitioner)
//   - Default layout
//
// When no codec is configured:
//   - Write expects a single []byte element
//   - Read returns a single []byte element
//   - RowCount in manifest is always 1
//
// Run with: go run ./examples/blob_upload
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/justapithecus/lode/internal/testutil"
	"github.com/justapithecus/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-blob-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer testutil.RemoveAll(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// WRITE: Create dataset with default bundle and write raw blob
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Create dataset with default bundle (no codec = raw blob mode)
	// Default bundle per PUBLIC_API.md:
	//   - Layout: DefaultLayout
	//   - Partitioner: NoOp
	//   - Compressor: NoOp
	//   - Codec: none (raw blob storage)
	ds, err := lode.NewDataset(
		"artifacts",
		storeFactory,
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Prepare raw bytes (could be an image, binary file, etc.)
	blobData := []byte("Hello, Lode! This is raw binary data. 🎉\x00\x01\x02\x03")
	fmt.Printf("Original blob size: %d bytes\n", len(blobData))
	fmt.Printf("Original blob content: %q\n", string(blobData[:min(50, len(blobData))]))

	// Write raw blob with explicit metadata (metadata is always required)
	snapshot, err := ds.Write(ctx, []any{blobData}, lode.Metadata{
		"content_type": "application/octet-stream",
		"filename":     "greeting.bin",
	})
	if err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}

	fmt.Printf("\nCreated snapshot: %s\n", snapshot.ID)
	fmt.Printf("Manifest details:\n")
	fmt.Printf("  Row count: %d (always 1 for raw blobs)\n", snapshot.Manifest.RowCount)
	fmt.Printf("  Codec: %q (empty = no codec)\n", snapshot.Manifest.Codec)
	fmt.Printf("  Compressor: %s\n", snapshot.Manifest.Compressor)
	fmt.Printf("  Partitioner: %s\n", snapshot.Manifest.Partitioner)
	fmt.Printf("  Files:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("    - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Read raw blob back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	// Read data back from snapshot
	data, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read blob: %w", err)
	}

	// In raw blob mode, Read returns a single []byte element
	if len(data) != 1 {
		return fmt.Errorf("expected 1 element, got %d", len(data))
	}

	readBlob, ok := data[0].([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", data[0])
	}

	fmt.Printf("Read blob size: %d bytes\n", len(readBlob))
	fmt.Printf("Read blob content: %q\n", string(readBlob[:min(50, len(readBlob))]))

	// Verify data integrity
	if !bytes.Equal(blobData, readBlob) {
		return fmt.Errorf("data mismatch: blob was corrupted")
	}
	fmt.Println("\nData integrity verified: blobs match!")

	// -------------------------------------------------------------------------
	// VERIFY: Check metadata from manifest
	// -------------------------------------------------------------------------
	fmt.Println("\n=== VERIFY METADATA ===")

	latest, err := ds.Latest(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	fmt.Printf("Metadata from manifest:\n")
	for k, v := range latest.Manifest.Metadata {
		fmt.Printf("  %s: %v\n", k, v)
	}

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Raw blob upload and read complete!")
	fmt.Println("\nKey points demonstrated:")
	fmt.Println("  1. Default bundle uses no codec (raw blob storage)")
	fmt.Println("  2. Write expects single []byte element")
	fmt.Println("  3. Read returns single []byte element")
	fmt.Println("  4. Metadata is explicit and required")
	fmt.Println("  5. RowCount is always 1 for raw blobs")

	return nil
}
