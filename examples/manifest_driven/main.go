// Example: Manifest-Driven Discovery
//
// This example demonstrates that discovery is driven entirely by manifests:
//   - Manifests are the commit signal (presence = visibility)
//   - Data files without manifests are not discovered
//   - All metadata comes from manifests, not file inspection
//
// This aligns with CONTRACT_READ_API.md: "manifest presence = commit signal"
//
// Run with: go run ./examples/manifest_driven
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/justapithecus/lode/internal/read"
	"github.com/justapithecus/lode/internal/storage"
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
	tmpDir, err := os.MkdirTemp("", "lode-manifest-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem-backed store
	store, err := storage.NewFS(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	reader := read.NewReader(store)

	// -------------------------------------------------------------------------
	// Step 1: Write data files WITHOUT manifests (uncommitted)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 1: Write uncommitted data ===")

	// Write a data file directly (simulating incomplete write)
	dataPath := "datasets/uncommitted-ds/snapshots/snap-1/data/data.jsonl"
	dataContent := []byte(`{"id":1,"status":"orphan"}`)
	if err := store.Put(ctx, dataPath, bytes.NewReader(dataContent)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	fmt.Printf("Wrote data file: %s\n", dataPath)

	// Try to discover datasets - should find nothing
	datasets, err := reader.ListDatasets(ctx, read.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets discovered: %d (expected: 0 - no manifest yet)\n\n", len(datasets))

	// -------------------------------------------------------------------------
	// Step 2: Write a manifest (commit signal)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 2: Write manifest (commit) ===")

	// Create a proper manifest
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "committed-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{"source": "manifest-driven-example"},
		Files: []lode.FileRef{
			{
				Path:      "datasets/committed-ds/snapshots/snap-1/data/events.jsonl",
				SizeBytes: 100,
			},
		},
		RowCount:    5,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "noop",
	}

	manifestData, _ := json.MarshalIndent(manifest, "", "  ")
	manifestPath := "datasets/committed-ds/snapshots/snap-1/manifest.json"
	if err := store.Put(ctx, manifestPath, bytes.NewReader(manifestData)); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	fmt.Printf("Wrote manifest: %s\n", manifestPath)

	// Now discover datasets - should find the committed one
	datasets, err = reader.ListDatasets(ctx, read.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets discovered: %v (uncommitted-ds still invisible!)\n\n", datasets)

	// -------------------------------------------------------------------------
	// Step 3: Demonstrate manifest-driven metadata
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 3: Manifest-driven metadata ===")

	// List segments
	segments, err := reader.ListSegments(ctx, "committed-ds", "", read.SegmentListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}
	fmt.Printf("Segments in 'committed-ds': %v\n", segments)

	// Get manifest metadata
	loadedManifest, err := reader.GetManifest(ctx, "committed-ds", segments[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}

	fmt.Println("\nManifest provides all metadata:")
	fmt.Printf("  Schema: %s v%s\n", loadedManifest.SchemaName, loadedManifest.FormatVersion)
	fmt.Printf("  Dataset: %s\n", loadedManifest.DatasetID)
	fmt.Printf("  Snapshot: %s\n", loadedManifest.SnapshotID)
	fmt.Printf("  Row count: %d\n", loadedManifest.RowCount)
	fmt.Printf("  Codec: %s\n", loadedManifest.Codec)
	fmt.Printf("  Compressor: %s\n", loadedManifest.Compressor)
	fmt.Printf("  Partitioner: %s\n", loadedManifest.Partitioner)
	fmt.Printf("  Files: %d\n", len(loadedManifest.Files))
	for _, f := range loadedManifest.Files {
		fmt.Printf("    - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Printf("  Metadata: %v\n", loadedManifest.Metadata)

	// -------------------------------------------------------------------------
	// Step 4: Show that data file paths come from manifest
	// -------------------------------------------------------------------------
	fmt.Println("\n=== STEP 4: Data paths from manifest ===")

	fmt.Println("Data file paths are stored IN the manifest, not discovered from storage.")
	fmt.Println("This enables:")
	fmt.Println("  - Atomic commits (manifest = single commit point)")
	fmt.Println("  - No race conditions during writes")
	fmt.Println("  - Clean rollback (delete manifest = uncommit)")
	fmt.Println("  - Portable manifests (can be copied/moved)")

	// -------------------------------------------------------------------------
	// Cleanup summary
	// -------------------------------------------------------------------------
	fmt.Println("\n=== KEY INSIGHTS ===")
	fmt.Println("1. Manifest presence is the ONLY commit signal")
	fmt.Println("2. Data files without manifests are invisible to discovery")
	fmt.Println("3. All metadata (row count, codec, files) comes from manifest")
	fmt.Println("4. Discovery never inspects data file contents")
	fmt.Println("5. This enables safe, atomic snapshot semantics")

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Manifest-driven discovery demonstration complete!")

	return nil
}
