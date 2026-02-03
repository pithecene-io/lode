// Example: Hive Layout Round-Trip
//
// This example demonstrates write → list → read with Hive (partition-first) layout:
//
//	datasets/<dataset>/partitions/<k=v>/segments/<segment>/
//	  manifest.json
//	  data/filename
//
// Hive layout places partitions at the dataset level, enabling efficient
// partition pruning at the storage layer.
//
// Run with: go run ./examples/hive_layout
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	tmpDir, err := os.MkdirTemp("", "lode-hive-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer testutil.RemoveAll(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// WRITE: Create dataset with Hive layout and partitioned data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Create dataset with HiveLayout.
	// NewHiveLayout("day") configures BOTH:
	//   - Hive (partition-first) path topology
	//   - Hive partitioner that extracts "day" field from records
	ds, err := lode.NewDataset(
		"events",
		storeFactory,
		lode.WithLayout(lode.NewHiveLayout("day")),
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write records with different days (will create partitions)
	records := lode.R(
		lode.D{"id": 1, "event": "login", "day": "2024-01-15", "user": "alice"},
		lode.D{"id": 2, "event": "click", "day": "2024-01-15", "user": "bob"},
		lode.D{"id": 3, "event": "logout", "day": "2024-01-16", "user": "alice"},
		lode.D{"id": 4, "event": "login", "day": "2024-01-16", "user": "charlie"},
	)

	snapshot, err := ds.Write(ctx, records, lode.Metadata{"source": "hive-example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Files in manifest:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("  - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// Show the actual file structure
	fmt.Println("File structure (Hive layout):")
	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tmpDir, path)
		if rel != "." {
			if info.IsDir() {
				fmt.Printf("  %s/\n", rel)
			} else {
				fmt.Printf("  %s\n", rel)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// LIST: Discover datasets, segments, and partitions
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	// Create reader with HiveLayout
	reader, err := lode.NewReader(
		storeFactory,
		lode.WithLayout(lode.NewHiveLayout("day")),
	)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	// List all datasets
	datasets, err := reader.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	// List segments in the dataset
	segments, err := reader.ListSegments(ctx, "events", "", lode.SegmentListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}
	fmt.Printf("Segments in 'events': %d segment(s)\n", len(segments))
	for _, seg := range segments {
		fmt.Printf("  - %s (partition: %s)\n", seg.ID, seg.Partition)
	}

	// List partitions
	partitions, err := reader.ListPartitions(ctx, "events", lode.PartitionListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}
	fmt.Printf("Partitions in 'events': %d partition(s)\n", len(partitions))
	for _, p := range partitions {
		fmt.Printf("  - %s\n", p.Path)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Load manifest and read data back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	// Get manifest for the first segment
	manifest, err := reader.GetManifest(ctx, "events", segments[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Partitioner: %s\n", manifest.Partitioner)

	// Read data through the dataset
	readRecords, err := ds.Read(ctx, segments[0].ID)
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	fmt.Printf("\nRecords read back (%d total):\n", len(readRecords))
	for _, r := range readRecords {
		fmt.Printf("  %v\n", r)
	}

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Hive layout round-trip complete!")

	return nil
}
