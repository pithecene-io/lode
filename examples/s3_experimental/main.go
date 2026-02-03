// Example: S3 (Experimental)
//
// This example uses the internal S3 adapter for demonstration purposes.
// It is technically valid but EXPERIMENTAL and not part of the stable public API.
//
// Requirements:
// - An S3-compatible service (LocalStack or MinIO)
// - The bucket must exist (e.g., `aws --endpoint-url=http://localhost:4566 s3 mb s3://lode-example`)
// - Environment variables (optional, defaults provided below)
//
// Run with: go run ./examples/s3_experimental
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	s3store "github.com/justapithecus/lode/internal/s3"
	"github.com/justapithecus/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	bucket := getenv("LODE_S3_BUCKET", "lode-example")
	endpoint := getenv("LODE_S3_ENDPOINT", "http://localhost:4566")
	region := getenv("LODE_S3_REGION", "us-east-1")
	accessKey := getenv("LODE_S3_ACCESS_KEY", "test")
	secretKey := getenv("LODE_S3_SECRET_KEY", "test")

	// Unique prefix to avoid collisions between runs.
	prefix := fmt.Sprintf("examples/s3/%d/", time.Now().UnixNano())

	client, err := s3store.NewClient(ctx, s3store.ClientConfig{
		Region:       region,
		Endpoint:     endpoint,
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	})
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	store, err := s3store.New(client, s3store.Config{
		Bucket: bucket,
		Prefix: prefix,
	})
	if err != nil {
		return fmt.Errorf("failed to create s3 store: %w", err)
	}

	storeFactory := func() (lode.Store, error) {
		return store, nil
	}

	ds, err := lode.NewDataset(
		"events",
		storeFactory,
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	records := lode.R(
		lode.D{"id": 1, "event": "login", "user": "alice"},
		lode.D{"id": 2, "event": "click", "user": "bob"},
	)

	snapshot, err := ds.Write(ctx, records, lode.Metadata{"source": "s3-example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("S3 prefix: %s\n", prefix)
	fmt.Printf("Files in manifest: %d\n", len(snapshot.Manifest.Files))

	reader, err := lode.NewReader(storeFactory)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	datasets, err := reader.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	return nil
}

func getenv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
