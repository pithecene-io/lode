//go:build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/justapithecus/lode/lode"
)

// Integration tests for S3-compatible backends.
// These require running docker-compose services.
//
// To run:
//   docker compose -f internal/s3/docker-compose.yaml up -d
//   go test -v -tags=integration ./internal/s3/...
//   docker compose -f internal/s3/docker-compose.yaml down

func skipIfNoS3(t *testing.T) {
	if os.Getenv("LODE_S3_TESTS") != "1" {
		t.Skip("LODE_S3_TESTS=1 not set; skipping integration tests")
	}
}

// -----------------------------------------------------------------------------
// LocalStack Integration Tests
// -----------------------------------------------------------------------------

func TestLocalStack_Integration(t *testing.T) {
	skipIfNoS3(t)

	ctx := context.Background()
	client, err := NewLocalStackClient(ctx)
	if err != nil {
		t.Fatalf("failed to create LocalStack client: %v", err)
	}

	bucket := fmt.Sprintf("lode-test-%d", time.Now().UnixNano())

	// Create bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	defer func() {
		// Clean up: delete all objects then bucket
		out, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		for _, obj := range out.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}
		_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()

	store, err := New(client, Config{Bucket: bucket})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	runStoreIntegrationTests(t, store)
}

// -----------------------------------------------------------------------------
// MinIO Integration Tests
// -----------------------------------------------------------------------------

func TestMinIO_Integration(t *testing.T) {
	skipIfNoS3(t)

	ctx := context.Background()
	client, err := NewMinIOClient(ctx)
	if err != nil {
		t.Fatalf("failed to create MinIO client: %v", err)
	}

	bucket := fmt.Sprintf("lode-test-%d", time.Now().UnixNano())

	// Create bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	defer func() {
		// Clean up: delete all objects then bucket
		out, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		for _, obj := range out.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}
		_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()

	store, err := New(client, Config{Bucket: bucket})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	runStoreIntegrationTests(t, store)
}

// -----------------------------------------------------------------------------
// Common Integration Test Suite
// -----------------------------------------------------------------------------

func runStoreIntegrationTests(t *testing.T, store *Store) {
	ctx := context.Background()

	t.Run("write_list_read", func(t *testing.T) {
		content := []byte("hello world")
		key := "test/file.txt"

		// Write
		err := store.Put(ctx, key, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// List
		keys, err := store.List(ctx, "test/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if !contains(keys, key) {
			t.Errorf("expected key %q in list, got %v", key, keys)
		}

		// Read
		rc, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("reading body failed: %v", err)
		}
		if string(data) != string(content) {
			t.Errorf("expected %q, got %q", string(content), string(data))
		}
	})

	t.Run("range_read", func(t *testing.T) {
		content := []byte("0123456789")
		key := "test/range.txt"

		err := store.Put(ctx, key, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Read middle portion
		data, err := store.ReadRange(ctx, key, 3, 4)
		if err != nil {
			t.Fatalf("ReadRange failed: %v", err)
		}
		if string(data) != "3456" {
			t.Errorf("expected '3456', got %q", string(data))
		}

		// Read beyond EOF - should return available bytes
		data, err = store.ReadRange(ctx, key, 8, 10)
		if err != nil {
			t.Fatalf("ReadRange beyond EOF failed: %v", err)
		}
		if string(data) != "89" {
			t.Errorf("expected '89', got %q", string(data))
		}
	})

	t.Run("reader_at", func(t *testing.T) {
		content := []byte("hello world reader at")
		key := "test/readerat.txt"

		err := store.Put(ctx, key, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		ra, err := store.ReaderAt(ctx, key)
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}

		// Read at different offsets
		buf := make([]byte, 5)

		n, err := ra.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt(0) failed: %v", err)
		}
		if n != 5 || string(buf) != "hello" {
			t.Errorf("ReadAt(0): expected 'hello', got %q", string(buf[:n]))
		}

		n, err = ra.ReadAt(buf, 6)
		if err != nil {
			t.Fatalf("ReadAt(6) failed: %v", err)
		}
		if n != 5 || string(buf) != "world" {
			t.Errorf("ReadAt(6): expected 'world', got %q", string(buf[:n]))
		}
	})

	t.Run("immutability_enforcement", func(t *testing.T) {
		content := []byte("immutable")
		key := "test/immutable.txt"

		err := store.Put(ctx, key, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("first Put failed: %v", err)
		}

		// Second write should fail
		err = store.Put(ctx, key, bytes.NewReader([]byte("modified")))
		if !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("expected ErrPathExists on second write, got: %v", err)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		_, err := store.Get(ctx, "nonexistent/path.txt")
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}

		_, err = store.ReadRange(ctx, "nonexistent/path.txt", 0, 10)
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound for ReadRange, got: %v", err)
		}

		_, err = store.ReaderAt(ctx, "nonexistent/path.txt")
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound for ReaderAt, got: %v", err)
		}
	})

	t.Run("manifest_last_visibility", func(t *testing.T) {
		// Write data objects first, then manifest
		// This simulates commit semantics where manifest presence = commit signal

		dataKey := "datasets/test-ds/snapshots/snap1/data/file.jsonl"
		manifestKey := "datasets/test-ds/snapshots/snap1/manifest.json"

		// Write data first
		err := store.Put(ctx, dataKey, bytes.NewReader([]byte(`{"id": 1}`)))
		if err != nil {
			t.Fatalf("Put data failed: %v", err)
		}

		// Before manifest, listing should not find the manifest
		keys, err := store.List(ctx, "datasets/test-ds/snapshots/snap1/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		hasManifest := false
		for _, k := range keys {
			if k == manifestKey {
				hasManifest = true
			}
		}
		if hasManifest {
			t.Error("manifest should not exist before being written")
		}

		// Write manifest (commit signal)
		manifestJSON := `{"schema_name":"lode-manifest","format_version":"1.0","dataset_id":"test-ds","snapshot_id":"snap1","created_at":"2024-01-01T00:00:00Z","metadata":{},"files":[{"path":"data/file.jsonl","size_bytes":10}],"row_count":1,"compressor":"noop","partitioner":"noop"}`
		err = store.Put(ctx, manifestKey, bytes.NewReader([]byte(manifestJSON)))
		if err != nil {
			t.Fatalf("Put manifest failed: %v", err)
		}

		// After manifest, listing should find both
		keys, err = store.List(ctx, "datasets/test-ds/snapshots/snap1/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		hasManifest = false
		hasData := false
		for _, k := range keys {
			if k == manifestKey {
				hasManifest = true
			}
			if k == dataKey {
				hasData = true
			}
		}
		if !hasManifest {
			t.Error("manifest should exist after being written")
		}
		if !hasData {
			t.Error("data file should exist")
		}
	})
}
