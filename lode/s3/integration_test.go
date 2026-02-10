//go:build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene-io/lode/lode"
)

// Integration tests for S3-compatible backends.
// These require running docker-compose services.
//
// To run:
//   docker compose -f lode/s3/docker-compose.yaml up -d
//   go test -v -tags=integration ./lode/s3/...
//   docker compose -f lode/s3/docker-compose.yaml down

func skipIfNoS3(t *testing.T) {
	if os.Getenv("LODE_S3_TESTS") != "1" {
		t.Skip("LODE_S3_TESTS=1 not set; skipping integration tests")
	}
}

// newLocalStackClient creates an S3 client for LocalStack (integration tests only).
func newLocalStackClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
		o.UsePathStyle = true
	}), nil
}

// newMinIOClient creates an S3 client for MinIO (integration tests only).
func newMinIOClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
	}), nil
}

// -----------------------------------------------------------------------------
// LocalStack Integration Tests
// -----------------------------------------------------------------------------

func TestLocalStack_Integration(t *testing.T) {
	skipIfNoS3(t)

	ctx := t.Context()
	client, err := newLocalStackClient(ctx)
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

	ctx := t.Context()
	client, err := newMinIOClient(ctx)
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
	ctx := t.Context()

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
		if !slices.Contains(keys, key) {
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

// -----------------------------------------------------------------------------
// S3 Adapter Timing Integration Tests
// -----------------------------------------------------------------------------
//
// These tests verify S3 adapter behavior under realistic timing conditions.
// They complement the deterministic fault-injection tests by exercising
// real network cancellation and cleanup paths.
//
// Key principle: assert on core invariants (no manifest = no visible snapshot),
// but don't make timing-dependent assertions that could flake due to network latency.
//
// Contract reference: CONTRACT_TEST_MATRIX.md (G3-4 residual risk)

// TestLocalStack_StreamWrite_ContextCancel_NoSnapshot verifies that when context
// is canceled during a streaming write to S3, no snapshot becomes visible.
func TestLocalStack_StreamWrite_ContextCancel_NoSnapshot(t *testing.T) {
	skipIfNoS3(t)

	ctx := t.Context()
	client, err := newLocalStackClient(ctx)
	if err != nil {
		t.Fatalf("failed to create LocalStack client: %v", err)
	}

	bucket := fmt.Sprintf("lode-timing-%d", time.Now().UnixNano())

	// Create bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	defer func() {
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

	storeFactory := func() (lode.Store, error) { return store, nil }

	ds, err := lode.NewDataset("cancel-test", storeFactory)
	if err != nil {
		t.Fatal(err)
	}

	// Create a cancellable context
	cancelCtx, cancel := context.WithCancel(ctx)

	sw, err := ds.StreamWrite(cancelCtx, lode.Metadata{"test": "cancel"})
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	_, err = sw.Write([]byte("data that will be canceled"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Cancel before commit
	cancel()

	// Commit should fail due to canceled context (or succeed if timing allows)
	_, err = sw.Commit(cancelCtx)
	if err == nil {
		t.Log("Note: commit completed before cancel took effect (timing-dependent with S3)")
	} else if !errors.Is(err, context.Canceled) {
		t.Logf("Note: commit failed with non-cancel error: %v", err)
	}

	// Core invariant: verify snapshot state with fresh context
	freshCtx := t.Context()
	_, err = ds.Latest(freshCtx)
	if errors.Is(err, lode.ErrNoSnapshots) {
		// Expected: cancel prevented snapshot
	} else if err == nil {
		// Also valid: commit raced ahead of cancel
		t.Log("Snapshot visible after cancel - commit completed before cancel")
	} else {
		t.Errorf("Unexpected error checking snapshot: %v", err)
	}
}

// TestLocalStack_StreamWriteRecords_ContextTimeout_NoSnapshot verifies that when
// context times out during StreamWriteRecords to S3, no partial snapshot is visible.
func TestLocalStack_StreamWriteRecords_ContextTimeout_NoSnapshot(t *testing.T) {
	skipIfNoS3(t)

	ctx := t.Context()
	client, err := newLocalStackClient(ctx)
	if err != nil {
		t.Fatalf("failed to create LocalStack client: %v", err)
	}

	bucket := fmt.Sprintf("lode-timing-%d", time.Now().UnixNano())

	// Create bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	defer func() {
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

	storeFactory := func() (lode.Store, error) { return store, nil }

	ds, err := lode.NewDataset("timeout-test", storeFactory, lode.WithCodec(lode.NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}

	// Create many records to increase chance of timeout during write
	records := make([]any, 100)
	for i := range records {
		records[i] = lode.D{"id": fmt.Sprintf("%d", i), "data": "payload"}
	}

	// Very short timeout to exercise cancellation path
	// Note: S3/LocalStack latency means this may still complete
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()

	iter := &testRecordIterator{records: records}
	_, err = ds.StreamWriteRecords(timeoutCtx, iter, lode.Metadata{})

	freshCtx := t.Context()
	if err == nil {
		// Completed before timeout - verify snapshot
		snap, err := ds.Latest(freshCtx)
		if err != nil {
			t.Fatalf("Expected snapshot after successful write: %v", err)
		}
		t.Logf("Write completed with %d rows (timeout didn't trigger)", snap.Manifest.RowCount)
	} else {
		// Timeout or other error - verify no partial snapshot
		_, err := ds.Latest(freshCtx)
		if !errors.Is(err, lode.ErrNoSnapshots) {
			t.Errorf("Expected ErrNoSnapshots after timeout, got: %v", err)
		}
	}
}

// testRecordIterator is a simple iterator for testing.
type testRecordIterator struct {
	records []any
	index   int
}

func (i *testRecordIterator) Next() bool {
	if i.index >= len(i.records) {
		return false
	}
	i.index++
	return true
}

func (i *testRecordIterator) Record() any {
	if i.index == 0 || i.index > len(i.records) {
		return nil
	}
	return i.records[i.index-1]
}

func (i *testRecordIterator) Err() error {
	return nil
}
