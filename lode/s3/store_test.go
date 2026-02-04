package s3

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/justapithecus/lode/lode"
)

// -----------------------------------------------------------------------------
// Unit tests for S3 store
// These use the mock client and don't require real S3/LocalStack/MinIO.
// -----------------------------------------------------------------------------

func TestNew_RequiresClient(t *testing.T) {
	_, err := New(nil, Config{Bucket: "test"})
	if err == nil {
		t.Error("expected error for nil client")
	}
}

func TestNew_RequiresBucket(t *testing.T) {
	_, err := New(NewMockS3Client(), Config{})
	if err == nil {
		t.Error("expected error for empty bucket")
	}
}

func TestNew_PrefixNormalization(t *testing.T) {
	tests := []struct {
		prefix   string
		expected string
	}{
		{"", ""},
		{"foo", "foo/"},
		{"foo/", "foo/"},
		{"foo/bar", "foo/bar/"},
		{"foo/bar/", "foo/bar/"},
	}

	for _, tt := range tests {
		store, err := New(NewMockS3Client(), Config{Bucket: "test", Prefix: tt.prefix})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		if store.prefix != tt.expected {
			t.Errorf("prefix %q: expected %q, got %q", tt.prefix, tt.expected, store.prefix)
		}
	}
}

// -----------------------------------------------------------------------------
// Put tests
// -----------------------------------------------------------------------------

func TestStore_Put_Success(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

func TestStore_Put_ErrPathExists(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// First write should succeed
	err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second write to same path should return ErrPathExists
	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("world")))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected ErrPathExists, got: %v", err)
	}
}

func TestStore_Put_ErrInvalidPath_Empty(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	err := store.Put(ctx, "", bytes.NewReader([]byte("hello")))
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for empty path, got: %v", err)
	}
}

func TestStore_Put_ErrInvalidPath_Escaping(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	tests := []string{
		"..",
		"../foo",
		"foo/../..",
		"foo/../../bar",
	}

	for _, path := range tests {
		err := store.Put(ctx, path, bytes.NewReader([]byte("hello")))
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("path %q: expected ErrInvalidPath, got: %v", path, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Put routing boundary tests (per CONTRACT_STORAGE.md)
//
// Threshold: maxSinglePutSize = 100MB
// - size ≤ threshold: one-shot path (PutObject with If-None-Match)
// - size > threshold: multipart path (preflight HeadObject + multipart upload)
// -----------------------------------------------------------------------------

func TestStore_Put_Routing_AtThresholdMinusOne_UsesOneShot(t *testing.T) {
	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	// threshold - 1 byte should use one-shot path
	size := maxSinglePutSize - 1
	data := make([]byte, size)

	err := store.Put(ctx, "at-threshold-minus-one.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify one-shot path was taken (PutObject called, no multipart)
	mock.mu.RLock()
	putCalls := mock.PutObjectCalls
	multipartCalls := mock.CreateMultipartUploadCalls
	stored := mock.objects["at-threshold-minus-one.bin"]
	mock.mu.RUnlock()

	if putCalls != 1 {
		t.Errorf("expected 1 PutObject call (one-shot), got %d", putCalls)
	}
	if multipartCalls != 0 {
		t.Errorf("expected 0 CreateMultipartUpload calls, got %d", multipartCalls)
	}
	if len(stored) != size {
		t.Errorf("expected %d bytes stored, got %d", size, len(stored))
	}
}

func TestStore_Put_Routing_AtThreshold_UsesOneShot(t *testing.T) {
	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	// Exactly at threshold should use one-shot path
	size := maxSinglePutSize
	data := make([]byte, size)

	err := store.Put(ctx, "at-threshold.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify one-shot path was taken
	mock.mu.RLock()
	putCalls := mock.PutObjectCalls
	multipartCalls := mock.CreateMultipartUploadCalls
	stored := mock.objects["at-threshold.bin"]
	mock.mu.RUnlock()

	if putCalls != 1 {
		t.Errorf("expected 1 PutObject call (one-shot), got %d", putCalls)
	}
	if multipartCalls != 0 {
		t.Errorf("expected 0 CreateMultipartUpload calls, got %d", multipartCalls)
	}
	if len(stored) != size {
		t.Errorf("expected %d bytes stored, got %d", size, len(stored))
	}
}

func TestStore_Put_Routing_AtThresholdPlusOne_UsesMultipart(t *testing.T) {
	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	// threshold + 1 byte should use multipart path
	size := maxSinglePutSize + 1
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := store.Put(ctx, "at-threshold-plus-one.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify multipart path was taken (no PutObject, CreateMultipartUpload called)
	mock.mu.RLock()
	putCalls := mock.PutObjectCalls
	multipartCalls := mock.CreateMultipartUploadCalls
	stored := mock.objects["at-threshold-plus-one.bin"]
	mock.mu.RUnlock()

	if putCalls != 0 {
		t.Errorf("expected 0 PutObject calls (multipart path), got %d", putCalls)
	}
	if multipartCalls != 1 {
		t.Errorf("expected 1 CreateMultipartUpload call, got %d", multipartCalls)
	}
	if len(stored) != size {
		t.Errorf("expected %d bytes stored, got %d", size, len(stored))
	}
	if !bytes.Equal(data, stored) {
		t.Error("stored data does not match original")
	}
}

// -----------------------------------------------------------------------------
// Put duplicate behavior tests (per CONTRACT_STORAGE.md)
// -----------------------------------------------------------------------------

func TestStore_Put_OneShot_Duplicate_ReturnsErrPathExists(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// Small file uses one-shot path with If-None-Match
	data := []byte("hello")

	// First write should succeed
	err := store.Put(ctx, "oneshot-dup.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second write should return ErrPathExists (atomic detection via PreconditionFailed)
	err = store.Put(ctx, "oneshot-dup.txt", bytes.NewReader(data))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected ErrPathExists, got: %v", err)
	}
}

func TestStore_Put_Multipart_PreExisting_ReturnsErrPathExists(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// Large file uses multipart path with preflight check
	size := maxSinglePutSize + 1
	data := make([]byte, size)

	// First write should succeed
	err := store.Put(ctx, "multipart-dup.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second write should return ErrPathExists (detected at preflight HeadObject)
	err = store.Put(ctx, "multipart-dup.bin", bytes.NewReader(data))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected ErrPathExists, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Put multipart content integrity test
// -----------------------------------------------------------------------------

func TestStore_Put_Multipart_ContentIntegrity(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// Create data larger than threshold with known pattern
	size := maxSinglePutSize + (5 * 1024 * 1024) // threshold + 5MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := store.Put(ctx, "large-file.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put large file failed: %v", err)
	}

	// Verify the data was stored correctly
	rc, err := store.Get(ctx, "large-file.bin")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	retrieved, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(retrieved) != size {
		t.Errorf("expected %d bytes, got %d", size, len(retrieved))
	}

	if !bytes.Equal(data, retrieved) {
		t.Error("retrieved data does not match original")
	}
}

// -----------------------------------------------------------------------------
// Multipart abort cleanup test
// -----------------------------------------------------------------------------

func TestStore_Put_Multipart_FailureTriggersAbort(t *testing.T) {
	// Verify that when multipart upload fails, AbortMultipartUpload is called.
	// This tests the actual abort path, not just successful completion.

	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	// Configure mock to fail on second part upload (first part succeeds, then fails)
	mock.UploadPartFailOnCall = 2

	// Create data that requires multiple parts (threshold + 10MB to ensure multiple chunks)
	size := maxSinglePutSize + (10 * 1024 * 1024)
	data := make([]byte, size)

	// Put should fail due to simulated UploadPart failure
	err := store.Put(ctx, "will-fail.bin", bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected Put to fail due to simulated UploadPart error")
	}

	// Verify abort was called
	mock.mu.RLock()
	abortCalls := mock.AbortMultipartUploadCalls
	createCalls := mock.CreateMultipartUploadCalls
	numUploads := len(mock.uploads)
	mock.mu.RUnlock()

	if createCalls != 1 {
		t.Errorf("expected 1 CreateMultipartUpload call, got %d", createCalls)
	}
	if abortCalls != 1 {
		t.Errorf("expected 1 AbortMultipartUpload call (cleanup), got %d", abortCalls)
	}
	if numUploads != 0 {
		t.Errorf("expected 0 in-progress uploads after abort, got %d", numUploads)
	}

	// Verify object was not created
	mock.mu.RLock()
	_, exists := mock.objects["will-fail.bin"]
	mock.mu.RUnlock()
	if exists {
		t.Error("object should not exist after failed upload")
	}
}

// -----------------------------------------------------------------------------
// Multipart TOCTOU documentation test
//
// This test documents and asserts the known limitation: multipart path has a
// TOCTOU window. Per CONTRACT_STORAGE.md, this is expected behavior and
// single-writer or external coordination is required.
// -----------------------------------------------------------------------------

func TestStore_Put_Multipart_TOCTOU_Limitation(t *testing.T) {
	// Assert: maxSinglePutSize is 100MB (the documented threshold)
	const expectedThreshold = 100 * 1024 * 1024
	if maxSinglePutSize != expectedThreshold {
		t.Fatalf("maxSinglePutSize should be %d (100MB), got %d", expectedThreshold, maxSinglePutSize)
	}

	// Assert: One-shot path uses If-None-Match (atomic)
	// Verified by TestStore_Put_OneShot_Duplicate_ReturnsErrPathExists

	// Assert: Multipart path uses preflight HeadObject (best-effort)
	// The TOCTOU window is between HeadObject and CompleteMultipartUpload.
	// We verify the multipart path checks existence before upload:

	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	// Pre-create an object
	size := maxSinglePutSize + 1
	data := make([]byte, size)
	err := store.Put(ctx, "existing.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// Second multipart Put should fail at preflight (ErrPathExists)
	mock.ResetCounts()
	err = store.Put(ctx, "existing.bin", bytes.NewReader(data))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("expected ErrPathExists from preflight check, got: %v", err)
	}

	// Verify no new multipart upload was started (failed at preflight HeadObject)
	mock.mu.RLock()
	multipartCalls := mock.CreateMultipartUploadCalls
	mock.mu.RUnlock()

	if multipartCalls != 0 {
		t.Errorf("expected 0 CreateMultipartUpload calls (should fail at preflight), got %d", multipartCalls)
	}

	// Document the limitation
	t.Log("TOCTOU limitation: Between HeadObject and CompleteMultipartUpload,")
	t.Log("a concurrent writer could create the same key. This test verifies")
	t.Log("preflight detection works, but cannot test the race itself.")
	t.Log("Per CONTRACT_STORAGE.md: single-writer or external coordination required.")
}

// -----------------------------------------------------------------------------
// Get tests
// -----------------------------------------------------------------------------

func TestStore_Get_Success(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello world")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	rc, err := store.Get(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, _ := io.ReadAll(rc)
	if string(data) != string(content) {
		t.Errorf("expected %q, got %q", string(content), string(data))
	}
}

func TestStore_Get_ErrNotFound(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.Get(ctx, "nonexistent.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestStore_Get_ErrInvalidPath(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.Get(ctx, "")
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for empty path, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Exists tests
// -----------------------------------------------------------------------------

func TestStore_Exists_True(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("hello")))

	exists, err := store.Exists(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected exists=true")
	}
}

func TestStore_Exists_False(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	exists, err := store.Exists(ctx, "nonexistent.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected exists=false")
	}
}

func TestStore_Exists_ErrInvalidPath(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.Exists(ctx, "")
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Delete tests
// -----------------------------------------------------------------------------

func TestStore_Delete_Exists(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("hello")))

	err := store.Delete(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	exists, _ := store.Exists(ctx, "test.txt")
	if exists {
		t.Error("file should not exist after delete")
	}
}

func TestStore_Delete_NotExists_Idempotent(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// Delete on non-existent file should not error (idempotent)
	err := store.Delete(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("Delete on non-existent file should not error, got: %v", err)
	}
}

func TestStore_Delete_ErrInvalidPath(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	err := store.Delete(ctx, "")
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// List tests
// -----------------------------------------------------------------------------

func TestStore_List_Empty(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	keys, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected empty list, got %d keys", len(keys))
	}
}

func TestStore_List_WithPrefix(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_ = store.Put(ctx, "a/1.txt", bytes.NewReader([]byte("1")))
	_ = store.Put(ctx, "a/2.txt", bytes.NewReader([]byte("2")))
	_ = store.Put(ctx, "b/3.txt", bytes.NewReader([]byte("3")))

	keys, err := store.List(ctx, "a/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestStore_List_WithStorePrefix(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test", Prefix: "datasets/"})

	_ = store.Put(ctx, "foo/1.txt", bytes.NewReader([]byte("1")))
	_ = store.Put(ctx, "foo/2.txt", bytes.NewReader([]byte("2")))

	keys, err := store.List(ctx, "foo/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	// Keys should be relative (without store prefix)
	for _, key := range keys {
		if !contains([]string{"foo/1.txt", "foo/2.txt"}, key) {
			t.Errorf("unexpected key: %s", key)
		}
	}
}

func TestStore_List_ErrInvalidPath(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.List(ctx, "../")
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// ReadRange tests
// -----------------------------------------------------------------------------

func TestStore_ReadRange_Basic(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello world")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	// Read "world"
	data, err := store.ReadRange(ctx, "test.txt", 6, 5)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("expected 'world', got %q", string(data))
	}
}

func TestStore_ReadRange_BeyondEOF(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	// Read beyond EOF - should return available bytes
	data, err := store.ReadRange(ctx, "test.txt", 3, 100)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if string(data) != "lo" {
		t.Errorf("expected 'lo', got %q", string(data))
	}
}

func TestStore_ReadRange_OffsetBeyondEOF(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	// Offset beyond EOF - should return empty slice
	data, err := store.ReadRange(ctx, "test.txt", 100, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty slice, got %d bytes", len(data))
	}
}

func TestStore_ReadRange_NotFound(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 10)
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestStore_ReadRange_NegativeOffset(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReadRange(ctx, "test.txt", -1, 10)
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
	}
}

func TestStore_ReadRange_NegativeLength(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReadRange(ctx, "test.txt", 0, -1)
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for negative length, got: %v", err)
	}
}

func TestStore_ReadRange_LengthOverflow(t *testing.T) {
	// This test validates 32-bit platform protection.
	if math.MaxInt == math.MaxInt64 {
		t.Skip("length overflow check only applies to 32-bit platforms")
	}

	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
	}
}

func TestStore_ReadRange_OffsetPlusLengthOverflow(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// offset + length would overflow int64
	_, err := store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
	}
}

func TestStore_ReadRange_ZeroLength(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello world")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	// Zero-length read should return empty slice without error
	data, err := store.ReadRange(ctx, "test.txt", 5, 0)
	if err != nil {
		t.Fatalf("ReadRange with length=0 failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty slice for length=0, got %d bytes", len(data))
	}
}

func TestStore_ReadRange_ZeroLength_NotFound(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	// Zero-length read on non-existent file must return ErrNotFound per contract
	_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 0)
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound for zero-length read on missing file, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// ReaderAt tests
// -----------------------------------------------------------------------------

func TestStore_ReaderAt_Basic(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello world")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	ra, err := store.ReaderAt(ctx, "test.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}

	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 5 || string(buf) != "world" {
		t.Errorf("expected 'world', got %q", string(buf[:n]))
	}
}

func TestStore_ReaderAt_ConcurrentReads(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("0123456789")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	ra, err := store.ReaderAt(ctx, "test.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}

	// Read different offsets concurrently
	done := make(chan bool, 2)

	go func() {
		buf := make([]byte, 3)
		n, err := ra.ReadAt(buf, 0)
		if err != nil || n != 3 || string(buf) != "012" {
			t.Errorf("read at 0: expected '012', got %q (err=%v)", string(buf[:n]), err)
		}
		done <- true
	}()

	go func() {
		buf := make([]byte, 3)
		n, err := ra.ReadAt(buf, 7)
		if err != nil || n != 3 || string(buf) != "789" {
			t.Errorf("read at 7: expected '789', got %q (err=%v)", string(buf[:n]), err)
		}
		done <- true
	}()

	<-done
	<-done
}

func TestStore_ReaderAt_NotFound(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReaderAt(ctx, "nonexistent.txt")
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestStore_ReaderAt_ErrInvalidPath(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	_, err := store.ReaderAt(ctx, "")
	if !errors.Is(err, lode.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

func TestStore_ReaderAt_EOF(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

	content := []byte("hello")
	_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

	ra, err := store.ReaderAt(ctx, "test.txt")
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}

	// Read beyond EOF
	buf := make([]byte, 10)
	_, err = ra.ReadAt(buf, 100)
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------------

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
