// Package s3 provides an S3-compatible storage adapter for Lode.
//
// This adapter is EXPERIMENTAL. It supports AWS S3, MinIO, LocalStack,
// and other S3-compatible object stores.
//
// Consistency: S3 provides strong read-after-write consistency (since Dec 2020).
// List operations are also strongly consistent. However, commit semantics
// rely on manifest presence: writers MUST write data objects before the manifest.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/justapithecus/lode/lode"
)

// maxReadRangeLength is the maximum length for ReadRange to prevent overflow
// when converting int64 to int on 32-bit platforms.
const maxReadRangeLength = int64(math.MaxInt)

// API defines the subset of the S3 client interface used by the store.
// This enables testing with mock implementations.
type API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Config holds configuration for the S3 store.
type Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// Prefix is an optional key prefix for all operations.
	// If set, all keys are prefixed with this value (with a trailing slash added if missing).
	Prefix string
}

// Store implements lode.Store using an S3-compatible backend.
type Store struct {
	client API
	bucket string
	prefix string
}

// New creates a new S3 store with the given client and configuration.
//
// The client must be pre-configured with credentials, region, and endpoint.
// Use github.com/aws/aws-sdk-go-v2/config to load configuration.
//
// Example:
//
//	cfg, err := config.LoadDefaultConfig(ctx)
//	client := s3.NewFromConfig(cfg)
//	store, err := s3store.New(client, s3store.Config{Bucket: "my-bucket"})
func New(client API, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3: client is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}

	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

// Put writes data to the given path.
// Returns ErrPathExists if the path already exists.
// Returns ErrInvalidPath for empty or escaping paths.
//
// Note: The current implementation buffers the entire object in memory before
// upload. For large files, callers should be aware of memory implications.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	// Read all data into memory for PutObject.
	// Note: For large files, a streaming approach with known Content-Length would be better.
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("s3: reading data: %w", err)
	}

	// Use conditional write with If-None-Match to enforce immutability atomically.
	// This avoids the race condition of check-then-write.
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(fullKey),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		// Check for PreconditionFailed (object already exists)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := apiErr.ErrorCode()
			if code == "PreconditionFailed" || code == "412" {
				return lode.ErrPathExists
			}
		}
		return fmt.Errorf("s3: put object: %w", err)
	}

	return nil
}

// Get retrieves data from the given path.
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		return nil, fmt.Errorf("s3: get object: %w", err)
	}

	return out.Body, nil
}

// Exists checks whether a path exists.
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return false, err
	}

	return s.exists(ctx, fullKey)
}

// List returns all paths under the given prefix.
// Pagination is handled automatically; all matching keys are returned.
// Returns ErrInvalidPath for escaping prefixes.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := s.validatePrefix(prefix)
	if err != nil {
		return nil, err
	}

	var keys []string
	var continuationToken *string

	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list objects: %w", err)
		}

		for _, obj := range out.Contents {
			if obj.Key != nil {
				// Strip the store prefix to return relative keys
				relKey := strings.TrimPrefix(*obj.Key, s.prefix)
				keys = append(keys, relKey)
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

// Delete removes the path if it exists.
// Safe to call on missing paths (idempotent).
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Delete(ctx context.Context, key string) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		// S3 DeleteObject is idempotent; it doesn't error on missing keys
		return fmt.Errorf("s3: delete object: %w", err)
	}

	return nil
}

// ReadRange reads a byte range from the given path.
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath for negative offset/length, overflow, or invalid paths.
// If offset is beyond EOF, returns empty slice.
// If range extends beyond EOF, returns available bytes.
// If length is 0, returns empty slice without making a request.
func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	// Validate offset and length per CONTRACT_STORAGE.md
	if offset < 0 || length < 0 || length > maxReadRangeLength {
		return nil, lode.ErrInvalidPath
	}
	if offset > math.MaxInt64-length {
		return nil, lode.ErrInvalidPath
	}

	// Zero-length read returns empty slice (no request needed).
	// This also avoids invalid range header (offset to offset-1).
	if length == 0 {
		return []byte{}, nil
	}

	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	// S3 Range header format: "bytes=start-end" (inclusive)
	// end = offset + length - 1
	end := offset + length - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, end)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		// Check for InvalidRange (offset beyond EOF)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("s3: range read: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: reading range body: %w", err)
	}

	return data, nil
}

// ReaderAt returns an io.ReaderAt for random access reads.
// Returns ErrNotFound if the path does not exist.
// The returned ReaderAt supports concurrent reads at different offsets.
// Callers should close the ReaderAt if it implements io.Closer.
func (s *Store) ReaderAt(ctx context.Context, key string) (io.ReaderAt, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	// Verify the object exists
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		return nil, fmt.Errorf("s3: head object: %w", err)
	}

	return &readerAt{
		store:   s,
		bucket:  s.bucket,
		key:     fullKey,
		baseCtx: ctx,
	}, nil
}

// readerAt implements io.ReaderAt using S3 range reads.
// It is safe for concurrent use.
type readerAt struct {
	store   *Store
	bucket  string
	key     string
	baseCtx context.Context
}

// ReadAt implements io.ReaderAt.
func (r *readerAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("s3: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}

	end := off + int64(len(p)) - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	out, err := r.store.client.GetObject(r.baseCtx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		// Check for InvalidRange (offset beyond EOF)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("s3: range read: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	n, err = io.ReadFull(out.Body, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// Partial read (requested range extends beyond EOF)
		err = io.EOF
	}
	return n, err
}

// exists checks if an object exists (internal helper).
func (s *Store) exists(ctx context.Context, fullKey string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// validateKey validates and returns the full key for file operations.
func (s *Store) validateKey(key string) (string, error) {
	if key == "" {
		return "", lode.ErrInvalidPath
	}

	// Normalize the key
	cleaned := path.Clean(key)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", lode.ErrInvalidPath
	}
	// Remove leading slash
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" {
		return "", lode.ErrInvalidPath
	}

	return s.prefix + cleaned, nil
}

// validatePrefix validates and returns the full prefix for list operations.
func (s *Store) validatePrefix(prefix string) (string, error) {
	if prefix == "" {
		return s.prefix, nil
	}

	cleaned := path.Clean(prefix)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", lode.ErrInvalidPath
	}
	if cleaned == "." {
		return s.prefix, nil
	}
	// Remove leading slash
	cleaned = strings.TrimPrefix(cleaned, "/")

	return s.prefix + cleaned, nil
}

// isNotFound checks if an error indicates the object was not found.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nsb *types.NoSuchBucket
	if errors.As(err, &nsb) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchKey" || code == "404"
	}
	return false
}

// -----------------------------------------------------------------------------
// Mock S3 Client for Testing
// -----------------------------------------------------------------------------

// MockS3Client is a test double for API.
type MockS3Client struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMockS3Client creates a new mock S3 client for testing.
func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string][]byte),
	}
}

// PutObject implements API.PutObject for testing.
func (m *MockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(params.Key)
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle If-None-Match: "*" (conditional write for immutability)
	if aws.ToString(params.IfNoneMatch) == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "object already exists"}
		}
	}

	m.objects[key] = data
	return &s3.PutObjectOutput{}, nil
}

// GetObject implements API.GetObject for testing.
func (m *MockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.RLock()
	data, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, &types.NoSuchKey{}
	}

	// Handle range requests
	if params.Range != nil {
		rangeStr := aws.ToString(params.Range)
		var start, end int64
		_, _ = fmt.Sscanf(rangeStr, "bytes=%d-%d", &start, &end)

		if start >= int64(len(data)) {
			return nil, &smithyAPIError{code: "InvalidRange"}
		}

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		data = data[start : end+1]
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

// HeadObject implements API.HeadObject for testing.
func (m *MockS3Client) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.RLock()
	_, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, &types.NoSuchKey{}
	}

	return &s3.HeadObjectOutput{}, nil
}

// DeleteObject implements API.DeleteObject for testing.
func (m *MockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()

	return &s3.DeleteObjectOutput{}, nil
}

// ListObjectsV2 implements API.ListObjectsV2 for testing.
func (m *MockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(params.Prefix)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var contents []types.Object
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			k := key
			contents = append(contents, types.Object{Key: &k})
		}
	}

	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

// smithyAPIError implements smithy.APIError for testing.
type smithyAPIError struct {
	code    string
	message string
}

func (e *smithyAPIError) Error() string {
	return e.message
}

func (e *smithyAPIError) ErrorCode() string {
	return e.code
}

func (e *smithyAPIError) ErrorMessage() string {
	return e.message
}

func (e *smithyAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}
