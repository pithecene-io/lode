package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ClientConfig holds configuration for creating an S3 client.
// This is internal configuration for S3-compatible backends.
type ClientConfig struct {
	// Region is the AWS region (required).
	Region string

	// Endpoint is an optional custom endpoint URL.
	// Used for S3-compatible services (MinIO, LocalStack, R2).
	// Example: "http://localhost:4566" for LocalStack.
	Endpoint string

	// UsePathStyle enables path-style addressing instead of virtual-hosted style.
	// Required for some S3-compatible services (e.g., LocalStack, MinIO with default config).
	// AWS S3 uses virtual-hosted style by default.
	UsePathStyle bool

	// Credentials are the AWS credentials to use.
	// If nil, uses the default credential chain.
	Credentials aws.CredentialsProvider
}

// NewClient creates a new S3 client with the given configuration.
//
// For AWS S3:
//
//	client, err := s3store.NewClient(ctx, s3store.ClientConfig{
//	    Region: "us-east-1",
//	})
//
// For LocalStack:
//
//	client, err := s3store.NewClient(ctx, s3store.ClientConfig{
//	    Region:       "us-east-1",
//	    Endpoint:     "http://localhost:4566",
//	    UsePathStyle: true,
//	    Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
//	})
//
// For MinIO:
//
//	client, err := s3store.NewClient(ctx, s3store.ClientConfig{
//	    Region:       "us-east-1",
//	    Endpoint:     "http://localhost:9000",
//	    UsePathStyle: true,
//	    Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
//	})
func NewClient(ctx context.Context, cfg ClientConfig) (*s3.Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	if cfg.Credentials != nil {
		opts = append(opts, config.WithCredentialsProvider(cfg.Credentials))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	s3Opts := []func(*s3.Options){}

	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...), nil
}

// NewLocalStackClient creates an S3 client configured for LocalStack.
// Defaults: endpoint=http://localhost:4566, region=us-east-1, credentials=test/test.
func NewLocalStackClient(ctx context.Context) (*s3.Client, error) {
	return NewClient(ctx, ClientConfig{
		Region:       "us-east-1",
		Endpoint:     "http://localhost:4566",
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// NewMinIOClient creates an S3 client configured for MinIO.
// Defaults: endpoint=http://localhost:9000, region=us-east-1, credentials=minioadmin/minioadmin.
func NewMinIOClient(ctx context.Context) (*s3.Client, error) {
	return NewClient(ctx, ClientConfig{
		Region:       "us-east-1",
		Endpoint:     "http://localhost:9000",
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
	})
}

// NewR2Client creates an S3 client configured for Cloudflare R2.
// The accountID is your Cloudflare account ID.
// Credentials should be R2 API tokens.
func NewR2Client(
	ctx context.Context,
	accountID, accessKeyID, secretAccessKey string,
) (*s3.Client, error) {
	return NewClient(ctx, ClientConfig{
		Region:       "auto",
		Endpoint:     "https://" + accountID + ".r2.cloudflarestorage.com",
		UsePathStyle: false, // R2 supports virtual-hosted style
		Credentials:  credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
	})
}
