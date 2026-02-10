# Benchmarks

Central index of all Lode benchmarks.

## Benchmark inventory

| Benchmark | Location | Backend | Status |
|-----------|----------|---------|--------|
| `BenchmarkDataset_SequentialWrites` | `lode/dataset_bench_test.go` | In-memory (latency-injected) | Done |
| `BenchmarkDataset_SequentialWrites_StoreCallCount` | `lode/dataset_bench_test.go` | In-memory (fault store) | Done |
| `BenchmarkS3_WriteRoundTrip/LocalStack` | `lode/s3/bench_integration_test.go` | LocalStack | Done |
| `BenchmarkS3_WriteRoundTrip/MinIO` | `lode/s3/bench_integration_test.go` | MinIO | Done |

### Planned

| Benchmark | Backend | Notes |
|-----------|---------|-------|
| Streaming record write round-trip | S3 (LocalStack/MinIO) | `StreamWriteRecords` path |
| Compressed write round-trip | S3 (LocalStack/MinIO) | gzip/zstd codec variants |
| Volume commit round-trip | S3 (LocalStack/MinIO) | Multi-block Volume write path |

## Running benchmarks

`task bench` runs **all** benchmarks — in-memory and S3 integration:

```bash
task s3:up      # start LocalStack + MinIO
task bench      # run all benchmarks
task s3:down    # stop services
```

In-memory benchmarks always run. S3 benchmarks require services to be up first.

### Manual invocation

```bash
# All benchmarks (with S3 services running)
LODE_S3_TESTS=1 go test -bench=. -benchmem -tags=integration -run=^$ ./...

# In-memory only
go test -bench=. -benchmem -run=^$ ./lode/...
```
