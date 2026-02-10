# Benchmarks

Central index of all Lode benchmarks.

## Results

Collected on AMD Ryzen 9 5900XT (linux/amd64), Go 1.25, Docker-local S3 backends.

### In-memory

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `BenchmarkDataset_SequentialWrites` | 142,114 | 98,287 | 680 |
| `BenchmarkDataset_SequentialWrites_StoreCallCount` | 144,832 | 100,218 | 687 |

### S3 (Docker-local)

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `BenchmarkS3_WriteRoundTrip/LocalStack` | 5,572,537 | 367,305 | 2,969 |
| `BenchmarkS3_WriteRoundTrip/MinIO` | 11,680,989 | 371,071 | 3,040 |

> **Note:** S3 numbers reflect Docker-local round-trip latency, not production S3.
> In-memory benchmarks inject 10 µs simulated store latency.
> These results are informational — use them for relative comparison, not absolute targets.

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
# In-memory only
go test -bench=. -benchmem -run=^$ ./lode/...

# S3 benchmarks (with services running)
go test -bench=. -benchmem -run=^$ ./lode/s3/... -integration
```
