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

### Statistical comparison

`task bench:stat` runs all benchmarks with `-count=10` and pipes results through
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) for statistical
summaries. Raw output is saved to `bench-inmemory.txt` and `bench-s3.txt`.

```bash
task s3:up          # start LocalStack + MinIO
task bench:stat     # -count=10, benchstat summary
task s3:down        # stop services
```

To compare two runs (e.g. before and after a change):

```bash
# Save baseline
mv bench-inmemory.txt baseline-inmemory.txt

# Make changes, re-run
task bench:stat

# Compare
task bench:compare -- baseline-inmemory.txt bench-inmemory.txt
```

> **Note:** benchstat requires multiple samples (`-count=10`) to produce
> meaningful confidence intervals. Single-sample comparisons will show `~`
> (no significant difference) for most benchmarks.

### Manual invocation

```bash
# In-memory only
go test -bench=. -benchmem -run=^$ ./lode/...

# S3 benchmarks (with services running)
go test -bench=. -benchmem -run=^$ ./lode/s3/... -integration
```
