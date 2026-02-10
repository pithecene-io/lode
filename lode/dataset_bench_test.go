package lode

import (
	"context"
	"io"
	"testing"
	"time"
)

// latencyStore wraps a Store and injects artificial latency on List and Get
// calls. This simulates remote store behavior (S3/R2) where each API call
// costs 50-100ms, making O(n) store scans per write visible as wall-clock
// degradation.
type latencyStore struct {
	inner   Store
	latency time.Duration
}

func (s *latencyStore) Put(ctx context.Context, path string, r io.Reader) error {
	return s.inner.Put(ctx, path, r)
}

func (s *latencyStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	time.Sleep(s.latency)
	return s.inner.Get(ctx, path)
}

func (s *latencyStore) Exists(ctx context.Context, path string) (bool, error) {
	return s.inner.Exists(ctx, path)
}

func (s *latencyStore) List(ctx context.Context, prefix string) ([]string, error) {
	time.Sleep(s.latency)
	return s.inner.List(ctx, prefix)
}

func (s *latencyStore) Delete(ctx context.Context, path string) error {
	return s.inner.Delete(ctx, path)
}

func (s *latencyStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return s.inner.ReadRange(ctx, path, offset, length)
}

func (s *latencyStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	return s.inner.ReaderAt(ctx, path)
}

// BenchmarkDataset_SequentialWrites measures the cost of N sequential writes.
//
// With parent-ID caching (issue #108), writes 2..N resolve the parent in O(1).
// Without caching, each write would call Latest() → Snapshots() → List + N×Get,
// making total cost O(n²) and wall-clock time proportional to n² × store latency.
//
// The 1ms simulated latency makes quadratic regression obvious: 20 writes would
// take ~400ms uncached (20×20×1ms) vs ~20ms cached (20×1ms cold start only).
func BenchmarkDataset_SequentialWrites(b *testing.B) {
	const writeCount = 20

	ls := &latencyStore{
		inner:   NewMemory(),
		latency: 1 * time.Millisecond,
	}
	factory := func() (Store, error) { return ls, nil }

	ds, err := NewDataset("bench-ds", factory, WithCodec(NewJSONLCodec()))
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	data := R(D{"key": "value"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < writeCount; j++ {
			if _, err := ds.Write(ctx, data, Metadata{}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkDataset_SequentialWrites_StoreCallCount verifies that store.List
// calls remain O(1) regardless of snapshot count. This is a correctness
// benchmark — it asserts on call counts rather than timing.
func BenchmarkDataset_SequentialWrites_StoreCallCount(b *testing.B) {
	fs := newFaultStore(NewMemory())
	factory := newFaultStoreFactory(fs)

	ds, err := NewDataset("bench-ds", factory, WithCodec(NewJSONLCodec()))
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	data := R(D{"key": "value"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs.Reset()

		// Cold-start write (will call List)
		if _, err := ds.Write(ctx, data, Metadata{}); err != nil {
			b.Fatal(err)
		}
		coldListCalls := len(fs.ListCalls())

		// Subsequent writes (should NOT call List)
		for j := 1; j < 20; j++ {
			if _, err := ds.Write(ctx, data, Metadata{}); err != nil {
				b.Fatal(err)
			}
		}

		totalListCalls := len(fs.ListCalls())
		if totalListCalls != coldListCalls {
			b.Fatalf("expected %d List calls (cold start only), got %d after 20 writes",
				coldListCalls, totalListCalls)
		}
	}
}
