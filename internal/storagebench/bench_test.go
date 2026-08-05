// Package storagebench compares the durable disk storage backends as stream
// cardinality grows. Benchmarks use each backend's durable-write and operational
// defaults: they do not disable maintenance work or tune one backend in ways
// unavailable to the other.
package storagebench

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/badgerstore"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/seglog"
)

type backend struct {
	name string
	open func(tb testing.TB, dir string) durablestream.Storage
}

type discardBadgerLogger struct{}

func (discardBadgerLogger) Errorf(string, ...interface{})   {}
func (discardBadgerLogger) Warningf(string, ...interface{}) {}
func (discardBadgerLogger) Infof(string, ...interface{})    {}
func (discardBadgerLogger) Debugf(string, ...interface{})   {}

func backends() []backend {
	return []backend{
		{
			name: "badger",
			open: func(tb testing.TB, dir string) durablestream.Storage {
				tb.Helper()
				store, err := badgerstore.New(badgerstore.Options{
					Dir:     dir,
					SLogger: slog.New(slog.DiscardHandler),
					Logger:  discardBadgerLogger{},
				})
				if err != nil {
					tb.Fatalf("open badgerstore: %v", err)
				}
				tb.Cleanup(func() {
					if err := store.Close(); err != nil {
						tb.Errorf("close badgerstore: %v", err)
					}
				})
				return store
			},
		},
		{
			name: "seglog",
			open: func(tb testing.TB, dir string) durablestream.Storage {
				tb.Helper()
				store, err := seglog.New(seglog.Options{
					Dir:     dir,
					SLogger: slog.New(slog.DiscardHandler),
				})
				if err != nil {
					tb.Fatalf("open seglog: %v", err)
				}
				tb.Cleanup(func() {
					if err := store.Close(); err != nil {
						tb.Errorf("close seglog: %v", err)
					}
				})
				return store
			},
		},
	}
}

func BenchmarkCreate(b *testing.B) {
	for _, backend := range backends() {
		b.Run(backend.name, func(b *testing.B) {
			for _, streams := range []int{1_000, 10_000, 100_000} {
				b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
					if streams == 100_000 && testing.Short() {
						b.Skip("100k-stream fixture skipped in short mode")
					}

					root := b.TempDir()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						dir, err := os.MkdirTemp(root, "store-")
						if err != nil {
							b.Fatalf("create store directory: %v", err)
						}
						store := backend.open(b, dir)
						b.StartTimer()

						createStreams(b, store, streams)

						b.StopTimer()
						closeStore(b, store)
						if err := os.RemoveAll(dir); err != nil {
							b.Fatalf("remove store directory: %v", err)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*streams), "us/stream")
				})
			}
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	for _, backend := range backends() {
		b.Run(backend.name, func(b *testing.B) {
			for _, streams := range []int{1_000, 10_000, 100_000} {
				b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
					if streams == 100_000 && testing.Short() {
						b.Skip("100k-stream fixture skipped in short mode")
					}

					dir := b.TempDir()
					store := backend.open(b, dir)
					createStreams(b, store, streams)
					payload := payload256()
					var operation atomic.Uint64
					var latencyMu sync.Mutex
					latencies := make([]time.Duration, 0, b.N)

					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						local := make([]time.Duration, 0, b.N/runtime.GOMAXPROCS(0)+1)
						for pb.Next() {
							stream := int(operation.Add(1)-1) % streams
							start := time.Now()
							if _, err := store.Append(b.Context(), streamID(stream), payload, ""); err != nil {
								b.Errorf("append to stream %d: %v", stream, err)
								return
							}
							local = append(local, time.Since(start))
						}
						latencyMu.Lock()
						latencies = append(latencies, local...)
						latencyMu.Unlock()
					})
					b.StopTimer()

					reportLatencyPercentiles(b, latencies)
					bytes := directoryBytes(b, dir)
					// Approximate: files may be sparse/preallocated, Badger GC is periodic,
					// and seglog's asynchronous materializer may lag the committed WAL.
					b.ReportMetric(float64(bytes)/float64(streams), "disk-bytes/stream")
					runtime.GC()
					var memory runtime.MemStats
					runtime.ReadMemStats(&memory)
					// Approximate: HeapAlloc includes the benchmark process and backend
					// maintenance state, not only memory attributable to these streams.
					b.ReportMetric(float64(memory.HeapAlloc)/float64(streams), "heap-bytes/stream")
				})
			}
		})
	}
}

func BenchmarkReadTail(b *testing.B) {
	const streams = 10_000
	for _, backend := range backends() {
		b.Run(backend.name, func(b *testing.B) {
			b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
				store := backend.open(b, b.TempDir())
				createStreams(b, store, streams)
				payload := payload256()
				for stream := range streams {
					for range 8 {
						appendMessage(b, store, streamID(stream), payload)
					}
				}
				if backend.name == "seglog" {
					// Seglog publishes commits immediately but materializes WAL records
					// asynchronously; allow its default materializer to catch up so this
					// does not accidentally measure fixture timing.
					time.Sleep(200 * time.Millisecond)
				}

				var operation uint64
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					stream := int(operation % streams)
					operation++
					result, err := store.Read(b.Context(), streamID(stream), "", 0)
					if err != nil {
						b.Fatalf("read stream %d: %v", stream, err)
					}
					if len(result.Messages) != 8 {
						b.Fatalf("read stream %d returned %d messages, want 8", stream, len(result.Messages))
					}
				}
			})
		})
	}
}

func BenchmarkReopen(b *testing.B) {
	for _, backend := range backends() {
		b.Run(backend.name, func(b *testing.B) {
			for _, streams := range []int{1_000, 10_000} {
				b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
					dir := b.TempDir()
					store := backend.open(b, dir)
					createStreams(b, store, streams)
					payload := payload256()
					for stream := range streams {
						for range 4 {
							appendMessage(b, store, streamID(stream), payload)
						}
					}
					expected, err := store.Head(b.Context(), streamID(0))
					if err != nil {
						b.Fatalf("head fixture stream: %v", err)
					}
					closeStore(b, store)

					assertReopenTail(b, backend, dir, expected.NextOffset)

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						store := backend.open(b, dir)
						if _, err := store.Head(b.Context(), streamID(0)); err != nil {
							b.Fatalf("head after reopen: %v", err)
						}
						closeStore(b, store)
					}
				})
			}
		})
	}
}

func createStreams(tb testing.TB, store durablestream.Storage, count int) {
	tb.Helper()
	for stream := range count {
		created, err := store.Create(tb.Context(), streamID(stream), durablestream.StreamConfig{})
		if err != nil {
			tb.Fatalf("create stream %d: %v", stream, err)
		}
		if !created {
			tb.Fatalf("stream %d already existed in fresh fixture", stream)
		}
	}
}

func appendMessage(tb testing.TB, store durablestream.Storage, stream string, payload []byte) {
	tb.Helper()
	if _, err := store.Append(tb.Context(), stream, payload, ""); err != nil {
		tb.Fatalf("append to %q: %v", stream, err)
	}
}

func closeStore(tb testing.TB, store durablestream.Storage) {
	tb.Helper()
	if err := store.Close(); err != nil {
		tb.Fatalf("close store: %v", err)
	}
}

func assertReopenTail(tb testing.TB, backend backend, dir string, expected durablestream.Offset) {
	tb.Helper()
	store := backend.open(tb, dir)
	info, err := store.Head(tb.Context(), streamID(0))
	if err != nil {
		tb.Fatalf("head fixture after reopen: %v", err)
	}
	if info.NextOffset != expected {
		tb.Errorf("tail after reopen = %q, want %q", info.NextOffset, expected)
	}
	closeStore(tb, store)
}

func reportLatencyPercentiles(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(percent int) time.Duration {
		return samples[(len(samples)*percent-1)/100]
	}
	b.ReportMetric(float64(percentile(50))/float64(time.Microsecond), "p50-us")
	b.ReportMetric(float64(percentile(99))/float64(time.Microsecond), "p99-us")
}

func directoryBytes(tb testing.TB, root string) int64 {
	tb.Helper()
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("measure directory %q: %v", root, err)
	}
	return total
}

func streamID(stream int) string {
	return fmt.Sprintf("s-%06d", stream)
}

func payload256() []byte {
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}
