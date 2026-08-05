package badgerstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// BenchmarkDurableAppend measures fsync-bound append throughput at a fixed
// client concurrency, one stream per client, 256-byte payloads. The inflight
// sub-benchmarks sweep the committer's concurrent-commit bound.
//
// CAUTION: b.TempDir usually lands on tmpfs, where fsync is nearly free. That
// inverts the inflight comparison: concurrent commits win here but lose on
// real disks, where a serialized committer batches adaptively and fsyncs less
// (see defaultAppendCommitMaxInFlight). Treat cross-inflight results as valid
// only on a filesystem with real fsync cost; end-to-end numbers need ds-bench
// against a disk-backed data dir.
//
// ns/op is wall time per operation across all goroutines, so
// ops/s = 1e9 / (ns/op).
func BenchmarkDurableAppend(b *testing.B) {
	for _, concurrency := range []int{256} {
		for _, inFlight := range []int{1, 2, 4, 8, 16} {
			name := fmt.Sprintf("c%d/inflight%d", concurrency, inFlight)
			b.Run(name, func(b *testing.B) {
				benchmarkDurableAppend(b, concurrency, inFlight)
			})
		}
	}
}

func benchmarkDurableAppend(b *testing.B, concurrency, maxInFlight int) {
	s, err := New(Options{
		Dir:             b.TempDir(),
		SLogger:         quietSLog(),
		Logger:          &quietLogger{},
		GCInterval:      -1,
		CleanupInterval: -1,
		ReapInterval:    time.Hour,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	<-s.initialReapDone
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})

	// Swap in a committer with the requested in-flight bound. Nothing has been
	// submitted yet, so replacing it is race-free.
	s.appendCommits.close()
	<-s.appendCommits.done
	cfg := defaultAppendCommitConfig()
	cfg.maxInFlight = maxInFlight
	s.appendCommits = newAppendCommitter(s, cfg)
	go s.appendCommits.run()

	ctx := context.Background()
	streamIDs := make([]string, concurrency)
	for i := range streamIDs {
		streamIDs[i] = fmt.Sprintf("bench-%04d", i)
		if _, err := s.Create(ctx, streamIDs[i], durablestream.StreamConfig{ContentType: "application/octet-stream"}); err != nil {
			b.Fatalf("create %q: %v", streamIDs[i], err)
		}
	}
	payload := make([]byte, 256)

	b.ResetTimer()
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := range concurrency {
		wg.Go(func() {
			streamID := streamIDs[w]
			for {
				n := next.Add(1)
				if n > int64(b.N) {
					return
				}
				if _, err := s.Append(ctx, streamID, payload, ""); err != nil {
					b.Errorf("append %q: %v", streamID, err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()

	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
	b.ReportMetric(float64(s.appendCommits.transactionAttempts.Load()), "txns")
}
