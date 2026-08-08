package seglog

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func benchmarkOptions(dir string) Options {
	return Options{
		Dir:                  dir,
		Partitions:           8,
		MaxMessageSize:       1 << 20,
		WALSegmentBytes:      64 << 20,
		QueueDepth:           4096,
		SyncWrites:           SyncWritesEnabled,
		MaterializeInterval:  -1,
		RetentionInterval:    -1,
		DefaultSegmentPolicy: SegmentPolicy{TargetBytes: 8 << 20},
		FDCacheSize:          384,
	}
}

func benchmarkOpen(b *testing.B, opts Options) *Storage {
	b.Helper()
	s, err := New(opts)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return s
}

func benchmarkCreate(b *testing.B, s *Storage, id string) {
	b.Helper()
	if _, err := s.Create(context.Background(), id, durablestream.StreamConfig{}); err != nil {
		b.Fatalf("Create(%q): %v", id, err)
	}
}

func reportLatencyPercentiles(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(numerator, denominator int) time.Duration {
		return samples[(len(samples)*numerator-1)/denominator]
	}
	b.ReportMetric(float64(percentile(50, 100))/float64(time.Microsecond), "p50-us")
	b.ReportMetric(float64(percentile(95, 100))/float64(time.Microsecond), "p95-us")
	b.ReportMetric(float64(percentile(99, 100))/float64(time.Microsecond), "p99-us")
	b.ReportMetric(float64(percentile(999, 1000))/float64(time.Microsecond), "p99.9-us")
}

func BenchmarkAppendDurable(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 256)
	latencies := make([]time.Duration, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
		latencies[i] = time.Since(start)
	}
	b.StopTimer()
	reportLatencyPercentiles(b, latencies)
}

func BenchmarkAppendDurableParallel(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	opts.Partitions = 32
	s := benchmarkOpen(b, opts)
	streamIDs := make([]string, 1024)
	for i := range streamIDs {
		streamIDs[i] = fmt.Sprintf("stream-%04d", i)
		benchmarkCreate(b, s, streamIDs[i])
	}
	payload := make([]byte, 256)
	var workerID atomic.Uint64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		next := int(workerID.Add(1)-1) % len(streamIDs)
		local := make([]time.Duration, 0, 256)
		for pb.Next() {
			start := time.Now()
			if _, err := s.Append(context.Background(), streamIDs[next], payload, ""); err != nil {
				b.Error(err)
				return
			}
			local = append(local, time.Since(start))
			next = (next + 1) % len(streamIDs)
		}
		latencyMu.Lock()
		latencies = append(latencies, local...)
		latencyMu.Unlock()
	})
	b.StopTimer()
	reportLatencyPercentiles(b, latencies)
}

// BenchmarkAppendSingleStreamParallel measures concurrent producers appending
// 256-byte records to one stream: the single-stream throughput ceiling.
func BenchmarkAppendSingleStreamParallel(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 256)
	before := s.Stats()
	start := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	elapsed := time.Since(start).Seconds()
	after := s.Stats()
	acquisitions := after.CommitWaves - before.CommitWaves
	if acquisitions > 0 {
		b.ReportMetric(float64(acquisitions)/elapsed, "sync-acquisitions/s")
		b.ReportMetric(float64(b.N)/float64(acquisitions), "appends/sync-acquisition")
	}
	b.ReportMetric(float64(b.N)/elapsed, "appends/s")
}

func BenchmarkAppendNoSync(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	opts.SyncWrites = SyncWritesDisabled
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManyStreams(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("%dk", count/1000), func(b *testing.B) {
			if count == 100_000 && testing.Short() {
				b.Skip("100k-stream fixture skipped in short mode")
			}
			opts := benchmarkOptions(b.TempDir())
			opts.SyncWrites = SyncWritesDisabled // This benchmark measures stream cardinality, not fsync.
			s := benchmarkOpen(b, opts)
			ids := make([]string, count)
			for i := range ids {
				ids[i] = fmt.Sprintf("stream-%06d", i)
				benchmarkCreate(b, s, ids[i])
			}
			payload := make([]byte, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, id := range ids {
					if _, err := s.Append(context.Background(), id, payload, ""); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(count), "streams/op")
		})
	}
}

func BenchmarkReadHot(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 1024)
	for range 1024 {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
	var offset durablestream.Offset
	b.ReportAllocs()
	b.SetBytes(64 << 10)
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Read(context.Background(), "stream", offset, 64<<10)
		if err != nil {
			b.Fatal(err)
		}
		if res.NextOffset == res.TailOffset {
			offset = ""
		} else {
			offset = res.NextOffset
		}
	}
}

func BenchmarkReadColdSegments(b *testing.B) {
	dir := b.TempDir()
	opts := benchmarkOptions(dir)
	opts.Partitions = 1
	opts.MaterializeInterval = -1
	s, err := New(opts)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 1024)
	for range 4096 {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
	s.materializeRound(s.parts[0])
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	s = benchmarkOpen(b, opts)
	var offset durablestream.Offset
	b.ReportAllocs()
	b.SetBytes(64 << 10)
	b.ResetTimer()
	// Reopening removes WAL-resident state, but the OS page cache may remain warm.
	for b.Loop() {
		res, err := s.Read(context.Background(), "stream", offset, 64<<10)
		if err != nil {
			b.Fatal(err)
		}
		if res.NextOffset == res.TailOffset {
			offset = ""
		} else {
			offset = res.NextOffset
		}
	}
}

func BenchmarkRetentionDuringWrites(b *testing.B) {
	opts := benchmarkOptions(b.TempDir())
	opts.MaterializeInterval = time.Millisecond
	opts.RetentionInterval = time.Millisecond
	opts.DefaultSegmentPolicy.TargetBytes = 16 << 10
	opts.DefaultRetention = Retention{MaxBytes: 64 << 10}
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 1024)
	for range 256 {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecovery(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		clean bool
		count int
	}{{"clean", true, 4096}, {"largeSuffix", false, 16_384}} {
		b.Run(fixture.name, func(b *testing.B) {
			dir := b.TempDir()
			opts := benchmarkOptions(dir)
			opts.Partitions = 1
			opts.MaterializeInterval = -1
			s, err := New(opts)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkCreate(b, s, "stream")
			payload := make([]byte, 256)
			for range fixture.count {
				if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
					b.Fatal(err)
				}
			}
			if fixture.clean {
				s.materializeRound(s.parts[0])
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				recovered, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				if err := recovered.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
