package seglog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

type benchmarkErrorHandler struct {
	b *testing.B
}

func (h benchmarkErrorHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (h benchmarkErrorHandler) Handle(_ context.Context, record slog.Record) error {
	h.b.Errorf("seglog background error: %s", record.Message)
	return nil
}

func (h benchmarkErrorHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h benchmarkErrorHandler) WithGroup(_ string) slog.Handler      { return h }

// BenchmarkAppendPartitionSweep isolates the flush-sharing cost of partition
// count: the same 32-way durable append workload over 10k streams, with the
// WAL split 1/4/32 ways. On flush-honoring devices concurrent fdatasyncs
// largely serialize, so scattering W in-flight appends across P partition
// WALs divides the appends sharing each flush by P. Run with TMPDIR on a
// real disk; on tmpfs every variant measures CPU only.
func BenchmarkAppendPartitionSweep(b *testing.B) {
	const streams = 10000
	for _, parts := range []int{1, 4, 32} {
		for _, mat := range []time.Duration{25 * time.Millisecond, -1} {
			matName := "on"
			if mat == -1 {
				matName = "off"
			}
			b.Run(fmt.Sprintf("partitions=%d/materializer=%s", parts, matName), func(b *testing.B) {
				opts := benchmarkOptions(b.TempDir())
				opts.Partitions = parts
				opts.MaterializeInterval = mat
				s, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				ids := make([]string, streams)
				for i := range ids {
					ids[i] = fmt.Sprintf("s-%06d", i)
					if _, err := s.Create(ctx, ids[i], durablestream.StreamConfig{}); err != nil {
						b.Fatal(err)
					}
				}
				payload := make([]byte, 256)
				var counter atomic.Int64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						id := ids[int(counter.Add(1))%streams]
						if _, err := s.Append(ctx, id, payload, ""); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.StopTimer()
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}

// BenchmarkAppendBackgroundScheduling measures durable append latency while
// materialization and checkpoint work run. Run it for at least five seconds so
// the default cell includes the three-second checkpoint age. TMPDIR must be on
// the device under test; tmpfs does not measure durable-write latency.
func BenchmarkAppendBackgroundScheduling(b *testing.B) {
	const streams = 128
	scenarios := []struct {
		name      string
		configure func(*Options)
	}{
		{
			name: "off",
			configure: func(opts *Options) {
				opts.MaterializeMaxAge = -1
			},
		},
		{
			name: "default",
			configure: func(opts *Options) {
				opts.MaterializeMaxAge = DefaultMaterializeMaxAge
				opts.CheckpointMaxAge = DefaultCheckpointMaxAge
			},
		},
		{
			name: "checkpoint-pressure",
			configure: func(opts *Options) {
				opts.MaterializeBytes = 64 << 10
				opts.MaterializeMaxAge = 10 * time.Millisecond
				opts.CheckpointBytes = 256 << 10
				opts.CheckpointMaxAge = 50 * time.Millisecond
			},
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			opts := benchmarkOptions(b.TempDir())
			opts.Partitions = 1
			opts.SLogger = slog.New(benchmarkErrorHandler{b: b})
			opts.MaterializeInterval = 0
			opts.DefaultSegmentPolicy.MaxOpenAge = 0
			scenario.configure(&opts)
			s := benchmarkOpen(b, opts)
			ctx := b.Context()
			ids := make([]string, streams)
			for i := range ids {
				ids[i] = fmt.Sprintf("background-%03d", i)
				benchmarkCreate(b, s, ids[i])
			}
			if err := s.materializeRoundResult(s.parts[0]); err != nil {
				b.Fatalf("materialize setup: %v", err)
			}

			payload := make([]byte, 256)
			before := s.Stats()
			var next atomic.Uint64
			var latencyMu sync.Mutex
			latencies := make([]time.Duration, 0, b.N)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				local := make([]time.Duration, 0, 256)
				for pb.Next() {
					id := ids[int(next.Add(1)-1)%len(ids)]
					start := time.Now()
					if _, err := s.Append(ctx, id, payload, ""); err != nil {
						b.Error(err)
						return
					}
					local = append(local, time.Since(start))
				}
				latencyMu.Lock()
				latencies = append(latencies, local...)
				latencyMu.Unlock()
			})
			b.StopTimer()

			after := s.Stats()
			seconds := b.Elapsed().Seconds()
			reportLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(after.CheckpointRounds-before.CheckpointRounds)/seconds, "checkpoint-attempts/s")
			b.ReportMetric(float64(after.MaterializerSyncs-before.MaterializerSyncs)/seconds, "file-syncs/s")
			b.ReportMetric(float64(after.SyncfsCalls-before.SyncfsCalls)/seconds, "syncfs/s")
			b.ReportMetric(float64(after.UnmaterializedWALBytes), "unmaterialized-B")
			b.ReportMetric(float64(after.MaterializedNotCheckpointedBytes), "uncheckpointed-B")
			b.ReportMetric(float64(after.UnreclaimedWALBytes), "unreclaimed-B")
		})
	}
}
