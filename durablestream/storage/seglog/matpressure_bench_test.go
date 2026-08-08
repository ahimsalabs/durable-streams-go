package seglog

// BenchmarkMatPressure guards append throughput under materializer pressure:
// p32 durable appends over N streams while the materializer runs at varying
// intervals. Reports WAL fdatasync duration and rate, committer idle share,
// and checkpoint activity to separate device contention from pipeline stalls.
// History: materializer wake/timer churn and fdCache lock contention once
// collapsed the mat=25ms cells ~5x below the mat=off ceiling.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkMatPressure(b *testing.B) {
	for _, streams := range []int{1000, 10000} {
		for _, interval := range []time.Duration{-1, 250 * time.Millisecond, 25 * time.Millisecond} {
			name := "off"
			if interval > 0 {
				name = interval.String()
			}
			b.Run(fmt.Sprintf("streams=%d/mat=%s", streams, name), func(b *testing.B) {
				opts := benchmarkOptions(b.TempDir())
				opts.Partitions = 32
				opts.MaterializeInterval = interval
				s, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				ids := make([]string, streams)
				for i := range ids {
					ids[i] = fmt.Sprintf("s-%06d", i)
					benchmarkCreate(b, s, ids[i])
				}
				payload := make([]byte, 256)
				var counter atomic.Int64
				before := s.Stats()
				start := time.Now()
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
				elapsed := time.Since(start)
				after := s.Stats()
				groups := after.GroupsCommitted - before.GroupsCommitted
				syncNanos := after.CommitFdatasyncNanos - before.CommitFdatasyncNanos
				idleNanos := after.CommitterIdleNanos - before.CommitterIdleNanos
				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "appends/s")
				if groups > 0 {
					b.ReportMetric(float64(syncNanos)/float64(groups)/1e3, "fdatasync-us")
					b.ReportMetric(float64(groups)/elapsed.Seconds(), "wal-syncs/s")
					b.ReportMetric(float64(b.N)/float64(groups), "appends/sync")
				}
				// Idle share across the 32 committer goroutines: 1.0 means all
				// committers spent the whole window parked (no sync pressure).
				b.ReportMetric(float64(idleNanos)/float64(int64(opts.Partitions)*elapsed.Nanoseconds()), "committer-idle-frac")
				b.ReportMetric(float64(after.SyncfsCalls-before.SyncfsCalls)/elapsed.Seconds(), "syncfs/s")
				b.ReportMetric(float64(after.CheckpointRounds-before.CheckpointRounds)/elapsed.Seconds(), "ckpt/s")
				b.ReportMetric(float64(after.MaterializerSyncs-before.MaterializerSyncs)/elapsed.Seconds(), "mat-syncs/s")
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
