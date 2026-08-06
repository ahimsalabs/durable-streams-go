package seglog

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

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
