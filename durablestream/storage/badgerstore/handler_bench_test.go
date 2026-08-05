package badgerstore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// BenchmarkHandlerDurableAppend drives the full HTTP handler against durable
// Badger storage at fixed client concurrency, exercising the complete per-
// append path: routing, header parsing, Head/Touch (or fused TouchHead), and
// the group-committed append. This is the closest in-process analogue to the
// ds-bench multi-stream write workload.
func BenchmarkHandlerDurableAppend(b *testing.B) {
	// hideTouchHead narrows the storage to the base interface so the handler
	// falls back to separate Head+Touch calls, isolating the fused path's win.
	type hideTouchHead struct{ durablestream.Storage }
	b.Run("fused", func(b *testing.B) {
		benchmarkHandlerDurableAppend(b, func(s *Storage) durablestream.Storage { return s })
	})
	b.Run("separate", func(b *testing.B) {
		benchmarkHandlerDurableAppend(b, func(s *Storage) durablestream.Storage { return hideTouchHead{s} })
	})
}

func benchmarkHandlerDurableAppend(b *testing.B, wrap func(*Storage) durablestream.Storage) {
	const concurrency = 256

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

	handler := durablestream.NewHandler(wrap(s), nil)
	server := httptest.NewServer(handler)
	b.Cleanup(server.Close)

	ctx := context.Background()
	streamIDs := make([]string, concurrency)
	for i := range streamIDs {
		streamIDs[i] = fmt.Sprintf("/bench-%04d", i)
		if _, err := s.Create(ctx, streamIDs[i], durablestream.StreamConfig{ContentType: "application/octet-stream"}); err != nil {
			b.Fatalf("create %q: %v", streamIDs[i], err)
		}
	}
	payload := make([]byte, 256)
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
	}}

	b.ResetTimer()
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := range concurrency {
		wg.Go(func() {
			url := server.URL + streamIDs[w]
			for {
				n := next.Add(1)
				if n > int64(b.N) {
					return
				}
				req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
				if err != nil {
					b.Errorf("new request: %v", err)
					return
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, err := client.Do(req)
				if err != nil {
					b.Errorf("append %q: %v", streamIDs[w], err)
					return
				}
				if resp.StatusCode != http.StatusNoContent {
					b.Errorf("append %q: status %d", streamIDs[w], resp.StatusCode)
				}
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()
	b.StopTimer()

	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}
