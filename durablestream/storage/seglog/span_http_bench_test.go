package seglog

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// BenchmarkSpanHTTP compares copied and span-backed 64 KiB HTTP responses
// over plaintext and TLS. ns/op is the end-to-end CPU-ish benchmark cost;
// ReportMetric adds application payload throughput.
func BenchmarkSpanHTTP(b *testing.B) {
	for _, spans := range []bool{false, true} {
		for _, tls := range []bool{false, true} {
			name := map[bool]string{false: "copied", true: "span"}[spans] + "/" + map[bool]string{false: "plaintext", true: "TLS"}[tls]
			b.Run(name, func(b *testing.B) { benchmarkSpanHTTPCell(b, spans, tls) })
		}
	}
}

func benchmarkSpanHTTPCell(b *testing.B, spans, tls bool) {
	opts := benchmarkOptions(b.TempDir())
	opts.Partitions, opts.SyncWrites = 1, SyncWritesDisabled
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "stream")
	payload := make([]byte, 2<<10)
	for range 32 {
		if _, err := s.Append(context.Background(), "stream", payload, ""); err != nil {
			b.Fatal(err)
		}
	}
	sealSpanTestStream(b, s, "stream")

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if spans {
			res, err := s.ReadSpans(r.Context(), "stream", "", 64<<10)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, span := range res.Spans {
				_, err = span.WriteTo(w)
				_ = span.Close()
				if err != nil {
					return
				}
			}
			return
		}
		res, err := s.Read(r.Context(), "stream", "", 64<<10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, msg := range res.Messages {
			_, _ = w.Write(msg.Data)
		}
	})
	server := httptest.NewUnstartedServer(h)
	if tls {
		server.StartTLS()
	} else {
		server.Start()
	}
	b.Cleanup(server.Close)
	client := server.Client()
	b.SetBytes(64 << 10)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		resp, err := client.Get(server.URL)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

var _ durablestream.SpanReadStorage = (*Storage)(nil)
