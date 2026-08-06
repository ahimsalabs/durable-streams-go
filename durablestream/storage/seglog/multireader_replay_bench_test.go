package seglog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"golang.org/x/sys/unix"
)

const (
	replayRecordBytes = 2 << 10
	replayStreamBytes = 64 << 20
	replayReadBytes   = 1 << 20
)

// BenchmarkMultiReaderReplay measures full, concurrent HTTP replays from one
// fully materialized sealed stream. Set SEGLOG_REPLAY_BENCH_COLD=1 to add
// cold-page-cache cells (Linux and permission to write drop_caches required).
// SEGLOG_REPLAY_BENCH_SMOKE=1 reduces the fixture and fan-out for validation.
func BenchmarkMultiReaderReplay(b *testing.B) {
	streamBytes, readers := replayStreamBytes, []int{1, 10, 100, 1000}
	if os.Getenv("SEGLOG_REPLAY_BENCH_SMOKE") == "1" {
		streamBytes, readers = 1<<20, []int{1}
	}
	s := newReplayFixture(b, streamBytes)
	for _, spans := range []bool{false, true} {
		mode := map[bool]string{false: "copied", true: "spans"}[spans]
		server := newReplayServer(b, s, spans)
		for _, n := range readers {
			b.Run(fmt.Sprintf("%s/warm/readers=%d", mode, n), func(b *testing.B) {
				benchmarkReplayReaders(b, server, n, streamBytes, false)
			})
			b.Run(fmt.Sprintf("%s/cold/readers=%d", mode, n), func(b *testing.B) {
				if os.Getenv("SEGLOG_REPLAY_BENCH_COLD") != "1" {
					b.Skip("cold cache disabled; set SEGLOG_REPLAY_BENCH_COLD=1")
				}
				benchmarkReplayReaders(b, server, n, streamBytes, true)
			})
		}
	}
}

func newReplayFixture(b *testing.B, streamBytes int) *Storage {
	b.Helper()
	opts := benchmarkOptions(b.TempDir())
	opts.Partitions, opts.SyncWrites, opts.GroupMaxBytes = 1, SyncWritesDisabled, 1<<20
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: int64(replayStreamBytes + (1 << 20))}
	s := benchmarkOpen(b, opts)
	benchmarkCreate(b, s, "replay")
	record := make([]byte, replayRecordBytes)
	batch := make([][]byte, 128)
	for i := range batch {
		batch[i] = record
	}
	remaining := streamBytes / replayRecordBytes
	for remaining > 0 {
		n := min(remaining, len(batch))
		if _, err := s.AppendBatch(context.Background(), "replay", batch[:n], ""); err != nil {
			b.Fatalf("build replay fixture: %v", err)
		}
		remaining -= n
	}
	sealSpanTestStream(b, s, "replay")
	return s
}

func newReplayServer(b *testing.B, s *Storage, spans bool) *httptest.Server {
	b.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var offset durablestream.Offset
		for {
			if spans {
				res, err := s.ReadSpans(r.Context(), "replay", offset, replayReadBytes)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				for _, span := range res.Spans {
					_, writeErr := span.WriteTo(w)
					closeErr := span.Close()
					if writeErr != nil || closeErr != nil {
						return
					}
				}
				offset = res.NextOffset
				if offset == res.TailOffset {
					return
				}
				continue
			}
			res, err := s.Read(r.Context(), "replay", offset, replayReadBytes)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, msg := range res.Messages {
				if _, err := w.Write(msg.Data); err != nil {
					return
				}
			}
			offset = res.NextOffset
			if offset == res.TailOffset {
				return
			}
		}
	})
	server := httptest.NewServer(h)
	b.Cleanup(server.Close)
	return server
}

func benchmarkReplayReaders(b *testing.B, server *httptest.Server, readers, streamBytes int, cold bool) {
	client := server.Client()
	client.Timeout = 5 * time.Minute
	latencies := make([]time.Duration, 0, b.N*readers)
	// Subbenchmarks share this process. Return heap pages from the preceding
	// reader-count cell before measuring RSS so a large copied-path cell does
	// not become the next span cell's reported baseline.
	debug.FreeOSMemory()
	peakRSS := processRSSBytes()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if cold {
			b.StopTimer()
			if err := dropLinuxPageCache(); err != nil {
				b.Skipf("cold cache unavailable: %v", err)
			}
			b.StartTimer()
		}
		start := make(chan struct{})
		durations := make([]time.Duration, readers)
		errs := make([]error, readers)
		var wg sync.WaitGroup
		for i := range readers {
			wg.Go(func() {
				<-start
				begin := time.Now()
				resp, err := client.Get(server.URL)
				if err == nil {
					var copied int64
					copied, err = io.Copy(io.Discard, resp.Body)
					closeErr := resp.Body.Close()
					if err == nil {
						err = closeErr
					}
					if err == nil && copied != int64(streamBytes) {
						err = fmt.Errorf("replayed %d bytes, want %d", copied, streamBytes)
					}
				}
				durations[i], errs[i] = time.Since(begin), err
			})
		}
		close(start)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
		latencies = append(latencies, durations...)
		peakRSS = max(peakRSS, processRSSBytes())
	}
	b.StopTimer()
	reportReplayMetrics(b, latencies, int64(b.N*readers*streamBytes), peakRSS)
}

func reportReplayMetrics(b *testing.B, durations []time.Duration, bytes int64, rss int64) {
	b.Helper()
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(p int) time.Duration { return durations[(len(durations)*p-1)/100] }
	b.ReportMetric(float64(bytes)/(1<<20)/b.Elapsed().Seconds(), "aggregate-MB/s")
	b.ReportMetric(float64(percentile(50))/float64(time.Millisecond), "reader-p50-ms")
	b.ReportMetric(float64(percentile(99))/float64(time.Millisecond), "reader-p99-ms")
	if rss > 0 {
		b.ReportMetric(float64(rss)/(1<<20), "server-RSS-MiB")
	}
}

func dropLinuxPageCache() error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("requires Linux")
	}
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open /proc/sys/vm/drop_caches (root or delegated permission required): %w", err)
	}
	defer f.Close()
	unix.Sync()
	if _, err := io.WriteString(f, "3\n"); err != nil {
		return fmt.Errorf("write /proc/sys/vm/drop_caches (root or delegated permission required): %w", err)
	}
	return nil
}

func processRSSBytes() int64 {
	f, err := os.Open("/proc/self/statm")
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return 0
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}

// BenchmarkMultiReaderReplayMixed runs 100 historical catch-up readers while
// one appender publishes 1 MiB and 100 followers consume that live suffix.
func BenchmarkMultiReaderReplayMixed(b *testing.B) {
	streamBytes, catchups, followers := replayStreamBytes, 100, 100
	if os.Getenv("SEGLOG_REPLAY_BENCH_SMOKE") == "1" {
		streamBytes, catchups, followers = 1<<20, 2, 2
	}
	var durations []time.Duration
	var peakRSS int64
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		s := newReplayFixture(b, streamBytes)
		startTail, err := s.Read(context.Background(), "replay", "", 1)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		iterationDurations, rss := benchmarkMixedIteration(b, s, catchups, followers, startTail.TailOffset, streamBytes)
		durations = append(durations, iterationDurations...)
		peakRSS = max(peakRSS, rss)
		b.StopTimer()
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
	reportReplayMetrics(b, durations, int64(b.N*(catchups*streamBytes+followers*(1<<20))), peakRSS)
}

func benchmarkMixedIteration(b *testing.B, s *Storage, catchups, followers int, liveOffset durablestream.Offset, streamBytes int) ([]time.Duration, int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const liveRecords = (1 << 20) / replayRecordBytes
	start := make(chan struct{})
	type result struct {
		duration time.Duration
		err      error
	}
	results := make(chan result, catchups+followers+1)
	var wg sync.WaitGroup
	for range catchups {
		wg.Go(func() {
			<-start
			duration, err := replayCopied(ctx, s, "", int64(streamBytes))
			results <- result{duration, err}
		})
	}
	for range followers {
		wg.Go(func() {
			<-start
			duration, err := replayCopied(ctx, s, liveOffset, 1<<20)
			results <- result{duration, err}
		})
	}
	wg.Go(func() {
		<-start
		payload := make([]byte, replayRecordBytes)
		for range liveRecords {
			if _, err := s.Append(ctx, "replay", payload, ""); err != nil {
				results <- result{err: err}
				return
			}
		}
		results <- result{}
	})
	close(start)
	wg.Wait()
	close(results)
	durations := make([]time.Duration, 0, catchups+followers)
	for result := range results {
		if result.err != nil {
			b.Fatal(result.err)
		}
		if result.duration != 0 {
			durations = append(durations, result.duration)
		}
	}
	return durations, processRSSBytes()
}

func replayCopied(ctx context.Context, s *Storage, offset durablestream.Offset, want int64) (time.Duration, error) {
	start := time.Now()
	var read int64
	for read < want {
		res, err := s.WaitForData(ctx, "replay", offset, replayReadBytes)
		if err != nil {
			return 0, err
		}
		for _, msg := range res.Messages {
			read += int64(len(msg.Data))
		}
		offset = res.NextOffset
	}
	return time.Since(start), nil
}
