package badgerstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// silentLogger suppresses all Badger output during fuzz tests.
type silentLogger struct{}

func (l *silentLogger) Errorf(string, ...interface{})   {}
func (l *silentLogger) Warningf(string, ...interface{}) {}
func (l *silentLogger) Infof(string, ...interface{})    {}
func (l *silentLogger) Debugf(string, ...interface{})   {}

// silentSLogger returns an slog.Logger that discards all output.
func silentSLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newFuzzStorage creates a silent in-memory storage for fuzz tests.
func newFuzzStorage() (*Storage, error) {
	return New(Options{
		InMemory:        true,
		MaxMessageSize:  10 * 1024 * 1024,
		GCInterval:      -1,
		CleanupInterval: -1,
		Logger:          &silentLogger{},
		SLogger:         silentSLogger(),
	})
}

// FuzzParseFormatOffsetRoundtrip tests that formatOffset and parseOffset are inverses.
func FuzzParseFormatOffsetRoundtrip(f *testing.F) {
	// Seed with interesting values
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(255))
	f.Add(uint64(256))
	f.Add(uint64(65535))
	f.Add(uint64(1<<32 - 1))
	f.Add(uint64(1 << 32))
	f.Add(uint64(1<<64 - 1))

	f.Fuzz(func(t *testing.T, n uint64) {
		formatted := formatOffset(n)
		parsed, err := parseOffset(formatted)
		if err != nil {
			t.Errorf("parseOffset(%q) error: %v", formatted, err)
			return
		}
		if parsed != n {
			t.Errorf("roundtrip failed: %d -> %q -> %d", n, formatted, parsed)
		}
	})
}

// FuzzParseOffset tests parseOffset with arbitrary strings.
func FuzzParseOffset(f *testing.F) {
	// Seed with valid and invalid values
	f.Add("")
	f.Add("-1")
	f.Add("0000000000000000")
	f.Add("0000000000000001")
	f.Add("FFFFFFFFFFFFFFFF")
	f.Add("invalid")
	f.Add("ZZZZZZZZZZZZZZZZ")
	f.Add("000000000000000G")
	f.Add("too-long-offset-string")
	f.Add("short")

	f.Fuzz(func(t *testing.T, s string) {
		// parseOffset should never panic
		idx, err := parseOffset(durablestream.Offset(s))

		// If no error, verify the result is reasonable
		if err == nil {
			// For empty string or "-1", should return 0
			if s == "" || s == "-1" {
				if idx != 0 {
					t.Errorf("parseOffset(%q) = %d, want 0", s, idx)
				}
			}
		}
	})
}

// FuzzValidateStreamID tests validateStreamID with arbitrary strings.
func FuzzValidateStreamID(f *testing.F) {
	// Seed with valid and invalid stream IDs
	f.Add("valid-stream")
	f.Add("stream123")
	f.Add("stream_with_underscore")
	f.Add("") // invalid: empty
	f.Add("stream:with:colons") // invalid: contains colon
	f.Add("a")
	f.Add("verylongstreamidthatismorethan100characterslongverylongstreamidthatismorethan100characterslongverylongstreamid")

	f.Fuzz(func(t *testing.T, s string) {
		err := validateStreamID(s)

		// Check invariants:
		// 1. Empty string should always error
		if s == "" && err == nil {
			t.Error("validateStreamID(\"\") should return error")
		}

		// 2. String with colon should always error
		for _, r := range s {
			if r == ':' {
				if err == nil {
					t.Errorf("validateStreamID(%q) should return error (contains colon)", s)
				}
				return
			}
		}

		// 3. Non-empty string without colon should succeed
		if s != "" && err != nil {
			hasColon := false
			for _, r := range s {
				if r == ':' {
					hasColon = true
					break
				}
			}
			if !hasColon {
				t.Errorf("validateStreamID(%q) returned unexpected error: %v", s, err)
			}
		}
	})
}

// FuzzContentTypesMatch tests contentTypesMatch with arbitrary strings.
func FuzzContentTypesMatch(f *testing.F) {
	// Seed with realistic content types
	f.Add("text/plain", "text/plain")
	f.Add("TEXT/PLAIN", "text/plain")
	f.Add("application/json", "application/json")
	f.Add("text/plain; charset=utf-8", "text/plain")
	f.Add("application/json", "application/xml")
	f.Add("", "")
	f.Add("text/plain", "")
	f.Add("", "text/plain")

	f.Fuzz(func(t *testing.T, a, b string) {
		// contentTypesMatch should never panic
		result := contentTypesMatch(a, b)

		// Check symmetry: match(a, b) == match(b, a)
		reverse := contentTypesMatch(b, a)
		if result != reverse {
			t.Errorf("contentTypesMatch not symmetric: (%q, %q)=%v, (%q, %q)=%v",
				a, b, result, b, a, reverse)
		}

		// Check reflexivity: match(a, a) should be true
		if !contentTypesMatch(a, a) {
			t.Errorf("contentTypesMatch(%q, %q) should be true (reflexive)", a, a)
		}
	})
}

// FuzzStreamOperations tests stream operations with arbitrary data.
func FuzzStreamOperations(f *testing.F) {
	// Seed with various data patterns
	f.Add("stream1", []byte("hello world"), "seq001")
	f.Add("test", []byte{0x00, 0x01, 0x02, 0xff}, "")
	f.Add("binary", []byte{0xff, 0xfe, 0xfd}, "seq_abc")
	f.Add("unicode", []byte("日本語テスト"), "")
	f.Add("empty-seq", []byte("data"), "")
	f.Add("", []byte("data"), "")                     // invalid: empty stream ID
	f.Add("stream:colon", []byte("data"), "")         // invalid: contains colon
	f.Add("valid", []byte{}, "")                      // invalid: empty data
	f.Add("valid", make([]byte, 2*1024*1024), "seq")  // large data

	f.Fuzz(func(t *testing.T, streamID string, data []byte, seq string) {
		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		// Let the system validate - don't pre-filter
		_, err = s.Create(ctx, streamID, durablestream.StreamConfig{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			// System rejected invalid input - that's fine
			return
		}

		// Append data - system may reject empty data or oversized data
		offset, err := s.Append(ctx, streamID, data, seq)
		if err != nil {
			// Expected rejections: empty data, oversized, sequence conflicts
			return
		}

		// If append succeeded, data must not be empty
		if len(data) == 0 {
			t.Fatal("Append succeeded with empty data - should have been rejected")
		}

		// Read back and verify
		result, err := s.Read(ctx, streamID, "", 0)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if len(result.Messages) != 1 {
			t.Fatalf("expected 1 message, got %d", len(result.Messages))
		}

		if !bytes.Equal(result.Messages[0].Data, data) {
			t.Errorf("data mismatch: wrote %v, read %v", data, result.Messages[0].Data)
		}

		if result.Messages[0].Offset != offset {
			t.Errorf("offset mismatch: expected %s, got %s", offset, result.Messages[0].Offset)
		}

		// Head should return correct info
		info, err := s.Head(ctx, streamID)
		if err != nil {
			t.Fatalf("Head failed: %v", err)
		}
		if info.NextOffset != offset {
			t.Errorf("Head offset mismatch: expected %s, got %s", offset, info.NextOffset)
		}

		// Delete should work
		err = s.Delete(ctx, streamID)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// After delete, Head should return not found
		_, err = s.Head(ctx, streamID)
		if err != durablestream.ErrNotFound {
			t.Errorf("expected ErrNotFound after delete, got: %v", err)
		}
	})
}

// FuzzMultipleAppends tests appending multiple messages with various sequences.
func FuzzMultipleAppends(f *testing.F) {
	f.Add("stream", []byte("msg1"), []byte("msg2"), []byte("msg3"))
	f.Add("test", []byte{0x00}, []byte{0xff}, []byte{0x7f})
	f.Add("unicode", []byte("こんにちは"), []byte("世界"), []byte("🎉"))
	f.Add("", []byte("a"), []byte("b"), []byte("c"))         // invalid stream ID
	f.Add("valid", []byte{}, []byte("b"), []byte("c"))       // empty data1
	f.Add("valid", []byte("a"), []byte{}, []byte("c"))       // empty data2
	f.Add("valid", []byte("a"), []byte("b"), []byte{})       // empty data3

	f.Fuzz(func(t *testing.T, streamID string, data1, data2, data3 []byte) {
		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		// Let system validate stream ID
		_, err = s.Create(ctx, streamID, durablestream.StreamConfig{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return // Invalid stream ID rejected
		}

		// Append three messages - track which succeed
		offset1, err1 := s.Append(ctx, streamID, data1, "")
		offset2, err2 := s.Append(ctx, streamID, data2, "")
		offset3, err3 := s.Append(ctx, streamID, data3, "")

		// Count successful appends
		var successCount int
		var offsets []durablestream.Offset
		var datas [][]byte

		if err1 == nil {
			successCount++
			offsets = append(offsets, offset1)
			datas = append(datas, data1)
		}
		if err2 == nil {
			successCount++
			offsets = append(offsets, offset2)
			datas = append(datas, data2)
		}
		if err3 == nil {
			successCount++
			offsets = append(offsets, offset3)
			datas = append(datas, data3)
		}

		if successCount == 0 {
			return // All appends rejected (e.g., all empty data)
		}

		// Read all messages
		result, err := s.Read(ctx, streamID, "", 0)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if len(result.Messages) != successCount {
			t.Fatalf("expected %d messages, got %d", successCount, len(result.Messages))
		}

		// Verify data integrity for each successful append
		for i := 0; i < successCount; i++ {
			if !bytes.Equal(result.Messages[i].Data, datas[i]) {
				t.Errorf("data[%d] mismatch", i)
			}
			if result.Messages[i].Offset != offsets[i] {
				t.Errorf("offset[%d] mismatch: expected %s, got %s",
					i, offsets[i], result.Messages[i].Offset)
			}
		}

		// Verify offsets are monotonically increasing
		for i := 1; i < len(result.Messages); i++ {
			if result.Messages[i-1].Offset >= result.Messages[i].Offset {
				t.Errorf("offsets not increasing: %s >= %s",
					result.Messages[i-1].Offset, result.Messages[i].Offset)
			}
		}
	})
}

// FuzzStreamIDCharacters tests that various unicode characters work in stream IDs.
func FuzzStreamIDCharacters(f *testing.F) {
	f.Add("simple")
	f.Add("with-dash")
	f.Add("with_underscore")
	f.Add("with.dot")
	f.Add("UPPERCASE")
	f.Add("MixedCase")
	f.Add("123numbers")
	f.Add("unicode日本語")
	f.Add("emoji🎉")
	f.Add("")                               // invalid: empty
	f.Add("has:colon")                      // invalid: contains colon
	f.Add(string([]byte{0xff, 0xfe, 0xfd})) // invalid UTF-8

	f.Fuzz(func(t *testing.T, streamID string) {
		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		// Let system validate stream ID
		_, err = s.Create(ctx, streamID, durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		if err != nil {
			return // System rejected invalid stream ID
		}

		// If create succeeded, full round-trip must work
		testData := []byte("test data")
		_, err = s.Append(ctx, streamID, testData, "")
		if err != nil {
			t.Fatalf("Append(%q) failed after successful Create: %v", streamID, err)
		}

		result, err := s.Read(ctx, streamID, "", 0)
		if err != nil {
			t.Fatalf("Read(%q) failed: %v", streamID, err)
		}

		if len(result.Messages) != 1 || !bytes.Equal(result.Messages[0].Data, testData) {
			t.Errorf("data integrity failed for streamID %q", streamID)
		}

		err = s.Delete(ctx, streamID)
		if err != nil {
			t.Fatalf("Delete(%q) failed: %v", streamID, err)
		}
	})
}

// FuzzSequenceOrdering tests that sequence ordering is enforced correctly.
func FuzzSequenceOrdering(f *testing.F) {
	f.Add("0001", "0002", "0003")
	f.Add("a", "b", "c")
	f.Add("001", "010", "100")
	f.Add("AAA", "AAB", "AAC")
	f.Add("seq_001", "seq_002", "seq_003")

	f.Fuzz(func(t *testing.T, seq1, seq2, seq3 string) {
		// Skip empty sequences (allowed but not tested here)
		if seq1 == "" || seq2 == "" || seq3 == "" {
			return
		}

		// Only test when sequences are strictly ordered
		if !(seq1 < seq2 && seq2 < seq3) {
			return
		}

		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		_, _ = s.Create(ctx, "test", durablestream.StreamConfig{
			ContentType: "text/plain",
		})

		// First append should succeed
		_, err = s.Append(ctx, "test", []byte("data1"), seq1)
		if err != nil {
			t.Fatalf("first append with seq %q failed: %v", seq1, err)
		}

		// Second append with higher seq should succeed
		_, err = s.Append(ctx, "test", []byte("data2"), seq2)
		if err != nil {
			t.Fatalf("second append with seq %q failed: %v", seq2, err)
		}

		// Third append with highest seq should succeed
		_, err = s.Append(ctx, "test", []byte("data3"), seq3)
		if err != nil {
			t.Fatalf("third append with seq %q failed: %v", seq3, err)
		}

		// Append with seq1 again should fail (duplicate/regression)
		_, err = s.Append(ctx, "test", []byte("data4"), seq1)
		if err == nil {
			t.Errorf("append with duplicate seq %q should fail", seq1)
		}

		// Append with seq2 again should fail (duplicate/regression)
		_, err = s.Append(ctx, "test", []byte("data5"), seq2)
		if err == nil {
			t.Errorf("append with duplicate seq %q should fail", seq2)
		}
	})
}

// FuzzConcurrentAppends tests concurrent appends to the same stream.
// Run with -race to detect data races.
func FuzzConcurrentAppends(f *testing.F) {
	f.Add("stream", uint8(2), uint8(5))  // 2 workers, 5 messages each
	f.Add("stream", uint8(4), uint8(10)) // 4 workers, 10 messages each
	f.Add("stream", uint8(8), uint8(3))  // 8 workers, 3 messages each

	f.Fuzz(func(t *testing.T, streamID string, numWorkers, msgsPerWorker uint8) {
		// Bound inputs to reasonable ranges
		if numWorkers < 1 || numWorkers > 16 {
			return
		}
		if msgsPerWorker < 1 || msgsPerWorker > 50 {
			return
		}

		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		// Let system validate stream ID
		_, err = s.Create(ctx, streamID, durablestream.StreamConfig{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return // Invalid stream ID rejected
		}

		var wg sync.WaitGroup
		var successCount atomic.Int64
		var errorCount atomic.Int64

		// Spawn workers that append concurrently
		for w := 0; w < int(numWorkers); w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for m := 0; m < int(msgsPerWorker); m++ {
					data := []byte(fmt.Sprintf("worker-%d-msg-%d", workerID, m))
					_, err := s.Append(ctx, streamID, data, "")
					if err != nil {
						errorCount.Add(1)
					} else {
						successCount.Add(1)
					}
				}
			}(w)
		}

		wg.Wait()

		// All appends should succeed (no sequences used, so no conflicts)
		expectedTotal := int64(numWorkers) * int64(msgsPerWorker)
		if successCount.Load() != expectedTotal {
			t.Errorf("expected %d successful appends, got %d (errors: %d)",
				expectedTotal, successCount.Load(), errorCount.Load())
		}

		// Read back and verify count
		result, err := s.Read(ctx, streamID, "", 0)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if int64(len(result.Messages)) != expectedTotal {
			t.Errorf("expected %d messages, got %d", expectedTotal, len(result.Messages))
		}

		// Verify all offsets are unique
		offsets := make(map[durablestream.Offset]bool)
		for _, msg := range result.Messages {
			if offsets[msg.Offset] {
				t.Errorf("duplicate offset: %s", msg.Offset)
			}
			offsets[msg.Offset] = true
		}

		// Verify offsets are ordered (messages should be in offset order)
		for i := 1; i < len(result.Messages); i++ {
			if result.Messages[i-1].Offset >= result.Messages[i].Offset {
				t.Errorf("messages not in offset order: %s >= %s",
					result.Messages[i-1].Offset, result.Messages[i].Offset)
			}
		}
	})
}

// FuzzConcurrentStreams tests concurrent operations on different streams.
// Run with -race to detect data races.
func FuzzConcurrentStreams(f *testing.F) {
	f.Add(uint8(3)) // 3 concurrent streams
	f.Add(uint8(5)) // 5 concurrent streams
	f.Add(uint8(8)) // 8 concurrent streams

	f.Fuzz(func(t *testing.T, numStreams uint8) {
		if numStreams < 1 || numStreams > 16 {
			return
		}

		s, err := newFuzzStorage()
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		defer s.Close()

		ctx := context.Background()

		var wg sync.WaitGroup
		errors := make(chan error, int(numStreams)*10)

		// Each goroutine creates its own stream and does CRUD operations
		for i := 0; i < int(numStreams); i++ {
			wg.Add(1)
			go func(streamID string) {
				defer wg.Done()

				// Create
				_, err := s.Create(ctx, streamID, durablestream.StreamConfig{
					ContentType: "text/plain",
				})
				if err != nil {
					errors <- fmt.Errorf("Create(%s): %w", streamID, err)
					return
				}

				// Append multiple messages
				for j := 0; j < 5; j++ {
					data := []byte(fmt.Sprintf("stream-%s-msg-%d", streamID, j))
					_, err := s.Append(ctx, streamID, data, "")
					if err != nil {
						errors <- fmt.Errorf("Append(%s, msg %d): %w", streamID, j, err)
						return
					}
				}

				// Read back
				result, err := s.Read(ctx, streamID, "", 0)
				if err != nil {
					errors <- fmt.Errorf("Read(%s): %w", streamID, err)
					return
				}
				if len(result.Messages) != 5 {
					errors <- fmt.Errorf("Read(%s): expected 5 messages, got %d", streamID, len(result.Messages))
					return
				}

				// Head should succeed
				_, err = s.Head(ctx, streamID)
				if err != nil {
					errors <- fmt.Errorf("Head(%s): %w", streamID, err)
					return
				}

				// Delete
				err = s.Delete(ctx, streamID)
				if err != nil {
					errors <- fmt.Errorf("Delete(%s): %w", streamID, err)
					return
				}

				// Verify deleted
				_, err = s.Head(ctx, streamID)
				if err != durablestream.ErrNotFound {
					errors <- fmt.Errorf("Head(%s) after delete: expected ErrNotFound, got %v", streamID, err)
					return
				}
			}(fmt.Sprintf("stream-%d", i))
		}

		wg.Wait()
		close(errors)

		// Collect and report all errors
		var allErrors []error
		for err := range errors {
			allErrors = append(allErrors, err)
		}
		if len(allErrors) > 0 {
			for _, err := range allErrors {
				t.Error(err)
			}
		}
	})
}
