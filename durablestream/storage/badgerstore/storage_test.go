package badgerstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

// quietLogger suppresses all Badger output during tests.
type quietLogger struct{}

func (l *quietLogger) Errorf(string, ...interface{})   {}
func (l *quietLogger) Warningf(string, ...interface{}) {}
func (l *quietLogger) Infof(string, ...interface{})    {}
func (l *quietLogger) Debugf(string, ...interface{})   {}

// quietSLog returns a silent slog.Logger for tests.
func quietSLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestStorage creates an in-memory storage for testing with background
// goroutines disabled for deterministic behavior.
func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		GCInterval:      -1, // Disable background GC
		CleanupInterval: -1, // Disable background cleanup
	})
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	// Wait for the startup sweep so tests that write keys directly are not
	// racing the reaper.
	<-s.initialReapDone
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close storage: %v", err)
		}
	})
	return s
}

// currentGeneration returns the generation a stream is currently using.
func currentGeneration(t *testing.T, s *Storage, streamID string) generation {
	t.Helper()
	var gen generation
	err := s.db.View(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("stream %q does not exist", streamID)
		}
		gen = rec.Gen
		return nil
	})
	if err != nil {
		t.Fatalf("read generation for %q: %v", streamID, err)
	}
	return gen
}

// countKeys returns the number of keys with the given prefix.
func countKeys(t *testing.T, s *Storage, prefix []byte) int {
	t.Helper()
	var n int
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count keys %q: %v", prefix, err)
	}
	return n
}

// concatMessages concatenates all message data from a ReadResult.
func concatMessages(result *durablestream.ReadResult) []byte {
	if len(result.Messages) == 0 {
		return nil
	}
	if len(result.Messages) == 1 {
		return result.Messages[0].Data
	}
	var total int
	for _, m := range result.Messages {
		total += len(m.Data)
	}
	data := make([]byte, 0, total)
	for _, m := range result.Messages {
		data = append(data, m.Data...)
	}
	return data
}

func TestNew(t *testing.T) {
	t.Run("creates in-memory storage", func(t *testing.T) {
		s, err := New(Options{InMemory: true, Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
		if s == nil {
			t.Fatal("New() returned nil")
		}
		if s.tempDir != "" {
			t.Fatalf("InMemory storage created disk directory %q", s.tempDir)
		}
	})

	t.Run("creates ephemeral disk storage with empty dir", func(t *testing.T) {
		s, err := New(Options{Dir: "", Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tempDir := s.tempDir
		if tempDir == "" {
			t.Fatal("empty Dir did not create an ephemeral disk directory")
		}
		if _, err := os.Stat(tempDir); err != nil {
			t.Fatalf("stat ephemeral directory: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("close ephemeral storage: %v", err)
		}
		if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
			t.Fatalf("ephemeral directory still exists after Close: %v", err)
		}
	})

	t.Run("creates storage with custom logger", func(t *testing.T) {
		s, err := New(Options{
			InMemory:        true,
			Logger:          &quietLogger{},
			GCInterval:      -1,
			CleanupInterval: -1,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
	})

	t.Run("creates storage with directory", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Options{Dir: dir, Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
	})

	t.Run("uses in-memory default max message size", func(t *testing.T) {
		s, err := New(Options{InMemory: true, Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
		if s.maxMessageSize != DefaultInMemoryMaxMessageSize {
			t.Errorf("expected in-memory max message size %d, got %d", DefaultInMemoryMaxMessageSize, s.maxMessageSize)
		}
		if _, err := s.Create(t.Context(), "in-memory-limit", durablestream.StreamConfig{ContentType: "application/octet-stream"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := s.Append(t.Context(), "in-memory-limit", make([]byte, DefaultInMemoryMaxMessageSize), ""); err != nil {
			t.Fatalf("Append at in-memory limit: %v", err)
		}
	})

	t.Run("rejects in-memory message limit above Badger maximum", func(t *testing.T) {
		_, err := New(Options{
			InMemory:       true,
			MaxMessageSize: DefaultInMemoryMaxMessageSize + 1,
			Logger:         &quietLogger{},
		})
		if err == nil || !strings.Contains(err.Error(), "exceeds in-memory limit") {
			t.Fatalf("New() error = %v, want in-memory limit error", err)
		}
	})

	t.Run("rejects directory in memory-only mode", func(t *testing.T) {
		_, err := New(Options{InMemory: true, Dir: t.TempDir(), Logger: &quietLogger{}})
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("New() error = %v, want mutually exclusive error", err)
		}
	})

	t.Run("uses custom max message size", func(t *testing.T) {
		s, err := New(Options{InMemory: true, Logger: &quietLogger{}, MaxMessageSize: 1024, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
		if s.maxMessageSize != 1024 {
			t.Errorf("expected max message size 1024, got %d", s.maxMessageSize)
		}
	})
}

func TestCreate(t *testing.T) {
	t.Run("creates new stream", func(t *testing.T) {
		s := newTestStorage(t)
		created, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !created {
			t.Error("expected created=true for new stream")
		}
	})

	t.Run("idempotent with same config", func(t *testing.T) {
		s := newTestStorage(t)
		cfg := durablestream.StreamConfig{ContentType: "text/plain"}

		created1, err := s.Create(context.Background(), "test", cfg)
		if err != nil {
			t.Fatalf("first create: %v", err)
		}
		if !created1 {
			t.Error("first create should return created=true")
		}

		created2, err := s.Create(context.Background(), "test", cfg)
		if err != nil {
			t.Fatalf("second create: %v", err)
		}
		if created2 {
			t.Error("second create should return created=false (idempotent)")
		}
	})

	t.Run("conflict with different content type", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		_, err = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict, got: %v", err)
		}
	})

	t.Run("conflict with different TTL", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			TTL:         time.Hour,
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		_, err = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			TTL:         2 * time.Hour,
		})
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict, got: %v", err)
		}
	})

	t.Run("conflict with different ExpiresAt", func(t *testing.T) {
		s := newTestStorage(t)
		now := time.Now()

		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   now.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		_, err = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   now.Add(2 * time.Hour),
		})
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict, got: %v", err)
		}
	})

	t.Run("content type matching is case-insensitive", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "Application/JSON",
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		created, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("second create: %v", err)
		}
		if created {
			t.Error("expected idempotent match with case-insensitive content type")
		}
	})

	t.Run("allows recreation of expired stream", func(t *testing.T) {
		s := newTestStorage(t)

		// Create with already-expired ExpiresAt
		_, err := s.Create(context.Background(), "expired-test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		// Should be able to recreate since original is expired
		created, err := s.Create(context.Background(), "expired-test", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("recreation should succeed for expired stream: %v", err)
		}
		if !created {
			t.Error("expected created=true when replacing expired stream")
		}
	})
}

func TestAppend(t *testing.T) {
	t.Run("appends data successfully", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		offset, err := s.Append(context.Background(), "test", []byte("hello"), "")
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if offset != "0000000000000000_0000000000000001" {
			t.Errorf("expected offset 0000000000000000_0000000000000001, got %s", offset)
		}
	})

	t.Run("rejects empty append", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		_, err := s.Append(context.Background(), "test", []byte{}, "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest, got: %v", err)
		}
	})

	t.Run("rejects oversized message", func(t *testing.T) {
		s, _ := New(Options{InMemory: true, Logger: &quietLogger{}, MaxMessageSize: 100, GCInterval: -1, CleanupInterval: -1})
		defer s.Close()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		data := make([]byte, 101)
		_, err := s.Append(context.Background(), "test", data, "")
		if !errors.Is(err, durablestream.ErrPayloadTooLarge) {
			t.Errorf("expected ErrPayloadTooLarge for oversized message, got: %v", err)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Append(context.Background(), "nonexistent", []byte("data"), "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.Append(context.Background(), "test", []byte("data"), "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("validates sequence numbers", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		_, err := s.Append(context.Background(), "test", []byte("data1"), "seq_002")
		if err != nil {
			t.Fatalf("first append: %v", err)
		}

		_, err = s.Append(context.Background(), "test", []byte("data2"), "seq_003")
		if err != nil {
			t.Fatalf("second append: %v", err)
		}

		_, err = s.Append(context.Background(), "test", []byte("data3"), "seq_001")
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict for sequence regression, got: %v", err)
		}

		_, err = s.Append(context.Background(), "test", []byte("data4"), "seq_003")
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict for duplicate sequence, got: %v", err)
		}
	})

	t.Run("notifies waiters via WaitForData", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Append in background
		go func() {
			time.Sleep(10 * time.Millisecond)
			_, _ = s.Append(context.Background(), "test", []byte("data"), "")
		}()

		// Use WaitForData to wait for notification
		result, err := s.WaitForData(ctx, "test", "", 0)
		if err != nil {
			t.Fatalf("WaitForData error: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(result.Messages))
		}
		if string(result.Messages[0].Data) != "data" {
			t.Errorf("expected 'data', got %s", result.Messages[0].Data)
		}
	})
}

func TestAppendFrom(t *testing.T) {
	t.Run("appends from reader", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		r := strings.NewReader("hello from reader")
		offset, err := s.AppendFrom(context.Background(), "test", r, "")
		if err != nil {
			t.Fatalf("append reader: %v", err)
		}
		if offset != "0000000000000000_0000000000000001" {
			t.Errorf("expected offset 0000000000000000_0000000000000001, got %s", offset)
		}

		result, _ := s.Read(context.Background(), "test", "", 0)
		if string(concatMessages(result)) != "hello from reader" {
			t.Errorf("unexpected data: %s", concatMessages(result))
		}
	})

	t.Run("propagates read errors", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		r := &errorReader{err: errors.New("read failed")}
		_, err := s.AppendFrom(context.Background(), "test", r, "")
		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("rejects oversized reader", func(t *testing.T) {
		s, _ := New(Options{InMemory: true, Logger: &quietLogger{}, MaxMessageSize: 100, GCInterval: -1, CleanupInterval: -1})
		defer s.Close()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		data := make([]byte, 101)
		r := bytes.NewReader(data)
		_, err := s.AppendFrom(context.Background(), "test", r, "")
		if !errors.Is(err, durablestream.ErrPayloadTooLarge) {
			t.Errorf("expected ErrPayloadTooLarge for oversized reader, got: %v", err)
		}
	})
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestRead(t *testing.T) {
	t.Run("reads from start", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("hello"), "")
		_, _ = s.Append(context.Background(), "test", []byte(" world"), "")

		result, err := s.Read(context.Background(), "test", "", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(concatMessages(result)) != "hello world" {
			t.Errorf("unexpected data: %s", concatMessages(result))
		}
	})

	t.Run("reads with -1 offset", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		result, err := s.Read(context.Background(), "test", "-1", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(concatMessages(result)) != "data" {
			t.Errorf("unexpected data: %s", concatMessages(result))
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Read(context.Background(), "nonexistent", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.Read(context.Background(), "test", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("returns empty result for offset beyond tail", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000099", 0)
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}
		if len(result.Messages) != 0 {
			t.Errorf("expected empty messages, got %d", len(result.Messages))
		}
		if result.TailOffset != "0000000000000000_0000000000000001" {
			t.Errorf("expected TailOffset 0000000000000000_0000000000000001, got %s", result.TailOffset)
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("hello"), "")
		_, _ = s.Append(context.Background(), "test", []byte("world"), "")
		_, _ = s.Append(context.Background(), "test", []byte("extra"), "")

		result, err := s.Read(context.Background(), "test", "", 8)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message with limit 8, got %d", len(result.Messages))
		}
		if string(concatMessages(result)) != "hello" {
			t.Errorf("unexpected data: %s", concatMessages(result))
		}
	})

	t.Run("returns correct next offset", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("msg1"), "")
		_, _ = s.Append(context.Background(), "test", []byte("msg2"), "")

		result, _ := s.Read(context.Background(), "test", "", 0)
		if result.NextOffset != result.TailOffset {
			t.Errorf("NextOffset (%s) should equal TailOffset (%s) when at end", result.NextOffset, result.TailOffset)
		}
	})

	t.Run("read from middle offset", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("first"), "")
		_, _ = s.Append(context.Background(), "test", []byte("second"), "")

		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000001", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(concatMessages(result)) != "second" {
			t.Errorf("expected 'second', got '%s'", concatMessages(result))
		}
	})

	t.Run("read empty stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		result, err := s.Read(context.Background(), "test", "", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(result.Messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(result.Messages))
		}
		if result.NextOffset != "0000000000000000_0000000000000000" {
			t.Errorf("expected next offset 0000000000000000_0000000000000000, got %s", result.NextOffset)
		}
		if result.TailOffset != "0000000000000000_0000000000000000" {
			t.Errorf("expected tail offset 0000000000000000_0000000000000000, got %s", result.TailOffset)
		}
	})

	t.Run("handles offset gaps correctly", func(t *testing.T) {
		// Simulate gaps by manually inserting messages with non-contiguous offsets
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// Manually insert messages at offsets 1, 3, 5 (gaps at 2, 4)
		gen := currentGeneration(t, s, "test")
		err := s.db.Update(func(txn *badger.Txn) error {
			_ = txn.Set(messageKey("test", gen, "0000000000000000_0000000000000001"), []byte("msg1"))
			_ = txn.Set(messageKey("test", gen, "0000000000000000_0000000000000003"), []byte("msg3"))
			_ = txn.Set(messageKey("test", gen, "0000000000000000_0000000000000005"), []byte("msg5"))
			return nil
		})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Read from gap position (offset 2) - should skip to next available (3)
		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000002", 0)
		if err != nil {
			t.Fatalf("read from gap: %v", err)
		}
		if len(result.Messages) != 2 {
			t.Errorf("expected 2 messages (3 and 5), got %d", len(result.Messages))
		}
		if len(result.Messages) > 0 && string(result.Messages[0].Data) != "msg3" {
			t.Errorf("expected first message 'msg3', got '%s'", result.Messages[0].Data)
		}

		// Read from another gap (offset 4) - should skip to next available (5)
		result, err = s.Read(context.Background(), "test", "0000000000000000_0000000000000004", 0)
		if err != nil {
			t.Fatalf("read from gap: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message (5), got %d", len(result.Messages))
		}
		if len(result.Messages) > 0 && string(result.Messages[0].Data) != "msg5" {
			t.Errorf("expected message 'msg5', got '%s'", result.Messages[0].Data)
		}

		// Verify tail offset is correct
		if result.TailOffset != "0000000000000000_0000000000000005" {
			t.Errorf("expected tail offset 0000000000000000_0000000000000005, got %s", result.TailOffset)
		}
	})
}

func TestHead(t *testing.T) {
	t.Run("returns stream info", func(t *testing.T) {
		s := newTestStorage(t)
		expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "application/json",
			TTL:         time.Hour,
			ExpiresAt:   expiresAt,
		})
		_, _ = s.Append(context.Background(), "test", []byte(`{"test":1}`), "")

		info, err := s.Head(context.Background(), "test")
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if info.ContentType != "application/json" {
			t.Errorf("unexpected content type: %s", info.ContentType)
		}
		if info.NextOffset != "0000000000000000_0000000000000001" {
			t.Errorf("unexpected next offset: %s", info.NextOffset)
		}
		if info.TTL != time.Hour {
			t.Errorf("unexpected TTL: %v", info.TTL)
		}
		if !info.ExpiresAt.Equal(expiresAt) {
			t.Errorf("unexpected ExpiresAt: %v", info.ExpiresAt)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Head(context.Background(), "nonexistent")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.Head(context.Background(), "test")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("returns zero offset for empty stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		info, err := s.Head(context.Background(), "test")
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if info.NextOffset != "0000000000000000_0000000000000000" {
			t.Errorf("expected next offset 0000000000000000_0000000000000000 for empty stream, got %s", info.NextOffset)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("deletes stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		err := s.Delete(context.Background(), "test")
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		_, err = s.Head(context.Background(), "test")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got: %v", err)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		err := s.Delete(context.Background(), "nonexistent")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("wakes WaitForData callers on delete", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		// Start WaitForData in goroutine
		waitDone := make(chan error, 1)
		go func() {
			_, err := s.WaitForData(ctx, "test", "", 0)
			waitDone <- err
		}()

		// Give waiter time to register
		time.Sleep(10 * time.Millisecond)

		// Delete the stream
		_ = s.Delete(context.Background(), "test")

		// WaitForData should return ErrNotFound
		select {
		case err := <-waitDone:
			if !errors.Is(err, durablestream.ErrNotFound) {
				t.Errorf("expected ErrNotFound, got: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForData did not return after delete")
		}
	})

	t.Run("deletes stream with messages", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("msg1"), "")
		_, _ = s.Append(context.Background(), "test", []byte("msg2"), "")

		err := s.Delete(context.Background(), "test")
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		_, err = s.Head(context.Background(), "test")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got: %v", err)
		}
	})

	t.Run("deletes stream with sequence tracking", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("msg1"), "seq_001")

		err := s.Delete(context.Background(), "test")
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, err = s.Append(context.Background(), "test", []byte("msg1"), "seq_001")
		if err != nil {
			t.Errorf("should be able to use same seq after delete/recreate: %v", err)
		}
	})
}

func TestWaitForData(t *testing.T) {
	t.Run("returns immediately when data exists", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("existing data"), "")

		result, err := s.WaitForData(context.Background(), "test", "", 0)
		if err != nil {
			t.Fatalf("WaitForData: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(result.Messages))
		}
		if string(result.Messages[0].Data) != "existing data" {
			t.Errorf("unexpected data: %s", result.Messages[0].Data)
		}
	})

	t.Run("blocks until data arrives", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Append in background
		go func() {
			time.Sleep(10 * time.Millisecond)
			_, _ = s.Append(context.Background(), "test", []byte("new data"), "")
		}()

		result, err := s.WaitForData(ctx, "test", "", 0)
		if err != nil {
			t.Fatalf("WaitForData: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(result.Messages))
		}
		if string(result.Messages[0].Data) != "new data" {
			t.Errorf("unexpected data: %s", result.Messages[0].Data)
		}
	})

	t.Run("returns ctx.Err on timeout", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err := s.WaitForData(ctx, "test", "", 0)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("returns ErrNotFound for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.WaitForData(context.Background(), "nonexistent", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns ErrNotFound for expired stream", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.WaitForData(context.Background(), "test", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("returns ErrNotFound when stream deleted while waiting", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		// Start WaitForData in goroutine
		waitDone := make(chan error, 1)
		go func() {
			_, err := s.WaitForData(ctx, "test", "", 0)
			waitDone <- err
		}()

		// Give waiter time to register
		time.Sleep(10 * time.Millisecond)

		// Delete stream - should wake waiter with ErrNotFound
		_ = s.Delete(context.Background(), "test")

		select {
		case err := <-waitDone:
			if !errors.Is(err, durablestream.ErrNotFound) {
				t.Errorf("expected ErrNotFound, got: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForData did not return after delete")
		}
	})

	t.Run("respects limit parameter", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("aaaa"), "")
		_, _ = s.Append(context.Background(), "test", []byte("bbbb"), "")
		_, _ = s.Append(context.Background(), "test", []byte("cccc"), "")

		// Limit of 6 should return only first message (4 bytes fits, 8 would exceed)
		result, err := s.WaitForData(context.Background(), "test", "", 6)
		if err != nil {
			t.Fatalf("WaitForData: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message with limit 6, got %d", len(result.Messages))
		}
	})

	t.Run("handles concurrent waiters", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Start multiple waiters
		const numWaiters = 5
		results := make(chan *durablestream.ReadResult, numWaiters)
		for i := 0; i < numWaiters; i++ {
			go func() {
				result, _ := s.WaitForData(ctx, "test", "", 0)
				results <- result
			}()
		}

		// Give waiters time to register
		time.Sleep(10 * time.Millisecond)

		// Single append should wake all waiters
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		// All waiters should receive the data
		for i := 0; i < numWaiters; i++ {
			select {
			case result := <-results:
				if result == nil || len(result.Messages) != 1 {
					t.Errorf("waiter %d: expected 1 message", i)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("waiter %d: did not receive data", i)
			}
		}
	})

	t.Run("waits for data without prior state", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// Remove in-memory state to simulate restart
		gen := currentGeneration(t, s, "test")
		s.streams.LoadAndDelete(streamStateKey("test", gen))

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Append in background
		go func() {
			time.Sleep(10 * time.Millisecond)
			_, _ = s.Append(context.Background(), "test", []byte("data"), "")
		}()

		result, err := s.WaitForData(ctx, "test", "", 0)
		if err != nil {
			t.Fatalf("WaitForData: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(result.Messages))
		}
	})
}

func TestConcurrentAccess(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				data := []byte(string(rune('A'+n)) + string(rune('0'+j%10)))
				_, _ = s.Append(context.Background(), "test", data, "")
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _ = s.Read(context.Background(), "test", "", 0)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _ = s.Head(context.Background(), "test")
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}

	info, _ := s.Head(context.Background(), "test")
	if info.NextOffset != "0000000000000000_0000000000001000" {
		t.Errorf("expected 1000 appends, got offset %s", info.NextOffset)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	s1, err := New(Options{Dir: dir, Logger: &quietLogger{}, SLogger: quietSLog(), GCInterval: -1, CleanupInterval: -1})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	_, err = s1.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	_, err = s1.Append(context.Background(), "test", []byte("persisted data"), "")
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := New(Options{Dir: dir, Logger: &quietLogger{}, SLogger: quietSLog(), GCInterval: -1, CleanupInterval: -1})
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer s2.Close()

	result, err := s2.Read(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}

	if string(concatMessages(result)) != "persisted data" {
		t.Errorf("expected 'persisted data', got '%s'", concatMessages(result))
	}
}

func TestMultipleStreams(t *testing.T) {
	s := newTestStorage(t)

	streams := []string{"stream1", "stream2", "stream3"}
	for _, id := range streams {
		_, err := s.Create(context.Background(), id, durablestream.StreamConfig{ContentType: "text/plain"})
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		_, err = s.Append(context.Background(), id, []byte("data for "+id), "")
		if err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	for _, id := range streams {
		result, err := s.Read(context.Background(), id, "", 0)
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		expected := "data for " + id
		if string(concatMessages(result)) != expected {
			t.Errorf("stream %s: expected '%s', got '%s'", id, expected, concatMessages(result))
		}
	}

	if err := s.Delete(context.Background(), "stream2"); err != nil {
		t.Fatalf("delete stream2: %v", err)
	}

	for _, id := range []string{"stream1", "stream3"} {
		_, err := s.Head(context.Background(), id)
		if err != nil {
			t.Errorf("head %s after deleting stream2: %v", id, err)
		}
	}

	_, err := s.Head(context.Background(), "stream2")
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected ErrNotFound for deleted stream2, got: %v", err)
	}
}

// testLogger implements badger.Logger for testing
type testLogger struct{}

func (l *testLogger) Errorf(string, ...interface{})   {}
func (l *testLogger) Warningf(string, ...interface{}) {}
func (l *testLogger) Infof(string, ...interface{})    {}
func (l *testLogger) Debugf(string, ...interface{})   {}

func TestNewWithCustomLogger(t *testing.T) {
	s, err := New(Options{
		InMemory:        true,
		Logger:          &testLogger{},
		GCInterval:      -1,
		CleanupInterval: -1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
}

func TestNewWithInvalidDir(t *testing.T) {
	f, err := os.CreateTemp("", "badger-test-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	_, err = New(Options{Dir: path, Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
	if err == nil {
		t.Error("expected error when using file as badger dir")
	}
}

func TestGetTailOffset(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Initial tail offset should be 0
	err := s.db.View(func(txn *badger.Txn) error {
		tailOffset, err := getTailOffset(txn, "test", currentGeneration(t, s, "test"))
		if err != nil {
			return err
		}
		if tailOffset != "0000000000000000_0000000000000000" {
			t.Errorf("expected tail offset 0000000000000000_0000000000000000, got %s", tailOffset)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}

	// After appends, tail offset should update
	_, _ = s.Append(context.Background(), "test", []byte("msg1"), "")
	_, _ = s.Append(context.Background(), "test", []byte("msg2"), "")

	err = s.db.View(func(txn *badger.Txn) error {
		tailOffset, err := getTailOffset(txn, "test", currentGeneration(t, s, "test"))
		if err != nil {
			return err
		}
		if tailOffset != "0000000000000000_0000000000000002" {
			t.Errorf("expected tail offset 0000000000000000_0000000000000002, got %s", tailOffset)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestValidateStreamID(t *testing.T) {
	t.Run("rejects empty streamID", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Create(context.Background(), "", durablestream.StreamConfig{ContentType: "text/plain"})
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for empty streamID, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Create(context.Background(), "stream:with:colons", durablestream.StreamConfig{ContentType: "text/plain"})
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in Append", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Append(context.Background(), "bad:id", []byte("data"), "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in Append, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in Read", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Read(context.Background(), "bad:id", "", 0)
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in Read, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in Head", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Head(context.Background(), "bad:id")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in Head, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in Delete", func(t *testing.T) {
		s := newTestStorage(t)
		err := s.Delete(context.Background(), "bad:id")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in Delete, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in WaitForData", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.WaitForData(context.Background(), "bad:id", "", 0)
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in WaitForData, got: %v", err)
		}
	})

	t.Run("rejects streamID with colon in AppendFrom", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.AppendFrom(context.Background(), "bad:id", strings.NewReader("data"), "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in AppendFrom, got: %v", err)
		}
	})
}

func TestReadWithInvalidOffset(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
	_, _ = s.Append(context.Background(), "test", []byte("data"), "")

	_, err := s.Read(context.Background(), "test", "invalid-offset", 0)
	if !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("expected ErrBadRequest for invalid offset, got: %v", err)
	}
}

func TestReadAtMaxInt64OffsetDoesNotOverflow(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "max-offset", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Append(ctx, "max-offset", []byte("first"), ""); err != nil {
		t.Fatalf("append: %v", err)
	}

	offset := storage.FormatSimpleOffset(math.MaxInt64)
	res, err := s.Read(ctx, "max-offset", offset, 0)
	if err != nil {
		t.Fatalf("read at MaxInt64 offset: %v", err)
	}
	if len(res.Messages) != 0 {
		t.Fatalf("read at MaxInt64 returned %d messages, want none", len(res.Messages))
	}
	if res.NextOffset != offset {
		t.Errorf("NextOffset = %q, want requested offset %q", res.NextOffset, offset)
	}
}

func TestReadWithNegativeLimit(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	_, err := s.Read(context.Background(), "test", "", -1)
	if !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("expected ErrBadRequest for negative limit, got: %v", err)
	}
}

func TestReadContextCancellation(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Add some messages
	for i := 0; i < 100; i++ {
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")
	}

	// Cancel context before read
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.Read(ctx, "test", "", 0)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestDeleteContextCancellation(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Add many messages to trigger batched deletion
	for i := 0; i < 100; i++ {
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")
	}

	// Cancel context before delete
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Delete(ctx, "test")
	// Should return context.Canceled (after deleting config but before all messages)
	if err != nil && err != context.Canceled {
		t.Errorf("expected nil or context.Canceled, got: %v", err)
	}
}

func TestConcurrentOffsetHighWater(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	const numGoroutines = 50
	offsets := make(chan durablestream.Offset, numGoroutines)
	errs := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			offset, err := s.Append(t.Context(), "test", []byte("value"), "")
			offsets <- offset
			errs <- err
		}()
	}

	seen := make(map[durablestream.Offset]struct{}, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Append: %v", err)
		}
		offset := <-offsets
		if _, duplicate := seen[offset]; duplicate {
			t.Fatalf("duplicate offset %q", offset)
		}
		seen[offset] = struct{}{}
	}

	gen := currentGeneration(t, s, "test")
	var highWater uint64
	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(seqKey("test", gen))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			highWater = binary.BigEndian.Uint64(value)
			return nil
		})
	}); err != nil {
		t.Fatalf("read offset high-water: %v", err)
	}
	if highWater != numGoroutines {
		t.Fatalf("offset high-water = %d, want %d", highWater, numGoroutines)
	}
}
