package badgerstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
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
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close storage: %v", err)
		}
	})
	return s
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
	})

	t.Run("creates storage with empty dir (in-memory)", func(t *testing.T) {
		s, err := New(Options{Dir: "", Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
		if s == nil {
			t.Fatal("New() returned nil")
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

	t.Run("uses default max message size", func(t *testing.T) {
		s, err := New(Options{InMemory: true, Logger: &quietLogger{}, GCInterval: -1, CleanupInterval: -1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Close()
		if s.maxMessageSize != DefaultMaxMessageSize {
			t.Errorf("expected default max message size %d, got %d", DefaultMaxMessageSize, s.maxMessageSize)
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
}

func TestAppend(t *testing.T) {
	t.Run("appends data successfully", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		offset, err := s.Append(context.Background(), "test", []byte("hello"), "")
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if offset != "0000000000000001" {
			t.Errorf("expected offset 1, got %s", offset)
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
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for oversized message, got: %v", err)
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

	t.Run("notifies subscribers", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, _ := s.Subscribe(ctx, "test", "")

		go func() {
			time.Sleep(10 * time.Millisecond)
			_, _ = s.Append(context.Background(), "test", []byte("data"), "")
		}()

		select {
		case offset := <-ch:
			if offset != "0000000000000001" {
				t.Errorf("expected offset 1, got %s", offset)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("subscriber not notified")
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
		if offset != "0000000000000001" {
			t.Errorf("expected offset 1, got %s", offset)
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
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for oversized reader, got: %v", err)
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

		result, err := s.Read(context.Background(), "test", "", 0)
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
		// Per protocol Section 5.5: reading beyond tail returns 200 OK with empty body
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		result, err := s.Read(context.Background(), "test", "0000000000000063", 0)
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}
		if len(result.Messages) != 0 {
			t.Errorf("expected empty messages, got %d", len(result.Messages))
		}
		// NextOffset should be the tail offset when beyond tail
		if result.TailOffset != "0000000000000001" {
			t.Errorf("expected TailOffset 1, got %s", result.TailOffset)
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

		result, err := s.Read(context.Background(), "test", "0000000000000001", 0)
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
		if result.NextOffset != "0000000000000000" {
			t.Errorf("expected next offset 0, got %s", result.NextOffset)
		}
		if result.TailOffset != "0000000000000000" {
			t.Errorf("expected tail offset 0, got %s", result.TailOffset)
		}
	})

	t.Run("handles offset gaps correctly", func(t *testing.T) {
		// Simulate gaps by manually inserting messages with non-contiguous offsets
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// Manually insert messages at offsets 1, 3, 5 (gaps at 2, 4)
		err := s.db.Update(func(txn *badger.Txn) error {
			_ = txn.Set([]byte("m:test:0000000000000001"), []byte("msg1"))
			_ = txn.Set([]byte("m:test:0000000000000003"), []byte("msg3"))
			_ = txn.Set([]byte("m:test:0000000000000005"), []byte("msg5"))
			return nil
		})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Read from gap position (offset 2) - should skip to next available (3)
		result, err := s.Read(context.Background(), "test", "0000000000000002", 0)
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
		result, err = s.Read(context.Background(), "test", "0000000000000004", 0)
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
		if result.TailOffset != "0000000000000005" {
			t.Errorf("expected tail offset 5, got %s", result.TailOffset)
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
		if info.NextOffset != "0000000000000001" {
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
		if info.NextOffset != "0000000000000000" {
			t.Errorf("expected next offset 0 for empty stream, got %s", info.NextOffset)
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

	t.Run("closes subscriber channels", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		_ = cancel

		ch, _ := s.Subscribe(ctx, "test", "")

		_ = s.Delete(context.Background(), "test")

		select {
		case _, ok := <-ch:
			if ok {
				_, ok = <-ch
			}
			if ok {
				t.Error("expected channel to be closed after delete")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("channel not closed after delete")
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

func TestSubscribe(t *testing.T) {
	t.Run("subscribes successfully", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, err := s.Subscribe(ctx, "test", "")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if ch == nil {
			t.Fatal("expected channel, got nil")
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := newTestStorage(t)

		_, err := s.Subscribe(context.Background(), "nonexistent", "")
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

		_, err := s.Subscribe(context.Background(), "test", "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("unsubscribes on context cancellation", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		ch, _ := s.Subscribe(ctx, "test", "")

		cancel()

		time.Sleep(50 * time.Millisecond)

		select {
		case _, ok := <-ch:
			if ok {
				t.Error("expected channel to be closed after context cancellation")
			}
		default:
			time.Sleep(50 * time.Millisecond)
			select {
			case _, ok := <-ch:
				if ok {
					t.Error("expected channel to be closed after context cancellation")
				}
			default:
				t.Error("channel not closed after context cancellation")
			}
		}
	})

	t.Run("non-blocking notification", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _ = s.Subscribe(ctx, "test", "")

		for i := 0; i < 20; i++ {
			_, err := s.Append(context.Background(), "test", []byte("data"), "")
			if err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
	})

	t.Run("subscribe to stream without prior state", func(t *testing.T) {
		s := newTestStorage(t)
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		s.streams.LoadAndDelete("test") // Remove in-memory state to simulate restart

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, err := s.Subscribe(ctx, "test", "")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if ch == nil {
			t.Fatal("expected channel, got nil")
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
	if info.NextOffset != "00000000000003E8" {
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

func TestNotifySubscribersNoState(t *testing.T) {
	s := newTestStorage(t)
	// No stream created, should not panic
	s.notifySubscribers("nonexistent", "0000000000000001")
}

func TestGetTailOffset(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Initial tail offset should be 0
	err := s.db.View(func(txn *badger.Txn) error {
		tailOffset, err := s.getTailOffset(txn, "test")
		if err != nil {
			return err
		}
		if tailOffset != "0000000000000000" {
			t.Errorf("expected tail offset 0, got %s", tailOffset)
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
		tailOffset, err := s.getTailOffset(txn, "test")
		if err != nil {
			return err
		}
		if tailOffset != "0000000000000002" {
			t.Errorf("expected tail offset 2, got %s", tailOffset)
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

	t.Run("rejects streamID with colon in Subscribe", func(t *testing.T) {
		s := newTestStorage(t)
		_, err := s.Subscribe(context.Background(), "bad:id", "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for streamID with colon in Subscribe, got: %v", err)
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

func TestParseOffset(t *testing.T) {
	t.Run("parses valid hex offset", func(t *testing.T) {
		idx, err := parseOffset("0000000000000001")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx != 1 {
			t.Errorf("expected 1, got %d", idx)
		}
	})

	t.Run("parses empty string as zero", func(t *testing.T) {
		idx, err := parseOffset("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx != 0 {
			t.Errorf("expected 0, got %d", idx)
		}
	})

	t.Run("parses -1 as zero", func(t *testing.T) {
		idx, err := parseOffset("-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx != 0 {
			t.Errorf("expected 0, got %d", idx)
		}
	})

	t.Run("rejects invalid hex", func(t *testing.T) {
		_, err := parseOffset("not-a-hex-value")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for invalid hex, got: %v", err)
		}
	})

	t.Run("rejects invalid characters", func(t *testing.T) {
		_, err := parseOffset("ZZZZZZZZZZZZZZZZ")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for invalid hex chars, got: %v", err)
		}
	})
}

func TestFormatParseOffsetRoundtrip(t *testing.T) {
	testCases := []uint64{0, 1, 255, 256, 65535, 1<<32 - 1, 1 << 32, 1<<64 - 1}
	for _, tc := range testCases {
		formatted := formatOffset(tc)
		parsed, err := parseOffset(formatted)
		if err != nil {
			t.Errorf("parseOffset(%q) error: %v", formatted, err)
			continue
		}
		if parsed != tc {
			t.Errorf("roundtrip failed: %d -> %q -> %d", tc, formatted, parsed)
		}
	}
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

func TestConcurrentSequenceCreation(t *testing.T) {
	// Test that concurrent getOrCreateSequence calls don't cause duplicate sequences
	s := newTestStorage(t)
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Launch many goroutines trying to get sequence simultaneously
	const numGoroutines = 50
	results := make(chan *badger.Sequence, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			seq, err := s.getOrCreateSequence("test")
			if err != nil {
				t.Errorf("getOrCreateSequence error: %v", err)
				results <- nil
				return
			}
			results <- seq
		}()
	}

	// All goroutines should get the same sequence
	var firstSeq *badger.Sequence
	for i := 0; i < numGoroutines; i++ {
		seq := <-results
		if seq == nil {
			continue
		}
		if firstSeq == nil {
			firstSeq = seq
		} else if seq != firstSeq {
			t.Error("got different sequence instances - race condition in getOrCreateSequence")
		}
	}
}
