package memorystorage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestNew(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

func TestCreate(t *testing.T) {
	t.Run("creates new stream", func(t *testing.T) {
		s := New()
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
		s := New()
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
		s := New()

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
		s := New()

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
		s := New()
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

	t.Run("initializes JSON messages slice for JSON content type", func(t *testing.T) {
		s := New()
		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Verify by appending and reading JSON
		_, err = s.Append(context.Background(), "test", []byte(`{"key":"value"}`), "")
		if err != nil {
			t.Fatalf("append: %v", err)
		}

		result, err := s.Read(context.Background(), "test", "0000000000", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(result.Messages))
		}
	})

	t.Run("content type matching is case-insensitive", func(t *testing.T) {
		s := New()

		_, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "Application/JSON",
		})
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		// Should match with different case
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
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		offset, err := s.Append(context.Background(), "test", []byte("hello"), "")
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if offset != "0000000001" {
			t.Errorf("expected offset 0000000001, got %s", offset)
		}
	})

	t.Run("rejects empty append", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		_, err := s.Append(context.Background(), "test", []byte{}, "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest, got: %v", err)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := New()

		_, err := s.Append(context.Background(), "nonexistent", []byte("data"), "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour), // Already expired
		})

		_, err := s.Append(context.Background(), "test", []byte("data"), "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("validates sequence numbers", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// First append with seq
		_, err := s.Append(context.Background(), "test", []byte("data1"), "seq_002")
		if err != nil {
			t.Fatalf("first append: %v", err)
		}

		// Append with higher seq should succeed
		_, err = s.Append(context.Background(), "test", []byte("data2"), "seq_003")
		if err != nil {
			t.Fatalf("second append: %v", err)
		}

		// Append with lower seq should fail
		_, err = s.Append(context.Background(), "test", []byte("data3"), "seq_001")
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict for sequence regression, got: %v", err)
		}

		// Append with equal seq should fail
		_, err = s.Append(context.Background(), "test", []byte("data4"), "seq_003")
		if !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("expected ErrConflict for duplicate sequence, got: %v", err)
		}
	})

	t.Run("tracks JSON messages", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "application/json"})

		_, _ = s.Append(context.Background(), "test", []byte(`{"a":1}`), "")
		_, _ = s.Append(context.Background(), "test", []byte(`{"b":2}`), "")

		result, _ := s.Read(context.Background(), "test", "0000000000", 0)
		if len(result.Messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(result.Messages))
		}
	})

	t.Run("notifies subscribers", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, _ := s.Subscribe(ctx, "test", "")

		// Append should notify
		go func() {
			time.Sleep(10 * time.Millisecond)
			_, _ = s.Append(context.Background(), "test", []byte("data"), "")
		}()

		select {
		case offset := <-ch:
			if offset != "0000000001" {
				t.Errorf("expected offset 0000000001, got %s", offset)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("subscriber not notified")
		}
	})
}

func TestAppendReader(t *testing.T) {
	t.Run("appends from reader", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		r := strings.NewReader("hello from reader")
		offset, err := s.AppendReader(context.Background(), "test", r, "")
		if err != nil {
			t.Fatalf("append reader: %v", err)
		}
		if offset != "0000000001" {
			t.Errorf("expected offset 0000000001, got %s", offset)
		}

		result, _ := s.Read(context.Background(), "test", "0000000000", 0)
		if string(result.Data) != "hello from reader" {
			t.Errorf("unexpected data: %s", result.Data)
		}
	})

	t.Run("propagates read errors", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		r := &errorReader{err: errors.New("read failed")}
		_, err := s.AppendReader(context.Background(), "test", r, "")
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest, got: %v", err)
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
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("hello"), "")
		_, _ = s.Append(context.Background(), "test", []byte(" world"), "")

		result, err := s.Read(context.Background(), "test", "", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(result.Data) != "hello world" {
			t.Errorf("unexpected data: %s", result.Data)
		}
	})

	t.Run("reads with -1 offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		result, err := s.Read(context.Background(), "test", "-1", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(result.Data) != "data" {
			t.Errorf("unexpected data: %s", result.Data)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := New()

		_, err := s.Read(context.Background(), "nonexistent", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.Read(context.Background(), "test", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})

	t.Run("returns gone for invalid offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		// Offset beyond current tail
		_, err := s.Read(context.Background(), "test", "0000000099", 0)
		if !errors.Is(err, durablestream.ErrGone) {
			t.Errorf("expected ErrGone, got: %v", err)
		}
	})

	t.Run("returns error for malformed offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		_, err := s.Read(context.Background(), "test", "invalid", 0)
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest, got: %v", err)
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("hello world this is a lot of data"), "")

		result, err := s.Read(context.Background(), "test", "0000000000", 5)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(result.Data) != 5 {
			t.Errorf("expected 5 bytes, got %d", len(result.Data))
		}
		if string(result.Data) != "hello" {
			t.Errorf("unexpected data: %s", result.Data)
		}
	})

	t.Run("returns JSON messages", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "application/json"})
		_, _ = s.Append(context.Background(), "test", []byte(`{"a":1}`), "")
		_, _ = s.Append(context.Background(), "test", []byte(`{"b":2}`), "")
		_, _ = s.Append(context.Background(), "test", []byte(`{"c":3}`), "")

		result, err := s.Read(context.Background(), "test", "0000000000", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(result.Messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(result.Messages))
		}
	})

	t.Run("returns correct next offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("msg1"), "")
		_, _ = s.Append(context.Background(), "test", []byte("msg2"), "")

		result, _ := s.Read(context.Background(), "test", "0000000000", 0)
		if result.NextOffset != result.TailOffset {
			t.Errorf("NextOffset (%s) should equal TailOffset (%s) when at end", result.NextOffset, result.TailOffset)
		}
	})

	t.Run("read from middle offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("first"), "")
		_, _ = s.Append(context.Background(), "test", []byte("second"), "")

		// Read from offset 1 (after first message)
		result, err := s.Read(context.Background(), "test", "0000000001", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(result.Data) != "second" {
			t.Errorf("expected 'second', got '%s'", result.Data)
		}
	})

	t.Run("returns gone for negative offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		// -2 is not a valid offset (only -1 is special)
		_, err := s.Read(context.Background(), "test", "-2", 0)
		if !errors.Is(err, durablestream.ErrGone) {
			t.Errorf("expected ErrGone for negative offset, got: %v", err)
		}
	})
}

func TestHead(t *testing.T) {
	t.Run("returns stream info", func(t *testing.T) {
		s := New()
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
		if info.NextOffset != "0000000001" {
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
		s := New()

		_, err := s.Head(context.Background(), "nonexistent")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})

		_, err := s.Head(context.Background(), "test")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound for expired stream, got: %v", err)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("deletes stream", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		err := s.Delete(context.Background(), "test")
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		// Verify stream is gone
		_, err = s.Head(context.Background(), "test")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got: %v", err)
		}
	})

	t.Run("returns not found for non-existent stream", func(t *testing.T) {
		s := New()

		err := s.Delete(context.Background(), "nonexistent")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("closes subscriber channels", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// Use a long timeout to avoid triggering context cancellation during test
		// (There's a known issue with double-close if context is cancelled after delete)
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		_ = cancel // Don't cancel during test

		ch, _ := s.Subscribe(ctx, "test", "")

		// Delete the stream
		_ = s.Delete(context.Background(), "test")

		// Channel should be closed
		select {
		case _, ok := <-ch:
			if ok {
				// We might receive pending data, wait for close
				_, ok = <-ch
			}
			if ok {
				t.Error("expected channel to be closed after delete")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("channel not closed after delete")
		}
	})
}

func TestSubscribe(t *testing.T) {
	t.Run("subscribes successfully", func(t *testing.T) {
		s := New()
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
		s := New()

		_, err := s.Subscribe(context.Background(), "nonexistent", "")
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns not found for expired stream", func(t *testing.T) {
		s := New()
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
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		ch, _ := s.Subscribe(ctx, "test", "")

		// Cancel context
		cancel()

		// Wait a bit for unsubscribe goroutine to run
		time.Sleep(50 * time.Millisecond)

		// Channel should be closed
		select {
		case _, ok := <-ch:
			if ok {
				t.Error("expected channel to be closed after context cancellation")
			}
		default:
			// Channel is empty but might not be closed yet, try again
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

	t.Run("handles stream deletion while subscribed", func(t *testing.T) {
		// Test that deleting a stream properly cleans up subscribers.
		// NOTE: There is a known issue if context is cancelled after stream is
		// deleted (potential double-close panic). This test avoids that by
		// using a long-lived context.
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		// Use a timeout context that won't expire during the test
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		_ = cancel // Don't cancel during test to avoid double-close

		ch, _ := s.Subscribe(ctx, "test", "")

		// Delete stream - this should close the channel
		_ = s.Delete(context.Background(), "test")

		// Channel should be closed by delete
		select {
		case _, ok := <-ch:
			if ok {
				t.Error("expected channel to be closed after delete")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("channel not closed after delete")
		}
	})

	t.Run("non-blocking notification", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Subscribe but don't read from channel
		_, _ = s.Subscribe(ctx, "test", "")

		// Fill up the channel buffer (size 10)
		for i := 0; i < 20; i++ {
			_, err := s.Append(context.Background(), "test", []byte("data"), "")
			if err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}

		// Should not have blocked
	})
}

func TestFormatOffset(t *testing.T) {
	tests := []struct {
		idx  int
		want durablestream.Offset
	}{
		{0, "0000000000"},
		{1, "0000000001"},
		{123, "0000000123"},
		{9999999999, "9999999999"},
	}

	for _, tt := range tests {
		got := formatOffset(tt.idx)
		if got != tt.want {
			t.Errorf("formatOffset(%d) = %s, want %s", tt.idx, got, tt.want)
		}
	}
}

func TestParseOffset(t *testing.T) {
	tests := []struct {
		offset  durablestream.Offset
		want    int
		wantErr bool
	}{
		{"", 0, false},
		{"-1", 0, false},
		{"0000000000", 0, false},
		{"0000000001", 1, false},
		{"0000000123", 123, false},
		{"123", 123, false}, // Without zero-padding
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		got, err := parseOffset(tt.offset)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseOffset(%q) expected error, got nil", tt.offset)
			}
		} else {
			if err != nil {
				t.Errorf("parseOffset(%q) unexpected error: %v", tt.offset, err)
			}
			if got != tt.want {
				t.Errorf("parseOffset(%q) = %d, want %d", tt.offset, got, tt.want)
			}
		}
	}
}

func TestIsJSONContentType(t *testing.T) {
	tests := []struct {
		contentType string
		want        bool
	}{
		{"application/json", true},
		{"APPLICATION/JSON", true},
		{"Application/Json", true},
		{"application/json; charset=utf-8", true},
		{"text/plain", false},
		{"application/octet-stream", false},
		{"text/json", false}, // Not standard
	}

	for _, tt := range tests {
		got := isJSONContentType(tt.contentType)
		if got != tt.want {
			t.Errorf("isJSONContentType(%q) = %v, want %v", tt.contentType, got, tt.want)
		}
	}
}

func TestContentTypesMatch(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"text/plain", "text/plain", true},
		{"TEXT/PLAIN", "text/plain", true},
		{"application/json", "APPLICATION/JSON", true},
		{"text/plain; charset=utf-8", "text/plain", true},
		{"text/plain", "text/html", false},
		{"application/json", "application/xml", false},
	}

	for _, tt := range tests {
		got := contentTypesMatch(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("contentTypesMatch(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestIsExpired(t *testing.T) {
	t.Run("not expired with zero ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{}
		if isExpired(cfg) {
			t.Error("expected not expired with zero ExpiresAt")
		}
	})

	t.Run("not expired with future ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(time.Hour),
		}
		if isExpired(cfg) {
			t.Error("expected not expired with future ExpiresAt")
		}
	})

	t.Run("expired with past ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(-time.Hour),
		}
		if !isExpired(cfg) {
			t.Error("expected expired with past ExpiresAt")
		}
	})
}

func TestConfigsMatch(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		a, b durablestream.StreamConfig
		want bool
	}{
		{
			name: "identical configs",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now},
			want: true,
		},
		{
			name: "different content type",
			a:    durablestream.StreamConfig{ContentType: "text/plain"},
			b:    durablestream.StreamConfig{ContentType: "application/json"},
			want: false,
		},
		{
			name: "different TTL",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: 2 * time.Hour},
			want: false,
		},
		{
			name: "different ExpiresAt",
			a:    durablestream.StreamConfig{ContentType: "text/plain", ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", ExpiresAt: now.Add(time.Hour)},
			want: false,
		},
		{
			name: "case-insensitive content type",
			a:    durablestream.StreamConfig{ContentType: "TEXT/PLAIN"},
			b:    durablestream.StreamConfig{ContentType: "text/plain"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := configsMatch(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("configsMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	done := make(chan struct{})

	// Multiple concurrent appends
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				data := []byte(string(rune('A'+n)) + string(rune('0'+j%10)))
				_, _ = s.Append(context.Background(), "test", data, "")
			}
			done <- struct{}{}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _ = s.Read(context.Background(), "test", "0000000000", 0)
			}
			done <- struct{}{}
		}()
	}

	// Concurrent heads
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _ = s.Head(context.Background(), "test")
			}
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Verify data integrity
	info, _ := s.Head(context.Background(), "test")
	if info.NextOffset != "0000001000" {
		t.Errorf("expected 1000 appends, got offset %s", info.NextOffset)
	}
}

func TestReadWithPartialLimit(t *testing.T) {
	s := New()
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Append multiple messages
	_, _ = s.Append(context.Background(), "test", []byte("aaaa"), "") // offset 1, bytes 0-4
	_, _ = s.Append(context.Background(), "test", []byte("bbbb"), "") // offset 2, bytes 4-8
	_, _ = s.Append(context.Background(), "test", []byte("cccc"), "") // offset 3, bytes 8-12

	// Read from start with limit that cuts into second message
	result, err := s.Read(context.Background(), "test", "0000000000", 6)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(result.Data) != "aaaabb" {
		t.Errorf("expected 'aaaabb', got '%s'", result.Data)
	}
}

func TestReadJSONWithMultipleMessages(t *testing.T) {
	s := New()
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "application/json"})

	// Append multiple JSON messages
	msgs := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`}
	for _, msg := range msgs {
		_, _ = s.Append(context.Background(), "test", []byte(msg), "")
	}

	// Read from middle
	result, err := s.Read(context.Background(), "test", "0000000002", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Should return messages from offset 2 onwards
	if len(result.Messages) != 3 {
		t.Errorf("expected 3 messages from offset 2, got %d", len(result.Messages))
	}
	if !bytes.Equal(result.Messages[0], []byte(`{"n":3}`)) {
		t.Errorf("first message should be {\"n\":3}, got %s", result.Messages[0])
	}
}
