package memorystorage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

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

	t.Run("allows recreation of expired stream", func(t *testing.T) {
		s := New()

		// Create with already-expired ExpiresAt
		_, err := s.Create(context.Background(), "expired-test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour), // Already expired
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

	t.Run("idempotent create with same TTL but different computed ExpiresAt", func(t *testing.T) {
		s := New()
		now := time.Now()

		// First create with TTL (simulating what handler does)
		cfg1 := durablestream.StreamConfig{
			ContentType: "text/plain",
			TTL:         time.Hour,
			ExpiresAt:   now.Add(time.Hour),
		}
		created1, err := s.Create(context.Background(), "ttl-idem", cfg1)
		if err != nil {
			t.Fatalf("first create: %v", err)
		}
		if !created1 {
			t.Error("first create should return created=true")
		}

		// Second create with same TTL but different ExpiresAt (computed later)
		cfg2 := durablestream.StreamConfig{
			ContentType: "text/plain",
			TTL:         time.Hour,
			ExpiresAt:   now.Add(time.Hour + time.Second), // Slightly later
		}
		created2, err := s.Create(context.Background(), "ttl-idem", cfg2)
		if err != nil {
			t.Fatalf("second create should be idempotent: %v", err)
		}
		if created2 {
			t.Error("second create should return created=false (idempotent)")
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

		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 0)
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
		if offset != "0000000000000000_0000000000000001" {
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

		result, _ := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 0)
		if len(result.Messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(result.Messages))
		}
	})

	t.Run("notifies waiters via WaitForData", func(t *testing.T) {
		s := New()
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
		if string(concatMessages(result)) != "hello world" {
			t.Errorf("unexpected data: %s", concatMessages(result))
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
		if string(concatMessages(result)) != "data" {
			t.Errorf("unexpected data: %s", concatMessages(result))
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

	t.Run("returns empty result past the tail", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		// Offset beyond current tail. Per the Storage contract this is not an
		// error: ErrGone is reserved for retention/compaction.
		const past = durablestream.Offset("0000000000000000_0000000000000099")
		result, err := s.Read(context.Background(), "test", past, 0)
		if err != nil {
			t.Fatalf("read past tail: %v", err)
		}
		if len(result.Messages) != 0 {
			t.Errorf("got %d messages past the tail, want 0", len(result.Messages))
		}
		if result.NextOffset != past {
			t.Errorf("NextOffset = %q, want the requested offset %q", result.NextOffset, past)
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
		// With message-based storage, limit applies to total bytes but we always return whole messages
		// So a single large message will be returned in full even if it exceeds limit
		_, _ = s.Append(context.Background(), "test", []byte("hello"), "")
		_, _ = s.Append(context.Background(), "test", []byte("world"), "")
		_, _ = s.Append(context.Background(), "test", []byte("extra"), "")

		// Limit of 8 should return first two messages (10 bytes total, but we return whole messages)
		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 8)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// First message (5 bytes) fits, second message (5 bytes) would exceed but we include at least one
		// With 8 byte limit: first msg (5) fits, second msg would make 10 > 8, but we have at least one
		// Actually the logic is: if adding msg exceeds limit AND we have at least one, stop
		// So: msg1 (5) fits (5 <= 8), msg2 (5+5=10 > 8) but we have 1, stop
		if len(result.Messages) != 1 {
			t.Errorf("expected 1 message with limit 8, got %d", len(result.Messages))
		}
		if string(concatMessages(result)) != "hello" {
			t.Errorf("unexpected data: %s", concatMessages(result))
		}
	})

	t.Run("returns JSON messages", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "application/json"})
		_, _ = s.Append(context.Background(), "test", []byte(`{"a":1}`), "")
		_, _ = s.Append(context.Background(), "test", []byte(`{"b":2}`), "")
		_, _ = s.Append(context.Background(), "test", []byte(`{"c":3}`), "")

		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 0)
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

		result, _ := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 0)
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
		result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000001", 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(concatMessages(result)) != "second" {
			t.Errorf("expected 'second', got '%s'", concatMessages(result))
		}
	})

	t.Run("returns bad request for negative offset", func(t *testing.T) {
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
		_, _ = s.Append(context.Background(), "test", []byte("data"), "")

		// -2 is not a valid offset (only -1 is special sentinel for stream start)
		// Negative offsets are invalid input, not "gone" (client ahead of stream)
		_, err := s.Read(context.Background(), "test", "-2", 0)
		if !errors.Is(err, durablestream.ErrBadRequest) {
			t.Errorf("expected ErrBadRequest for negative offset, got: %v", err)
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

	t.Run("wakes WaitForData callers on delete", func(t *testing.T) {
		s := New()
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
}

func TestWaitForData(t *testing.T) {
	t.Run("returns immediately when data exists", func(t *testing.T) {
		s := New()
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
		s := New()
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
		s := New()
		_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err := s.WaitForData(ctx, "test", "", 0)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("returns ErrNotFound for non-existent stream", func(t *testing.T) {
		s := New()

		_, err := s.WaitForData(context.Background(), "nonexistent", "", 0)
		if !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("returns ErrNotFound for expired stream", func(t *testing.T) {
		s := New()
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
		s := New()
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
		s := New()
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
		s := New()
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
}

func TestFormatSimpleOffset(t *testing.T) {
	tests := []struct {
		idx  int64
		want durablestream.Offset
	}{
		{0, "0000000000000000_0000000000000000"},
		{1, "0000000000000000_0000000000000001"},
		{123, "0000000000000000_0000000000000123"},
		{9999999999, "0000000000000000_0000009999999999"},
	}

	for _, tt := range tests {
		got := storage.FormatSimpleOffset(tt.idx)
		if got != tt.want {
			t.Errorf("FormatSimpleOffset(%d) = %s, want %s", tt.idx, got, tt.want)
		}
	}
}

func TestParseOffset(t *testing.T) {
	tests := []struct {
		offset  durablestream.Offset
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"-1", 0, false},
		{"0000000000000000_0000000000000000", 0, false},
		{"0000000000000000_0000000000000001", 1, false},
		{"0000000000000000_0000000000000123", 123, false},
		{"0_123", 123, false}, // Without zero-padding
		{"-5", 0, true},       // Invalid format
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		_, got, err := storage.ParseOffset(tt.offset)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseOffset(%q) expected error, got nil", tt.offset)
			}
		} else {
			if err != nil {
				t.Errorf("ParseOffset(%q) unexpected error: %v", tt.offset, err)
			}
			if got != tt.want {
				t.Errorf("ParseOffset(%q) = %d, want %d", tt.offset, got, tt.want)
			}
		}
	}
}

func TestStreamConfig_IsExpired(t *testing.T) {
	t.Run("not expired with zero ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{}
		if cfg.IsExpired() {
			t.Error("expected not expired with zero ExpiresAt")
		}
	})

	t.Run("not expired with future ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(time.Hour),
		}
		if cfg.IsExpired() {
			t.Error("expected not expired with future ExpiresAt")
		}
	})

	t.Run("expired with past ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(-time.Hour),
		}
		if !cfg.IsExpired() {
			t.Error("expected expired with past ExpiresAt")
		}
	})
}

func TestStreamConfig_Matches(t *testing.T) {
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
		{
			name: "same TTL with different ExpiresAt should match (idempotent)",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now.Add(time.Second)},
			want: true,
		},
		{
			name: "different ExpiresAt without TTL should not match",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: 0, ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: 0, ExpiresAt: now.Add(time.Hour)},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Matches(tt.b)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
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
				_, _ = s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 0)
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
	if info.NextOffset != "0000000000000000_0000000000001000" {
		t.Errorf("expected 1000 appends, got offset %s", info.NextOffset)
	}
}

// TestDeleteConcurrentWithExpiredCreate stresses the window in which Create
// replaces an expired stream while Delete removes it. Both paths wake waiters
// by closing notifyCh; before the fix one of them could close it twice and
// panic with "close of closed channel". Run with -race.
func TestDeleteConcurrentWithExpiredCreate(t *testing.T) {
	const iterations = 2000

	for i := 0; i < iterations; i++ {
		s := New()
		if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		}); err != nil {
			t.Fatalf("seed create: %v", err)
		}

		// start gates both goroutines so they contend on the same window.
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{
				ContentType: "application/json",
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = s.Delete(context.Background(), "test")
		}()
		close(start)
		wg.Wait()
	}
}

// TestCapturedStreamRejectsAppendAfterDelete covers the stale-pointer window in
// Append: it can load a stream just before Delete removes and marks it, then
// acquire the stream lock only after Delete has closed notifyCh. The old code
// closed notifyCh a second time and panicked.
func TestCapturedStreamRejectsAppendAfterDelete(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	stream, ok := s.streams.Load("test")
	if !ok {
		t.Fatal("created stream is missing from map")
	}
	if err := s.Delete(context.Background(), "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := appendBatchToStream(stream, [][]byte{[]byte("stale")}, ""); !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("append through pointer captured before Delete = %v, want ErrNotFound", err)
	}
	if _, err := readStream(context.Background(), stream, durablestream.ZeroOffset, 0); !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("read through pointer captured before Delete = %v, want ErrNotFound", err)
	}
}

// TestWaitStaysBoundToDeletedIncarnation verifies that a waiter holding the old
// stream pointer cannot consume data from a replacement with the same ID.
func TestWaitStaysBoundToDeletedIncarnation(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create old stream: %v", err)
	}
	old, ok := s.streams.Load("test")
	if !ok {
		t.Fatal("old stream is missing from map")
	}

	if err := s.Delete(context.Background(), "test"); err != nil {
		t.Fatalf("delete old stream: %v", err)
	}
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "application/json"}); err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	if _, err := s.Append(context.Background(), "test", []byte(`{"replacement":true}`), ""); err != nil {
		t.Fatalf("append replacement data: %v", err)
	}

	res, err := s.waitForStream(context.Background(), old, durablestream.ZeroOffset, 0)
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("wait on deleted incarnation = (%v, %v), want ErrNotFound", res, err)
	}
}

// TestConcurrentCreateOfExpiredStream asserts that when many goroutines race to
// replace the same expired stream, exactly one observes created=true. Before the
// fix, LoadOrStore followed by an unconditional Store let several callers each
// claim creation and install their own replacement.
func TestConcurrentCreateOfExpiredStream(t *testing.T) {
	const iterations = 500
	const creators = 4

	for i := 0; i < iterations; i++ {
		s := New()
		if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		}); err != nil {
			t.Fatalf("seed create: %v", err)
		}

		cfg := durablestream.StreamConfig{ContentType: "application/json"}
		start := make(chan struct{})
		results := make(chan bool, creators)
		var wg sync.WaitGroup
		for j := 0; j < creators; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				created, err := s.Create(context.Background(), "test", cfg)
				if err != nil {
					t.Errorf("create: %v", err)
					return
				}
				results <- created
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		createdCount := 0
		for created := range results {
			if created {
				createdCount++
			}
		}
		if createdCount != 1 {
			t.Fatalf("expected exactly 1 creator to observe created=true, got %d", createdCount)
		}
	}
}

// TestReadReturnsCopy verifies Read hands out message data the caller owns:
// mutating it must not corrupt the stored log.
func TestReadReturnsCopy(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Append(context.Background(), "test", []byte("hello"), ""); err != nil {
		t.Fatalf("append: %v", err)
	}

	first, err := s.Read(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if len(first.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(first.Messages))
	}
	copy(first.Messages[0].Data, "XXXXX")

	second, err := s.Read(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if len(second.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(second.Messages))
	}
	if got := string(second.Messages[0].Data); got != "hello" {
		t.Errorf("stored data mutated by caller: got %q, want %q", got, "hello")
	}
}

// TestWaitForDataReturnsCopy covers the same ownership contract through
// WaitForData, which returns results produced by Read.
func TestWaitForDataReturnsCopy(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Append(context.Background(), "test", []byte("hello"), ""); err != nil {
		t.Fatalf("append: %v", err)
	}

	first, err := s.WaitForData(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("first WaitForData: %v", err)
	}
	copy(first.Messages[0].Data, "XXXXX")

	second, err := s.Read(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(second.Messages[0].Data); got != "hello" {
		t.Errorf("stored data mutated by caller: got %q, want %q", got, "hello")
	}
}

// TestAppendCopiesInput verifies the storage copies caller-provided data, so a
// caller reusing its buffer cannot rewrite history.
func TestAppendCopiesInput(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	buf := []byte("hello")
	if _, err := s.Append(context.Background(), "test", buf, ""); err != nil {
		t.Fatalf("append: %v", err)
	}
	copy(buf, "XXXXX")

	result, err := s.Read(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(result.Messages[0].Data); got != "hello" {
		t.Errorf("stored data reflects caller buffer reuse: got %q, want %q", got, "hello")
	}
}

// TestCancelledContext verifies every method fails fast when the caller's
// context is already cancelled, rather than doing work for a dead request.
func TestCancelledContext(t *testing.T) {
	s := New()
	if _, err := s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Append(context.Background(), "test", []byte("data"), ""); err != nil {
		t.Fatalf("append: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Create", func(t *testing.T) {
		if _, err := s.Create(ctx, "other", durablestream.StreamConfig{ContentType: "text/plain"}); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
		if _, err := s.Head(context.Background(), "other"); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("cancelled Create should not have created the stream, got: %v", err)
		}
	})

	t.Run("Append", func(t *testing.T) {
		if _, err := s.Append(ctx, "test", []byte("more"), ""); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("Read", func(t *testing.T) {
		if _, err := s.Read(ctx, "test", "", 0); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("Head", func(t *testing.T) {
		if _, err := s.Head(ctx, "test"); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("WaitForData", func(t *testing.T) {
		// Data is available, so only an entry check can surface cancellation.
		if _, err := s.WaitForData(ctx, "test", "", 0); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		if err := s.Delete(ctx, "test"); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
		if _, err := s.Head(context.Background(), "test"); err != nil {
			t.Errorf("cancelled Delete should not have removed the stream, got: %v", err)
		}
	})
}

func TestReadWithPartialLimit(t *testing.T) {
	s := New()
	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})

	// Append multiple messages
	_, _ = s.Append(context.Background(), "test", []byte("aaaa"), "") // offset 1
	_, _ = s.Append(context.Background(), "test", []byte("bbbb"), "") // offset 2
	_, _ = s.Append(context.Background(), "test", []byte("cccc"), "") // offset 3

	// Read from start with limit of 6 bytes
	// With message-based storage, we return whole messages until limit exceeded
	// First msg (4 bytes) fits, second msg (4+4=8 > 6) exceeds but we have one, stop
	result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000000", 6)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(result.Messages) != 1 {
		t.Errorf("expected 1 message with limit 6, got %d", len(result.Messages))
	}
	if string(concatMessages(result)) != "aaaa" {
		t.Errorf("expected 'aaaa', got '%s'", concatMessages(result))
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
	result, err := s.Read(context.Background(), "test", "0000000000000000_0000000000000002", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Should return messages from offset 2 onwards
	if len(result.Messages) != 3 {
		t.Errorf("expected 3 messages from offset 2, got %d", len(result.Messages))
	}
	if !bytes.Equal(result.Messages[0].Data, []byte(`{"n":3}`)) {
		t.Errorf("first message should be {\"n\":3}, got %s", result.Messages[0].Data)
	}
}
