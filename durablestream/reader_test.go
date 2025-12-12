package durablestream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReader_Offset(t *testing.T) {
	server, _, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream with initial data
	_, err := client.Create(ctx, "/stream1", &CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader := client.Reader("/stream1", ZeroOffset)
	defer reader.Close()

	// Initial offset should be zero
	if reader.Offset() != ZeroOffset {
		t.Errorf("initial offset = %v, want %v", reader.Offset(), ZeroOffset)
	}

	// Read and check offset advances
	_, err = reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if reader.Offset() == ZeroOffset {
		t.Error("offset should have advanced after read")
	}
}

func TestReader_Seek(t *testing.T) {
	server, storage, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream with initial data
	_, err := client.Create(ctx, "/stream1", &CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Append more data
	_, err = storage.Append(ctx, "/stream1", []byte("world"), "")
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	reader := client.Reader("/stream1", ZeroOffset)
	defer reader.Close()

	// Read all data (may be in one or multiple chunks)
	result1, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("first read failed: %v", err)
	}
	if len(result1.Data) == 0 {
		t.Fatal("expected data from first read")
	}

	// Save the offset after first read
	savedOffset := reader.Offset()
	if savedOffset == ZeroOffset {
		t.Error("offset should advance after read")
	}

	// Seek back to start
	r := reader.Seek(ZeroOffset)
	if r != reader {
		t.Error("Seek should return same reader for chaining")
	}
	if reader.Offset() != ZeroOffset {
		t.Errorf("offset after Seek = %v, want %v", reader.Offset(), ZeroOffset)
	}

	// Read again from start - should get same data
	result2, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("second read failed: %v", err)
	}
	if string(result2.Data) != string(result1.Data) {
		t.Errorf("data after seek = %q, want %q", result2.Data, result1.Data)
	}

	// Seek to saved offset and verify
	reader.Seek(savedOffset)
	if reader.Offset() != savedOffset {
		t.Errorf("offset after Seek(savedOffset) = %v, want %v", reader.Offset(), savedOffset)
	}
}

func TestReader_SeekTail(t *testing.T) {
	server, storage, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream with initial data
	_, err := client.Create(ctx, "/stream1", &CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("old data"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader := client.Reader("/stream1", ZeroOffset)
	defer reader.Close()

	// Seek to tail
	err = reader.SeekTail(ctx)
	if err != nil {
		t.Fatalf("SeekTail failed: %v", err)
	}

	// Offset should be at tail (past initial data)
	if reader.Offset() == ZeroOffset {
		t.Error("offset should not be zero after SeekTail")
	}

	// Append new data
	_, _ = storage.Append(ctx, "/stream1", []byte("new data"), "")

	// Read should only get new data (since we're already at tail, reader won't catch up)
	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(result.Data) != "new data" {
		t.Errorf("data = %q, want %q (should only see data after SeekTail)", string(result.Data), "new data")
	}
}

func TestReader_CatchUpAndLive(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{LongPollTimeout: 200 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 300 * time.Millisecond})

	ctx := context.Background()

	// Create stream
	_, _ = storage.Create(ctx, "/stream", StreamConfig{ContentType: "text/plain"})
	offset, _ := storage.Append(ctx, "/stream", []byte("initial"), "")

	t.Run("catch-up returns immediately with available data", func(t *testing.T) {
		// Start from beginning of stream (catch-up mode)
		reader := client.Reader("/stream", ZeroOffset)
		defer reader.Close()

		start := time.Now()
		result, err := reader.Read(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if duration > 100*time.Millisecond {
			t.Errorf("should return immediately, took %v", duration)
		}
		if string(result.Data) != "initial" {
			t.Errorf("data = %q, want %q", string(result.Data), "initial")
		}
	})

	t.Run("live mode timeout returns no content", func(t *testing.T) {
		reader := client.Reader("/stream", offset)
		defer reader.Close()

		// First read catches up (should be up to date)
		result, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("catch-up read failed: %v", err)
		}
		if !result.UpToDate {
			t.Error("expected UpToDate after catch-up")
		}

		// Next read should be in long-poll mode and timeout
		start := time.Now()
		result, err = reader.Read(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("live read failed: %v", err)
		}
		// Should wait approximately the timeout duration
		if duration < 150*time.Millisecond {
			t.Errorf("should wait for timeout, returned in %v", duration)
		}
		if !result.UpToDate {
			t.Error("expected UpToDate to be true after timeout")
		}
	})

	t.Run("live mode returns when data arrives", func(t *testing.T) {
		reader := client.Reader("/stream", offset)
		defer reader.Close()

		// First read catches up
		_, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("catch-up read failed: %v", err)
		}

		// Append data after short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			_, _ = storage.Append(ctx, "/stream", []byte("new data"), "")
		}()

		start := time.Now()
		result, err := reader.Read(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("live read failed: %v", err)
		}
		// Should return after data arrives, not after full timeout
		if duration > 150*time.Millisecond {
			t.Errorf("should return when data arrives, took %v", duration)
		}
		if string(result.Data) != "new data" {
			t.Errorf("data = %q, want %q", string(result.Data), "new data")
		}
	})
}

func TestReader_MessagesIterator(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{LongPollTimeout: 100 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 200 * time.Millisecond})

	ctx := context.Background()

	t.Run("Messages iterator yields JSON messages", func(t *testing.T) {
		_, _ = storage.Create(ctx, "/iter-stream", StreamConfig{ContentType: "application/json"})
		_, _ = storage.Append(ctx, "/iter-stream", []byte(`{"msg":1}`), "")
		_, _ = storage.Append(ctx, "/iter-stream", []byte(`{"msg":2}`), "")

		reader := client.Reader("/iter-stream", ZeroOffset)
		defer reader.Close()

		// Use context with timeout
		iterCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		count := 0
		for msg, err := range reader.Messages(iterCtx) {
			if err != nil {
				// Context timeout is expected
				if err == context.DeadlineExceeded {
					break
				}
				t.Fatalf("unexpected error: %v", err)
			}

			var data map[string]int
			if err := msg.Decode(&data); err == nil {
				count++
			}

			// Break after getting some messages to avoid timeout
			if count >= 2 {
				break
			}
		}

		if count < 1 {
			t.Errorf("expected at least 1 message, got %d", count)
		}
	})

	t.Run("Messages iterator handles context cancellation", func(t *testing.T) {
		_, _ = storage.Create(ctx, "/cancel-stream", StreamConfig{ContentType: "application/json"})

		reader := client.Reader("/cancel-stream", ZeroOffset)
		defer reader.Close()

		// Cancel context immediately
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		for _, err := range reader.Messages(cancelCtx) {
			if err == nil {
				t.Error("expected error from cancelled context")
			}
			break // Only check first iteration
		}
	})
}

func TestReader_Close(t *testing.T) {
	server, _, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	_, err := client.Create(ctx, "/close-stream", &CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("data"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader := client.Reader("/close-stream", ZeroOffset)

	// Close should not error
	if err := reader.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	// Second close should be idempotent
	if err := reader.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}

	// Read after close should error
	_, err = reader.Read(ctx)
	if err == nil {
		t.Error("expected error reading after close")
	}
	if err != ErrClosed {
		t.Errorf("error = %v, want ErrClosed", err)
	}
}

func TestReader_SSEMode(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{SSECloseAfter: 500 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	// Create client with SSE mode
	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})

	ctx := context.Background()

	t.Run("SSE mode reads data events", func(t *testing.T) {
		// Create text stream
		_, _ = storage.Create(ctx, "/sse-stream", StreamConfig{ContentType: "text/plain"})
		_, _ = storage.Append(ctx, "/sse-stream", []byte("hello"), "")

		reader := client.Reader("/sse-stream", ZeroOffset)
		defer reader.Close()

		// First read catches up
		result, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("catch-up read failed: %v", err)
		}
		if !result.UpToDate {
			t.Error("expected UpToDate after catch-up")
		}

		// Append more data and read in SSE mode
		_, _ = storage.Append(ctx, "/sse-stream", []byte("world"), "")

		// Use timeout context to prevent hanging
		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		result, err = reader.Read(sseCtx)
		if err != nil {
			t.Fatalf("SSE read failed: %v", err)
		}

		if len(result.Data) == 0 {
			t.Error("expected data in SSE result")
		}
	})

	t.Run("SSE rejects binary content type", func(t *testing.T) {
		_, _ = storage.Create(ctx, "/binary-stream", StreamConfig{ContentType: "application/octet-stream"})
		_, _ = storage.Append(ctx, "/binary-stream", []byte("data"), "")

		reader := client.Reader("/binary-stream", ZeroOffset)
		defer reader.Close()

		// Catch up first
		_, _ = reader.Read(ctx)

		// SSE read should fail for binary content
		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		_, err := reader.Read(sseCtx)
		if err == nil {
			t.Error("expected error for binary content type with SSE")
		}
	})
}

func TestReader_SSE_ConnectionErrors(t *testing.T) {
	// Create client with invalid URL and SSE mode
	badClient := NewClient("http://localhost:1", &ClientConfig{ReadMode: ReadModeSSE})

	reader := badClient.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase
	reader.catching = false

	_, err := reader.Read(context.Background())
	if err == nil {
		t.Error("expected error for connection failure")
	}
}

func TestReader_SSE_UnknownEventType(t *testing.T) {
	// Create a mock SSE server that sends an unknown event type
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send unknown event type
		_, _ = w.Write([]byte("event: unknown\ndata: test\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase
	reader.catching = false

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := reader.Read(ctx)
	if err == nil {
		t.Error("expected error for unknown event type")
	}
	if !strings.Contains(err.Error(), "unknown SSE event type") {
		t.Errorf("error = %v, want 'unknown SSE event type'", err)
	}
}

func TestReader_SSE_WrongContentType(t *testing.T) {
	// Create a mock server that returns wrong content type
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain") // Wrong!
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not SSE"))
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase
	reader.catching = false

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := reader.Read(ctx)
	if err == nil {
		t.Error("expected error for wrong content type")
	}
	if !strings.Contains(err.Error(), "unexpected content type") {
		t.Errorf("error = %v, want 'unexpected content type'", err)
	}
}

func TestReader_SSE_ControlEvent(t *testing.T) {
	// Create a mock SSE server that sends control event then data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send control event first
		_, _ = w.Write([]byte("event: control\ndata: {\"streamNextOffset\":\"123_456\",\"streamCursor\":\"abc\"}\n\n"))
		flusher.Flush()

		// Then send data event
		_, _ = w.Write([]byte("event: data\ndata: hello world\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase
	reader.catching = false

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Should skip control event and return data event
	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(result.Data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(result.Data))
	}

	// Offset should be updated from control event
	if reader.Offset() != Offset("123_456") {
		t.Errorf("expected offset 123_456, got %v", reader.Offset())
	}
}

func TestReader_SSE_ReadError(t *testing.T) {
	// Create a mock SSE server that closes connection mid-stream
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Don't write anything, just close
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase
	reader.catching = false

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := reader.Read(ctx)
	if err == nil {
		t.Error("expected error when SSE stream closes")
	}

	// Verify SSE stream is cleaned up
	if reader.sseStream != nil {
		t.Error("expected sseStream to be nil after error")
	}
}

func TestReader_Seek_ClearsSSEConnection(t *testing.T) {
	// Create a mock SSE server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("event: data\ndata: hello\n\n"))
		w.(http.Flusher).Flush()
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Force past catch-up phase and establish SSE connection
	reader.catching = false

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Read to establish SSE connection
	_, _ = reader.Read(ctx)

	// Seek should clear SSE connection
	reader.Seek(Offset("999_999"))

	if reader.sseStream != nil {
		t.Error("expected sseStream to be nil after Seek")
	}
	if reader.cursor != "" {
		t.Error("expected cursor to be cleared after Seek")
	}
	if !reader.catching {
		t.Error("expected catching to be true after Seek")
	}
}

func TestReader_Messages_NonJSONData(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{LongPollTimeout: 100 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 200 * time.Millisecond})
	ctx := context.Background()

	// Create text/plain stream with non-JSON data
	_, _ = storage.Create(ctx, "/text-stream", StreamConfig{ContentType: "text/plain"})
	_, _ = storage.Append(ctx, "/text-stream", []byte("hello world"), "")

	reader := client.Reader("/text-stream", ZeroOffset)
	defer reader.Close()

	iterCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	msgCount := 0
	for msg, err := range reader.Messages(iterCtx) {
		if err != nil {
			if err == context.DeadlineExceeded {
				break
			}
			t.Fatalf("unexpected error: %v", err)
		}

		// Non-JSON should still yield as single message
		if string(msg.Bytes()) == "hello world" {
			msgCount++
			break
		}
	}

	if msgCount < 1 {
		t.Error("expected at least 1 message")
	}
}

func TestReader_Messages_ReadError(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{LongPollTimeout: 100 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 200 * time.Millisecond})
	ctx := context.Background()

	// Reader for non-existent stream
	reader := client.Reader("/nonexistent", ZeroOffset)
	defer reader.Close()

	iterCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	errCount := 0
	for _, err := range reader.Messages(iterCtx) {
		if err != nil {
			errCount++
			break // Exit after first error
		}
	}

	if errCount < 1 {
		t.Error("expected at least 1 error")
	}
}

func TestReader_Read_DefaultModeCase(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{LongPollTimeout: 100 * time.Millisecond})
	server := httptest.NewServer(handler)
	defer server.Close()

	// Create client with an invalid/unknown ReadMode value
	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 200 * time.Millisecond})
	// Force an unknown mode (default case in switch)
	client.readMode = ReadMode(99) // Unknown mode

	ctx := context.Background()
	_, _ = storage.Create(ctx, "/default-mode-stream", StreamConfig{ContentType: "text/plain"})
	_, _ = storage.Append(ctx, "/default-mode-stream", []byte("data"), "")

	reader := client.Reader("/default-mode-stream", ZeroOffset)
	defer reader.Close()

	// Skip catch-up phase
	_, _ = reader.Read(ctx) // Catches up
	reader.readMode = ReadMode(99)

	// Second read should use default case (long-poll)
	_, err := reader.Read(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseJSONMessages(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{
			name:    "empty",
			data:    []byte{},
			want:    0,
			wantErr: false,
		},
		{
			name:    "single message",
			data:    []byte(`[{"msg":"hello"}]`),
			want:    1,
			wantErr: false,
		},
		{
			name:    "multiple messages",
			data:    []byte(`[{"msg":"a"},{"msg":"b"},{"msg":"c"}]`),
			want:    3,
			wantErr: false,
		},
		{
			name:    "not array",
			data:    []byte(`{"msg":"hello"}`),
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid json",
			data:    []byte(`not json`),
			want:    0,
			wantErr: true,
		},
		{
			name:    "whitespace",
			data:    []byte("  [  ]  "),
			want:    0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs, err := parseJSONMessages(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJSONMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(msgs) != tt.want {
				t.Errorf("parseJSONMessages() = %d messages, want %d", len(msgs), tt.want)
			}
		})
	}
}
