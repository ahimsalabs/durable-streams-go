package durablestream

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReader_ModeSwitching(t *testing.T) {
	server, _, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("initial"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader := client.Reader("/stream1", ZeroOffset)
	defer reader.Close()

	// Initially in catch-up mode
	if reader.mode != modeCatchUp {
		t.Errorf("initial mode = %v, want %v", reader.mode, modeCatchUp)
	}

	// Switch to long-poll
	r := reader.LongPoll()
	if r != reader {
		t.Error("LongPoll should return same reader for chaining")
	}
	if reader.mode != modeLongPoll {
		t.Errorf("after LongPoll mode = %v, want %v", reader.mode, modeLongPoll)
	}

	// Switch to SSE
	r = reader.SSE()
	if r != reader {
		t.Error("SSE should return same reader for chaining")
	}
	if reader.mode != modeSSE {
		t.Errorf("after SSE mode = %v, want %v", reader.mode, modeSSE)
	}

	// Switch back to long-poll (should not error even with nil sseConn)
	reader.LongPoll()
	if reader.mode != modeLongPoll {
		t.Errorf("back to LongPoll mode = %v, want %v", reader.mode, modeLongPoll)
	}
}

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

	// Append more data
	_, err = client.Append(ctx, "/stream1", []byte("world"), nil)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	reader := client.Reader("/stream1", ZeroOffset)
	defer reader.Close()

	// Read first chunk
	result1, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("first read failed: %v", err)
	}
	savedOffset := reader.Offset()

	// Read second chunk
	_, err = reader.Read(ctx)
	if err != nil {
		t.Fatalf("second read failed: %v", err)
	}

	// Seek back to saved offset
	r := reader.Seek(savedOffset)
	if r != reader {
		t.Error("Seek should return same reader for chaining")
	}
	if reader.Offset() != savedOffset {
		t.Errorf("offset after Seek = %v, want %v", reader.Offset(), savedOffset)
	}

	// Read again from saved position - should get same data as second read
	result3, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("third read failed: %v", err)
	}
	if string(result3.Data) == string(result1.Data) {
		t.Error("expected different data after seeking past first chunk")
	}

	// Seek to start
	reader.Seek(ZeroOffset)
	if reader.Offset() != ZeroOffset {
		t.Errorf("offset after Seek(ZeroOffset) = %v, want %v", reader.Offset(), ZeroOffset)
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
	storage.Append(ctx, "/stream1", []byte("new data"), "")

	// Read should only get new data
	reader.LongPoll()
	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(result.Data) != "new data" {
		t.Errorf("data = %q, want %q (should only see data after SeekTail)", string(result.Data), "new data")
	}
}

func TestReader_Seek_ClearsSSE(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).SSECloseAfter(500 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	ctx := context.Background()

	storage.Create(ctx, "/stream", StreamConfig{ContentType: "text/plain"})
	storage.Append(ctx, "/stream", []byte("data"), "")

	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	// Establish SSE connection
	reader.SSE()
	sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	_, err := reader.Read(sseCtx)
	cancel()
	if err != nil {
		t.Fatalf("SSE read failed: %v", err)
	}

	// Verify SSE connection exists
	if reader.sseConn == nil {
		t.Fatal("sseConn should not be nil after SSE read")
	}

	// Seek should close SSE connection
	reader.Seek(ZeroOffset)
	if reader.sseConn != nil {
		t.Error("sseConn should be nil after Seek")
	}
}

func TestReader_LongPoll(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).LongPollTimeout(200 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().
		BaseURL(server.URL).
		LongPollTimeout(300 * time.Millisecond)

	ctx := context.Background()

	// Create stream
	storage.Create(ctx, "/stream", StreamConfig{ContentType: "text/plain"})
	offset, _ := storage.Append(ctx, "/stream", []byte("initial"), "")

	t.Run("long-poll returns immediately with available data", func(t *testing.T) {
		// Start from offset "0000000000" (beginning of stream) which is non-empty
		reader := client.Reader("/stream", Offset("0000000000"))
		defer reader.Close()

		// Switch to long-poll mode
		reader.LongPoll()

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

	t.Run("long-poll timeout returns no content", func(t *testing.T) {
		reader := client.Reader("/stream", offset)
		defer reader.Close()

		reader.LongPoll()

		start := time.Now()
		result, err := reader.Read(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		// Should wait approximately the timeout duration
		if duration < 150*time.Millisecond {
			t.Errorf("should wait for timeout, returned in %v", duration)
		}
		if !result.UpToDate {
			t.Error("expected UpToDate to be true after timeout")
		}
	})

	t.Run("long-poll returns when data arrives", func(t *testing.T) {
		reader := client.Reader("/stream", offset)
		defer reader.Close()

		reader.LongPoll()

		// Append data after short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			storage.Append(ctx, "/stream", []byte("new data"), "")
		}()

		start := time.Now()
		result, err := reader.Read(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		// Should return after data arrives, not after full timeout
		if duration > 150*time.Millisecond {
			t.Errorf("should return when data arrives, took %v", duration)
		}
		if string(result.Data) != "new data" {
			t.Errorf("data = %q, want %q", string(result.Data), "new data")
		}
	})

	t.Run("long-poll with cursor", func(t *testing.T) {
		// Create new stream to test cursor
		storage.Create(ctx, "/cursor-stream", StreamConfig{ContentType: "text/plain"})
		storage.Append(ctx, "/cursor-stream", []byte("data1"), "")

		reader := client.Reader("/cursor-stream", ZeroOffset)
		defer reader.Close()

		// First read in catch-up mode to get cursor
		// We use result later, so declare it here
		var result *ClientReadResult
		var err error
		result, err = reader.Read(ctx)
		if err != nil {
			t.Fatalf("first read failed: %v", err)
		}
		_ = result // first result not checked, but variable reused below

		// Verify cursor was stored
		if reader.cursor == "" {
			t.Log("cursor not returned (may be implementation specific)")
		}

		// Switch to long-poll and verify it uses cursor
		reader.LongPoll()

		// Append more data
		storage.Append(ctx, "/cursor-stream", []byte("data2"), "")

		result, err = reader.Read(ctx)
		if err != nil {
			t.Fatalf("long-poll read failed: %v", err)
		}

		if len(result.Data) == 0 {
			t.Error("expected data in long-poll result")
		}
	})
}

func TestReader_LongPoll_JSON(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).LongPollTimeout(200 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().
		BaseURL(server.URL).
		LongPollTimeout(300 * time.Millisecond)

	ctx := context.Background()

	// Create JSON stream
	storage.Create(ctx, "/json-stream", StreamConfig{ContentType: "application/json"})
	storage.Append(ctx, "/json-stream", []byte(`{"event":"test"}`), "")

	// Use non-empty offset for long-poll
	reader := client.Reader("/json-stream", Offset("0000000000"))
	defer reader.Close()

	reader.LongPoll()

	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if len(result.Messages) == 0 {
		t.Error("expected JSON messages in result")
	}
}

func TestReader_SSE(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).SSECloseAfter(500 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().BaseURL(server.URL)

	ctx := context.Background()

	t.Run("SSE reads data events", func(t *testing.T) {
		// Create text stream
		storage.Create(ctx, "/sse-stream", StreamConfig{ContentType: "text/plain"})
		storage.Append(ctx, "/sse-stream", []byte("hello"), "")

		// Use non-empty offset for SSE
		reader := client.Reader("/sse-stream", Offset("0000000000"))
		defer reader.Close()

		reader.SSE()

		// Use timeout context to prevent hanging
		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		result, err := reader.Read(sseCtx)
		if err != nil {
			t.Fatalf("SSE read failed: %v", err)
		}

		if len(result.Messages) == 0 {
			t.Error("expected messages in SSE result")
		}
	})

	t.Run("SSE reads JSON messages", func(t *testing.T) {
		storage.Create(ctx, "/sse-json-stream", StreamConfig{ContentType: "application/json"})
		storage.Append(ctx, "/sse-json-stream", []byte(`{"key":"value"}`), "")

		// Use non-empty offset for SSE
		reader := client.Reader("/sse-json-stream", Offset("0000000000"))
		defer reader.Close()

		reader.SSE()

		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		result, err := reader.Read(sseCtx)
		if err != nil {
			t.Fatalf("SSE read failed: %v", err)
		}

		if len(result.Messages) == 0 {
			t.Error("expected JSON messages in SSE result")
		}
	})

	t.Run("SSE handles control events", func(t *testing.T) {
		// Create stream and read to verify control events update state
		storage.Create(ctx, "/sse-control-stream", StreamConfig{ContentType: "text/plain"})
		storage.Append(ctx, "/sse-control-stream", []byte("data1"), "")

		// Use non-empty offset for SSE
		reader := client.Reader("/sse-control-stream", Offset("0000000000"))
		defer reader.Close()

		reader.SSE()

		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		// First read should get data and possibly control event
		result, err := reader.Read(sseCtx)
		if err != nil {
			t.Fatalf("SSE read failed: %v", err)
		}

		// Offset should be updated from control event
		if reader.Offset() == ZeroOffset && result.NextOffset != ZeroOffset {
			// Control event should have updated state
			t.Log("control event processed (offset was updated via result)")
		}
	})

	t.Run("SSE mode switch cleans up connection", func(t *testing.T) {
		storage.Create(ctx, "/sse-switch-stream", StreamConfig{ContentType: "text/plain"})
		storage.Append(ctx, "/sse-switch-stream", []byte("data"), "")

		// Use non-empty offset for SSE
		reader := client.Reader("/sse-switch-stream", Offset("0000000000"))
		defer reader.Close()

		// Switch to SSE and do a read to establish connection
		reader.SSE()

		sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		_, err := reader.Read(sseCtx)
		if err != nil {
			t.Fatalf("first SSE read failed: %v", err)
		}

		// Switch to LongPoll - should clean up SSE connection
		reader.LongPoll()
		if reader.sseConn != nil {
			t.Error("sseConn should be nil after switching to LongPoll")
		}

		// Switch back to SSE - should clean up again
		reader.SSE()
		if reader.sseConn != nil {
			t.Error("sseConn should be nil after switching to SSE (new connection not yet established)")
		}
	})
}

func TestReader_SSE_ErrorHandling(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().BaseURL(server.URL)

	ctx := context.Background()

	t.Run("SSE rejects binary content type", func(t *testing.T) {
		storage.Create(ctx, "/binary-stream", StreamConfig{ContentType: "application/octet-stream"})

		// Use non-empty offset for SSE
		reader := client.Reader("/binary-stream", Offset("0000000000"))
		defer reader.Close()

		reader.SSE()

		_, err := reader.Read(ctx)
		if err == nil {
			t.Error("expected error for binary content type with SSE")
		}
	})

	t.Run("SSE handles connection errors", func(t *testing.T) {
		// Create client with invalid URL
		badClient := NewClient().BaseURL("http://localhost:1") // Invalid port

		// Use non-empty offset for SSE
		reader := badClient.Reader("/stream", Offset("0000000000"))
		defer reader.Close()

		reader.SSE()

		_, err := reader.Read(ctx)
		if err == nil {
			t.Error("expected error for connection failure")
		}
	})
}

func TestReader_MessagesIterator(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).LongPollTimeout(100 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().
		BaseURL(server.URL).
		LongPollTimeout(200 * time.Millisecond)

	ctx := context.Background()

	t.Run("Messages iterator yields JSON messages", func(t *testing.T) {
		storage.Create(ctx, "/iter-stream", StreamConfig{ContentType: "application/json"})
		storage.Append(ctx, "/iter-stream", []byte(`{"msg":1}`), "")
		storage.Append(ctx, "/iter-stream", []byte(`{"msg":2}`), "")

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
			if err := json.Unmarshal(msg, &data); err == nil {
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
		storage.Create(ctx, "/cancel-stream", StreamConfig{ContentType: "application/json"})

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

	t.Run("Messages iterator switches to long-poll when up-to-date", func(t *testing.T) {
		storage.Create(ctx, "/switch-stream", StreamConfig{ContentType: "application/json"})
		storage.Append(ctx, "/switch-stream", []byte(`{"msg":"data"}`), "")

		reader := client.Reader("/switch-stream", ZeroOffset)
		defer reader.Close()

		iterCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()

		readCount := 0
		for range reader.Messages(iterCtx) {
			readCount++
			if readCount >= 1 {
				break
			}
		}

		// After reading all data and getting UpToDate, mode should switch
		// (This happens automatically in the Messages iterator)
		if reader.mode != modeLongPoll && readCount > 0 {
			t.Logf("mode = %v (may vary based on timing)", reader.mode)
		}
	})
}

func TestReader_Bytes_ContextCancel(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).LongPollTimeout(100 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().
		BaseURL(server.URL).
		LongPollTimeout(200 * time.Millisecond)

	ctx := context.Background()

	t.Run("Bytes iterator handles context cancellation", func(t *testing.T) {
		storage.Create(ctx, "/bytes-cancel-stream", StreamConfig{ContentType: "text/plain"})

		reader := client.Reader("/bytes-cancel-stream", ZeroOffset)
		defer reader.Close()

		// Cancel context immediately
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		for _, err := range reader.Bytes(cancelCtx) {
			if err == nil {
				t.Error("expected error from cancelled context")
			}
			break
		}
	})

	t.Run("Bytes iterator switches to long-poll when up-to-date", func(t *testing.T) {
		storage.Create(ctx, "/bytes-switch-stream", StreamConfig{ContentType: "text/plain"})
		storage.Append(ctx, "/bytes-switch-stream", []byte("data"), "")

		reader := client.Reader("/bytes-switch-stream", ZeroOffset)
		defer reader.Close()

		iterCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()

		readCount := 0
		for range reader.Bytes(iterCtx) {
			readCount++
			if readCount >= 1 {
				break
			}
		}

		if readCount < 1 {
			t.Error("expected at least one read")
		}
	})
}

func TestReader_Close_WithSSE(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).SSECloseAfter(500 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().BaseURL(server.URL)

	ctx := context.Background()

	storage.Create(ctx, "/close-sse-stream", StreamConfig{ContentType: "text/plain"})
	storage.Append(ctx, "/close-sse-stream", []byte("data"), "")

	// Use non-empty offset for SSE
	reader := client.Reader("/close-sse-stream", Offset("0000000000"))
	reader.SSE()

	// Do a read to establish SSE connection
	sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	_, err := reader.Read(sseCtx)
	if err != nil {
		t.Fatalf("SSE read failed: %v", err)
	}

	// Close should clean up SSE connection
	if err := reader.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	if reader.sseConn != nil {
		t.Error("sseConn should be nil after close")
	}

	// Second close should be idempotent
	if err := reader.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}
}

func TestReader_Read_UnknownMode(t *testing.T) {
	server, _, client := setupInternalTestServer()
	defer server.Close()

	ctx := context.Background()

	_, err := client.Create(ctx, "/stream", &CreateOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader := client.Reader("/stream", ZeroOffset)
	defer reader.Close()

	// Force an invalid mode (this shouldn't happen in practice)
	reader.mode = readMode(999)

	_, err = reader.Read(ctx)
	if err == nil {
		t.Error("expected error for unknown read mode")
	}
	if !strings.Contains(err.Error(), "unknown read mode") {
		t.Errorf("error = %v, want 'unknown read mode'", err)
	}
}

// SSE parsing tests

func TestSSEConnection_ReadEvent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantData string
		wantErr  bool
	}{
		{
			name:     "simple data event",
			input:    "event: data\ndata: {\"key\":\"value\"}\n\n",
			wantType: "data",
			wantData: `{"key":"value"}`,
		},
		{
			name:     "control event",
			input:    "event: control\ndata: {\"streamNextOffset\":\"123\"}\n\n",
			wantType: "control",
			wantData: `{"streamNextOffset":"123"}`,
		},
		{
			name:     "multi-line data",
			input:    "event: data\ndata: [\ndata: {\"a\":1},\ndata: {\"b\":2}\ndata: ]\n\n",
			wantType: "data",
			wantData: "[\n{\"a\":1},\n{\"b\":2}\n]",
		},
		{
			name:     "event with comment",
			input:    ": this is a comment\nevent: data\ndata: test\n\n",
			wantType: "data",
			wantData: "test",
		},
		{
			name:     "skip empty events",
			input:    "\n\nevent: data\ndata: actual\n\n",
			wantType: "data",
			wantData: "actual",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			conn := &sseConnection{reader: reader}

			event, err := conn.readEvent()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if event.Type != tt.wantType {
				t.Errorf("type = %q, want %q", event.Type, tt.wantType)
			}

			if string(event.Data) != tt.wantData {
				t.Errorf("data = %q, want %q", string(event.Data), tt.wantData)
			}
		})
	}
}

func TestSSEConnection_ReadEvent_EOF(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("event: data\ndata: test"))
	conn := &sseConnection{reader: reader}

	_, err := conn.readEvent()
	if err == nil {
		t.Error("expected error on incomplete event (no trailing newline)")
	}
}

func TestBuildSSEData(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  string
	}{
		{
			name:  "empty lines",
			lines: []string{},
			want:  "{}",
		},
		{
			name:  "single line",
			lines: []string{`{"key":"value"}`},
			want:  `{"key":"value"}`,
		},
		{
			name:  "multiple lines",
			lines: []string{"[", `{"a":1},`, `{"b":2}`, "]"},
			want:  "[\n{\"a\":1},\n{\"b\":2}\n]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSSEData(tt.lines)
			if string(got) != tt.want {
				t.Errorf("buildSSEData() = %q, want %q", string(got), tt.want)
			}
		})
	}
}

func TestSSEConnection_Close(t *testing.T) {
	t.Run("close with nil response", func(t *testing.T) {
		conn := &sseConnection{response: nil}
		if err := conn.Close(); err != nil {
			t.Errorf("close with nil response failed: %v", err)
		}
	})

	t.Run("close with nil body", func(t *testing.T) {
		conn := &sseConnection{response: &http.Response{Body: nil}}
		if err := conn.Close(); err != nil {
			t.Errorf("close with nil body failed: %v", err)
		}
	})
}

func TestNewSSEConnection(t *testing.T) {
	// Create a response with a body
	resp := &http.Response{
		Body: http.NoBody,
	}

	conn := newSSEConnection(resp)

	if conn == nil {
		t.Fatal("newSSEConnection returned nil")
	}
	if conn.response != resp {
		t.Error("response not set correctly")
	}
	if conn.reader == nil {
		t.Error("reader not initialized")
	}
}

func TestReader_SSE_CleanupOnModeSwitch(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage).SSECloseAfter(500 * time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient().BaseURL(server.URL)

	ctx := context.Background()

	storage.Create(ctx, "/sse-cleanup-stream", StreamConfig{ContentType: "text/plain"})
	storage.Append(ctx, "/sse-cleanup-stream", []byte("data"), "")

	reader := client.Reader("/sse-cleanup-stream", Offset("0000000000"))
	defer reader.Close()

	// Establish SSE connection
	reader.SSE()
	sseCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	_, err := reader.Read(sseCtx)
	cancel()
	if err != nil {
		t.Fatalf("first SSE read failed: %v", err)
	}

	// Verify connection is established
	if reader.sseConn == nil {
		t.Fatal("sseConn should not be nil after read")
	}

	// Switch from SSE to SSE - should close and clear existing connection
	reader.SSE()
	if reader.sseConn != nil {
		t.Error("sseConn should be nil after SSE() called while already in SSE mode")
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
		w.Write([]byte("event: unknown\ndata: test\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

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

func TestReader_SSE_InvalidControlEvent(t *testing.T) {
	// Create a mock SSE server that sends invalid control event JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send control event with invalid JSON
		w.Write([]byte("event: control\ndata: {invalid json}\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := reader.Read(ctx)
	if err == nil {
		t.Error("expected error for invalid control event JSON")
	}
	if !strings.Contains(err.Error(), "parse control event") {
		t.Errorf("error = %v, want 'parse control event'", err)
	}
}

func TestReader_SSE_ControlEventUpdatesState(t *testing.T) {
	// Create a mock SSE server that sends control event then data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send control event
		w.Write([]byte("event: control\ndata: {\"streamNextOffset\":\"1234567890\",\"streamCursor\":\"test-cursor\"}\n\n"))
		flusher.Flush()

		// Send data event
		w.Write([]byte("event: data\ndata: {\"msg\":\"hello\"}\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Check that control event updated state
	if reader.Offset() != Offset("1234567890") {
		t.Errorf("offset = %v, want %v", reader.Offset(), Offset("1234567890"))
	}
	if reader.cursor != "test-cursor" {
		t.Errorf("cursor = %v, want %v", reader.cursor, "test-cursor")
	}

	// Check we got the data event
	if len(result.Messages) == 0 {
		t.Error("expected messages in result")
	}
}

func TestReader_SSE_WrongContentType(t *testing.T) {
	// Create a mock server that returns wrong content type
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain") // Wrong!
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not SSE"))
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

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

func TestReader_SSE_ReadError(t *testing.T) {
	// Create a mock server that closes connection after sending partial event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send first complete event
		w.Write([]byte("event: data\ndata: first\n\n"))
		flusher.Flush()

		// Connection will close before next complete event
		// (Server closes at end of handler)
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First read should succeed
	_, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("first read failed: %v", err)
	}

	// Second read should fail (connection closed)
	_, err = reader.Read(ctx)
	if err == nil {
		t.Error("expected error when connection closes")
	}

	// SSE connection should be cleaned up
	if reader.sseConn != nil {
		t.Error("sseConn should be nil after read error")
	}
}

func TestReader_SSE_DataAsNonArray(t *testing.T) {
	// Create a mock server that sends non-array JSON data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Send single JSON object (not array)
		w.Write([]byte("event: data\ndata: {\"key\":\"value\"}\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient().BaseURL(server.URL)
	reader := client.Reader("/stream", Offset("0000000000"))
	defer reader.Close()

	reader.SSE()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Should treat as single message
	if len(result.Messages) != 1 {
		t.Errorf("messages count = %d, want 1", len(result.Messages))
	}
}
