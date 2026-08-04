package durablestream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

func TestClient_Config(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		client := NewClient("http://example.com", nil)

		if client.timeout != 30*time.Second {
			t.Errorf("timeout = %v, want %v", client.timeout, 30*time.Second)
		}
		if client.readMode != ReadModeAuto {
			t.Errorf("readMode = %v, want %v", client.readMode, ReadModeAuto)
		}
		if client.transport == nil {
			t.Error("transport should not be nil")
		}
	})

	t.Run("custom config with timeout", func(t *testing.T) {
		client := NewClient("http://example.com", &ClientConfig{
			Timeout:  10 * time.Second,
			ReadMode: ReadModeSSE,
		})

		if client.timeout != 10*time.Second {
			t.Errorf("timeout = %v, want %v", client.timeout, 10*time.Second)
		}
		if client.readMode != ReadModeSSE {
			t.Errorf("readMode = %v, want %v", client.readMode, ReadModeSSE)
		}
	})

	t.Run("custom transport via NewClientWithTransport", func(t *testing.T) {
		customTransport := transport.NewHTTPTransport("http://custom.com", nil)
		client := NewClientWithTransport(customTransport, nil)

		if client.transport != customTransport {
			t.Error("transport should be the custom transport")
		}
	})

	t.Run("NewClientWithTransport with config", func(t *testing.T) {
		customTransport := transport.NewHTTPTransport("http://custom.com", nil)
		client := NewClientWithTransport(customTransport, &TransportClientConfig{
			Timeout:  15 * time.Second,
			ReadMode: ReadModeSSE,
		})

		if client.timeout != 15*time.Second {
			t.Errorf("timeout = %v, want %v", client.timeout, 15*time.Second)
		}
		if client.readMode != ReadModeSSE {
			t.Errorf("readMode = %v, want %v", client.readMode, ReadModeSSE)
		}
	})
}

// mockStorage is a minimal storage implementation for testing handler config.
type mockStorage struct{}

func (m *mockStorage) Create(_ context.Context, _ string, _ StreamConfig) (bool, error) {
	return true, nil
}

func (m *mockStorage) Append(_ context.Context, _ string, _ []byte, _ string) (Offset, error) {
	return "", nil
}

func (m *mockStorage) Read(_ context.Context, _ string, _ Offset, _ int) (*ReadResult, error) {
	return &ReadResult{Messages: nil}, nil
}

func (m *mockStorage) Head(_ context.Context, _ string) (*StreamInfo, error) {
	return nil, nil
}

func (m *mockStorage) Touch(_ context.Context, _ string) error {
	return nil
}

func (m *mockStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (m *mockStorage) WaitForData(_ context.Context, _ string, _ Offset, _ int) (*ReadResult, error) {
	// Mock simply delegates to Read behavior - returns empty result
	return &ReadResult{Messages: nil}, nil
}

func (m *mockStorage) Close() error {
	return nil
}

func TestHandler_Config(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		handler := NewHandler(&mockStorage{}, nil)

		if handler.chunkSize != defaultChunkSize {
			t.Errorf("chunkSize = %d, want %d", handler.chunkSize, defaultChunkSize)
		}
		if handler.maxAppendSize != defaultMaxAppendSize {
			t.Errorf("maxAppendSize = %d, want %d", handler.maxAppendSize, defaultMaxAppendSize)
		}
		if handler.longPollTimeout != defaultLongPollTimeout {
			t.Errorf("longPollTimeout = %v, want %v", handler.longPollTimeout, defaultLongPollTimeout)
		}
		if handler.sseCloseAfter != defaultSSECloseAfter {
			t.Errorf("sseCloseAfter = %v, want %v", handler.sseCloseAfter, defaultSSECloseAfter)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		handler := NewHandler(&mockStorage{}, &HandlerConfig{
			ChunkSize:       1024,
			MaxAppendSize:   2048,
			LongPollTimeout: 5 * time.Second,
			SSECloseAfter:   10 * time.Second,
		})

		if handler.chunkSize != 1024 {
			t.Errorf("chunkSize = %d, want 1024", handler.chunkSize)
		}
		if handler.maxAppendSize != 2048 {
			t.Errorf("maxAppendSize = %d, want 2048", handler.maxAppendSize)
		}
		if handler.longPollTimeout != 5*time.Second {
			t.Errorf("longPollTimeout = %v, want %v", handler.longPollTimeout, 5*time.Second)
		}
		if handler.sseCloseAfter != 10*time.Second {
			t.Errorf("sseCloseAfter = %v, want %v", handler.sseCloseAfter, 10*time.Second)
		}
	})
}

func TestMessage_Bytes(t *testing.T) {
	msg := Message{data: []byte("hello world")}

	got := msg.Bytes()
	if string(got) != "hello world" {
		t.Errorf("Bytes() = %q, want %q", got, "hello world")
	}
}

func TestMessage_String(t *testing.T) {
	msg := Message{data: []byte("hello world")}

	got := msg.String()
	if got != "hello world" {
		t.Errorf("String() = %q, want %q", got, "hello world")
	}
}

func TestConvertTransportError(t *testing.T) {
	tests := []struct {
		name      string
		inputErr  error
		wantErr   error
		wantIsErr error // For errors that should match with errors.Is
	}{
		{
			name:     "nil error",
			inputErr: nil,
			wantErr:  nil,
		},
		{
			name:      "not found",
			inputErr:  &transport.Error{Code: "NOT_FOUND", Message: "stream not found"},
			wantIsErr: ErrNotFound,
		},
		{
			name:      "not found empty message returns sentinel directly",
			inputErr:  &transport.Error{Code: "NOT_FOUND", Message: ""},
			wantIsErr: ErrNotFound,
		},
		{
			name:      "not found lowercase",
			inputErr:  &transport.Error{Code: "not_found", Message: "stream not found"},
			wantIsErr: ErrNotFound,
		},
		{
			name:      "conflict",
			inputErr:  &transport.Error{Code: "CONFLICT", Message: "conflict"},
			wantIsErr: ErrConflict,
		},
		{
			name:      "conflict lowercase",
			inputErr:  &transport.Error{Code: "conflict", Message: "conflict"},
			wantIsErr: ErrConflict,
		},
		{
			name:      "sequence conflict",
			inputErr:  &transport.Error{Code: "SEQUENCE_CONFLICT", Message: "sequence conflict"},
			wantIsErr: ErrSequenceConflict,
		},
		{
			name:      "sequence conflict lowercase",
			inputErr:  &transport.Error{Code: "sequence_conflict", Message: "sequence conflict"},
			wantIsErr: ErrSequenceConflict,
		},
		{
			name:      "gone",
			inputErr:  &transport.Error{Code: "GONE", Message: "gone"},
			wantIsErr: ErrGone,
		},
		{
			name:      "gone lowercase",
			inputErr:  &transport.Error{Code: "gone", Message: "gone"},
			wantIsErr: ErrGone,
		},
		{
			name:      "bad request",
			inputErr:  &transport.Error{Code: "BAD_REQUEST", Message: "bad"},
			wantIsErr: ErrBadRequest,
		},
		{
			name:      "bad request lowercase",
			inputErr:  &transport.Error{Code: "bad_request", Message: "bad"},
			wantIsErr: ErrBadRequest,
		},
		{
			name:      "payload too large uppercase",
			inputErr:  &transport.Error{Code: "PAYLOAD_TOO_LARGE", Message: "too large"},
			wantIsErr: ErrPayloadTooLarge,
		},
		{
			name:      "payload too large lowercase",
			inputErr:  &transport.Error{Code: "payload_too_large", Message: "too large"},
			wantIsErr: ErrPayloadTooLarge,
		},
		{
			name:      "malformed protocol response",
			inputErr:  &transport.Error{Code: "PARSE_ERROR", Message: "missing response header"},
			wantIsErr: ErrParseError,
		},
		{
			name: "wrapped transport error",
			inputErr: fmt.Errorf("middleware: %w", &transport.Error{
				Code: "NOT_FOUND", Message: "stream not found",
			}),
			wantIsErr: ErrNotFound,
		},
		{
			name:     "rate limited uppercase",
			inputErr: &transport.Error{Code: "RATE_LIMITED", Message: "slow down"},
			// Rate limited returns a protoError, not a sentinel
		},
		{
			name:     "rate limited lowercase",
			inputErr: &transport.Error{Code: "too_many_requests", Message: "slow down"},
			// Rate limited returns a protoError, not a sentinel
		},
		{
			name:     "unknown code returns original error",
			inputErr: &transport.Error{Code: "UNKNOWN", Message: "unknown"},
			// Returns original error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertTransportError(tt.inputErr)

			if tt.wantErr != nil {
				if got != tt.wantErr {
					t.Errorf("convertTransportError() = %v, want %v", got, tt.wantErr)
				}
				return
			}

			if tt.wantIsErr != nil {
				if !errors.Is(got, tt.wantIsErr) {
					t.Errorf("convertTransportError() = %v, want errors.Is(%v)", got, tt.wantIsErr)
				}
				return
			}

			// For rate limit and unknown, just verify it returns an error
			if tt.inputErr != nil && got == nil {
				t.Error("convertTransportError() returned nil for non-nil input")
			}
		})
	}

	t.Run("non-transport error passes through", func(t *testing.T) {
		originalErr := io.EOF
		got := convertTransportError(originalErr)
		if got != originalErr {
			t.Errorf("expected original error, got %v", got)
		}
	})
}

func TestWrapSentinel(t *testing.T) {
	t.Run("empty message returns sentinel directly", func(t *testing.T) {
		got := wrapSentinel("", ErrNotFound)
		if got != ErrNotFound {
			t.Errorf("wrapSentinel(\"\", ErrNotFound) = %v, want ErrNotFound", got)
		}
	})

	t.Run("non-empty message wraps sentinel", func(t *testing.T) {
		got := wrapSentinel("stream not found", ErrNotFound)
		if !errors.Is(got, ErrNotFound) {
			t.Errorf("errors.Is(wrapSentinel(...), ErrNotFound) = false, want true")
		}
		if got.Error() != "stream not found: stream not found" {
			t.Errorf("wrapSentinel(...).Error() = %q, want %q", got.Error(), "stream not found: stream not found")
		}
	})

	t.Run("wrapped error preserves message for inspection", func(t *testing.T) {
		got := wrapSentinel("sequence regression detected", ErrConflict)
		if !errors.Is(got, ErrConflict) {
			t.Errorf("errors.Is(wrapSentinel(...), ErrConflict) = false, want true")
		}
		msg := got.Error()
		if msg != "sequence regression detected: conflict" {
			t.Errorf("Error() = %q, want %q", msg, "sequence regression detected: conflict")
		}
	})
}

func TestProtoError(t *testing.T) {
	t.Run("Error() formats correctly", func(t *testing.T) {
		err := newError(codeNotFound, "stream /test not found")
		expected := "not_found: stream /test not found"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Is() matches ErrNotFound", func(t *testing.T) {
		err := newError(codeNotFound, "not found")
		if !err.Is(ErrNotFound) {
			t.Error("Is(ErrNotFound) should return true")
		}
		if err.Is(ErrConflict) {
			t.Error("Is(ErrConflict) should return false")
		}
	})

	t.Run("Is() matches ErrGone", func(t *testing.T) {
		err := newError(codeGone, "gone")
		if !err.Is(ErrGone) {
			t.Error("Is(ErrGone) should return true")
		}
		if err.Is(ErrNotFound) {
			t.Error("Is(ErrNotFound) should return false")
		}
	})

	t.Run("Is() matches ErrConflict", func(t *testing.T) {
		err := newError(codeConflict, "conflict")
		if !err.Is(ErrConflict) {
			t.Error("Is(ErrConflict) should return true")
		}
		if err.Is(ErrGone) {
			t.Error("Is(ErrGone) should return false")
		}
	})

	t.Run("Is() returns false for unknown code", func(t *testing.T) {
		err := newError(codeInternal, "internal error")
		if err.Is(ErrNotFound) {
			t.Error("Is(ErrNotFound) should return false for internal error")
		}
		if err.Is(ErrGone) {
			t.Error("Is(ErrGone) should return false for internal error")
		}
		if err.Is(ErrConflict) {
			t.Error("Is(ErrConflict) should return false for internal error")
		}
	})
}

func TestErrorCode_HttpStatus(t *testing.T) {
	tests := []struct {
		code   errorCode
		status int
	}{
		{codeBadRequest, 400},
		{codeNotFound, 404},
		{codeConflict, 409},
		{codeGone, 410},
		{codePayloadTooLarge, 413},
		{codeTooManyRequests, 429},
		{codeInternal, 500},
		{codeNotImplemented, 501},
		{errorCode("unknown"), 500}, // default case
	}

	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			got := tt.code.httpStatus()
			if got != tt.status {
				t.Errorf("%s.httpStatus() = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

func TestHttpStatusToErrorCode(t *testing.T) {
	tests := []struct {
		status int
		code   errorCode
	}{
		{400, codeBadRequest},
		{404, codeNotFound},
		{409, codeConflict},
		{410, codeGone},
		{413, codePayloadTooLarge},
		{429, codeTooManyRequests},
		{501, codeNotImplemented},
		{500, codeInternal}, // explicit
		{502, codeInternal}, // default case
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.status)), func(t *testing.T) {
			got := httpStatusToErrorCode(tt.status)
			if got != tt.code {
				t.Errorf("httpStatusToErrorCode(%d) = %s, want %s", tt.status, got, tt.code)
			}
		})
	}
}

func TestClient_RetryOnTransientErrors(t *testing.T) {
	t.Run("retries on 500 error", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if r.Method == http.MethodHead && attempts == 1 {
				// First HEAD request fails with 500
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// Subsequent requests succeed
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL, nil)
		ctx := context.Background()

		// Head should succeed after retry
		_, err := client.Head(ctx, "/test")
		if err != nil {
			t.Fatalf("Head() error = %v, expected success after retry", err)
		}

		// Verify retry happened (at least 2 attempts)
		if attempts < 2 {
			t.Errorf("expected at least 2 attempts, got %d", attempts)
		}
	})

	t.Run("does not retry appends without producer headers", func(t *testing.T) {
		// A 503 from a proxy can arrive after the origin committed the append,
		// so a plain (non-idempotent) append must never be replayed.
		var posts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("unexpected method %s", r.Method)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			posts.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		client := NewClient(server.URL, nil)

		// Create writer directly for internal testing
		writer := &StreamWriter{
			client:      client,
			path:        "/test",
			contentType: "text/plain",
		}

		if err := writer.Send([]byte("hello"), nil); err == nil {
			t.Fatal("Send() = nil, want error for 503 response")
		}

		if got := posts.Load(); got != 1 {
			t.Errorf("append attempts = %d, want 1 (appends must not be retried)", got)
		}
	})

	t.Run("does not retry on 400 error", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		client := NewClient(server.URL, nil)
		ctx := context.Background()

		_, err := client.Head(ctx, "/test")
		if err == nil {
			t.Fatal("expected error for 400 response")
		}

		// Should not retry 4xx errors
		if attempts > 1 {
			t.Errorf("expected 1 attempt (no retry for 4xx), got %d", attempts)
		}
	})
}

func TestClient_Writer_Error(t *testing.T) {
	// Create client pointing to non-existent server
	client := NewClient("http://localhost:1", nil)

	ctx := context.Background()
	_, err := client.Writer(ctx, "/test")
	if err == nil {
		t.Error("expected error for connection failure")
	}
}

func TestStreamWriter_SendError(t *testing.T) {
	// Create a writer with invalid client config
	client := NewClient("http://localhost:1", nil)

	// Manually create a writer to bypass the Head check
	writer := &StreamWriter{
		client:      client,
		path:        "/test",
		contentType: "text/plain",
	}

	err := writer.SendContext(context.Background(), []byte("data"), nil)
	if err == nil {
		t.Error("expected error for connection failure")
	}
}

func TestStreamWriter_LegacySendRetainsWriterContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "tenant-a")

	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		appendFunc: func(gotCtx context.Context, _ transport.AppendRequest) (*transport.AppendResponse, error) {
			if got := gotCtx.Value(contextKey{}); got != "tenant-a" {
				t.Errorf("append context value = %v, want tenant-a", got)
			}
			return &transport.AppendResponse{NextOffset: "0_1"}, nil
		},
	}

	writer, err := NewClientWithTransport(ft, nil).Writer(ctx, "/test")
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	if err := writer.Send([]byte("data"), nil); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
}

func TestMessage_Decode(t *testing.T) {
	type event struct {
		Name string `json:"name"`
		ID   int    `json:"id"`
	}

	tests := []struct {
		name    string
		data    []byte
		want    event
		wantErr bool
	}{
		{
			name:    "valid json",
			data:    []byte(`{"name":"alice","id":123}`),
			want:    event{Name: "alice", ID: 123},
			wantErr: false,
		},
		{
			name:    "invalid json",
			data:    []byte(`not json`),
			want:    event{},
			wantErr: true,
		},
		{
			name:    "empty",
			data:    []byte{},
			want:    event{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := Message{data: tt.data}
			var got event
			err := msg.Decode(&got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Decode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- Operation timeouts (ClientConfig.Timeout) ---

// stalledServer returns a server that accepts requests and never responds until
// the test finishes.
func stalledServer(t *testing.T) *httptest.Server {
	t.Helper()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	return server
}

func TestClient_TimeoutAppliedToOperations(t *testing.T) {
	server := stalledServer(t)
	client := NewClient(server.URL, &ClientConfig{Timeout: 50 * time.Millisecond})

	// The writer cannot be built against a stalled server, so construct one
	// directly to exercise Send.
	writer := &StreamWriter{client: client, path: "/test", contentType: "text/plain"}

	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{"Create", func(ctx context.Context) error {
			_, err := client.Create(ctx, "/test", nil)
			return err
		}},
		{"Head", func(ctx context.Context) error {
			_, err := client.Head(ctx, "/test")
			return err
		}},
		{"Delete", func(ctx context.Context) error {
			return client.Delete(ctx, "/test")
		}},
		{"Send", func(ctx context.Context) error {
			return writer.SendContext(ctx, []byte("data"), nil)
		}},
		{"Reader.Read", func(ctx context.Context) error {
			reader := client.Reader("/test", ZeroOffset)
			defer reader.Close()
			_, err := reader.Read(ctx)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The caller's context has no deadline: only the configured client
			// timeout can end these calls.
			done := make(chan error, 1)
			go func() { done <- tt.call(context.Background()) }()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("call succeeded against a stalled server, want timeout error")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("error = %v, want context.DeadlineExceeded", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("call did not time out; ClientConfig.Timeout was not applied")
			}
		})
	}
}

func TestClient_ResponseSizeLimitsPropagateToHTTPTransport(t *testing.T) {
	t.Run("non-streaming response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Stream-Next-Offset", "0_0")
				return
			}
			w.Header().Set("Stream-Next-Offset", "0_5")
			_, _ = io.WriteString(w, "12345")
		}))
		defer server.Close()

		client := NewClient(server.URL, &ClientConfig{MaxResponseSize: 4})
		reader := client.Reader("/test", ZeroOffset)
		defer reader.Close()

		if _, err := reader.Read(t.Context()); !errors.Is(err, transport.ErrResponseTooLarge) {
			t.Fatalf("Read() error = %v, want transport.ErrResponseTooLarge", err)
		}
	})

	t.Run("SSE event", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Stream-Next-Offset", "0_0")
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: data\ndata: this event is too large\n\n")
		}))
		defer server.Close()

		client := NewClient(server.URL, &ClientConfig{
			ReadMode:        ReadModeSSE,
			MaxSSEEventSize: 24,
		})
		reader := client.Reader("/test", ZeroOffset)
		defer reader.Close()

		if _, err := reader.Read(t.Context()); !errors.Is(err, transport.ErrResponseTooLarge) {
			t.Fatalf("Read() error = %v, want transport.ErrResponseTooLarge", err)
		}
	})
}

func TestReader_JSONGETRequiresArray(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{name: "object", data: []byte(`{"message":"not an array"}`)},
		{name: "scalar", data: []byte(`42`)},
		{name: "empty body", data: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ft := &fakeTransport{
				headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
					return &transport.HeadResponse{ContentType: "application/json"}, nil
				},
				readFunc: func(context.Context, transport.ReadRequest) (*transport.ReadResponse, error) {
					return &transport.ReadResponse{
						Data:       tt.data,
						NextOffset: "0_99",
						UpToDate:   true,
					}, nil
				},
			}
			reader := NewClientWithTransport(ft, nil).Reader("/json", ZeroOffset)
			defer reader.Close()

			if _, err := reader.Read(t.Context()); !errors.Is(err, ErrParseError) {
				t.Fatalf("Read() error = %v, want ErrParseError", err)
			}
			if got := reader.Offset(); got != ZeroOffset {
				t.Fatalf("offset after invalid response = %q, want %q", got, ZeroOffset)
			}
		})
	}
}

func TestReader_LongPollJSONRequiresArray(t *testing.T) {
	ft := &fakeTransport{
		longPollFunc: func(context.Context, transport.LongPollRequest) (*transport.ReadResponse, error) {
			return &transport.ReadResponse{Data: []byte(`{"not":"an array"}`), NextOffset: "0_1"}, nil
		},
	}
	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeLongPoll}).Reader("/json", Offset("now"))
	reader.contentType = "application/json"
	defer reader.Close()

	if _, err := reader.Read(t.Context()); !errors.Is(err, ErrParseError) {
		t.Fatalf("Read() error = %v, want ErrParseError", err)
	}
}

func TestReader_LongPollPreservesCursorWhenResponseOmitsIt(t *testing.T) {
	var gotCursor string
	ft := &fakeTransport{
		longPollFunc: func(_ context.Context, req transport.LongPollRequest) (*transport.ReadResponse, error) {
			gotCursor = req.Cursor
			return &transport.ReadResponse{NextOffset: "0_0", UpToDate: true}, nil
		},
	}
	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeLongPoll}).Reader("/text", Offset("now"))
	reader.contentType = "text/plain"
	reader.cursor = "cached-cursor"
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if gotCursor != "cached-cursor" {
		t.Fatalf("request cursor = %q, want cached-cursor", gotCursor)
	}
	if reader.cursor != "cached-cursor" || result.Cursor != "cached-cursor" {
		t.Fatalf("cursor after response = reader %q, result %q; want cached-cursor", reader.cursor, result.Cursor)
	}
}

// --- Reader iteration and SSE control handling ---

// fakeTransport is a Transport whose behavior each test supplies.
type fakeTransport struct {
	readFunc     func(context.Context, transport.ReadRequest) (*transport.ReadResponse, error)
	longPollFunc func(context.Context, transport.LongPollRequest) (*transport.ReadResponse, error)
	appendFunc   func(context.Context, transport.AppendRequest) (*transport.AppendResponse, error)
	headFunc     func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error)
	sseFunc      func(context.Context, transport.SSERequest) (transport.EventStream, error)
}

func (f *fakeTransport) Read(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
	if f.readFunc != nil {
		return f.readFunc(ctx, req)
	}
	return &transport.ReadResponse{}, nil
}

func (f *fakeTransport) LongPoll(ctx context.Context, req transport.LongPollRequest) (*transport.ReadResponse, error) {
	if f.longPollFunc != nil {
		return f.longPollFunc(ctx, req)
	}
	return &transport.ReadResponse{}, nil
}

func (f *fakeTransport) SSE(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
	if f.sseFunc != nil {
		return f.sseFunc(ctx, req)
	}
	return nil, errors.New("SSE not configured")
}

func (f *fakeTransport) Append(ctx context.Context, req transport.AppendRequest) (*transport.AppendResponse, error) {
	if f.appendFunc != nil {
		return f.appendFunc(ctx, req)
	}
	return &transport.AppendResponse{}, nil
}

func (f *fakeTransport) Create(ctx context.Context, req transport.CreateRequest) (*transport.CreateResponse, error) {
	return &transport.CreateResponse{}, nil
}

func (f *fakeTransport) Delete(ctx context.Context, req transport.DeleteRequest) error {
	return nil
}

func (f *fakeTransport) Head(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
	if f.headFunc != nil {
		return f.headFunc(ctx, req)
	}
	return &transport.HeadResponse{ContentType: "text/plain"}, nil
}

func TestReader_Messages_StopsOnTerminalError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"not found", &transport.Error{Code: "NOT_FOUND", Message: "no such stream", StatusCode: 404}},
		{"bad request", &transport.Error{Code: "BAD_REQUEST", Message: "bad offset", StatusCode: 400}},
		{"gone", &transport.Error{Code: "GONE", Message: "compacted", StatusCode: 410}},
		{"response too large", fmt.Errorf("read: %w", transport.ErrResponseTooLarge)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reads atomic.Int32
			ft := &fakeTransport{
				readFunc: func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
					reads.Add(1)
					return nil, tt.err
				},
			}
			client := NewClientWithTransport(ft, nil)
			reader := client.Reader("/test", ZeroOffset)
			defer reader.Close()

			// The caller keeps iterating after the error; the iterator must stop
			// anyway rather than hammer the server.
			var errCount int
			for _, err := range reader.Messages(t.Context()) {
				if err != nil {
					errCount++
				}
			}

			if errCount != 1 {
				t.Errorf("errors yielded = %d, want 1", errCount)
			}
			if got := reads.Load(); got != 1 {
				t.Errorf("read attempts = %d, want 1", got)
			}
		})
	}
}

func TestReader_Messages_RetriesTransientError(t *testing.T) {
	var reads atomic.Int32
	ft := &fakeTransport{
		readFunc: func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
			if reads.Add(1) <= 2 {
				return nil, &transport.Error{Code: "UNKNOWN", Message: "boom", StatusCode: 503}
			}
			return &transport.ReadResponse{
				Data:       []byte("hello"),
				NextOffset: "0_5",
				UpToDate:   true,
			}, nil
		},
	}

	client := NewClientWithTransport(ft, nil)
	reader := client.Reader("/test", ZeroOffset)
	defer reader.Close()

	var errCount int
	var got string
	for msg, err := range reader.Messages(t.Context()) {
		if err != nil {
			errCount++
			continue
		}
		got = msg.String()
		break
	}

	if errCount != 2 {
		t.Errorf("errors yielded = %d, want 2", errCount)
	}
	if got != "hello" {
		t.Errorf("message = %q, want %q", got, "hello")
	}
}

func TestReader_Messages_YieldsCallerCancellationOnce(t *testing.T) {
	readStarted := make(chan struct{})
	ft := &fakeTransport{
		readFunc: func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
			close(readStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	client := NewClientWithTransport(ft, nil)
	reader := client.Reader("/test", ZeroOffset)
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan []error, 1)
	go func() {
		var errs []error
		for _, err := range reader.Messages(ctx) {
			errs = append(errs, err)
		}
		done <- errs
	}()

	<-readStarted
	cancel()

	select {
	case errs := <-done:
		if len(errs) != 1 {
			t.Fatalf("errors yielded = %d (%v), want one cancellation", len(errs), errs)
		}
		if !errors.Is(errs[0], context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", errs[0])
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Messages did not stop after context cancellation")
	}
}

// scriptedEventStream returns a fixed sequence of events. requestCtx models the
// context passed to Transport.SSE: cancelling it invalidates the whole stream,
// as net/http does for a streaming response body.
type scriptedEventStream struct {
	requestCtx  context.Context
	events      []*transport.Event
	terminalErr error
	closed      atomic.Bool
}

func (s *scriptedEventStream) Next(ctx context.Context) (*transport.Event, error) {
	if err := s.requestCtx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, io.EOF
	}
	if len(s.events) > 0 {
		event := s.events[0]
		s.events = s.events[1:]
		return event, nil
	}
	if s.terminalErr != nil {
		return nil, s.terminalErr
	}
	select {
	case <-s.requestCtx.Done():
		return nil, s.requestCtx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *scriptedEventStream) Close() error {
	s.closed.Store(true)
	return nil
}

func TestReader_SSEAllowsSingleJSONValue(t *testing.T) {
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "application/json"}, nil
		},
		sseFunc: func(ctx context.Context, _ transport.SSERequest) (transport.EventStream, error) {
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte(`{"single":true}`)},
					{Type: "control", NextOffset: "0_1"},
				},
			}, nil
		},
	}
	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/json", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got, want := string(result.Data), `{"single":true}`; got != want {
		t.Fatalf("data = %q, want %q", got, want)
	}
}

func TestReader_SSE_ReadContextDoesNotOwnRetainedConnection(t *testing.T) {
	var opens atomic.Int32
	ft := &fakeTransport{
		headFunc: func(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			opens.Add(1)
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte("one")},
					{Type: "control", NextOffset: "0_1"},
					{Type: "data", Data: []byte("two")},
					{Type: "control", NextOffset: "0_2"},
				},
			}, nil
		},
	}

	client := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/test", ZeroOffset)
	defer reader.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first, err := reader.Read(firstCtx)
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	cancelFirst() // A per-call context may be cancelled as soon as Read returns.

	second, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("second Read() after cancelling first context error = %v", err)
	}
	if string(first.Data) != "one" || string(second.Data) != "two" {
		t.Fatalf("Read data = (%q, %q), want (one, two)", first.Data, second.Data)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("SSE opens = %d, want retained connection to be reused", got)
	}
}

func TestReader_SSE_JSONParseErrorReconnectsFromConfirmedOffset(t *testing.T) {
	var opens atomic.Int32
	var offsets []string
	var firstStream *scriptedEventStream
	ft := &fakeTransport{
		headFunc: func(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "application/json"}, nil
		},
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			offsets = append(offsets, req.Offset)
			if opens.Add(1) == 1 {
				firstStream = &scriptedEventStream{
					requestCtx: ctx,
					events: []*transport.Event{
						{Type: "data", Data: []byte(`[{`)},
						{Type: "control", NextOffset: "0_99"},
					},
				}
				return firstStream, nil
			}
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte(`[{"ok":true}]`)},
					{Type: "control", NextOffset: "0_1"},
				},
			}, nil
		},
	}

	client := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/test", ZeroOffset)
	defer reader.Close()

	if _, err := reader.Read(t.Context()); !errors.Is(err, ErrParseError) {
		t.Fatalf("first Read() error = %v, want ErrParseError", err)
	}
	if firstStream == nil || !firstStream.closed.Load() {
		t.Fatal("malformed SSE stream was not closed")
	}
	if got := reader.Offset(); got != ZeroOffset {
		t.Fatalf("offset after parse error = %q, want %q", got, ZeroOffset)
	}

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("retry Read() error = %v", err)
	}
	if string(result.Data) != `[{"ok":true}]` {
		t.Fatalf("retry data = %q", result.Data)
	}
	if len(offsets) != 2 || offsets[0] != offsets[1] {
		t.Fatalf("SSE offsets = %v, want reconnect from same confirmed offset", offsets)
	}
}

func TestReader_SSEMissingControlReconnectsFromConfirmedOffset(t *testing.T) {
	var opens atomic.Int32
	var offsets []string
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			offsets = append(offsets, req.Offset)
			if opens.Add(1) == 1 {
				return &scriptedEventStream{
					requestCtx:  ctx,
					events:      []*transport.Event{{Type: "data", Data: []byte("unconfirmed")}},
					terminalErr: io.EOF,
				}, nil
			}
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte("confirmed")},
					{Type: "control", NextOffset: "0_1"},
				},
			}, nil
		},
	}

	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/test", ZeroOffset)
	defer reader.Close()

	if _, err := reader.Read(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("first Read() error = %v, want io.EOF", err)
	}
	if got := reader.Offset(); got != ZeroOffset {
		t.Fatalf("offset after missing control = %q, want %q", got, ZeroOffset)
	}

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("retry Read() error = %v", err)
	}
	if got, want := string(result.Data), "confirmed"; got != want {
		t.Fatalf("retry data = %q, want %q", got, want)
	}
	if len(offsets) != 2 || offsets[0] != offsets[1] {
		t.Fatalf("SSE offsets = %v, want reconnect from same confirmed offset", offsets)
	}
}

func TestReader_SSEReconnectEchoesConfirmedCursor(t *testing.T) {
	var opens atomic.Int32
	var requests []transport.SSERequest
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			requests = append(requests, req)
			if opens.Add(1) == 1 {
				return &scriptedEventStream{
					requestCtx:  ctx,
					events:      []*transport.Event{{Type: "control", NextOffset: "0_5", Cursor: "cursor-1"}},
					terminalErr: io.EOF,
				}, nil
			}
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte("next")},
					{Type: "control", NextOffset: "0_9", Cursor: "cursor-2"},
				},
			}, nil
		},
	}

	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/test", ZeroOffset)
	defer reader.Close()

	if _, err := reader.Read(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("first Read() error = %v, want io.EOF after control event", err)
	}
	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("reconnect Read() error = %v", err)
	}
	if got, want := string(result.Data), "next"; got != want {
		t.Fatalf("reconnect data = %q, want %q", got, want)
	}
	if len(requests) != 2 {
		t.Fatalf("SSE requests = %d, want 2", len(requests))
	}
	if got, want := requests[1].Offset, "0_5"; got != want {
		t.Errorf("reconnect offset = %q, want %q", got, want)
	}
	if got, want := requests[1].Cursor, "cursor-1"; got != want {
		t.Errorf("reconnect cursor = %q, want %q", got, want)
	}
}

func TestReader_SSERejectsConsecutiveDataEvents(t *testing.T) {
	var stream *scriptedEventStream
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, _ transport.SSERequest) (transport.EventStream, error) {
			stream = &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte("first")},
					{Type: "data", Data: []byte("second")},
				},
			}
			return stream, nil
		},
	}

	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/test", ZeroOffset)
	defer reader.Close()

	if _, err := reader.Read(t.Context()); !errors.Is(err, ErrParseError) {
		t.Fatalf("Read() error = %v, want ErrParseError", err)
	}
	if stream == nil || !stream.closed.Load() {
		t.Fatal("malformed SSE stream was not closed")
	}
	if got := reader.Offset(); got != ZeroOffset {
		t.Fatalf("offset after consecutive data = %q, want %q", got, ZeroOffset)
	}
}

// controlEventStream yields n control events, then a data event followed by its
// trailing control event (as the protocol requires), then blocks until closed.
type controlEventStream struct {
	remaining int
	sentData  bool
	sentFinal bool
	closed    chan struct{}
	closeOnce sync.Once
}

func newControlEventStream(controls int) *controlEventStream {
	return &controlEventStream{remaining: controls, closed: make(chan struct{})}
}

func (s *controlEventStream) Next(ctx context.Context) (*transport.Event, error) {
	if s.remaining > 0 {
		s.remaining--
		return &transport.Event{
			Type:       "control",
			NextOffset: fmt.Sprintf("0_%d", s.remaining),
			Cursor:     "cursor",
		}, nil
	}
	if !s.sentData {
		s.sentData = true
		return &transport.Event{Type: "data", Data: []byte("payload")}, nil
	}
	if !s.sentFinal {
		s.sentFinal = true
		return &transport.Event{Type: "control", NextOffset: "9_9", Cursor: "cursor"}, nil
	}
	select {
	case <-s.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *controlEventStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestReader_SSE_ConsecutiveControlEvents(t *testing.T) {
	// Control events carry no data. Handling them must not consume stack per
	// event, so a long run of them is processed iteratively.
	const controls = 50000

	stream := newControlEventStream(controls)
	t.Cleanup(func() { stream.Close() })

	ft := &fakeTransport{
		headFunc: func(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			return stream, nil
		},
	}

	client := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/test", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(result.Data) != "payload" {
		t.Errorf("data = %q, want %q", result.Data, "payload")
	}
	// Offset comes from the control event trailing the data event.
	if result.NextOffset != Offset("9_9") {
		t.Errorf("NextOffset = %q, want 9_9", result.NextOffset)
	}
	if stream.remaining != 0 {
		t.Errorf("%d control events left unread", stream.remaining)
	}
}

// blockingEventStream blocks in Next until it is closed.
type blockingEventStream struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *blockingEventStream) Next(ctx context.Context) (*transport.Event, error) {
	select {
	case <-s.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *blockingEventStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestReader_CloseUnblocksRead(t *testing.T) {
	stream := &blockingEventStream{closed: make(chan struct{})}
	opened := make(chan struct{})

	ft := &fakeTransport{
		sseFunc: func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
			close(opened)
			return stream, nil
		},
	}

	client := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE})
	reader := client.Reader("/test", ZeroOffset)

	done := make(chan error, 1)
	go func() {
		_, err := reader.Read(context.Background())
		done <- err
	}()

	<-opened // the read is now blocked on the event stream

	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Read() error = %v, want ErrClosed", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Read() did not return after Close")
	}
}

func TestReader_CloseUnblocksHTTPReadPhases(t *testing.T) {
	for _, phase := range []string{"head", "catch-up", "long-poll"} {
		t.Run(phase, func(t *testing.T) {
			started := make(chan struct{})
			block := func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}

			ft := &fakeTransport{}
			switch phase {
			case "head":
				ft.headFunc = func(ctx context.Context, _ transport.HeadRequest) (*transport.HeadResponse, error) {
					return nil, block(ctx)
				}
			case "catch-up":
				ft.headFunc = func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
					return &transport.HeadResponse{ContentType: "text/plain"}, nil
				}
				ft.readFunc = func(ctx context.Context, _ transport.ReadRequest) (*transport.ReadResponse, error) {
					return nil, block(ctx)
				}
			case "long-poll":
				ft.longPollFunc = func(ctx context.Context, _ transport.LongPollRequest) (*transport.ReadResponse, error) {
					return nil, block(ctx)
				}
			}

			reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeLongPoll}).Reader("/test", ZeroOffset)
			if phase == "long-poll" {
				reader.contentType = "text/plain"
				reader.catching = false
			}

			done := make(chan error, 1)
			go func() {
				_, err := reader.Read(context.Background())
				done <- err
			}()

			<-started
			if err := reader.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			select {
			case err := <-done:
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("Read() error = %v, want ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Read() did not return after Close")
			}
		})
	}
}
