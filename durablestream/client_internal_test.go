package durablestream

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/transport"
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

	t.Run("custom transport", func(t *testing.T) {
		customTransport := transport.NewHTTPTransport("http://custom.com", nil)
		client := NewClient("http://example.com", &ClientConfig{
			Transport: customTransport,
		})

		if client.transport != customTransport {
			t.Error("transport should be the custom transport")
		}
	})
}

func TestClient_Transport(t *testing.T) {
	client := NewClient("http://example.com", nil)

	tr := client.Transport()
	if tr == nil {
		t.Error("Transport() should not return nil")
	}
}

// mockStorage is a minimal storage implementation for testing handler config.
type mockStorage struct{}

func (m *mockStorage) Create(_ context.Context, _ string, _ StreamConfig) (bool, error) {
	return true, nil
}

func (m *mockStorage) Append(_ context.Context, _ string, _ []byte, _ string) (Offset, error) {
	return "", nil
}

func (m *mockStorage) AppendFrom(_ context.Context, _ string, _ io.Reader, _ string) (Offset, error) {
	return "", nil
}

func (m *mockStorage) Read(_ context.Context, _ string, _ Offset, _ int) (*ReadResult, error) {
	return &ReadResult{Messages: nil}, nil
}

func (m *mockStorage) Head(_ context.Context, _ string) (*StreamInfo, error) {
	return nil, nil
}

func (m *mockStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (m *mockStorage) Subscribe(_ context.Context, _ string, _ Offset) (<-chan Offset, error) {
	return nil, nil
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
				if got != tt.wantIsErr {
					t.Errorf("convertTransportError() = %v, want %v", got, tt.wantIsErr)
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
		ctx:         context.Background(),
		path:        "/test",
		contentType: "text/plain",
	}

	err := writer.Send([]byte("data"))
	if err == nil {
		t.Error("expected error for connection failure")
	}
}

func TestStreamWriter_SendWithSeqError(t *testing.T) {
	// Create a writer with invalid client config
	client := NewClient("http://localhost:1", nil)

	// Manually create a writer to bypass the Head check
	writer := &StreamWriter{
		client:      client,
		ctx:         context.Background(),
		path:        "/test",
		contentType: "text/plain",
	}

	err := writer.SendWithSeq("seq1", []byte("data"))
	if err == nil {
		t.Error("expected error for connection failure")
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
