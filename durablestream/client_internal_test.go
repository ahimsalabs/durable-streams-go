package durablestream

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestClient_Config(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		client := NewClient("http://example.com", nil)

		if client.baseURL != "http://example.com" {
			t.Errorf("baseURL = %q, want %q", client.baseURL, "http://example.com")
		}
		if client.httpClient != http.DefaultClient {
			t.Error("httpClient should be http.DefaultClient")
		}
		if client.timeout != 30*time.Second {
			t.Errorf("timeout = %v, want %v", client.timeout, 30*time.Second)
		}
		if client.longPollTimeout != 60*time.Second {
			t.Errorf("longPollTimeout = %v, want %v", client.longPollTimeout, 60*time.Second)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		customHTTPClient := &http.Client{Timeout: 5 * time.Second}

		client := NewClient("http://example.com", &ClientConfig{
			HTTPClient:      customHTTPClient,
			Timeout:         10 * time.Second,
			LongPollTimeout: 30 * time.Second,
		})

		if client.baseURL != "http://example.com" {
			t.Errorf("baseURL = %q, want %q", client.baseURL, "http://example.com")
		}
		if client.httpClient != customHTTPClient {
			t.Error("httpClient not set correctly")
		}
		if client.timeout != 10*time.Second {
			t.Errorf("timeout = %v, want %v", client.timeout, 10*time.Second)
		}
		if client.longPollTimeout != 30*time.Second {
			t.Errorf("longPollTimeout = %v, want %v", client.longPollTimeout, 30*time.Second)
		}
	})

	t.Run("trailing slash stripped from baseURL", func(t *testing.T) {
		client := NewClient("http://example.com/", nil)
		if client.baseURL != "http://example.com" {
			t.Errorf("baseURL = %q, want %q", client.baseURL, "http://example.com")
		}
	})
}

func TestClient_buildURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		path    string
		want    string
	}{
		{
			name:    "no base URL",
			baseURL: "",
			path:    "/stream1",
			want:    "/stream1",
		},
		{
			name:    "with base URL",
			baseURL: "http://example.com",
			path:    "/stream1",
			want:    "http://example.com/stream1",
		},
		{
			name:    "trailing slash in base",
			baseURL: "http://example.com/",
			path:    "/stream1",
			want:    "http://example.com/stream1",
		},
		{
			name:    "path without leading slash",
			baseURL: "http://example.com",
			path:    "stream1",
			want:    "http://example.com/stream1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.baseURL, nil)
			got := client.buildURL(tt.path)
			if got != tt.want {
				t.Errorf("buildURL() = %q, want %q", got, tt.want)
			}
		})
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

func (m *mockStorage) AppendReader(_ context.Context, _ string, _ io.Reader, _ string) (Offset, error) {
	return "", nil
}

func (m *mockStorage) Read(_ context.Context, _ string, _ Offset, _ int) (*ReadResult, error) {
	return nil, nil
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
