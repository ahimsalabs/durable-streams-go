package durablestream

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestClient_BuilderPattern(t *testing.T) {
	// Test client builder pattern
	customHTTPClient := &http.Client{Timeout: 5 * time.Second}

	client := NewClient().
		BaseURL("http://example.com").
		HTTPClient(customHTTPClient).
		Timeout(10 * time.Second).
		LongPollTimeout(30 * time.Second)

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
			client := NewClient().BaseURL(tt.baseURL)
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

func TestHandler_ChunkSize(t *testing.T) {
	handler := NewHandler(&mockStorage{}).ChunkSize(1024)

	if handler.chunkSize != 1024 {
		t.Errorf("chunkSize = %d, want 1024", handler.chunkSize)
	}
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
