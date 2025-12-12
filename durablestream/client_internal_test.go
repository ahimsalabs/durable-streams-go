package durablestream

import (
	"context"
	"encoding/json"
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

func TestStreamData_Bytes(t *testing.T) {
	tests := []struct {
		name     string
		data     *StreamData
		wantData []byte
		wantOK   bool
	}{
		{
			name:     "with data",
			data:     &StreamData{Data: []byte("hello")},
			wantData: []byte("hello"),
			wantOK:   true,
		},
		{
			name:     "empty data",
			data:     &StreamData{},
			wantData: nil,
			wantOK:   false,
		},
		{
			name:     "json mode (no data)",
			data:     &StreamData{Messages: []json.RawMessage{json.RawMessage(`{"a":1}`)}},
			wantData: nil,
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.data.Bytes()
			if ok != tt.wantOK {
				t.Errorf("Bytes() ok = %v, want %v", ok, tt.wantOK)
			}
			if string(got) != string(tt.wantData) {
				t.Errorf("Bytes() = %q, want %q", got, tt.wantData)
			}
		})
	}
}

func TestStreamData_JSON(t *testing.T) {
	type msg struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name   string
		data   *StreamData
		wantOK bool
		want   msg
	}{
		{
			name:   "from messages",
			data:   &StreamData{Messages: []json.RawMessage{json.RawMessage(`{"name":"alice"}`)}},
			wantOK: true,
			want:   msg{Name: "alice"},
		},
		{
			name:   "from json-shaped bytes",
			data:   &StreamData{Data: []byte(`{"name":"bob"}`)},
			wantOK: true,
			want:   msg{Name: "bob"},
		},
		{
			name:   "empty",
			data:   &StreamData{},
			wantOK: false,
			want:   msg{},
		},
		{
			name:   "invalid json in data",
			data:   &StreamData{Data: []byte("not json")},
			wantOK: false,
			want:   msg{},
		},
		{
			name:   "messages takes precedence over data",
			data:   &StreamData{Messages: []json.RawMessage{json.RawMessage(`{"name":"from_msg"}`)}, Data: []byte(`{"name":"from_data"}`)},
			wantOK: true,
			want:   msg{Name: "from_msg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got msg
			ok := tt.data.JSON(&got)
			if ok != tt.wantOK {
				t.Errorf("JSON() ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("JSON() decoded = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestStreamData_JSON_Slice(t *testing.T) {
	type msg struct {
		ID int `json:"id"`
	}

	// JSON array in Data unmarshals to slice
	data := &StreamData{Data: []byte(`[{"id":3},{"id":4}]`)}
	var got []msg
	if !data.JSON(&got) {
		t.Fatal("JSON() returned false")
	}
	if len(got) != 2 || got[0].ID != 3 || got[1].ID != 4 {
		t.Errorf("JSON() = %+v, want [{3} {4}]", got)
	}
}
