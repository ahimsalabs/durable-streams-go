package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mockTransport implements Transport for testing middleware.
type mockTransport struct {
	readFunc     func(context.Context, ReadRequest) (*ReadResponse, error)
	longPollFunc func(context.Context, LongPollRequest) (*ReadResponse, error)
	sseFunc      func(context.Context, SSERequest) (EventStream, error)
	appendFunc   func(context.Context, AppendRequest) (*AppendResponse, error)
	createFunc   func(context.Context, CreateRequest) (*CreateResponse, error)
	deleteFunc   func(context.Context, DeleteRequest) error
	headFunc     func(context.Context, HeadRequest) (*HeadResponse, error)
}

func (m *mockTransport) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, req)
	}
	return &ReadResponse{}, nil
}

func (m *mockTransport) LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
	if m.longPollFunc != nil {
		return m.longPollFunc(ctx, req)
	}
	return &ReadResponse{}, nil
}

func (m *mockTransport) SSE(ctx context.Context, req SSERequest) (EventStream, error) {
	if m.sseFunc != nil {
		return m.sseFunc(ctx, req)
	}
	return nil, nil
}

func (m *mockTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	if m.appendFunc != nil {
		return m.appendFunc(ctx, req)
	}
	return &AppendResponse{}, nil
}

func (m *mockTransport) Create(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, req)
	}
	return &CreateResponse{}, nil
}

func (m *mockTransport) Delete(ctx context.Context, req DeleteRequest) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, req)
	}
	return nil
}

func (m *mockTransport) Head(ctx context.Context, req HeadRequest) (*HeadResponse, error) {
	if m.headFunc != nil {
		return m.headFunc(ctx, req)
	}
	return &HeadResponse{}, nil
}

// --- HTTPTransport Tests ---

func TestNewHTTPTransport(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", nil)
		if tr.baseURL != "http://example.com" {
			t.Errorf("expected baseURL http://example.com, got %s", tr.baseURL)
		}
		if tr.client != http.DefaultClient {
			t.Error("expected default client")
		}
		if tr.longPollTimeout != 60*time.Second {
			t.Errorf("expected 60s timeout, got %v", tr.longPollTimeout)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		customClient := &http.Client{Timeout: 30 * time.Second}
		tr := NewHTTPTransport("http://example.com/", &HTTPConfig{
			Client:          customClient,
			LongPollTimeout: 120 * time.Second,
		})
		if tr.baseURL != "http://example.com" {
			t.Errorf("expected trailing slash trimmed, got %s", tr.baseURL)
		}
		if tr.client != customClient {
			t.Error("expected custom client")
		}
		if tr.longPollTimeout != 120*time.Second {
			t.Errorf("expected 120s timeout, got %v", tr.longPollTimeout)
		}
	})

	t.Run("with headers provider", func(t *testing.T) {
		headerProvider := func(ctx context.Context) (http.Header, error) {
			h := http.Header{}
			h.Set("Authorization", "Bearer token")
			return h, nil
		}
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: headerProvider,
		})
		if tr.headers == nil {
			t.Error("expected headers to be set")
		}
	})
}

func TestHTTPTransport_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("expected GET, got %s", r.Method)
			}
			if r.URL.Query().Get("offset") != "123_456" {
				t.Errorf("expected offset 123_456, got %s", r.URL.Query().Get("offset"))
			}
			w.Header().Set("Stream-Next-Offset", "123_789")
			w.Header().Set("Stream-Cursor", "cursor123")
			w.Header().Set("Stream-Up-To-Date", "true")
			_, _ = w.Write([]byte(`[{"msg":"hello"}]`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Read(context.Background(), ReadRequest{
			Path:   "/test",
			Offset: "123_456",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "123_789" {
			t.Errorf("expected NextOffset 123_789, got %s", resp.NextOffset)
		}
		if resp.Cursor != "cursor123" {
			t.Errorf("expected cursor cursor123, got %s", resp.Cursor)
		}
		if !resp.UpToDate {
			t.Error("expected UpToDate to be true")
		}
		if string(resp.Data) != `[{"msg":"hello"}]` {
			t.Errorf("unexpected data: %s", resp.Data)
		}
	})

	t.Run("no offset", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("offset") != "" {
				t.Errorf("expected no offset, got %s", r.URL.Query().Get("offset"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Read(context.Background(), ReadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "0_0" {
			t.Errorf("expected NextOffset 0_0, got %s", resp.NextOffset)
		}
	})

	t.Run("not found error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"not_found","message":"stream not found"}`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Read(context.Background(), ReadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
		var tErr *Error
		if !errors.As(err, &tErr) {
			t.Fatalf("expected transport.Error, got %T", err)
		}
		if tErr.Code != "not_found" {
			t.Errorf("expected code not_found, got %s", tErr.Code)
		}
	})

	t.Run("with headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer secret" {
				t.Errorf("expected Authorization header, got %s", r.Header.Get("Authorization"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				h := http.Header{}
				h.Set("Authorization", "Bearer secret")
				return h, nil
			},
		})
		_, err := tr.Read(context.Background(), ReadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("auth failed")
			},
		})
		_, err := tr.Read(context.Background(), ReadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "auth failed") {
			t.Errorf("expected auth error, got %v", err)
		}
	})
}

func TestHTTPTransport_LongPoll(t *testing.T) {
	t.Run("success with data", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("live") != "long-poll" {
				t.Errorf("expected live=long-poll, got %s", r.URL.Query().Get("live"))
			}
			if r.URL.Query().Get("offset") != "123_456" {
				t.Errorf("expected offset, got %s", r.URL.Query().Get("offset"))
			}
			if r.URL.Query().Get("cursor") != "mycursor" {
				t.Errorf("expected cursor, got %s", r.URL.Query().Get("cursor"))
			}
			w.Header().Set("Stream-Next-Offset", "123_789")
			_, _ = w.Write([]byte(`[{"new":"data"}]`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.LongPoll(context.Background(), LongPollRequest{
			Path:   "/test",
			Offset: "123_456",
			Cursor: "mycursor",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "123_789" {
			t.Errorf("expected NextOffset 123_789, got %s", resp.NextOffset)
		}
	})

	t.Run("204 no content (timeout)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Stream-Next-Offset", "123_456")
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.LongPoll(context.Background(), LongPollRequest{
			Path:   "/test",
			Offset: "123_456",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.UpToDate {
			t.Error("expected UpToDate on 204")
		}
		if len(resp.Data) != 0 {
			t.Errorf("expected empty data, got %s", resp.Data)
		}
	})

	t.Run("custom timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Stream-Next-Offset", "0_0")
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.LongPoll(context.Background(), LongPollRequest{
			Path:    "/test",
			Offset:  "0_0",
			Timeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestHTTPTransport_SSE(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("live") != "sse" {
				t.Errorf("expected live=sse, got %s", r.URL.Query().Get("live"))
			}
			if r.Header.Get("Accept") != "text/event-stream" {
				t.Errorf("expected Accept header, got %s", r.Header.Get("Accept"))
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			_, _ = fmt.Fprintf(w, "event: data\ndata: {\"msg\":\"hello\"}\n\n")
			flusher.Flush()
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		stream, err := tr.SSE(context.Background(), SSERequest{
			Path:   "/test",
			Offset: "0_0",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer stream.Close()

		event, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if event.Type != "data" {
			t.Errorf("expected event type data, got %s", event.Type)
		}
		if string(event.Data) != `{"msg":"hello"}` {
			t.Errorf("unexpected data: %s", event.Data)
		}
	})

	t.Run("control event", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "event: control\ndata: {\"streamNextOffset\":\"123_456\",\"streamCursor\":\"abc\"}\n\n")
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		stream, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer stream.Close()

		event, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if event.Type != "control" {
			t.Errorf("expected control event, got %s", event.Type)
		}
		if event.NextOffset != "123_456" {
			t.Errorf("expected NextOffset 123_456, got %s", event.NextOffset)
		}
		if event.Cursor != "abc" {
			t.Errorf("expected cursor abc, got %s", event.Cursor)
		}
	})

	t.Run("wrong content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err == nil {
			t.Fatal("expected error for wrong content type")
		}
		if !strings.Contains(err.Error(), "unexpected content type") {
			t.Errorf("expected content type error, got %v", err)
		}
	})

	t.Run("multi-line data", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			// Multi-line data field
			_, _ = fmt.Fprintf(w, "event: data\ndata: [\ndata: {\"a\":1},\ndata: {\"b\":2}\ndata: ]\n\n")
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		stream, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer stream.Close()

		event, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := "[\n{\"a\":1},\n{\"b\":2}\n]"
		if string(event.Data) != expected {
			t.Errorf("expected %q, got %q", expected, string(event.Data))
		}
	})

	t.Run("context already cancelled", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "event: data\ndata: first\n\n")
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		stream, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer stream.Close()

		// Cancel context before calling Next
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = stream.Next(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestHTTPTransport_Append(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected content type, got %s", r.Header.Get("Content-Type"))
			}
			body, _ := io.ReadAll(r.Body)
			if string(body) != `{"msg":"test"}` {
				t.Errorf("unexpected body: %s", body)
			}
			w.Header().Set("Stream-Next-Offset", "123_789")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Append(context.Background(), AppendRequest{
			Path:        "/test",
			Data:        []byte(`{"msg":"test"}`),
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "123_789" {
			t.Errorf("expected NextOffset 123_789, got %s", resp.NextOffset)
		}
	})

	t.Run("with seq header", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Stream-Seq") != "seq123" {
				t.Errorf("expected Stream-Seq header, got %s", r.Header.Get("Stream-Seq"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Append(context.Background(), AppendRequest{
			Path: "/test",
			Data: []byte("data"),
			Seq:  "seq123",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty data rejected", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", nil)
		_, err := tr.Append(context.Background(), AppendRequest{
			Path: "/test",
			Data: []byte{},
		})
		if err == nil {
			t.Fatal("expected error for empty append")
		}
		if !strings.Contains(err.Error(), "empty append") {
			t.Errorf("expected empty append error, got %v", err)
		}
	})
}

func TestHTTPTransport_Create(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("expected PUT, got %s", r.Method)
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected content type, got %s", r.Header.Get("Content-Type"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Create(context.Background(), CreateRequest{
			Path:        "/test",
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "0_0" {
			t.Errorf("expected NextOffset 0_0, got %s", resp.NextOffset)
		}
	})

	t.Run("with TTL", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Stream-TTL") != "3600" {
				t.Errorf("expected TTL 3600, got %s", r.Header.Get("Stream-TTL"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Create(context.Background(), CreateRequest{
			Path: "/test",
			TTL:  time.Hour,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("with expires at", func(t *testing.T) {
		expiresAt := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Stream-Expires-At") != "2025-12-31T23:59:59Z" {
				t.Errorf("expected ExpiresAt, got %s", r.Header.Get("Stream-Expires-At"))
			}
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Create(context.Background(), CreateRequest{
			Path:      "/test",
			ExpiresAt: expiresAt,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("with initial data", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if string(body) != "initial" {
				t.Errorf("expected initial data, got %s", body)
			}
			w.Header().Set("Stream-Next-Offset", "0_7")
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Create(context.Background(), CreateRequest{
			Path:        "/test",
			InitialData: []byte("initial"),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.NextOffset != "0_7" {
			t.Errorf("expected NextOffset 0_7, got %s", resp.NextOffset)
		}
	})
}

func TestHTTPTransport_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete {
				t.Errorf("expected DELETE, got %s", r.Method)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		err := tr.Delete(context.Background(), DeleteRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		err := tr.Delete(context.Background(), DeleteRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
		var tErr *Error
		if !errors.As(err, &tErr) {
			t.Fatalf("expected transport.Error, got %T", err)
		}
		if tErr.Code != "NOT_FOUND" {
			t.Errorf("expected code NOT_FOUND, got %s", tErr.Code)
		}
	})
}

func TestHTTPTransport_Head(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodHead {
				t.Errorf("expected HEAD, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Stream-Next-Offset", "123_456")
			w.Header().Set("Stream-TTL", "3600")
			w.Header().Set("Stream-Expires-At", "2025-12-31T23:59:59Z")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Head(context.Background(), HeadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.ContentType != "application/json" {
			t.Errorf("expected content type application/json, got %s", resp.ContentType)
		}
		if resp.NextOffset != "123_456" {
			t.Errorf("expected NextOffset 123_456, got %s", resp.NextOffset)
		}
		if resp.TTL != time.Hour {
			t.Errorf("expected TTL 1h, got %v", resp.TTL)
		}
		if resp.ExpiresAt.Year() != 2025 {
			t.Errorf("expected ExpiresAt 2025, got %v", resp.ExpiresAt)
		}
	})

	t.Run("minimal response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Stream-Next-Offset", "0_0")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		resp, err := tr.Head(context.Background(), HeadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.TTL != 0 {
			t.Errorf("expected no TTL, got %v", resp.TTL)
		}
		if !resp.ExpiresAt.IsZero() {
			t.Errorf("expected no ExpiresAt, got %v", resp.ExpiresAt)
		}
	})
}

func TestHTTPTransport_buildURL(t *testing.T) {
	t.Run("with base URL", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com/api", nil)
		u, err := tr.buildURL("/streams/test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if u.String() != "http://example.com/api/streams/test" {
			t.Errorf("unexpected URL: %s", u.String())
		}
	})

	t.Run("without base URL", func(t *testing.T) {
		tr := NewHTTPTransport("", nil)
		u, err := tr.buildURL("http://example.com/streams/test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if u.String() != "http://example.com/streams/test" {
			t.Errorf("unexpected URL: %s", u.String())
		}
	})
}

// --- Edge Case Tests ---

func TestHTTPTransport_NetworkErrors(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		tr := NewHTTPTransport("http://localhost:1", nil) // Port 1 won't have anything
		_, err := tr.Read(context.Background(), ReadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error for connection refused")
		}
	})

	t.Run("invalid URL", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", nil)
		// Invalid path that produces invalid URL
		_, err := tr.Read(context.Background(), ReadRequest{Path: "://invalid"})
		if err == nil {
			// Note: buildURL may or may not error depending on the URL
			// Just verify it doesn't panic
			t.Log("URL was accepted")
		}
	})
}

func TestHTTPTransport_LongPoll_Error(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"server_error","message":"boom"}`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.LongPoll(context.Background(), LongPollRequest{Path: "/test", Offset: "0_0"})
		if err == nil {
			t.Fatal("expected error")
		}
		var tErr *Error
		if !errors.As(err, &tErr) {
			t.Fatalf("expected transport.Error, got %T", err)
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		_, err := tr.LongPoll(context.Background(), LongPollRequest{Path: "/test", Offset: "0_0"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestHTTPTransport_SSE_Errors(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		_, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("stream EOF", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			// Write partial event then close connection
			_, _ = fmt.Fprintf(w, "event: data\n")
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		stream, err := tr.SSE(context.Background(), SSERequest{Path: "/test", Offset: "0_0"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer stream.Close()

		_, err = stream.Next(context.Background())
		if err == nil {
			t.Fatal("expected error on EOF")
		}
	})
}

func TestHTTPTransport_Append_Errors(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"bad_request","message":"invalid"}`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Append(context.Background(), AppendRequest{Path: "/test", Data: []byte("x")})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		_, err := tr.Append(context.Background(), AppendRequest{Path: "/test", Data: []byte("x")})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestHTTPTransport_Create_Errors(t *testing.T) {
	t.Run("conflict error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"conflict","message":"exists"}`))
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Create(context.Background(), CreateRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		_, err := tr.Create(context.Background(), CreateRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestHTTPTransport_Delete_Errors(t *testing.T) {
	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		err := tr.Delete(context.Background(), DeleteRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("connection error", func(t *testing.T) {
		tr := NewHTTPTransport("http://localhost:1", nil) // Port 1 won't respond
		err := tr.Delete(context.Background(), DeleteRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error for connection refused")
		}
	})
}

func TestHTTPTransport_Head_Errors(t *testing.T) {
	t.Run("not found error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		tr := NewHTTPTransport(server.URL, nil)
		_, err := tr.Head(context.Background(), HeadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("header provider error", func(t *testing.T) {
		tr := NewHTTPTransport("http://example.com", &HTTPConfig{
			Headers: func(ctx context.Context) (http.Header, error) {
				return nil, errors.New("header error")
			},
		})
		_, err := tr.Head(context.Background(), HeadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestHTTPEventStream_Close(t *testing.T) {
	t.Run("nil response", func(t *testing.T) {
		stream := &httpEventStream{response: nil}
		err := stream.Close()
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("nil body", func(t *testing.T) {
		stream := &httpEventStream{response: &http.Response{Body: nil}}
		err := stream.Close()
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// --- Error Tests ---

func TestError(t *testing.T) {
	t.Run("with message", func(t *testing.T) {
		err := &Error{Code: "NOT_FOUND", Message: "stream not found", StatusCode: 404}
		if err.Error() != "stream not found" {
			t.Errorf("expected message, got %s", err.Error())
		}
	})

	t.Run("without message", func(t *testing.T) {
		err := &Error{Code: "NOT_FOUND", StatusCode: 404}
		if err.Error() != "NOT_FOUND" {
			t.Errorf("expected code, got %s", err.Error())
		}
	})
}

func TestHttpStatusToCode(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{404, "NOT_FOUND"},
		{409, "CONFLICT"},
		{400, "BAD_REQUEST"},
		{410, "GONE"},
		{429, "RATE_LIMITED"},
		{401, "UNAUTHORIZED"},
		{403, "FORBIDDEN"},
		{500, "UNKNOWN"},
		{503, "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.status), func(t *testing.T) {
			got := httpStatusToCode(tt.status)
			if got != tt.want {
				t.Errorf("httpStatusToCode(%d) = %s, want %s", tt.status, got, tt.want)
			}
		})
	}
}

// --- SSE Parser Tests ---

func TestBuildSSEData(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		data := buildSSEData(nil)
		if string(data) != "{}" {
			t.Errorf("expected {}, got %s", data)
		}
	})

	t.Run("single line", func(t *testing.T) {
		data := buildSSEData([]string{`{"foo":"bar"}`})
		if string(data) != `{"foo":"bar"}` {
			t.Errorf("unexpected: %s", data)
		}
	})

	t.Run("multiple lines", func(t *testing.T) {
		data := buildSSEData([]string{"[", `{"a":1}`, "]"})
		expected := "[\n{\"a\":1}\n]"
		if string(data) != expected {
			t.Errorf("expected %q, got %q", expected, string(data))
		}
	})
}

// --- Middleware Tests ---

func TestWithLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mock := &mockTransport{
		readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
			return &ReadResponse{NextOffset: "0_0"}, nil
		},
	}

	logged := WithLogging(logger)(mock)

	_, err := logged.Read(context.Background(), ReadRequest{Path: "/test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(buf.String(), "Read") {
		t.Errorf("expected log to contain 'Read', got %s", buf.String())
	}
	if !strings.Contains(buf.String(), "/test") {
		t.Errorf("expected log to contain path, got %s", buf.String())
	}
}

func TestWithLogging_AllMethods(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mock := &mockTransport{}
	logged := WithLogging(logger)(mock)
	ctx := context.Background()

	// Test all methods are logged
	_, _ = logged.Read(ctx, ReadRequest{Path: "/read"})
	_, _ = logged.LongPoll(ctx, LongPollRequest{Path: "/poll"})
	_, _ = logged.SSE(ctx, SSERequest{Path: "/sse"})
	_, _ = logged.Append(ctx, AppendRequest{Path: "/append", Data: []byte("x")})
	_, _ = logged.Create(ctx, CreateRequest{Path: "/create"})
	_ = logged.Delete(ctx, DeleteRequest{Path: "/delete"})
	_, _ = logged.Head(ctx, HeadRequest{Path: "/head"})

	output := buf.String()
	for _, op := range []string{"Read", "LongPoll", "SSE", "Append", "Create", "Delete", "Head"} {
		if !strings.Contains(output, op) {
			t.Errorf("expected log to contain %s", op)
		}
	}
}

func TestWithLogging_Error(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	mock := &mockTransport{
		readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
			return nil, errors.New("test error")
		},
	}

	logged := WithLogging(logger)(mock)

	_, err := logged.Read(context.Background(), ReadRequest{Path: "/test"})
	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.Contains(buf.String(), "ERROR") {
		t.Errorf("expected error log level, got %s", buf.String())
	}
}

func TestWithRetry(t *testing.T) {
	t.Run("success on first try", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				return &ReadResponse{NextOffset: "0_0"}, nil
			},
		}

		retried := WithRetry(RetryOptions{MaxRetries: 3})(mock)
		_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if calls != 1 {
			t.Errorf("expected 1 call, got %d", calls)
		}
	})

	t.Run("success after retry", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				if calls < 3 {
					return nil, &Error{StatusCode: 500, Code: "SERVER_ERROR"}
				}
				return &ReadResponse{NextOffset: "0_0"}, nil
			},
		}

		retried := WithRetry(RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Millisecond,
		})(mock)
		_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if calls != 3 {
			t.Errorf("expected 3 calls, got %d", calls)
		}
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				return nil, &Error{StatusCode: 500, Code: "SERVER_ERROR"}
			},
		}

		retried := WithRetry(RetryOptions{
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
		})(mock)
		_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error after max retries")
		}
		// Initial attempt + 2 retries = 3 calls
		if calls != 3 {
			t.Errorf("expected 3 calls, got %d", calls)
		}
	})

	t.Run("non-retryable error", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				return nil, &Error{StatusCode: 404, Code: "NOT_FOUND"}
			},
		}

		retried := WithRetry(RetryOptions{MaxRetries: 3})(mock)
		_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
		if err == nil {
			t.Fatal("expected error")
		}
		if calls != 1 {
			t.Errorf("expected 1 call (no retry for 404), got %d", calls)
		}
	})

	t.Run("context cancellation during backoff", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				return nil, &Error{StatusCode: 500, Code: "SERVER_ERROR"}
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		retried := WithRetry(RetryOptions{
			MaxRetries:     5,
			InitialBackoff: 10 * time.Second, // Long backoff
		})(mock)

		done := make(chan error)
		go func() {
			_, err := retried.Read(ctx, ReadRequest{Path: "/test"})
			done <- err
		}()

		// Wait for first failure, then cancel
		time.Sleep(50 * time.Millisecond)
		cancel()

		err := <-done
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("rate limit (429) is retried", func(t *testing.T) {
		calls := 0
		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				calls++
				if calls == 1 {
					return nil, &Error{StatusCode: 429, Code: "RATE_LIMITED"}
				}
				return &ReadResponse{NextOffset: "0_0"}, nil
			},
		}

		retried := WithRetry(RetryOptions{
			MaxRetries:     1,
			InitialBackoff: time.Millisecond,
		})(mock)
		_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if calls != 2 {
			t.Errorf("expected 2 calls (retry on 429), got %d", calls)
		}
	})
}

func TestWithRetry_AllMethods(t *testing.T) {
	var callCount atomic.Int32
	serverErr := &Error{StatusCode: 500, Code: "SERVER_ERROR"}

	mock := &mockTransport{
		readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return &ReadResponse{}, nil
		},
		longPollFunc: func(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return &ReadResponse{}, nil
		},
		sseFunc: func(ctx context.Context, req SSERequest) (EventStream, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return nil, nil
		},
		appendFunc: func(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return &AppendResponse{}, nil
		},
		createFunc: func(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return &CreateResponse{}, nil
		},
		deleteFunc: func(ctx context.Context, req DeleteRequest) error {
			if callCount.Add(1) == 1 {
				return serverErr
			}
			return nil
		},
		headFunc: func(ctx context.Context, req HeadRequest) (*HeadResponse, error) {
			if callCount.Add(1) == 1 {
				return nil, serverErr
			}
			return &HeadResponse{}, nil
		},
	}

	retried := WithRetry(RetryOptions{
		MaxRetries:     1,
		InitialBackoff: time.Millisecond,
	})(mock)
	ctx := context.Background()

	// Test each method retries
	callCount.Store(0)
	_, _ = retried.Read(ctx, ReadRequest{Path: "/test"})

	callCount.Store(0)
	_, _ = retried.LongPoll(ctx, LongPollRequest{Path: "/test"})

	callCount.Store(0)
	_, _ = retried.SSE(ctx, SSERequest{Path: "/test"})

	callCount.Store(0)
	_, _ = retried.Append(ctx, AppendRequest{Path: "/test", Data: []byte("x")})

	callCount.Store(0)
	_, _ = retried.Create(ctx, CreateRequest{Path: "/test"})

	callCount.Store(0)
	_ = retried.Delete(ctx, DeleteRequest{Path: "/test"})

	callCount.Store(0)
	_, _ = retried.Head(ctx, HeadRequest{Path: "/test"})
}

func TestWithRetry_ZeroOptions(t *testing.T) {
	// Test that zero options get defaults applied
	calls := 0
	mock := &mockTransport{
		readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
			calls++
			if calls < 2 {
				return nil, &Error{StatusCode: 500, Code: "SERVER_ERROR"}
			}
			return &ReadResponse{NextOffset: "0_0"}, nil
		},
	}

	// Pass zero options - should use defaults
	retried := WithRetry(RetryOptions{})(mock)
	_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls < 2 {
		t.Errorf("expected at least 2 calls, got %d", calls)
	}
}

func TestDefaultRetryOptions(t *testing.T) {
	opts := DefaultRetryOptions()
	if opts.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", opts.MaxRetries)
	}
	if opts.InitialBackoff != 100*time.Millisecond {
		t.Errorf("expected InitialBackoff 100ms, got %v", opts.InitialBackoff)
	}
	if opts.MaxBackoff != 10*time.Second {
		t.Errorf("expected MaxBackoff 10s, got %v", opts.MaxBackoff)
	}
	if opts.Multiplier != 2.0 {
		t.Errorf("expected Multiplier 2.0, got %f", opts.Multiplier)
	}
	if opts.Retryable == nil {
		t.Error("expected Retryable to be set")
	}
}

func TestDefaultRetryable(t *testing.T) {
	tests := []struct {
		err       error
		retryable bool
	}{
		{&Error{StatusCode: 500}, true},
		{&Error{StatusCode: 502}, true},
		{&Error{StatusCode: 503}, true},
		{&Error{StatusCode: 429}, true},
		{&Error{StatusCode: 404}, false},
		{&Error{StatusCode: 400}, false},
		{errors.New("generic error"), false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%v", tt.err), func(t *testing.T) {
			got := defaultRetryable(tt.err)
			if got != tt.retryable {
				t.Errorf("defaultRetryable(%v) = %v, want %v", tt.err, got, tt.retryable)
			}
		})
	}
}

func TestChain(t *testing.T) {
	t.Run("empty chain", func(t *testing.T) {
		mock := &mockTransport{}
		chained := Chain()(mock)
		if chained != mock {
			t.Error("expected original transport for empty chain")
		}
	})

	t.Run("single middleware", func(t *testing.T) {
		var order []string
		m1 := func(next Transport) Transport {
			return &mockTransport{
				readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
					order = append(order, "m1-before")
					resp, err := next.Read(ctx, req)
					order = append(order, "m1-after")
					return resp, err
				},
			}
		}

		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				order = append(order, "handler")
				return &ReadResponse{}, nil
			},
		}

		chained := Chain(m1)(mock)
		_, _ = chained.Read(context.Background(), ReadRequest{})

		expected := []string{"m1-before", "handler", "m1-after"}
		if len(order) != len(expected) {
			t.Fatalf("expected %v, got %v", expected, order)
		}
		for i, v := range expected {
			if order[i] != v {
				t.Errorf("expected order[%d] = %s, got %s", i, v, order[i])
			}
		}
	})

	t.Run("multiple middleware in order", func(t *testing.T) {
		var order []string

		makeMiddleware := func(name string) Middleware {
			return func(next Transport) Transport {
				return &mockTransport{
					readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
						order = append(order, name+"-before")
						resp, err := next.Read(ctx, req)
						order = append(order, name+"-after")
						return resp, err
					},
				}
			}
		}

		mock := &mockTransport{
			readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
				order = append(order, "handler")
				return &ReadResponse{}, nil
			},
		}

		// Chain(a, b, c)(t) == a(b(c(t)))
		// So order should be: a-before, b-before, c-before, handler, c-after, b-after, a-after
		chained := Chain(
			makeMiddleware("a"),
			makeMiddleware("b"),
			makeMiddleware("c"),
		)(mock)
		_, _ = chained.Read(context.Background(), ReadRequest{})

		expected := []string{
			"a-before", "b-before", "c-before",
			"handler",
			"c-after", "b-after", "a-after",
		}
		if len(order) != len(expected) {
			t.Fatalf("expected %v, got %v", expected, order)
		}
		for i, v := range expected {
			if order[i] != v {
				t.Errorf("expected order[%d] = %s, got %s", i, v, order[i])
			}
		}
	})
}

func TestWithRetry_BackoffCapping(t *testing.T) {
	calls := 0
	mock := &mockTransport{
		readFunc: func(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
			calls++
			if calls <= 5 {
				return nil, &Error{StatusCode: 500, Code: "SERVER_ERROR"}
			}
			return &ReadResponse{NextOffset: "0_0"}, nil
		},
	}

	retried := WithRetry(RetryOptions{
		MaxRetries:     5,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond, // Cap at 2ms
		Multiplier:     10.0,                 // Would grow to 10ms, 100ms, etc without cap
	})(mock)

	start := time.Now()
	_, err := retried.Read(context.Background(), ReadRequest{Path: "/test"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With 5 retries and max 2ms backoff, should take at most ~10ms
	// Without capping, would take 1 + 10 + 100 + 1000 + 10000 = 11111ms
	if elapsed > 100*time.Millisecond {
		t.Errorf("backoff capping not working, took %v", elapsed)
	}
}
