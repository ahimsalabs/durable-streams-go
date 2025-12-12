package durablestream_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/memorystorage"
)

// setupTestServer creates a test server with a handler and storage.
func setupTestServer() (*httptest.Server, *memorystorage.Storage, *durablestream.Client) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage)
	server := httptest.NewServer(handler)
	client := durablestream.NewClient().BaseURL(server.URL)
	return server, storage, client
}

func ptrTo[T any](v T) *T { return &v }

func TestClient_Create(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		opts    *durablestream.CreateOptions
		wantErr bool
	}{
		{
			name: "create basic stream",
			path: "/stream1",
			opts: &durablestream.CreateOptions{ContentType: "application/json"},
		},
		{
			name: "create with TTL",
			path: "/stream2",
			opts: &durablestream.CreateOptions{
				ContentType: "text/plain",
				TTL:         ptrTo(1 * time.Hour),
			},
		},
		{
			name: "create with expiry",
			path: "/stream3",
			opts: &durablestream.CreateOptions{
				ContentType: "application/octet-stream",
				ExpiresAt:   ptrTo(time.Now().Add(1 * time.Hour)),
			},
		},
		{
			name: "create with initial data",
			path: "/stream4",
			opts: &durablestream.CreateOptions{
				ContentType: "application/json",
				InitialData: []byte(`[{"event":"created"}]`),
			},
		},
		{
			name: "create with nil opts",
			path: "/stream5",
			opts: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, client := setupTestServer()
			defer server.Close()

			ctx := context.Background()
			info, err := client.Create(ctx, tt.path, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil && tt.opts != nil && tt.opts.ContentType != "" {
				if info.ContentType != tt.opts.ContentType {
					t.Errorf("ContentType = %v, want %v", info.ContentType, tt.opts.ContentType)
				}
			}
		})
	}
}

func TestClient_Create_Idempotent(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream first time
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}

	// Create again with same config (should succeed)
	_, err = client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Errorf("idempotent create failed: %v", err)
	}
}

func TestClient_Head(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	ttl := 1 * time.Hour
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{
		ContentType: "application/json",
		TTL:         &ttl,
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Head request
	info, err := client.Head(ctx, "/stream1")
	if err != nil {
		t.Fatalf("head failed: %v", err)
	}

	if info.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", info.ContentType, "application/json")
	}

	if info.TTL == nil {
		t.Error("expected TTL to be set")
	}
}

func TestClient_Delete(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Delete stream
	err = client.Delete(ctx, "/stream1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deletion
	_, err = client.Head(ctx, "/stream1")
	if err == nil {
		t.Error("expected error when reading deleted stream")
	}
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected durablestream.ErrNotFound, got %v", err)
	}
}

func TestClient_ErrorHandling(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Test 404 error
	_, err := client.Head(ctx, "/nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent stream")
	}

	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected durablestream.ErrNotFound, got %v", err)
	}
}

func TestReader_Bytes(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("hello world"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Create reader
	reader := client.Reader("/stream1", durablestream.ZeroOffset)
	defer reader.Close()

	// Read bytes using iterator
	count := 0
	for data, err := range reader.Bytes(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if string(data) != "hello world" {
			t.Errorf("data = %q, want %q", string(data), "hello world")
		}

		count++
		// Break after first read to avoid long-poll timeout
		if count >= 1 {
			break
		}
	}

	if count != 1 {
		t.Errorf("read %d chunks, want 1", count)
	}
}

func TestReader_Close(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Create and close reader
	reader := client.Reader("/stream1", durablestream.ZeroOffset)
	if err := reader.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Try to read after close (should error)
	_, err = reader.Read(ctx)
	if !errors.Is(err, durablestream.ErrClosed) {
		t.Errorf("expected durablestream.ErrClosed, got %v", err)
	}
}

func TestStreamWriter(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Create writer
	writer, err := client.Writer(ctx, "/stream1")
	if err != nil {
		t.Fatalf("writer creation failed: %v", err)
	}

	// Send multiple messages
	messages := []string{"first", "second", "third"}
	for _, msg := range messages {
		if err := writer.Send([]byte(msg)); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	// Verify offset is non-empty
	if writer.Offset() == "" {
		t.Error("expected non-empty offset after sends")
	}
}

func TestStreamWriter_SendWithSeq(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Create writer
	writer, err := client.Writer(ctx, "/stream1")
	if err != nil {
		t.Fatalf("writer creation failed: %v", err)
	}

	// Send with sequence
	if err := writer.SendWithSeq("seq1", []byte("first")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Send with higher sequence
	if err := writer.SendWithSeq("seq2", []byte("second")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Send with lower sequence (should fail)
	err = writer.SendWithSeq("seq1", []byte("third"))
	if err == nil {
		t.Error("expected error for sequence regression")
	}
	if !errors.Is(err, durablestream.ErrConflict) {
		t.Errorf("expected durablestream.ErrConflict, got %v", err)
	}
}

func TestReader_Read(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream with initial data
	_, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("hello world"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Read from beginning
	reader := client.Reader("/stream1", durablestream.ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(result.Data) != "hello world" {
		t.Errorf("Data = %q, want %q", string(result.Data), "hello world")
	}

	if !result.UpToDate {
		t.Error("expected UpToDate to be true")
	}
}

func TestReader_ReadWithOffset(t *testing.T) {
	server, _, client := setupTestServer()
	defer server.Close()

	ctx := context.Background()

	// Create stream with initial data
	info, err := client.Create(ctx, "/stream1", &durablestream.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("first"),
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	firstOffset := info.NextOffset

	// Append more data using Writer
	writer, err := client.Writer(ctx, "/stream1")
	if err != nil {
		t.Fatalf("writer creation failed: %v", err)
	}
	if err := writer.Send([]byte("second")); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Read from first offset (should get "second")
	reader := client.Reader("/stream1", firstOffset)
	defer reader.Close()

	result, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(result.Data) != "second" {
		t.Errorf("Data = %q, want %q", string(result.Data), "second")
	}
}
