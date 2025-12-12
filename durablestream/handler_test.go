package durablestream_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
	"github.com/ahimsalabs/durable-streams-go/durablestream/memorystorage"
)

func TestHandler_PUT_CreateStream(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		headers     map[string]string
		wantStatus  int
		wantHeaders map[string]string
		wantErrCode string
	}{
		{
			name:        "create basic stream",
			contentType: "application/octet-stream",
			wantStatus:  http.StatusCreated,
			wantHeaders: map[string]string{
				"Content-Type":       "application/octet-stream",
				"Stream-Next-Offset": "0000000000",
				"Location":           "http://example.com/test-stream",
			},
		},
		{
			name:        "create JSON stream",
			contentType: "application/json",
			wantStatus:  http.StatusCreated,
			wantHeaders: map[string]string{
				"Content-Type": "application/json",
			},
		},
		{
			name:        "create with TTL",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-TTL": "3600",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:        "create with Expires-At",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-Expires-At": "2026-01-15T12:00:00Z",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:        "reject both TTL and Expires-At",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-TTL":        "3600",
				"Stream-Expires-At": "2026-01-15T12:00:00Z",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
		{
			name:        "reject invalid TTL",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-TTL": "invalid",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
		{
			name:        "reject invalid Expires-At",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-Expires-At": "not-a-timestamp",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
		{
			name:        "create with initial content",
			contentType: "text/plain",
			body:        "initial content",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "create JSON with initial content",
			contentType: "application/json",
			body:        `{"event":"created"}`,
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "create JSON with array flattening",
			contentType: "application/json",
			body:        `[{"event":"a"},{"event":"b"}]`,
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := memorystorage.New()
			handler := durablestream.NewHandler(storage, nil)

			req := httptest.NewRequest(http.MethodPut, "/test-stream", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d. Body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			for k, want := range tt.wantHeaders {
				if got := rec.Header().Get(k); got != want {
					t.Errorf("header %s = %q, want %q", k, got, want)
				}
			}

			if tt.wantErrCode != "" && rec.Code >= 400 {
				// Check error response contains expected code
				body := rec.Body.String()
				if !strings.Contains(body, tt.wantErrCode) {
					t.Errorf("error body %q does not contain expected code %q", body, tt.wantErrCode)
				}
			}
		})
	}
}

func TestHandler_PUT_Idempotent(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream first time
	req1 := httptest.NewRequest(http.MethodPut, "/stream", nil)
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want %d", rec1.Code, http.StatusCreated)
	}

	// Create again with same config (should be idempotent)
	req2 := httptest.NewRequest(http.MethodPut, "/stream", nil)
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated && rec2.Code != http.StatusOK {
		t.Errorf("second create status = %d, want 201 or 200", rec2.Code)
	}

	// Create with different config (should conflict)
	req3 := httptest.NewRequest(http.MethodPut, "/stream", nil)
	req3.Header.Set("Content-Type", "text/plain") // Different content type
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusConflict {
		t.Errorf("conflict create status = %d, want %d", rec3.Code, http.StatusConflict)
	}
}

func TestHandler_POST_Append(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		seq         string
		wantStatus  int
		wantErrCode string
	}{
		{
			name:        "append binary data",
			contentType: "application/octet-stream",
			body:        "binary data here",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "append JSON object",
			contentType: "application/json",
			body:        `{"event":"updated"}`,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "append JSON array (flattened)",
			contentType: "application/json",
			body:        `[{"event":"a"},{"event":"b"}]`,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "reject empty body",
			contentType: "application/json", // Match stream content type
			body:        "",
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
		{
			name:        "reject content type mismatch",
			contentType: "text/plain", // Stream is application/json
			body:        "text data",
			wantStatus:  http.StatusConflict,
			wantErrCode: "conflict",
		},
		{
			name:        "append with sequence number",
			contentType: "application/json",
			body:        `{"seq":1}`,
			seq:         "seq_001",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "reject invalid JSON",
			contentType: "application/json",
			body:        `{invalid json}`,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
		{
			name:        "reject empty JSON array",
			contentType: "application/json",
			body:        `[]`,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "bad_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := memorystorage.New()
			handler := durablestream.NewHandler(storage, nil)

			// Create stream first
			streamContentType := "application/json"
			if tt.name == "append binary data" || tt.name == "reject content type mismatch" {
				streamContentType = "application/octet-stream"
			}

			storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
				ContentType: streamContentType,
			})

			// Test append
			req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			if tt.seq != "" {
				req.Header.Set(protocol.HeaderStreamSeq, tt.seq)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d. Body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			if tt.wantStatus == http.StatusNoContent {
				nextOffset := rec.Header().Get(protocol.HeaderStreamNextOffset)
				if nextOffset == "" {
					t.Error("missing Stream-Next-Offset header")
				}
			}

			if tt.wantErrCode != "" && rec.Code >= 400 {
				body := rec.Body.String()
				if !strings.Contains(body, tt.wantErrCode) {
					t.Errorf("error body %q does not contain expected code %q", body, tt.wantErrCode)
				}
			}
		})
	}
}

func TestHandler_POST_SequenceValidation(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Append with seq_001
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data1"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderStreamSeq, "seq_001")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusNoContent {
		t.Fatalf("first append status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	// Append with seq_002 (should succeed)
	req2 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data2"))
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set(protocol.HeaderStreamSeq, "seq_002")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNoContent {
		t.Errorf("second append status = %d, want %d", rec2.Code, http.StatusNoContent)
	}

	// Append with seq_001 (should fail - regression)
	req3 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data3"))
	req3.Header.Set("Content-Type", "text/plain")
	req3.Header.Set(protocol.HeaderStreamSeq, "seq_001")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusConflict {
		t.Errorf("regressed append status = %d, want %d", rec3.Code, http.StatusConflict)
	}
}

func TestHandler_GET_CatchupRead(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream and append data
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	storage.Append(context.Background(), "/stream", []byte("line1\n"), "")
	storage.Append(context.Background(), "/stream", []byte("line2\n"), "")

	tests := []struct {
		name         string
		offset       string
		wantStatus   int
		wantBody     string
		wantUpToDate bool
	}{
		{
			name:         "read from start",
			offset:       "",
			wantStatus:   http.StatusOK,
			wantUpToDate: true,
		},
		{
			name:       "read from offset",
			offset:     "0000000000",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "/stream"
			if tt.offset != "" {
				url += "?offset=" + tt.offset
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			if tt.wantUpToDate {
				upToDate := rec.Header().Get(protocol.HeaderStreamUpToDate)
				if upToDate != "true" {
					t.Errorf("Stream-Up-To-Date = %q, want %q", upToDate, "true")
				}
			}

			// Check required headers
			if rec.Code == http.StatusOK {
				if rec.Header().Get(protocol.HeaderStreamNextOffset) == "" {
					t.Error("missing Stream-Next-Offset header")
				}
				if rec.Header().Get("Content-Type") == "" {
					t.Error("missing Content-Type header")
				}
			}
		})
	}
}

func TestHandler_GET_JSONMode(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create JSON stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "application/json",
	})

	// Append JSON messages
	storage.Append(context.Background(), "/stream", []byte(`{"event":"a"}`), "")
	storage.Append(context.Background(), "/stream", []byte(`{"event":"b"}`), "")

	req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Response should be JSON array
	body := rec.Body.String()
	if !strings.HasPrefix(body, "[") || !strings.HasSuffix(body, "]") {
		t.Errorf("JSON response not wrapped in array: %s", body)
	}

	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
	}
}

func TestHandler_GET_LongPoll(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
		LongPollTimeout: 100 * time.Millisecond,
	})

	// Create stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	offset, _ := storage.Append(context.Background(), "/stream", []byte("data1"), "")

	t.Run("immediate return with data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000&live=long-poll", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("timeout with no data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset="+offset.String()+"&live=long-poll", nil)
		rec := httptest.NewRecorder()

		start := time.Now()
		handler.ServeHTTP(rec, req)
		duration := time.Since(start)

		if rec.Code != http.StatusNoContent {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}

		// Should wait approximately the timeout duration
		if duration < 50*time.Millisecond {
			t.Errorf("returned too quickly: %v", duration)
		}
	})

	t.Run("return when data arrives", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset="+offset.String()+"&live=long-poll", nil)
		rec := httptest.NewRecorder()

		// Append data after a short delay
		go func() {
			time.Sleep(20 * time.Millisecond)
			storage.Append(context.Background(), "/stream", []byte("data2"), "")
		}()

		start := time.Now()
		handler.ServeHTTP(rec, req)
		duration := time.Since(start)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		// Should return quickly after data arrives
		if duration > 100*time.Millisecond {
			t.Errorf("returned too slowly: %v", duration)
		}
	})

	t.Run("require offset", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?live=long-poll", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

func TestHandler_GET_SSE(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
		SSECloseAfter: 200 * time.Millisecond,
	})

	t.Run("text content type", func(t *testing.T) {
		// Create text stream
		storage.Create(context.Background(), "/text-stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		storage.Append(context.Background(), "/text-stream", []byte("line1\n"), "")

		req := httptest.NewRequest(http.MethodGet, "/text-stream?offset=0000000000&live=sse", nil)
		rec := httptest.NewRecorder()

		// Use a timeout to prevent test hanging
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctx)

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		if rec.Header().Get("Content-Type") != "text/event-stream" {
			t.Errorf("Content-Type = %q, want text/event-stream", rec.Header().Get("Content-Type"))
		}

		body := rec.Body.String()
		if !strings.Contains(body, "event: data") {
			t.Errorf("SSE response missing data event: %s", body)
		}
		if !strings.Contains(body, "event: control") {
			t.Errorf("SSE response missing control event: %s", body)
		}
	})

	t.Run("JSON content type", func(t *testing.T) {
		// Create JSON stream
		storage.Create(context.Background(), "/json-stream", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		storage.Append(context.Background(), "/json-stream", []byte(`{"event":"test"}`), "")

		req := httptest.NewRequest(http.MethodGet, "/json-stream?offset=0000000000&live=sse", nil)
		rec := httptest.NewRecorder()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctx)

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("reject incompatible content type", func(t *testing.T) {
		// Create binary stream
		storage.Create(context.Background(), "/binary-stream", durablestream.StreamConfig{
			ContentType: "application/octet-stream",
		})

		req := httptest.NewRequest(http.MethodGet, "/binary-stream?offset=0000000000&live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("require offset", func(t *testing.T) {
		storage.Create(context.Background(), "/stream2", durablestream.StreamConfig{
			ContentType: "text/plain",
		})

		req := httptest.NewRequest(http.MethodGet, "/stream2?live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

func TestHandler_HEAD_Metadata(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream with metadata
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "application/json",
		TTL:         3600 * time.Second,
		ExpiresAt:   time.Now().Add(1 * time.Hour),
	})
	storage.Append(context.Background(), "/stream", []byte(`{"test":1}`), "")

	req := httptest.NewRequest(http.MethodHead, "/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Check headers
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
	}

	if rec.Header().Get(protocol.HeaderStreamNextOffset) == "" {
		t.Error("missing Stream-Next-Offset header")
	}

	if rec.Header().Get(protocol.HeaderStreamTTL) == "" {
		t.Error("missing Stream-TTL header")
	}

	if rec.Header().Get(protocol.HeaderStreamExpiresAt) == "" {
		t.Error("missing Stream-Expires-At header")
	}

	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
	}

	// Body should be empty for HEAD
	if rec.Body.Len() > 0 {
		t.Errorf("HEAD response has body: %s", rec.Body.String())
	}
}

func TestHandler_DELETE(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Delete stream
	req := httptest.NewRequest(http.MethodDelete, "/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	// Verify stream is deleted (HEAD should return 404)
	req2 := httptest.NewRequest(http.MethodHead, "/stream", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNotFound {
		t.Errorf("after delete status = %d, want %d", rec2.Code, http.StatusNotFound)
	}

	// Delete non-existent stream
	req3 := httptest.NewRequest(http.MethodDelete, "/nonexistent", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusNotFound {
		t.Errorf("delete nonexistent status = %d, want %d", rec3.Code, http.StatusNotFound)
	}
}

func TestHandler_MaxAppendSize(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
		MaxAppendSize: 100, // 100 bytes max
	})

	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Append data larger than limit
	largeData := strings.Repeat("x", 200)
	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(largeData))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "payload_too_large") {
		t.Errorf("error body %q does not contain expected code", body)
	}
}

func TestHandler_PathExtractor(t *testing.T) {
	storage := memorystorage.New()

	// Custom path extractor that strips /api/v1 prefix
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
		PathExtractor: func(r *http.Request) string {
			return strings.TrimPrefix(r.URL.Path, "/api/v1")
		},
	})

	// Create stream at /api/v1/stream (which extracts to /stream)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/stream", nil)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", rec.Code, http.StatusCreated)
	}

	// Verify we can read from the extracted path
	req2 := httptest.NewRequest(http.MethodHead, "/api/v1/stream", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("head status = %d, want %d", rec2.Code, http.StatusOK)
	}
}

func TestHandler_UnsupportedMethod(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	req := httptest.NewRequest(http.MethodPatch, "/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandler_ErrorFormat(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Try to read non-existent stream
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	// Check JSON error format
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("error Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"code"`) || !strings.Contains(body, `"message"`) {
		t.Errorf("error body missing expected fields: %s", body)
	}

	if !strings.Contains(body, "not_found") {
		t.Errorf("error body missing expected code: %s", body)
	}
}

func TestHandler_GET_InvalidParameters(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{ContentType: "text/plain"})

	t.Run("duplicate offset parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=123&offset=456", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "duplicate offset") {
			t.Errorf("body = %q, want 'duplicate offset'", rec.Body.String())
		}
	})

	t.Run("duplicate live parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=123&live=long-poll&live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "duplicate live") {
			t.Errorf("body = %q, want 'duplicate live'", rec.Body.String())
		}
	})

	t.Run("empty offset parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "offset cannot be empty") {
			t.Errorf("body = %q, want 'offset cannot be empty'", rec.Body.String())
		}
	})

	t.Run("invalid offset format with comma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=123,456", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "invalid offset format") {
			t.Errorf("body = %q, want 'invalid offset format'", rec.Body.String())
		}
	})

	t.Run("invalid offset format with space", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=123%20456", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "invalid offset format") {
			t.Errorf("body = %q, want 'invalid offset format'", rec.Body.String())
		}
	})

	t.Run("invalid live parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=123&live=invalid", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "invalid live") {
			t.Errorf("body = %q, want 'invalid live'", rec.Body.String())
		}
	})
}

func TestHandler_GET_NonexistentStream(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	t.Run("catch-up read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("long-poll read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nonexistent?offset=123&live=long-poll", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("SSE read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nonexistent?offset=123&live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestHandler_LongPoll_ContextCancellation(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
		LongPollTimeout: 500 * time.Millisecond,
	})

	// Create stream
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{ContentType: "text/plain"})
	offset, _ := storage.Append(context.Background(), "/stream", []byte("data"), "")

	// Use a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/stream?offset="+offset.String()+"&live=long-poll", nil)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Should return 204 when context times out before server timeout
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestHandler_CacheControlHeaders(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream with data
	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	storage.Append(context.Background(), "/stream", []byte("data"), "")

	t.Run("catch-up read has cache control", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		cacheControl := rec.Header().Get("Cache-Control")
		if !strings.Contains(cacheControl, "max-age") {
			t.Errorf("Cache-Control = %q, should contain max-age", cacheControl)
		}
	})

	t.Run("head has no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/stream", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
	})
}

// Benchmark tests
func BenchmarkHandler_CreateStream(b *testing.B) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		streamID := fmt.Sprintf("/stream-%d", i)
		req := httptest.NewRequest(http.MethodPut, streamID, nil)
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkHandler_Append(b *testing.B) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	data := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/stream", bytes.NewReader(data))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkHandler_Read(b *testing.B) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		storage.Append(context.Background(), "/stream", []byte("data"), "")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// Consume body to avoid measurement skew
		io.Copy(io.Discard, rec.Body)
	}
}
