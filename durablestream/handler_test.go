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
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/memorystorage"
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
				"Stream-Next-Offset": "0000000000000000_0000000000000000",
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
				"Stream-Expires-At": "2027-01-15T12:00:00Z",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:        "reject both TTL and Expires-At",
			contentType: "text/plain",
			headers: map[string]string{
				"Stream-TTL":        "3600",
				"Stream-Expires-At": "2027-01-15T12:00:00Z",
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

			_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	_, _ = storage.Append(context.Background(), "/stream", []byte("line1\n"), "")
	_, _ = storage.Append(context.Background(), "/stream", []byte("line2\n"), "")

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
			offset:     "0000000000000000_0000000000000000",
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "application/json",
	})

	// Append JSON messages
	_, _ = storage.Append(context.Background(), "/stream", []byte(`{"event":"a"}`), "")
	_, _ = storage.Append(context.Background(), "/stream", []byte(`{"event":"b"}`), "")

	req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	offset, _ := storage.Append(context.Background(), "/stream", []byte("data1"), "")

	t.Run("immediate return with data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000&live=long-poll", nil)
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
			_, _ = storage.Append(context.Background(), "/stream", []byte("data2"), "")
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
		_, _ = storage.Create(context.Background(), "/text-stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		_, _ = storage.Append(context.Background(), "/text-stream", []byte("line1\n"), "")

		req := httptest.NewRequest(http.MethodGet, "/text-stream?offset=0000000000000000_0000000000000000&live=sse", nil)
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
		_, _ = storage.Create(context.Background(), "/json-stream", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		_, _ = storage.Append(context.Background(), "/json-stream", []byte(`{"event":"test"}`), "")

		req := httptest.NewRequest(http.MethodGet, "/json-stream?offset=0000000000000000_0000000000000000&live=sse", nil)
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
		_, _ = storage.Create(context.Background(), "/binary-stream", durablestream.StreamConfig{
			ContentType: "application/octet-stream",
		})

		req := httptest.NewRequest(http.MethodGet, "/binary-stream?offset=0000000000000000_0000000000000000&live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("require offset", func(t *testing.T) {
		_, _ = storage.Create(context.Background(), "/stream2", durablestream.StreamConfig{
			ContentType: "text/plain",
		})

		req := httptest.NewRequest(http.MethodGet, "/stream2?live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("CRLF injection prevention with CR-only terminators", func(t *testing.T) {
		// This tests the critical security fix: CR characters must be treated as
		// line terminators to prevent SSE event injection attacks.
		// Per SSE spec, CR is a valid line terminator, so embedded CRs in data
		// must be split into separate data: lines to prevent injection.

		_, _ = storage.Create(context.Background(), "/cr-injection-test", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		// Payload with CR-only terminators attempting to inject a fake control event
		// Without proper handling, this would create multiple SSE events
		maliciousPayload := "start\r\revent: control\rdata: {\"cr_injected\":true}\r\rend"
		_, _ = storage.Append(context.Background(), "/cr-injection-test", []byte(maliciousPayload), "")

		req := httptest.NewRequest(http.MethodGet, "/cr-injection-test?offset=0000000000000000_0000000000000000&live=sse", nil)
		rec := httptest.NewRecorder()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctx)

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		body := rec.Body.String()

		// Count actual SSE event types (lines that START with "event: control")
		// vs escaped data (lines that start with "data: event: control").
		// The injected one should appear as a data line, not an SSE command.
		actualControlEvents := 0
		escapedControlStrings := 0
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "event: control") {
				actualControlEvents++
			}
			if strings.HasPrefix(line, "data: event: control") {
				escapedControlStrings++
			}
		}

		// We expect exactly 1 actual SSE control event (the real one from server)
		if actualControlEvents != 1 {
			t.Errorf("Expected exactly 1 actual SSE control event, got %d. Body:\n%s",
				actualControlEvents, body)
		}

		// We expect the injected "event: control" to appear as escaped data
		if escapedControlStrings != 1 {
			t.Errorf("Expected injected 'event: control' to appear as escaped data line, found %d. Body:\n%s",
				escapedControlStrings, body)
		}

		// Verify the injected data payload also appears as a data line
		if !strings.Contains(body, "data: data: {\"cr_injected\":true}") {
			t.Errorf("Expected injected data to be escaped as data line. Body:\n%s", body)
		}
	})
}

func TestHandler_HEAD_Metadata(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream with metadata
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "application/json",
		TTL:         3600 * time.Second,
		ExpiresAt:   time.Now().Add(1 * time.Hour),
	})
	_, _ = storage.Append(context.Background(), "/stream", []byte(`{"test":1}`), "")

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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
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

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{ContentType: "text/plain"})

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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{ContentType: "text/plain"})
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
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	_, _ = storage.Append(context.Background(), "/stream", []byte("data"), "")

	t.Run("catch-up read has cache control", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
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

	t.Run("public stream has public cache control", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		cacheControl := rec.Header().Get("Cache-Control")
		if !strings.Contains(cacheControl, "public") {
			t.Errorf("Cache-Control = %q, should contain 'public' for non-private stream", cacheControl)
		}
	})
}

func TestHandler_PrivateCacheControl(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create private stream via PUT with Stream-Private header
	req := httptest.NewRequest(http.MethodPut, "/private-stream", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Stream-Private", "true")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("PUT status = %d, want %d", rec.Code, http.StatusCreated)
	}

	// Append some data
	_, _ = storage.Append(context.Background(), "/private-stream", []byte("secret data"), "")

	t.Run("private stream has private cache control", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/private-stream?offset=0000000000000000_0000000000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
		}

		cacheControl := rec.Header().Get("Cache-Control")
		if !strings.Contains(cacheControl, "private") {
			t.Errorf("Cache-Control = %q, should contain 'private' for private stream", cacheControl)
		}
		if strings.Contains(cacheControl, "public") {
			t.Errorf("Cache-Control = %q, should not contain 'public' for private stream", cacheControl)
		}
	})

	t.Run("invalid Stream-Private header rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/bad-private", nil)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Stream-Private", "invalid")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("PUT with invalid Stream-Private status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("Stream-Private false is valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/explicit-public", nil)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Stream-Private", "false")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("PUT with Stream-Private: false status = %d, want %d", rec.Code, http.StatusCreated)
		}
	})
}

func TestHandler_GET_ETagAndIfNoneMatch(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream with data
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})
	_, _ = storage.Append(context.Background(), "/stream", []byte("data"), "")

	t.Run("catch-up read returns ETag header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		etag := rec.Header().Get("ETag")
		if etag == "" {
			t.Error("missing ETag header")
		}
		// ETag should be quoted per RFC 7232
		if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
			t.Errorf("ETag = %q, should be quoted", etag)
		}
	})

	t.Run("304 Not Modified when If-None-Match matches ETag", func(t *testing.T) {
		// First request to get the ETag
		req1 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		if rec1.Code != http.StatusOK {
			t.Fatalf("first request status = %d, want %d", rec1.Code, http.StatusOK)
		}
		etag := rec1.Header().Get("ETag")
		if etag == "" {
			t.Fatal("first request missing ETag header")
		}

		// Second request with If-None-Match should get 304
		req2 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		req2.Header.Set("If-None-Match", etag)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusNotModified {
			t.Errorf("status = %d, want %d", rec2.Code, http.StatusNotModified)
		}

		// 304 response should have ETag and Cache-Control but no body
		if rec2.Header().Get("ETag") != etag {
			t.Errorf("304 response ETag = %q, want %q", rec2.Header().Get("ETag"), etag)
		}
		if rec2.Header().Get("Cache-Control") == "" {
			t.Error("304 response missing Cache-Control header")
		}
		if rec2.Body.Len() > 0 {
			t.Errorf("304 response should have no body, got %s", rec2.Body.String())
		}
	})

	t.Run("200 OK when If-None-Match does not match ETag", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		req.Header.Set("If-None-Match", `"wrong-etag"`)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		// Should have body
		if rec.Body.Len() == 0 {
			t.Error("expected response body")
		}
	})

	t.Run("304 with If-None-Match containing multiple ETags", func(t *testing.T) {
		// First request to get the ETag
		req1 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		etag := rec1.Header().Get("ETag")

		// Request with multiple ETags (one matching)
		req2 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		req2.Header.Set("If-None-Match", `"other-etag", `+etag+`, "another-etag"`)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusNotModified {
			t.Errorf("status = %d, want %d", rec2.Code, http.StatusNotModified)
		}
	})

	t.Run("304 with If-None-Match wildcard", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		req.Header.Set("If-None-Match", "*")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotModified {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotModified)
		}
	})

	t.Run("304 with weak ETag (W/ prefix)", func(t *testing.T) {
		// First request to get the ETag
		req1 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		etag := rec1.Header().Get("ETag")

		// Request with weak ETag prefix
		req2 := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		req2.Header.Set("If-None-Match", "W/"+etag)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusNotModified {
			t.Errorf("status = %d, want %d", rec2.Code, http.StatusNotModified)
		}
	})

	t.Run("ETag changes when data changes", func(t *testing.T) {
		// Get ETag at current offset
		info, _ := storage.Head(context.Background(), "/stream")
		req1 := httptest.NewRequest(http.MethodGet, "/stream?offset="+info.NextOffset.String(), nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		etag1 := rec1.Header().Get("ETag")

		// Append more data
		_, _ = storage.Append(context.Background(), "/stream", []byte("more data"), "")

		// Read from same offset - ETag should be different now
		info2, _ := storage.Head(context.Background(), "/stream")
		req2 := httptest.NewRequest(http.MethodGet, "/stream?offset="+info.NextOffset.String(), nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		etag2 := rec2.Header().Get("ETag")

		// New offset means new ETag
		if info2.NextOffset == info.NextOffset {
			t.Skip("offset unchanged, skipping ETag comparison")
		}

		if etag1 == etag2 {
			t.Errorf("ETag should change when data changes, got %q both times", etag1)
		}
	})

	t.Run("no 304 for offset=now", func(t *testing.T) {
		// offset=now should never return 304 (per spec: no ETag for offset=now responses)
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=now", nil)
		req.Header.Set("If-None-Match", "*")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// offset=now should always return 200
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("ETag is valid with stream ID containing control characters", func(t *testing.T) {
		storage := memorystorage.New()

		// Create stream with control character in path (null byte)
		// Storage allows arbitrary stream IDs even with control characters
		streamPath := "/stream\x00test"
		_, _ = storage.Create(context.Background(), streamPath, durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		_, _ = storage.Append(context.Background(), streamPath, []byte("data"), "")

		// Create request with valid URL, then manually set path to include control char
		// This simulates what happens when a client sends a URL-encoded null byte
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		// Override the path extraction to return the stream path with control chars
		customHandler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
			PathExtractor: func(*http.Request) string { return streamPath },
		})
		rec := httptest.NewRecorder()
		customHandler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		// ETag should be present and properly encoded (no raw control chars)
		etag := rec.Header().Get("ETag")
		if etag == "" {
			t.Error("missing ETag header")
		}
		// ETag should not contain raw null bytes
		if strings.Contains(etag, "\x00") {
			t.Errorf("ETag contains raw null byte: %q", etag)
		}
		// ETag should contain the encoded null byte (%00)
		if !strings.Contains(etag, "%00") {
			t.Errorf("ETag should contain encoded null byte (%%00), got: %q", etag)
		}
	})
}

func TestHandler_GET_OffsetNow(t *testing.T) {
	t.Run("catch-up read JSON stream", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		// Create JSON stream with some data
		_, _ = storage.Create(context.Background(), "/json-stream", durablestream.StreamConfig{
			ContentType: "application/json",
		})
		tailOffset, _ := storage.Append(context.Background(), "/json-stream", []byte(`{"event":"test"}`), "")

		// Read with offset=now
		req := httptest.NewRequest(http.MethodGet, "/json-stream?offset=now", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		// Should return empty JSON array
		if body := rec.Body.String(); body != "[]" {
			t.Errorf("body = %q, want []", body)
		}

		// Should have tail offset
		nextOffset := rec.Header().Get(protocol.HeaderStreamNextOffset)
		if nextOffset != tailOffset.String() {
			t.Errorf("Stream-Next-Offset = %q, want %q", nextOffset, tailOffset.String())
		}

		// Should be up-to-date
		if rec.Header().Get(protocol.HeaderStreamUpToDate) != "true" {
			t.Errorf("Stream-Up-To-Date = %q, want true", rec.Header().Get(protocol.HeaderStreamUpToDate))
		}

		// Should have no-store cache control
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
	})

	t.Run("catch-up read non-JSON stream", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		// Create text stream with some data
		_, _ = storage.Create(context.Background(), "/text-stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		tailOffset, _ := storage.Append(context.Background(), "/text-stream", []byte("hello world"), "")

		// Read with offset=now
		req := httptest.NewRequest(http.MethodGet, "/text-stream?offset=now", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		// Should return empty body
		if body := rec.Body.String(); body != "" {
			t.Errorf("body = %q, want empty", body)
		}

		// Should have tail offset
		if rec.Header().Get(protocol.HeaderStreamNextOffset) != tailOffset.String() {
			t.Errorf("Stream-Next-Offset = %q, want %q", rec.Header().Get(protocol.HeaderStreamNextOffset), tailOffset.String())
		}
	})

	t.Run("catch-up read empty stream", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		// Create empty stream
		_, _ = storage.Create(context.Background(), "/empty-stream", durablestream.StreamConfig{
			ContentType: "application/json",
		})

		// Read with offset=now
		req := httptest.NewRequest(http.MethodGet, "/empty-stream?offset=now", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		// Should return empty JSON array
		if body := rec.Body.String(); body != "[]" {
			t.Errorf("body = %q, want []", body)
		}
	})

	t.Run("long-poll waits with offset=now", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
			LongPollTimeout: 100 * time.Millisecond,
		})

		// Create stream with some existing data
		_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		_, _ = storage.Append(context.Background(), "/stream", []byte("existing"), "")

		// Long-poll with offset=now should wait (not return existing data)
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=now&live=long-poll", nil)
		rec := httptest.NewRecorder()

		start := time.Now()
		handler.ServeHTTP(rec, req)
		duration := time.Since(start)

		// Should timeout with 204 (not return immediately with existing data)
		if rec.Code != http.StatusNoContent {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}

		// Should have waited for timeout
		if duration < 50*time.Millisecond {
			t.Errorf("returned too quickly: %v, expected to wait", duration)
		}
	})

	t.Run("long-poll returns new data with offset=now", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
			LongPollTimeout: 500 * time.Millisecond,
		})

		// Create stream
		_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})

		// Append new data after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			_, _ = storage.Append(context.Background(), "/stream", []byte("new data"), "")
		}()

		// Long-poll with offset=now
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=now&live=long-poll", nil)
		rec := httptest.NewRecorder()

		start := time.Now()
		handler.ServeHTTP(rec, req)
		duration := time.Since(start)

		// Should return 200 with data
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		// Should return quickly after data arrives
		if duration > 200*time.Millisecond {
			t.Errorf("returned too slowly: %v", duration)
		}

		// Should contain new data
		if !strings.Contains(rec.Body.String(), "new data") {
			t.Errorf("body = %q, want to contain 'new data'", rec.Body.String())
		}
	})

	t.Run("SSE sends initial control event with offset=now", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{
			SSECloseAfter: 100 * time.Millisecond,
		})

		// Create stream
		_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		})
		tailOffset, _ := storage.Append(context.Background(), "/stream", []byte("existing"), "")

		// SSE with offset=now
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=now&live=sse", nil)
		rec := httptest.NewRecorder()

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctx)

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		body := rec.Body.String()

		// Should contain initial control event with upToDate
		if !strings.Contains(body, "event: control") {
			t.Errorf("SSE response missing control event: %s", body)
		}
		if !strings.Contains(body, "upToDate") {
			t.Errorf("SSE response missing upToDate in control event: %s", body)
		}
		if !strings.Contains(body, tailOffset.String()) {
			t.Errorf("SSE response missing tail offset %q: %s", tailOffset.String(), body)
		}

		// Should NOT contain data event (we skipped existing data)
		if strings.Contains(body, "existing") {
			t.Errorf("SSE response should not contain existing data: %s", body)
		}
	})

	t.Run("offset=now on non-existent stream returns 404", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		// Try catch-up read
		req := httptest.NewRequest(http.MethodGet, "/nonexistent?offset=now", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("catch-up status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("offset=now with long-poll on non-existent stream returns 404", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		req := httptest.NewRequest(http.MethodGet, "/nonexistent?offset=now&live=long-poll", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("long-poll status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("offset=now with SSE on non-existent stream returns 404", func(t *testing.T) {
		storage := memorystorage.New()
		handler := durablestream.NewHandler(storage, nil)

		req := httptest.NewRequest(http.MethodGet, "/nonexistent?offset=now&live=sse", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("SSE status = %d, want %d", rec.Code, http.StatusNotFound)
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

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
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

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		_, _ = storage.Append(context.Background(), "/stream", []byte("data"), "")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/stream?offset=0000000000000000_0000000000000000", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// Consume body to avoid measurement skew
		_, _ = io.Copy(io.Discard, rec.Body)
	}
}

// ============================================================================
// Idempotent Producer Tests (Section 5.2.1)
// ============================================================================

func TestHandler_POST_IdempotentProducer_Basic(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	// Create stream
	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	t.Run("first append with producer headers returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("hello"))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set(protocol.HeaderProducerID, "test-producer")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		// Check response headers
		if rec.Header().Get(protocol.HeaderStreamNextOffset) == "" {
			t.Error("missing Stream-Next-Offset header")
		}
		if rec.Header().Get(protocol.HeaderProducerEpoch) != "0" {
			t.Errorf("Producer-Epoch = %q, want 0", rec.Header().Get(protocol.HeaderProducerEpoch))
		}
		if rec.Header().Get(protocol.HeaderProducerSeq) != "0" {
			t.Errorf("Producer-Seq = %q, want 0", rec.Header().Get(protocol.HeaderProducerSeq))
		}
	})
}

func TestHandler_POST_IdempotentProducer_Sequential(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Send seq=0
	req0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg0"))
	req0.Header.Set("Content-Type", "text/plain")
	req0.Header.Set(protocol.HeaderProducerID, "test-producer")
	req0.Header.Set(protocol.HeaderProducerEpoch, "0")
	req0.Header.Set(protocol.HeaderProducerSeq, "0")
	rec0 := httptest.NewRecorder()
	handler.ServeHTTP(rec0, req0)

	if rec0.Code != http.StatusOK {
		t.Fatalf("seq=0 status = %d, want %d", rec0.Code, http.StatusOK)
	}

	// Send seq=1
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg1"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderProducerID, "test-producer")
	req1.Header.Set(protocol.HeaderProducerEpoch, "0")
	req1.Header.Set(protocol.HeaderProducerSeq, "1")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("seq=1 status = %d, want %d", rec1.Code, http.StatusOK)
	}

	// Send seq=2
	req2 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg2"))
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set(protocol.HeaderProducerID, "test-producer")
	req2.Header.Set(protocol.HeaderProducerEpoch, "0")
	req2.Header.Set(protocol.HeaderProducerSeq, "2")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("seq=2 status = %d, want %d", rec2.Code, http.StatusOK)
	}
}

func TestHandler_POST_IdempotentProducer_Duplicate(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// First append
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("hello"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderProducerID, "test-producer")
	req1.Header.Set(protocol.HeaderProducerEpoch, "0")
	req1.Header.Set(protocol.HeaderProducerSeq, "0")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("first append status = %d, want %d", rec1.Code, http.StatusOK)
	}

	// Duplicate append (same seq=0) - should return 204
	req2 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("hello"))
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set(protocol.HeaderProducerID, "test-producer")
	req2.Header.Set(protocol.HeaderProducerEpoch, "0")
	req2.Header.Set(protocol.HeaderProducerSeq, "0")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNoContent {
		t.Errorf("duplicate append status = %d, want %d. Body: %s", rec2.Code, http.StatusNoContent, rec2.Body.String())
	}

	// Check response headers are still present
	if rec2.Header().Get(protocol.HeaderProducerEpoch) != "0" {
		t.Errorf("Producer-Epoch = %q, want 0", rec2.Header().Get(protocol.HeaderProducerEpoch))
	}
	if rec2.Header().Get(protocol.HeaderProducerSeq) != "0" {
		t.Errorf("Producer-Seq = %q, want 0", rec2.Header().Get(protocol.HeaderProducerSeq))
	}
}

func TestHandler_POST_IdempotentProducer_EpochUpgrade(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Establish epoch=0
	req0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("epoch0"))
	req0.Header.Set("Content-Type", "text/plain")
	req0.Header.Set(protocol.HeaderProducerID, "test-producer")
	req0.Header.Set(protocol.HeaderProducerEpoch, "0")
	req0.Header.Set(protocol.HeaderProducerSeq, "0")
	rec0 := httptest.NewRecorder()
	handler.ServeHTTP(rec0, req0)

	if rec0.Code != http.StatusOK {
		t.Fatalf("epoch=0 status = %d, want %d", rec0.Code, http.StatusOK)
	}

	// Upgrade to epoch=1, seq=0
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("epoch1"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderProducerID, "test-producer")
	req1.Header.Set(protocol.HeaderProducerEpoch, "1")
	req1.Header.Set(protocol.HeaderProducerSeq, "0")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("epoch=1 status = %d, want %d. Body: %s", rec1.Code, http.StatusOK, rec1.Body.String())
	}
	if rec1.Header().Get(protocol.HeaderProducerEpoch) != "1" {
		t.Errorf("Producer-Epoch = %q, want 1", rec1.Header().Get(protocol.HeaderProducerEpoch))
	}
}

func TestHandler_POST_IdempotentProducer_StaleEpoch(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Establish epoch=1
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderProducerID, "test-producer")
	req1.Header.Set(protocol.HeaderProducerEpoch, "1")
	req1.Header.Set(protocol.HeaderProducerSeq, "0")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("epoch=1 status = %d, want %d", rec1.Code, http.StatusOK)
	}

	// Try to write with epoch=0 (stale) - should get 403
	req0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("zombie"))
	req0.Header.Set("Content-Type", "text/plain")
	req0.Header.Set(protocol.HeaderProducerID, "test-producer")
	req0.Header.Set(protocol.HeaderProducerEpoch, "0")
	req0.Header.Set(protocol.HeaderProducerSeq, "0")
	rec0 := httptest.NewRecorder()
	handler.ServeHTTP(rec0, req0)

	if rec0.Code != http.StatusForbidden {
		t.Errorf("stale epoch status = %d, want %d. Body: %s", rec0.Code, http.StatusForbidden, rec0.Body.String())
	}

	// Response should include current epoch
	if rec0.Header().Get(protocol.HeaderProducerEpoch) != "1" {
		t.Errorf("Producer-Epoch = %q, want 1 (current epoch)", rec0.Header().Get(protocol.HeaderProducerEpoch))
	}
}

func TestHandler_POST_IdempotentProducer_SequenceGap(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Send seq=0
	req0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg0"))
	req0.Header.Set("Content-Type", "text/plain")
	req0.Header.Set(protocol.HeaderProducerID, "test-producer")
	req0.Header.Set(protocol.HeaderProducerEpoch, "0")
	req0.Header.Set(protocol.HeaderProducerSeq, "0")
	rec0 := httptest.NewRecorder()
	handler.ServeHTTP(rec0, req0)

	if rec0.Code != http.StatusOK {
		t.Fatalf("seq=0 status = %d, want %d", rec0.Code, http.StatusOK)
	}

	// Skip seq=1, send seq=2 (should fail with 409 Conflict)
	req2 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg2"))
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set(protocol.HeaderProducerID, "test-producer")
	req2.Header.Set(protocol.HeaderProducerEpoch, "0")
	req2.Header.Set(protocol.HeaderProducerSeq, "2")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Errorf("sequence gap status = %d, want %d. Body: %s", rec2.Code, http.StatusConflict, rec2.Body.String())
	}

	// Check gap error headers
	if rec2.Header().Get(protocol.HeaderProducerExpectedSeq) != "1" {
		t.Errorf("Producer-Expected-Seq = %q, want 1", rec2.Header().Get(protocol.HeaderProducerExpectedSeq))
	}
	if rec2.Header().Get(protocol.HeaderProducerReceivedSeq) != "2" {
		t.Errorf("Producer-Received-Seq = %q, want 2", rec2.Header().Get(protocol.HeaderProducerReceivedSeq))
	}
}

func TestHandler_POST_IdempotentProducer_NewEpochMustStartAtZero(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Establish epoch=0
	req0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg"))
	req0.Header.Set("Content-Type", "text/plain")
	req0.Header.Set(protocol.HeaderProducerID, "test-producer")
	req0.Header.Set(protocol.HeaderProducerEpoch, "0")
	req0.Header.Set(protocol.HeaderProducerSeq, "0")
	rec0 := httptest.NewRecorder()
	handler.ServeHTTP(rec0, req0)

	if rec0.Code != http.StatusOK {
		t.Fatalf("epoch=0 status = %d, want %d", rec0.Code, http.StatusOK)
	}

	// Try epoch=1 with seq=5 (should fail - new epoch must start at seq=0)
	req1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("msg"))
	req1.Header.Set("Content-Type", "text/plain")
	req1.Header.Set(protocol.HeaderProducerID, "test-producer")
	req1.Header.Set(protocol.HeaderProducerEpoch, "1")
	req1.Header.Set(protocol.HeaderProducerSeq, "5")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusBadRequest {
		t.Errorf("new epoch with seq!=0 status = %d, want %d. Body: %s", rec1.Code, http.StatusBadRequest, rec1.Body.String())
	}
}

func TestHandler_POST_IdempotentProducer_PartialHeaders(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	tests := []struct {
		name  string
		id    string
		epoch string
		seq   string
	}{
		{"only Producer-Id", "test", "", ""},
		{"only Producer-Epoch", "", "0", ""},
		{"only Producer-Seq", "", "", "0"},
		{"missing Producer-Seq", "test", "0", ""},
		{"missing Producer-Epoch", "test", "", "0"},
		{"missing Producer-Id", "", "0", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
			req.Header.Set("Content-Type", "text/plain")
			if tt.id != "" {
				req.Header.Set(protocol.HeaderProducerID, tt.id)
			}
			if tt.epoch != "" {
				req.Header.Set(protocol.HeaderProducerEpoch, tt.epoch)
			}
			if tt.seq != "" {
				req.Header.Set(protocol.HeaderProducerSeq, tt.seq)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestHandler_POST_IdempotentProducer_InvalidFormats(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	tests := []struct {
		name  string
		epoch string
		seq   string
	}{
		{"non-integer epoch", "abc", "0"},
		{"non-integer seq", "0", "xyz"},
		{"leading zero epoch", "01", "0"},
		{"leading zero seq", "0", "01"},
		{"plus sign epoch", "+1", "0"},
		{"plus sign seq", "0", "+1"},
		{"negative epoch", "-1", "0"},
		{"negative seq", "0", "-1"},
		{"floating point epoch", "1.5", "0"},
		{"floating point seq", "0", "1.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set(protocol.HeaderProducerID, "test-producer")
			req.Header.Set(protocol.HeaderProducerEpoch, tt.epoch)
			req.Header.Set(protocol.HeaderProducerSeq, tt.seq)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestHandler_POST_IdempotentProducer_MultipleProducers(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Producer A: seq=0
	reqA0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("A-msg0"))
	reqA0.Header.Set("Content-Type", "text/plain")
	reqA0.Header.Set(protocol.HeaderProducerID, "producer-A")
	reqA0.Header.Set(protocol.HeaderProducerEpoch, "0")
	reqA0.Header.Set(protocol.HeaderProducerSeq, "0")
	recA0 := httptest.NewRecorder()
	handler.ServeHTTP(recA0, reqA0)

	if recA0.Code != http.StatusOK {
		t.Fatalf("producer-A seq=0 status = %d, want %d", recA0.Code, http.StatusOK)
	}

	// Producer B: seq=0 (should be independent)
	reqB0 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("B-msg0"))
	reqB0.Header.Set("Content-Type", "text/plain")
	reqB0.Header.Set(protocol.HeaderProducerID, "producer-B")
	reqB0.Header.Set(protocol.HeaderProducerEpoch, "0")
	reqB0.Header.Set(protocol.HeaderProducerSeq, "0")
	recB0 := httptest.NewRecorder()
	handler.ServeHTTP(recB0, reqB0)

	if recB0.Code != http.StatusOK {
		t.Errorf("producer-B seq=0 status = %d, want %d. Body: %s", recB0.Code, http.StatusOK, recB0.Body.String())
	}

	// Producer A: seq=1
	reqA1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("A-msg1"))
	reqA1.Header.Set("Content-Type", "text/plain")
	reqA1.Header.Set(protocol.HeaderProducerID, "producer-A")
	reqA1.Header.Set(protocol.HeaderProducerEpoch, "0")
	reqA1.Header.Set(protocol.HeaderProducerSeq, "1")
	recA1 := httptest.NewRecorder()
	handler.ServeHTTP(recA1, reqA1)

	if recA1.Code != http.StatusOK {
		t.Errorf("producer-A seq=1 status = %d, want %d", recA1.Code, http.StatusOK)
	}

	// Producer B: seq=1
	reqB1 := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("B-msg1"))
	reqB1.Header.Set("Content-Type", "text/plain")
	reqB1.Header.Set(protocol.HeaderProducerID, "producer-B")
	reqB1.Header.Set(protocol.HeaderProducerEpoch, "0")
	reqB1.Header.Set(protocol.HeaderProducerSeq, "1")
	recB1 := httptest.NewRecorder()
	handler.ServeHTTP(recB1, reqB1)

	if recB1.Code != http.StatusOK {
		t.Errorf("producer-B seq=1 status = %d, want %d", recB1.Code, http.StatusOK)
	}
}

func TestHandler_POST_IdempotentProducer_WithStreamSeq(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Both producer headers and Stream-Seq should work together
	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set(protocol.HeaderProducerID, "test-producer")
	req.Header.Set(protocol.HeaderProducerEpoch, "0")
	req.Header.Set(protocol.HeaderProducerSeq, "0")
	req.Header.Set(protocol.HeaderStreamSeq, "seq_001")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestHandler_POST_IdempotentProducer_NoProducerHeadersStillWorks(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// Append without producer headers should return 204 (old behavior)
	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestHandler_POST_IdempotentProducer_JSON(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "application/json",
	})

	t.Run("JSON object with producer headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`{"event":"test"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderProducerID, "test-producer")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}
	})

	t.Run("JSON array (flattened) with producer headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`[{"id":1},{"id":2}]`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderProducerID, "test-producer")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "1")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}
	})

	t.Run("duplicate JSON append returns 204", func(t *testing.T) {
		// Retry seq=1
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`[{"id":1},{"id":2}]`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderProducerID, "test-producer")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "1")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
		}
	})
}

func TestHandler_POST_IdempotentProducer_InitialSequenceGap(t *testing.T) {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	_, _ = storage.Create(context.Background(), "/stream", durablestream.StreamConfig{
		ContentType: "text/plain",
	})

	// New producer must start at seq=0
	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set(protocol.HeaderProducerID, "new-producer")
	req.Header.Set(protocol.HeaderProducerEpoch, "0")
	req.Header.Set(protocol.HeaderProducerSeq, "5") // Should fail

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d. Body: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}

	// Check expected seq header
	if rec.Header().Get(protocol.HeaderProducerExpectedSeq) != "0" {
		t.Errorf("Producer-Expected-Seq = %q, want 0", rec.Header().Get(protocol.HeaderProducerExpectedSeq))
	}
}
