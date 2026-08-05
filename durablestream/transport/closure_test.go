package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPTransport_StreamClosure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get(headerStreamClosed), "true") {
			t.Errorf("%s %s Stream-Closed = %q, want true", r.Method, r.URL.Path, r.Header.Get(headerStreamClosed))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}

		switch r.URL.Path {
		case "/close-only":
			if r.Method != http.MethodPost || len(body) != 0 {
				t.Errorf("close-only request = %s body %q, want POST with empty body", r.Method, body)
			}
		case "/append-and-close":
			if r.Method != http.MethodPost || string(body) != "final" {
				t.Errorf("append-and-close request = %s body %q, want POST body final", r.Method, body)
			}
		case "/create-closed":
			if r.Method != http.MethodPut || string(body) != "complete" {
				t.Errorf("create request = %s body %q, want PUT body complete", r.Method, body)
			}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}

		w.Header().Set(headerStreamNextOffset, "0_5")
		w.Header().Set(headerStreamClosed, "TrUe")
		if r.Header.Get(headerProducerID) != "" {
			w.Header().Set(headerProducerEpoch, "0")
			w.Header().Set(headerProducerSeq, "0")
		}
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	tr := NewHTTPTransport(server.URL, nil)

	closeOnly, err := tr.Append(t.Context(), AppendRequest{
		Path: "/close-only", Close: true,
		ProducerID: "producer-1", HasProducerHeaders: true,
	})
	if err != nil {
		t.Fatalf("close-only Append() error = %v", err)
	}
	if !closeOnly.Closed || closeOnly.NextOffset != "0_5" {
		t.Fatalf("close-only response = %+v, want closed at 0_5", closeOnly)
	}
	if closeOnly.Duplicate {
		t.Fatal("close-only response reported Duplicate despite ambiguous 204 status")
	}

	final, err := tr.Append(t.Context(), AppendRequest{
		Path: "/append-and-close", Data: []byte("final"), ContentType: "text/plain", Close: true,
	})
	if err != nil {
		t.Fatalf("append-and-close Append() error = %v", err)
	}
	if !final.Closed {
		t.Fatal("append-and-close response did not report Closed")
	}

	created, err := tr.Create(t.Context(), CreateRequest{
		Path: "/create-closed", ContentType: "text/plain", InitialData: []byte("complete"), Closed: true,
	})
	if err != nil {
		t.Fatalf("closed Create() error = %v", err)
	}
	if !created.Closed || created.NextOffset != "0_5" {
		t.Fatalf("create response = %+v, want closed at 0_5", created)
	}
}

func TestHTTPTransport_ClosureRequiresServerConfirmation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerStreamNextOffset, "0_5")
		if r.Method == http.MethodPut {
			w.Header().Set(headerStreamClosed, "false")
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	tr := NewHTTPTransport(server.URL, nil)
	for _, test := range []struct {
		name string
		call func() error
	}{
		{
			name: "append response omits header",
			call: func() error {
				_, err := tr.Append(t.Context(), AppendRequest{Path: "/stream", Close: true})
				return err
			},
		},
		{
			name: "create response denies closure",
			call: func() error {
				_, err := tr.Create(t.Context(), CreateRequest{Path: "/stream", Closed: true})
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var transportErr *Error
			if err := test.call(); !errors.As(err, &transportErr) || transportErr.Code != "PARSE_ERROR" {
				t.Fatalf("operation error = %#v, want PARSE_ERROR", err)
			}
		})
	}
}

func TestHTTPTransport_ObservesClosureAcrossReadModes(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerStreamNextOffset, "0_4")
		w.Header().Set(headerStreamClosed, "true")

		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
		case r.URL.Query().Get(queryLive) == liveModeLongPoll:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "done")
		}
	}))
	defer server.Close()

	tr := NewHTTPTransport(server.URL, nil)
	read, err := tr.Read(t.Context(), ReadRequest{Path: "/stream"})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !read.Closed || !read.UpToDate || string(read.Data) != "done" {
		t.Fatalf("Read() = %+v, want final data and EOF", read)
	}

	poll, err := tr.LongPoll(t.Context(), LongPollRequest{Path: "/stream", Offset: "0_4"})
	if err != nil {
		t.Fatalf("LongPoll() error = %v", err)
	}
	if !poll.Closed || !poll.UpToDate {
		t.Fatalf("LongPoll() = %+v, want immediate EOF", poll)
	}

	head, err := tr.Head(t.Context(), HeadRequest{Path: "/stream"})
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if !head.Closed {
		t.Fatal("Head() did not report Closed")
	}
}

func TestHTTPTransport_SSEClosureControl(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: control\ndata: {\"streamNextOffset\":\"0_1\",\"streamClosed\":true}\n\n")
	}))
	defer server.Close()

	stream, err := NewHTTPTransport(server.URL, nil).SSE(t.Context(), SSERequest{Path: "/stream", Offset: "0_1"})
	if err != nil {
		t.Fatalf("SSE() error = %v", err)
	}
	defer stream.Close()

	event, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if !event.Closed || !event.UpToDate || event.NextOffset != "0_1" {
		t.Fatalf("control event = %+v, want closed/up-to-date at 0_1", event)
	}
}

func TestHTTPTransport_ClosedConflictUsesHeader(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerStreamClosed, "TRUE")
		w.Header().Set(headerStreamNextOffset, "0_9")
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"conflict","message":"implementation-specific"}`)
	}))
	defer server.Close()

	_, err := NewHTTPTransport(server.URL, nil).Append(t.Context(), AppendRequest{
		Path: "/stream", Data: []byte("late"), ContentType: "text/plain",
	})
	var transportErr *Error
	if !errors.As(err, &transportErr) || transportErr.Code != "STREAM_CLOSED" || transportErr.StatusCode != http.StatusConflict {
		t.Fatalf("Append() error = %#v, want STREAM_CLOSED 409", err)
	}
	if transportErr.FinalOffset != "0_9" {
		t.Fatalf("Append() final offset = %q, want 0_9", transportErr.FinalOffset)
	}
}

func TestHTTPTransport_ClosedHeaderDoesNotReclassifyCreateConflict(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerStreamClosed, "true")
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	_, err := NewHTTPTransport(server.URL, nil).Create(t.Context(), CreateRequest{Path: "/stream"})
	var transportErr *Error
	if !errors.As(err, &transportErr) || transportErr.Code != "CONFLICT" {
		t.Fatalf("Create() error = %#v, want generic CONFLICT", err)
	}
}

func TestResponseStreamClosed_ExactTrueValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value string
		want  bool
	}{
		{value: "true", want: true},
		{value: "TRUE", want: true},
		{value: "TrUe", want: true},
		{value: "false"},
		{value: "yes"},
		{value: "1"},
		{value: " true "},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			resp := &http.Response{Header: make(http.Header)}
			resp.Header.Set(headerStreamClosed, tt.value)
			if got := responseStreamClosed(resp); got != tt.want {
				t.Fatalf("responseStreamClosed(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestWithRetry_CloseSafety(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	mock := &mockTransport{appendFunc: func(context.Context, AppendRequest) (*AppendResponse, error) {
		calls.Add(1)
		return nil, &Error{Code: "UNAVAILABLE", StatusCode: http.StatusServiceUnavailable}
	}}
	retried := WithRetry(RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond})(mock)

	_, _ = retried.Append(t.Context(), AppendRequest{Path: "/stream", Close: true})
	if got := calls.Load(); got != 2 {
		t.Fatalf("close-only attempts = %d, want 2", got)
	}

	calls.Store(0)
	_, _ = retried.Append(t.Context(), AppendRequest{Path: "/stream", Data: []byte("final"), Close: true})
	if got := calls.Load(); got != 1 {
		t.Fatalf("non-idempotent append-and-close attempts = %d, want 1", got)
	}
}
