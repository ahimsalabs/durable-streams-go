package durablestream_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/memorystorage"
)

func closureRequest(
	t *testing.T,
	handler http.Handler,
	method, target, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestHandlerClosure_CreateClosedIsIdempotent(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	closedHeaders := map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}

	created := closureRequest(t, handler, http.MethodPut, "/closed", "", closedHeaders)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}
	if got := created.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("%s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	tail := created.Header().Get(protocol.HeaderStreamNextOffset)
	if tail == "" {
		t.Fatal("create response omitted Stream-Next-Offset")
	}

	replayed := closureRequest(t, handler, http.MethodPut, "/closed", "", closedHeaders)
	if replayed.Code != http.StatusOK {
		t.Fatalf("idempotent create status = %d, want %d: %s", replayed.Code, http.StatusOK, replayed.Body.String())
	}
	if got := replayed.Header().Get(protocol.HeaderStreamNextOffset); got != tail {
		t.Errorf("idempotent create tail = %q, want %q", got, tail)
	}
	if got := replayed.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("idempotent create %s = %q, want true", protocol.HeaderStreamClosed, got)
	}

	mismatch := closureRequest(t, handler, http.MethodPut, "/closed", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if mismatch.Code != http.StatusConflict {
		t.Errorf("open create over closed stream status = %d, want %d: %s", mismatch.Code, http.StatusConflict, mismatch.Body.String())
	}
}

func TestHandlerClosure_CloseOnlyIgnoresContentType(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := closureRequest(t, handler, http.MethodPut, "/close-only", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}
	tail := created.Header().Get(protocol.HeaderStreamNextOffset)

	closed := closureRequest(t, handler, http.MethodPost, "/close-only", "", map[string]string{
		"Content-Type":              "application/json",
		protocol.HeaderStreamClosed: "true",
	})
	if closed.Code != http.StatusNoContent {
		t.Fatalf("close status = %d, want %d: %s", closed.Code, http.StatusNoContent, closed.Body.String())
	}
	if got := closed.Header().Get(protocol.HeaderStreamNextOffset); got != tail {
		t.Errorf("close-only tail = %q, want unchanged tail %q", got, tail)
	}
	if got := closed.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("%s = %q, want true", protocol.HeaderStreamClosed, got)
	}
}

func TestHandlerClosure_AppendAndClose(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := closureRequest(t, handler, http.MethodPut, "/append-close", "hello ", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}
	initialTail := created.Header().Get(protocol.HeaderStreamNextOffset)

	closed := closureRequest(t, handler, http.MethodPost, "/append-close", "world", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	})
	if closed.Code != http.StatusNoContent {
		t.Fatalf("append-and-close status = %d, want %d: %s", closed.Code, http.StatusNoContent, closed.Body.String())
	}
	finalTail := closed.Header().Get(protocol.HeaderStreamNextOffset)
	if finalTail == "" || finalTail == initialTail {
		t.Fatalf("append-and-close tail = %q, want a new offset after %q", finalTail, initialTail)
	}
	if got := closed.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("append-and-close %s = %q, want true", protocol.HeaderStreamClosed, got)
	}

	read := closureRequest(t, handler, http.MethodGet, "/append-close?offset=-1", "", nil)
	if read.Code != http.StatusOK {
		t.Fatalf("read status = %d, want %d: %s", read.Code, http.StatusOK, read.Body.String())
	}
	if got := read.Body.String(); got != "hello world" {
		t.Errorf("read body = %q, want %q", got, "hello world")
	}
	if got := read.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
		t.Errorf("read tail = %q, want %q", got, finalTail)
	}
	if got := read.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("read %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	if got := read.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("read %s = %q, want true", protocol.HeaderStreamUpToDate, got)
	}

	head := closureRequest(t, handler, http.MethodHead, "/append-close", "", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d, want %d", head.Code, http.StatusOK)
	}
	if got := head.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
		t.Errorf("HEAD tail = %q, want %q", got, finalTail)
	}
	if got := head.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("HEAD %s = %q, want true", protocol.HeaderStreamClosed, got)
	}

	rejected := closureRequest(t, handler, http.MethodPost, "/append-close", "!", map[string]string{
		"Content-Type": "text/plain",
	})
	if rejected.Code != http.StatusConflict {
		t.Fatalf("append after close status = %d, want %d: %s", rejected.Code, http.StatusConflict, rejected.Body.String())
	}
	if got := rejected.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("conflict %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	if got := rejected.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
		t.Errorf("conflict tail = %q, want %q", got, finalTail)
	}
}

func TestHandlerClosure_ProducerCloseRetry(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := closureRequest(t, handler, http.MethodPut, "/producer-close", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}

	producerHeaders := func(seq string) map[string]string {
		return map[string]string{
			"Content-Type":               "text/plain",
			protocol.HeaderStreamClosed:  "true",
			protocol.HeaderProducerID:    "producer-1",
			protocol.HeaderProducerEpoch: "0",
			protocol.HeaderProducerSeq:   seq,
		}
	}
	closed := closureRequest(t, handler, http.MethodPost, "/producer-close", "final", producerHeaders("0"))
	if closed.Code != http.StatusOK {
		t.Fatalf("producer append-and-close status = %d, want %d: %s", closed.Code, http.StatusOK, closed.Body.String())
	}
	finalTail := closed.Header().Get(protocol.HeaderStreamNextOffset)

	duplicate := closureRequest(t, handler, http.MethodPost, "/producer-close", "ignored", producerHeaders("0"))
	if duplicate.Code != http.StatusNoContent {
		t.Fatalf("exact close retry status = %d, want %d: %s", duplicate.Code, http.StatusNoContent, duplicate.Body.String())
	}
	if got := duplicate.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
		t.Errorf("exact close retry tail = %q, want %q", got, finalTail)
	}
	if got := duplicate.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("exact close retry %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	if got := duplicate.Header().Get(protocol.HeaderProducerEpoch); got != "0" {
		t.Errorf("exact close retry Producer-Epoch = %q, want 0", got)
	}
	if got := duplicate.Header().Get(protocol.HeaderProducerSeq); got != "0" {
		t.Errorf("exact close retry Producer-Seq = %q, want 0", got)
	}

	different := closureRequest(t, handler, http.MethodPost, "/producer-close", "late", producerHeaders("1"))
	if different.Code != http.StatusConflict {
		t.Fatalf("different producer tuple status = %d, want %d: %s", different.Code, http.StatusConflict, different.Body.String())
	}
	if got := different.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
		t.Errorf("different tuple tail = %q, want %q", got, finalTail)
	}
	if got := different.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("different tuple %s = %q, want true", protocol.HeaderStreamClosed, got)
	}

	read := closureRequest(t, handler, http.MethodGet, "/producer-close?offset=-1", "", nil)
	if got := read.Body.String(); got != "final" {
		t.Errorf("body after producer retries = %q, want %q", got, "final")
	}
}

func TestHandlerClosure_ClosedPrecedesProducerReconciliation(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), &durablestream.HandlerConfig{MaxProducers: 1})
	created := closureRequest(t, handler, http.MethodPut, "/producer-precedence", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}

	producerHeaders := func(producerID, epoch, seq string, closeStream bool) map[string]string {
		headers := map[string]string{
			"Content-Type":               "text/plain",
			protocol.HeaderProducerID:    producerID,
			protocol.HeaderProducerEpoch: epoch,
			protocol.HeaderProducerSeq:   seq,
		}
		if closeStream {
			headers[protocol.HeaderStreamClosed] = "true"
		}
		return headers
	}

	first := closureRequest(t, handler, http.MethodPost, "/producer-precedence", "first", producerHeaders("producer-1", "1", "0", false))
	if first.Code != http.StatusOK {
		t.Fatalf("initial producer append status = %d, want %d: %s", first.Code, http.StatusOK, first.Body.String())
	}
	closed := closureRequest(t, handler, http.MethodPost, "/producer-precedence", "final", producerHeaders("producer-1", "1", "1", true))
	if closed.Code != http.StatusOK {
		t.Fatalf("producer close status = %d, want %d: %s", closed.Code, http.StatusOK, closed.Body.String())
	}
	finalTail := closed.Header().Get(protocol.HeaderStreamNextOffset)

	for _, test := range []struct {
		name                   string
		producerID, epoch, seq string
	}{
		{name: "accepted next sequence", producerID: "producer-1", epoch: "1", seq: "2"},
		{name: "earlier duplicate", producerID: "producer-1", epoch: "1", seq: "0"},
		{name: "sequence gap", producerID: "producer-1", epoch: "1", seq: "3"},
		{name: "stale epoch", producerID: "producer-1", epoch: "0", seq: "0"},
		{name: "invalid epoch restart", producerID: "producer-1", epoch: "2", seq: "1"},
		{name: "unknown producer at capacity", producerID: "producer-2", epoch: "0", seq: "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := closureRequest(t, handler, http.MethodPost, "/producer-precedence", "late", producerHeaders(test.producerID, test.epoch, test.seq, true))
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
			}
			if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
				t.Errorf("%s = %q, want true", protocol.HeaderStreamClosed, got)
			}
			if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != finalTail {
				t.Errorf("tail = %q, want %q", got, finalTail)
			}
		})
	}
}

func TestHandlerClosure_LongPollAtTailReturnsEOF(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), &durablestream.HandlerConfig{
		LongPollTimeout: 5 * time.Second,
	})
	created := closureRequest(t, handler, http.MethodPut, "/long-poll", "", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}
	tail := created.Header().Get(protocol.HeaderStreamNextOffset)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := httptest.NewRequest(
		http.MethodGet,
		"/long-poll?offset="+url.QueryEscape(tail)+"&live=long-poll",
		nil,
	).WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("long-poll at a closed tail waited instead of returning EOF")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("long-poll status = %d, want %d: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != tail {
		t.Errorf("long-poll tail = %q, want %q", got, tail)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("long-poll %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("long-poll %s = %q, want true", protocol.HeaderStreamUpToDate, got)
	}
}

func TestHandlerClosure_SSEEndsAfterFinalData(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := closureRequest(t, handler, http.MethodPut, "/sse-close", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}
	initialTail := created.Header().Get(protocol.HeaderStreamNextOffset)
	closed := closureRequest(t, handler, http.MethodPost, "/sse-close", "final", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	})
	if closed.Code != http.StatusNoContent {
		t.Fatalf("append-and-close status = %d, want %d: %s", closed.Code, http.StatusNoContent, closed.Body.String())
	}
	finalTail := closed.Header().Get(protocol.HeaderStreamNextOffset)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"/sse-close?offset="+url.QueryEscape(initialTail)+"&live=sse",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE GET: %v", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE response to EOF: %v", err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}

	dataEvent := "event: data\ndata:final\n\n"
	dataAt := strings.Index(body, dataEvent)
	controlAt := strings.Index(body, "event: control\n")
	if dataAt < 0 || controlAt < 0 || dataAt > controlAt {
		t.Fatalf("SSE events are not final data followed by control:\n%s", body)
	}
	controlData := body[controlAt+len("event: control\n"):]
	controlLine, _, ok := strings.Cut(controlData, "\n")
	if !ok || !strings.HasPrefix(controlLine, "data:") {
		t.Fatalf("SSE control event has no data line:\n%s", body)
	}
	var control map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(controlLine, "data:")), &control); err != nil {
		t.Fatalf("decode SSE control event: %v", err)
	}
	if got := control["streamNextOffset"]; got != finalTail {
		t.Errorf("control streamNextOffset = %v, want %q", got, finalTail)
	}
	if got := control["streamClosed"]; got != true {
		t.Errorf("control streamClosed = %v, want true", got)
	}
	if got := control["upToDate"]; got != true {
		t.Errorf("control upToDate = %v, want true", got)
	}
	if _, ok := control["streamCursor"]; ok {
		t.Errorf("closed control event unexpectedly contains streamCursor: %v", control)
	}
}

func TestHandlerClosure_ChangesETag(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := closureRequest(t, handler, http.MethodPut, "/etag-close", "", map[string]string{
		"Content-Type": "text/plain",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", created.Code, http.StatusCreated, created.Body.String())
	}

	before := closureRequest(t, handler, http.MethodGet, "/etag-close?offset=-1", "", nil)
	if before.Code != http.StatusOK {
		t.Fatalf("read before close status = %d, want %d: %s", before.Code, http.StatusOK, before.Body.String())
	}
	openETag := before.Header().Get("ETag")
	if openETag == "" {
		t.Fatal("read before close omitted ETag")
	}

	closed := closureRequest(t, handler, http.MethodPost, "/etag-close", "", map[string]string{
		protocol.HeaderStreamClosed: "true",
	})
	if closed.Code != http.StatusNoContent {
		t.Fatalf("close status = %d, want %d: %s", closed.Code, http.StatusNoContent, closed.Body.String())
	}

	after := closureRequest(t, handler, http.MethodGet, "/etag-close?offset=-1", "", nil)
	if after.Code != http.StatusOK {
		t.Fatalf("read after close status = %d, want %d: %s", after.Code, http.StatusOK, after.Body.String())
	}
	closedETag := after.Header().Get("ETag")
	if closedETag == "" {
		t.Fatal("read after close omitted ETag")
	}
	if closedETag == openETag {
		t.Errorf("ETag did not change when the stream closed: %s", closedETag)
	}
	if got := after.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("read after close %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
}
