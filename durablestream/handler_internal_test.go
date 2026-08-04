package durablestream

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

// TestValidateOffset tests offset validation per PROTOCOL.md Section 8.
func TestValidateOffset(t *testing.T) {
	tests := []struct {
		name    string
		offset  string
		wantErr bool
		desc    string
	}{
		// Valid offsets
		{
			name:    "empty offset",
			offset:  "",
			wantErr: false,
			desc:    "empty offset is valid (equivalent to stream start)",
		},
		{
			name:    "simple numeric",
			offset:  "123",
			wantErr: false,
			desc:    "simple numeric offset is valid",
		},
		{
			name:    "timestamp-style offset",
			offset:  "1234567890_123",
			wantErr: false,
			desc:    "timestamp-style offset with underscore is valid",
		},
		{
			name:    "offset with hyphen",
			offset:  "chunk-001-offset-123",
			wantErr: false,
			desc:    "hyphen is valid in offsets",
		},
		{
			name:    "offset with slash",
			offset:  "segment/123",
			wantErr: true,
			desc:    "slash MUST NOT be in offset per Section 8",
		},
		{
			name:    "sentinel -1",
			offset:  "-1",
			wantErr: false,
			desc:    "sentinel value -1 is valid",
		},
		{
			name:    "sentinel now",
			offset:  "now",
			wantErr: false,
			desc:    "sentinel value now is valid",
		},
		{
			name:    "single dot",
			offset:  ".",
			wantErr: false,
			desc:    "single dot is valid (not path traversal)",
		},
		{
			name:    "multiple separated dots",
			offset:  "a.b.c",
			wantErr: false,
			desc:    "dots separated by other chars are valid",
		},

		// Invalid: URL query parameter conflict characters (Section 8)
		{
			name:    "comma",
			offset:  "offset,value",
			wantErr: true,
			desc:    "comma MUST NOT be in offset per Section 8",
		},
		{
			name:    "ampersand",
			offset:  "offset&other",
			wantErr: true,
			desc:    "ampersand MUST NOT be in offset per Section 8",
		},
		{
			name:    "equals sign",
			offset:  "offset=value",
			wantErr: true,
			desc:    "equals sign MUST NOT be in offset per Section 8",
		},
		{
			name:    "question mark",
			offset:  "offset?query",
			wantErr: true,
			desc:    "question mark MUST NOT be in offset per Section 8",
		},

		// Invalid: whitespace and control characters
		{
			name:    "space",
			offset:  "offset value",
			wantErr: true,
			desc:    "space is not allowed in offset",
		},
		{
			name:    "tab",
			offset:  "offset\tvalue",
			wantErr: true,
			desc:    "tab is not allowed in offset",
		},
		{
			name:    "newline",
			offset:  "offset\nvalue",
			wantErr: true,
			desc:    "newline is not allowed in offset",
		},
		{
			name:    "carriage return",
			offset:  "offset\rvalue",
			wantErr: true,
			desc:    "carriage return is not allowed in offset",
		},

		// Invalid: path traversal patterns (Section 10.2)
		{
			name:    "double dot path traversal",
			offset:  "..",
			wantErr: true,
			desc:    ".. SHOULD be rejected per Section 10.2 (path traversal)",
		},
		{
			name:    "path traversal at start",
			offset:  "../etc/passwd",
			wantErr: true,
			desc:    ".. at start SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal in middle",
			offset:  "dir/../secret",
			wantErr: true,
			desc:    ".. in middle SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal at end",
			offset:  "file/..",
			wantErr: true,
			desc:    ".. at end SHOULD be rejected (path traversal)",
		},
		{
			name:    "numeric prefix with path traversal",
			offset:  "0/..",
			wantErr: true,
			desc:    "0/.. SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal with slash",
			offset:  "0/../../../etc/passwd",
			wantErr: true,
			desc:    "complex path traversal SHOULD be rejected",
		},
		{
			name:    "multiple consecutive dots beyond two",
			offset:  "...",
			wantErr: true,
			desc:    "... contains .. and SHOULD be rejected",
		},
		{
			name:    "path traversal without slash",
			offset:  "prefix..suffix",
			wantErr: true,
			desc:    ".. embedded in string SHOULD be rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOffset(tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOffset(%q) error = %v, wantErr %v\ndesc: %s",
					tt.offset, err, tt.wantErr, tt.desc)
			}
			if err != nil && err.Error() != "invalid offset format" {
				t.Errorf("validateOffset(%q) error message = %q, want 'invalid offset format'",
					tt.offset, err.Error())
			}
		})
	}
}

func TestSplitBySSELineTerminators(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty string",
			input: "",
			want:  []string{""},
		},
		{
			name:  "no terminators",
			input: "hello world",
			want:  []string{"hello world"},
		},
		{
			name:  "single LF",
			input: "hello\nworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "single CR",
			input: "hello\rworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "single CRLF",
			input: "hello\r\nworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "multiple LF",
			input: "a\nb\nc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "multiple CR",
			input: "a\rb\rc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "multiple CRLF",
			input: "a\r\nb\r\nc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "mixed terminators",
			input: "a\nb\rc\r\nd",
			want:  []string{"a", "b", "c", "d"},
		},
		{
			name:  "consecutive LF (empty line)",
			input: "a\n\nb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "consecutive CR (empty line)",
			input: "a\r\rb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "consecutive CRLF (empty line)",
			input: "a\r\n\r\nb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "trailing LF",
			input: "hello\n",
			want:  []string{"hello", ""},
		},
		{
			name:  "trailing CR",
			input: "hello\r",
			want:  []string{"hello", ""},
		},
		{
			name:  "trailing CRLF",
			input: "hello\r\n",
			want:  []string{"hello", ""},
		},
		{
			name:  "leading LF",
			input: "\nhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "leading CR",
			input: "\rhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "leading CRLF",
			input: "\r\nhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "CRLF injection attack payload",
			input: "start\r\revent: control\rdata: {\"cr_injected\":true}\r\rend",
			want:  []string{"start", "", "event: control", "data: {\"cr_injected\":true}", "", "end"},
		},
		{
			name:  "LF-only injection payload",
			input: "start\n\nevent: data\ndata: fake-event\n\nend",
			want:  []string{"start", "", "event: data", "data: fake-event", "", "end"},
		},
		{
			name:  "CRLF mixed injection payload",
			input: "safe content\r\n\r\nevent: control\r\ndata: {\"injected\":true}\r\n\r\nmore safe content",
			want:  []string{"safe content", "", "event: control", "data: {\"injected\":true}", "", "more safe content"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitBySSELineTerminators(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitBySSELineTerminators(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestMakeETag(t *testing.T) {
	if got := makeETag("", "start", "end"); got != "" {
		t.Fatalf("makeETag with no incarnation ID = %q, want empty", got)
	}

	const opaqueID = "opaque\x00\r\n\xffid"
	got := makeETag(opaqueID, "start", "end")
	if len(got) != 66 || got[0] != '"' || got[len(got)-1] != '"' {
		t.Fatalf("makeETag() = %q, want a quoted SHA-256 digest", got)
	}
	for _, forbidden := range []byte{0, '\r', '\n', 0xff} {
		if strings.IndexByte(got, forbidden) >= 0 {
			t.Fatalf("makeETag() exposed unsafe opaque-ID byte %#x: %q", forbidden, got)
		}
	}
	if again := makeETag(opaqueID, "start", "end"); again != got {
		t.Fatalf("makeETag() is not stable: first %q, second %q", got, again)
	}

	for name, changed := range map[string]string{
		"incarnation": makeETag("replacement", "start", "end"),
		"start":       makeETag(opaqueID, "different", "end"),
		"end":         makeETag(opaqueID, "start", "different"),
	} {
		if changed == got {
			t.Errorf("makeETag() did not change when %s changed", name)
		}
	}
}

// --- Regression tests -------------------------------------------------------

type waitObservedStorage struct {
	*testStorage
	waitEntered chan struct{}
	waitOnce    sync.Once
}

func newWaitObservedStorage() *waitObservedStorage {
	return &waitObservedStorage{
		testStorage: newTestStorage(),
		waitEntered: make(chan struct{}),
	}
}

func (s *waitObservedStorage) WaitForData(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	s.waitOnce.Do(func() { close(s.waitEntered) })
	return s.testStorage.WaitForData(ctx, streamID, offset, limit)
}

type mismatchedIncarnationStorage struct {
	*testStorage
}

func (s *mismatchedIncarnationStorage) Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	result, err := s.testStorage.Read(ctx, streamID, offset, limit)
	if result != nil {
		result.IncarnationID += "-replacement"
	}
	return result, err
}

func (s *mismatchedIncarnationStorage) WaitForData(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	result, err := s.testStorage.WaitForData(ctx, streamID, offset, limit)
	if result != nil {
		result.IncarnationID += "-replacement"
	}
	return result, err
}

func TestHandlerReadModesRejectMismatchedStorageIncarnation(t *testing.T) {
	base := newTestStorage()
	if created, err := base.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil || !created {
		t.Fatalf("Create() = (%v, %v), want (true, nil)", created, err)
	}
	if _, err := base.Append(context.Background(), "/stream", []byte("old data"), ""); err != nil {
		t.Fatalf("Append(): %v", err)
	}
	handler := NewHandler(&mismatchedIncarnationStorage{testStorage: base}, &HandlerConfig{
		LongPollTimeout: time.Second,
		SSECloseAfter:   time.Second,
	})
	zero := formatTestOffset(0).String()

	for _, query := range []string{
		"?offset=" + zero,
		"?offset=" + zero + "&live=long-poll",
		"?offset=" + zero + "&live=sse",
	} {
		req := httptest.NewRequest(http.MethodGet, "/stream"+query, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %q status = %d, want 404", query, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "old data") {
			t.Errorf("GET %q exposed bytes from a mismatched incarnation: %q", query, rec.Body.String())
		}
	}
}

func TestHandlerLiveReadsTerminateWhenSameHandlerRecreatesStream(t *testing.T) {
	for _, liveMode := range []string{protocol.LiveModeLongPoll, protocol.LiveModeSSE} {
		t.Run(liveMode, func(t *testing.T) {
			storage := newWaitObservedStorage()
			if created, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil || !created {
				t.Fatalf("Create(old) = (%v, %v), want (true, nil)", created, err)
			}
			handler := NewHandler(storage, &HandlerConfig{
				LongPollTimeout: 5 * time.Second,
				SSECloseAfter:   5 * time.Second,
			})
			zero := formatTestOffset(0).String()

			readDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/stream?offset="+zero+"&live="+liveMode, nil)
				handler.ServeHTTP(rec, req)
				readDone <- rec
			}()

			select {
			case <-storage.waitEntered:
			case <-time.After(time.Second):
				t.Fatal("live read did not enter WaitForData")
			}

			deleteRec := httptest.NewRecorder()
			handler.ServeHTTP(deleteRec, httptest.NewRequest(http.MethodDelete, "/stream", nil))
			if deleteRec.Code != http.StatusNoContent {
				t.Fatalf("DELETE status = %d, want 204", deleteRec.Code)
			}

			putReq := httptest.NewRequest(http.MethodPut, "/stream", strings.NewReader("replacement bytes"))
			putReq.Header.Set("Content-Type", "text/plain")
			putRec := httptest.NewRecorder()
			handler.ServeHTTP(putRec, putReq)
			if putRec.Code != http.StatusCreated {
				t.Fatalf("replacement PUT status = %d, want 201", putRec.Code)
			}

			var readRec *httptest.ResponseRecorder
			select {
			case readRec = <-readDone:
			case <-time.After(time.Second):
				t.Fatal("live read did not terminate after lifecycle replacement")
			}
			if strings.Contains(readRec.Body.String(), "replacement bytes") {
				t.Fatalf("old live read exposed replacement bytes: %q", readRec.Body.String())
			}
			if liveMode == protocol.LiveModeLongPoll && readRec.Code != http.StatusNotFound {
				t.Fatalf("long-poll status = %d, want 404", readRec.Code)
			}
			if liveMode == protocol.LiveModeSSE && readRec.Code != http.StatusOK {
				t.Fatalf("SSE status = %d, want already-committed 200", readRec.Code)
			}
		})
	}
}

// gateStorage wraps testStorage so a test can block inside Append and observe
// when requests reach Head. Used to drive request interleavings without sleeps.
type gateStorage struct {
	*testStorage

	headEntered   chan struct{} // Signaled on every Head
	appendEntered chan struct{} // Signaled on every gated Append
	releaseAppend chan struct{} // Gated appends block until this is closed
	gateAppends   bool
	appendErr     error // If set, Append fails with this error
}

// gatedReader announces the first read and then waits for the test to release
// it. It models a client that has sent the POST headers but is still uploading
// the body.
type gatedReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	reader  io.Reader
}

func (r *gatedReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	return r.reader.Read(p)
}

type failingTouchStorage struct {
	Storage
	err error
}

// baseStorageOnly deliberately hides optional capabilities implemented by its
// wrapped value so Handler fallback behavior can be tested.
type baseStorageOnly struct {
	Storage
}

type failOnceBatchStorage struct {
	AtomicBatchStorage
	mu       sync.Mutex
	failNext bool
}

func (s *failOnceBatchStorage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (Offset, error) {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return "", errors.New("injected batch failure")
	}
	s.mu.Unlock()
	return s.AtomicBatchStorage.AppendBatch(ctx, streamID, messages, seq)
}

func (s *failingTouchStorage) Touch(context.Context, string) error {
	return s.err
}

func newGateStorage() *gateStorage {
	return &gateStorage{
		testStorage:   newTestStorage(),
		headEntered:   make(chan struct{}, 16),
		appendEntered: make(chan struct{}, 16),
		releaseAppend: make(chan struct{}),
	}
}

func (g *gateStorage) Head(ctx context.Context, streamID string) (*StreamInfo, error) {
	select {
	case g.headEntered <- struct{}{}:
	default:
	}
	return g.testStorage.Head(ctx, streamID)
}

func (g *gateStorage) Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error) {
	if g.appendErr != nil {
		return "", g.appendErr
	}
	if g.gateAppends {
		select {
		case g.appendEntered <- struct{}{}:
		default:
		}
		<-g.releaseAppend
	}
	return g.testStorage.Append(ctx, streamID, data, seq)
}

func (g *gateStorage) messageCount(streamID string) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	stream, ok := g.streams[streamID]
	if !ok {
		return -1
	}
	return len(stream.messages)
}

func TestHandler_SlowPOSTBodyDoesNotBlockStreamMutation(t *testing.T) {
	storage := newTestStorage()
	if _, err := storage.Create(t.Context(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	handler := NewHandler(storage, nil)

	body := &gatedReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
		reader:  strings.NewReader("payload"),
	}
	postReq := httptest.NewRequest(http.MethodPost, "/stream", body)
	postReq.Header.Set("Content-Type", "text/plain")
	postRec := httptest.NewRecorder()
	postDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(postRec, postReq)
		close(postDone)
	}()

	<-body.started

	deleteReq := httptest.NewRequest(http.MethodDelete, "/stream", nil)
	deleteRec := httptest.NewRecorder()
	deleteDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(deleteRec, deleteReq)
		close(deleteDone)
	}()

	select {
	case <-deleteDone:
		if deleteRec.Code != http.StatusNoContent {
			t.Fatalf("DELETE status = %d, want 204 (body %q)", deleteRec.Code, deleteRec.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("DELETE blocked behind a POST request body")
	}

	close(body.release)
	select {
	case <-postDone:
		if postRec.Code != http.StatusNotFound {
			t.Fatalf("POST status after DELETE = %d, want 404 (body %q)", postRec.Code, postRec.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("POST did not finish after its body was released")
	}
}

func TestHandler_RequiresAtomicStorageForRequestBatches(t *testing.T) {
	t.Run("PUT with initial content", func(t *testing.T) {
		base := newTestStorage()
		handler := NewHandler(&baseStorageOnly{Storage: base}, nil)
		req := httptest.NewRequest(http.MethodPut, "/stream", strings.NewReader("initial"))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("PUT status = %d, want 501 (body %q)", rec.Code, rec.Body.String())
		}
		if _, err := base.Head(t.Context(), "/stream"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Head after rejected PUT error = %v, want ErrNotFound", err)
		}
	})

	t.Run("POST with multiple JSON messages", func(t *testing.T) {
		base := newTestStorage()
		if _, err := base.Create(t.Context(), "/stream", StreamConfig{ContentType: "application/json"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		handler := NewHandler(&baseStorageOnly{Storage: base}, nil)
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`[{"n":1},{"n":2}]`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("POST status = %d, want 501 (body %q)", rec.Code, rec.Body.String())
		}
		result, err := base.Read(t.Context(), "/stream", ZeroOffset, 0)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(result.Messages) != 0 {
			t.Fatalf("rejected POST stored %d messages, want none", len(result.Messages))
		}
	})
}

func TestHandler_FailedAtomicBatchDoesNotCommitProducerState(t *testing.T) {
	base := newTestStorage()
	if _, err := base.Create(t.Context(), "/stream", StreamConfig{ContentType: "application/json"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	storage := &failOnceBatchStorage{AtomicBatchStorage: base, failNext: true}
	handler := NewHandler(storage, nil)

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`[{"n":1},{"n":2}]`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderProducerID, "producer")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	if rec := post(); rec.Code != http.StatusInternalServerError {
		t.Fatalf("first POST status = %d, want 500 (body %q)", rec.Code, rec.Body.String())
	}
	if rec := post(); rec.Code != http.StatusOK {
		t.Fatalf("retry POST status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	result, err := base.Read(t.Context(), "/stream", ZeroOffset, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := len(result.Messages); got != 2 {
		t.Fatalf("messages after retry = %d, want 2", got)
	}
}

// TestHandler_PUT_BodyIsBounded covers the unbounded io.ReadAll on PUT: a body
// larger than MaxAppendSize must be rejected with 413 rather than buffered, and
// the oversized request must not leave a stream behind.
func TestHandler_PUT_BodyIsBounded(t *testing.T) {
	const maxSize = 64

	tests := []struct {
		name          string
		chunked       bool
		bodySize      int
		wantStatus    int
		wantStreamNew bool
	}{
		{name: "declared length over limit", bodySize: maxSize + 1, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "chunked over limit", chunked: true, bodySize: maxSize + 1, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "declared length at limit", bodySize: maxSize, wantStatus: http.StatusCreated, wantStreamNew: true},
		{name: "chunked at limit", chunked: true, bodySize: maxSize, wantStatus: http.StatusCreated, wantStreamNew: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newTestStorage()
			handler := NewHandler(storage, &HandlerConfig{MaxAppendSize: maxSize})

			body := strings.Repeat("x", tt.bodySize)
			req := httptest.NewRequest(http.MethodPut, "/stream", strings.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			if tt.chunked {
				// A chunked request declares no length, so only the read bound applies.
				req.ContentLength = -1
				req.TransferEncoding = []string{"chunked"}
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d (body %q)", rec.Code, tt.wantStatus, rec.Body.String())
			}

			_, err := storage.Head(context.Background(), "/stream")
			exists := err == nil
			if exists != tt.wantStreamNew {
				t.Errorf("stream exists = %v, want %v: an over-sized PUT must not create the stream", exists, tt.wantStreamNew)
			}
		})
	}
}

// A matching PUT is an idempotent replay of the create operation. Its initial
// content must not turn the replay into an append.
func TestHandler_PUT_ReplayDoesNotAppendInitialBody(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, nil)

	put := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/stream", strings.NewReader("initial"))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	first := put()
	if first.Code != http.StatusCreated {
		t.Fatalf("first PUT status = %d, want 201", first.Code)
	}
	second := put()
	if second.Code != http.StatusOK {
		t.Fatalf("replayed PUT status = %d, want 200", second.Code)
	}
	storage.mu.RLock()
	messageCount := len(storage.streams["/stream"].messages)
	storage.mu.RUnlock()
	if got := messageCount; got != 1 {
		t.Fatalf("stream contains %d messages after replay, want 1", got)
	}
	if got, want := second.Header().Get(protocol.HeaderStreamNextOffset), first.Header().Get(protocol.HeaderStreamNextOffset); got != want {
		t.Errorf("replayed PUT next offset = %q, want unchanged %q", got, want)
	}
}

// JSON validation precedes Create so a rejected representation has no durable
// side effect that a subsequent HEAD can observe.
func TestHandler_PUT_InvalidJSONDoesNotCreateStream(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, nil)

	req := httptest.NewRequest(http.MethodPut, "/stream", strings.NewReader(`{"unterminated"`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT status = %d, want 400", rec.Code)
	}
	if _, err := storage.Head(context.Background(), "/stream"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Head after rejected PUT = %v, want ErrNotFound", err)
	}
}

// Sliding expiry is part of the durable result of an active request. A backend
// failure must be surfaced while an HTTP error can still be written, rather
// than acknowledging activity that did not actually renew the stream.
func TestHandler_TouchFailurePreventsSuccess(t *testing.T) {
	tests := []struct {
		name string
		req  func() *http.Request
	}{
		{
			name: "PUT replay",
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodPut, "/stream", nil)
				req.Header.Set("Content-Type", "text/plain")
				return req
			},
		},
		{
			name: "POST",
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
				req.Header.Set("Content-Type", "text/plain")
				return req
			},
		},
		{name: "catch-up GET", req: func() *http.Request { return httptest.NewRequest(http.MethodGet, "/stream", nil) }},
		{name: "long-poll GET", req: func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/stream?offset=now&live=long-poll", nil)
		}},
		{name: "SSE GET", req: func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/stream?offset=now&live=sse", nil)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := newTestStorage()
			if _, err := base.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			handler := NewHandler(&failingTouchStorage{Storage: base, err: errors.New("touch failed")}, &HandlerConfig{
				LongPollTimeout: time.Millisecond,
				SSECloseAfter:   time.Millisecond,
			})
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, tt.req())
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (body %q)", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestHandler_ProducerStateClearedOnDelete covers producer state outliving its
// stream: after a delete and recreate, a producer restarting at seq=0 must have
// its append accepted, not deduplicated away as a 204.
func TestHandler_ProducerStateClearedOnDelete(t *testing.T) {
	appendReq := func(seq int) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Producer-Id", "p1")
		req.Header.Set("Producer-Epoch", "0")
		req.Header.Set("Producer-Seq", strconv.Itoa(seq))
		return req
	}

	create := func(t *testing.T, h *Handler) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPut, "/stream", nil)
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("PUT status = %d, want 201", rec.Code)
		}
	}

	for _, tt := range []struct {
		name  string
		reset func(t *testing.T, h *Handler)
	}{
		{
			name: "delete then recreate",
			reset: func(t *testing.T, h *Handler) {
				t.Helper()
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/stream", nil))
				if rec.Code != http.StatusNoContent {
					t.Fatalf("DELETE status = %d, want 204", rec.Code)
				}
				create(t, h)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			storage := newTestStorage()
			handler := NewHandler(storage, nil)
			create(t, handler)

			// Producer writes seq 0 and 1 to the original stream.
			for seq := 0; seq <= 1; seq++ {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, appendReq(seq))
				if rec.Code != http.StatusOK {
					t.Fatalf("append seq=%d status = %d, want 200", seq, rec.Code)
				}
			}

			tt.reset(t, handler)

			// The restarted producer starts over at seq=0 on the new stream.
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, appendReq(0))
			if rec.Code != http.StatusOK {
				t.Fatalf("append after reset: status = %d, want 200 (204 means the write was dropped as a duplicate)", rec.Code)
			}
			if n := len(storage.streams["/stream"].messages); n != 1 {
				t.Errorf("stream has %d messages, want 1", n)
			}
		})
	}
}

// TestHandler_ProducerPipelinedSeqsSerialized covers the check-then-act race in
// producer validation: seq=1 must not inspect the stream until seq=0 has
// committed, which would otherwise report a bogus sequence gap and wedge the
// producer.
func TestHandler_ProducerPipelinedSeqsSerialized(t *testing.T) {
	storage := newGateStorage()
	storage.gateAppends = true
	handler := NewHandler(storage, nil)

	if _, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	post := func(seq int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(fmt.Sprintf("msg%d", seq)))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Producer-Id", "p1")
		req.Header.Set("Producer-Epoch", "0")
		req.Header.Set("Producer-Seq", strconv.Itoa(seq))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		recs[0] = post(0)
	}()

	// seq=0 is now parked inside Append with its producer state uncommitted.
	<-storage.appendEntered
	<-storage.headEntered

	wg.Add(1)
	go func() {
		defer wg.Done()
		recs[1] = post(1)
	}()

	// The stream mutation lock keeps seq=1 from observing the in-flight stream
	// incarnation or its uncommitted producer state.
	select {
	case <-storage.headEntered:
		t.Fatal("seq=1 reached Head before seq=0 committed")
	case <-time.After(20 * time.Millisecond):
	}

	close(storage.releaseAppend)
	wg.Wait()

	for seq, rec := range recs {
		if rec.Code != http.StatusOK {
			t.Errorf("seq=%d status = %d, want 200 (body %q)", seq, rec.Code, rec.Body.String())
		}
	}
	if n := storage.messageCount("/stream"); n != 2 {
		t.Errorf("stream has %d messages, want 2", n)
	}
}

// A duplicate waits for the original append to commit before inspecting the
// stream, then reports the committed tail rather than a stale pre-append offset.
func TestHandler_ProducerDuplicateReportsCommittedTail(t *testing.T) {
	storage := newGateStorage()
	storage.gateAppends = true
	handler := NewHandler(storage, nil)

	if _, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("data"))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set(protocol.HeaderProducerID, "p1")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() { responses <- post() }()
	<-storage.appendEntered
	<-storage.headEntered

	go func() { responses <- post() }()
	select {
	case <-storage.headEntered:
		t.Fatal("duplicate reached Head before original append committed")
	case <-time.After(20 * time.Millisecond):
	}
	close(storage.releaseAppend)

	var original, duplicate *httptest.ResponseRecorder
	for range 2 {
		rec := <-responses
		switch rec.Code {
		case http.StatusOK:
			original = rec
		case http.StatusNoContent:
			duplicate = rec
		default:
			t.Fatalf("POST status = %d, want one 200 and one 204 (body %q)", rec.Code, rec.Body.String())
		}
	}
	if original == nil || duplicate == nil {
		t.Fatalf("responses = original %v, duplicate %v; want both", original != nil, duplicate != nil)
	}
	if got, want := duplicate.Header().Get(protocol.HeaderStreamNextOffset), original.Header().Get(protocol.HeaderStreamNextOffset); got != want {
		t.Errorf("duplicate next offset = %q, want accepted append tail %q", got, want)
	}
}

// New entries are pending until an append commits. Rejected traffic therefore
// cannot consume capacity and erase the state that makes prior appends idempotent.
func TestHandler_RejectedProducersDoNotEvictCommittedState(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{MaxProducers: 2})
	if _, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	post := func(producerID, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(body))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set(protocol.HeaderProducerID, producerID)
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	if rec := post("keeper", "committed"); rec.Code != http.StatusOK {
		t.Fatalf("initial append status = %d, want 200", rec.Code)
	}
	for i := 0; i < 10; i++ {
		if rec := post(fmt.Sprintf("rejected-%d", i), ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("rejected producer %d status = %d, want 400", i, rec.Code)
		}
	}

	if rec := post("keeper", "retry"); rec.Code != http.StatusNoContent {
		t.Fatalf("retry after rejected traffic status = %d, want 204", rec.Code)
	}
	storage.mu.RLock()
	messageCount := len(storage.streams["/stream"].messages)
	storage.mu.RUnlock()
	if got := messageCount; got != 1 {
		t.Fatalf("stream contains %d messages, want the committed append only", got)
	}
	if got := handler.producers.len(); got != 1 {
		t.Fatalf("registry contains %d committed producers, want 1", got)
	}
}

func TestHandler_ProducerCapacityRejectsNewButKeepsExisting(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{MaxProducers: 2})
	if _, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	post := func(id, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(body))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set(protocol.HeaderProducerID, id)
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, "0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	for _, id := range []string{"p1", "p2"} {
		if rec := post(id, id); rec.Code != http.StatusOK {
			t.Fatalf("initial append for %s status = %d, want 200", id, rec.Code)
		}
	}
	if rec := post("p3", "must not append"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("new producer at capacity status = %d, want 429", rec.Code)
	}
	if rec := post("p1", "retry"); rec.Code != http.StatusNoContent {
		t.Fatalf("existing producer at capacity status = %d, want 204", rec.Code)
	}
	storage.mu.RLock()
	messageCount := len(storage.streams["/stream"].messages)
	storage.mu.RUnlock()
	if messageCount != 2 {
		t.Fatalf("stream contains %d messages, want 2", messageCount)
	}
}

func TestHandler_ProducerKeySizeIsBounded(t *testing.T) {
	for _, tt := range []struct {
		name       string
		streamID   string
		producerID string
	}{
		{name: "producer ID", streamID: "/stream", producerID: strings.Repeat("p", maxProducerIDBytes+1)},
		{name: "combined key", streamID: "/" + strings.Repeat("s", maxProducerKeyBytes), producerID: "p"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			storage := newTestStorage()
			handler := NewHandler(storage, nil)
			if _, err := storage.Create(context.Background(), tt.streamID, StreamConfig{ContentType: "text/plain"}); err != nil {
				t.Fatalf("Create: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, tt.streamID, strings.NewReader("data"))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set(protocol.HeaderProducerID, tt.producerID)
			req.Header.Set(protocol.HeaderProducerEpoch, "0")
			req.Header.Set(protocol.HeaderProducerSeq, "0")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("POST status = %d, want 400", rec.Code)
			}
			if got := handler.producers.len(); got != 0 {
				t.Fatalf("registry contains %d entries after oversized key, want 0", got)
			}
		})
	}
}

// TestProducerEntry_CommitDoesNotRegress covers out-of-order commits rewinding
// lastSeq, which would let an already-accepted sequence be accepted again.
func TestProducerEntry_CommitDoesNotRegress(t *testing.T) {
	var e producerEntry

	e.commit(0, 5)
	e.commit(0, 3) // Late completion of an earlier sequence.

	if e.state.lastSeq != 5 {
		t.Errorf("lastSeq = %d, want 5", e.state.lastSeq)
	}
	if got := e.validate(0, 4); got.outcome != producerDuplicate {
		t.Errorf("validate(0,4) outcome = %v, want producerDuplicate", got.outcome)
	}

	// A new epoch resets the sequence space.
	e.commit(1, 0)
	if e.state.epoch != 1 || e.state.lastSeq != 0 {
		t.Errorf("state = %+v, want epoch 1 lastSeq 0", e.state)
	}
}

// TestProducerRegistry_BoundsEntries covers unbounded growth of producer state.
func TestProducerRegistry_BoundsEntries(t *testing.T) {
	const max = 4
	r := newProducerRegistry(max, defaultProducerStateTTL)

	for i := 0; i < max; i++ {
		key := producerKey{streamID: "/s", producerID: strconv.Itoa(i)}
		entry := r.acquire(key)
		entry.commit(0, 0)
		r.release(entry)
	}

	if got := r.len(); got != max {
		t.Errorf("registry holds %d entries, want %d", got, max)
	}

	if entry := r.acquire(producerKey{streamID: "/s", producerID: "new"}); entry != nil {
		r.release(entry)
		t.Fatal("brand-new producer was admitted past the registry bound")
	}

	// Capacity pressure never removes committed state: existing producers remain
	// available for duplicate detection and fencing.
	oldest := producerKey{streamID: "/s", producerID: "0"}
	entry := r.acquire(oldest)
	known := entry.state.known
	r.release(entry)
	if !known {
		t.Error("committed producer state was lost at capacity")
	}
}

func TestProducerRegistry_ExpiredStateFreesCapacity(t *testing.T) {
	const ttl = time.Hour
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	r := newProducerRegistryWithClock(1, ttl, func() time.Time { return now })

	firstKey := producerKey{streamID: "/expired", producerID: "p1"}
	entry := r.acquire(firstKey)
	if entry == nil {
		t.Fatal("initial producer was not admitted")
	}
	entry.commit(0, 0)
	r.release(entry)

	// The stream may have expired in Storage without passing through Handler's
	// Delete path. Once the producer state itself is idle for its documented TTL,
	// capacity admission must be able to reclaim it rather than returning 429
	// forever for unrelated streams.
	now = now.Add(ttl)
	secondKey := producerKey{streamID: "/new", producerID: "p2"}
	entry = r.acquire(secondKey)
	if entry == nil {
		t.Fatal("new producer was rejected after the only committed state expired")
	}
	entry.commit(0, 0)
	r.release(entry)

	if got := r.len(); got != 1 {
		t.Fatalf("registry contains %d committed entries, want only the replacement", got)
	}
	if stale := r.acquire(firstKey); stale != nil {
		r.release(stale)
		t.Fatal("expired producer state still occupied capacity")
	}
}

func TestProducerRegistry_NonExpiredAccessRefreshesTTL(t *testing.T) {
	const ttl = time.Hour
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	r := newProducerRegistryWithClock(1, ttl, func() time.Time { return now })
	key := producerKey{streamID: "/stream", producerID: "p1"}

	entry := r.acquire(key)
	entry.commit(0, 0)
	r.release(entry)

	// A duplicate just before expiry is activity and starts a fresh idle window.
	now = now.Add(45 * time.Minute)
	entry = r.acquire(key)
	if got := entry.validate(0, 0).outcome; got != producerDuplicate {
		r.release(entry)
		t.Fatalf("existing producer decision = %v, want duplicate", got)
	}
	r.release(entry)

	// This is past the original deadline but still within the refreshed window.
	now = now.Add(30 * time.Minute)
	if newcomer := r.acquire(producerKey{streamID: "/other", producerID: "p2"}); newcomer != nil {
		r.release(newcomer)
		t.Fatal("capacity admission evicted non-expired producer state")
	}
	entry = r.acquire(key)
	if got := entry.validate(0, 0).outcome; got != producerDuplicate {
		r.release(entry)
		t.Fatalf("refreshed producer decision = %v, want duplicate", got)
	}
	r.release(entry)
}

func TestProducerRegistry_DoesNotPrunePinnedState(t *testing.T) {
	const ttl = time.Hour
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	r := newProducerRegistryWithClock(1, ttl, func() time.Time { return now })
	key := producerKey{streamID: "/stream", producerID: "p1"}

	entry := r.acquire(key)
	entry.commit(0, 0)
	r.release(entry)

	// Hold the producer lock across more than one TTL. It represents an active
	// request and must not be detached just because another producer needs room.
	entry = r.acquire(key)
	now = now.Add(2 * ttl)
	if newcomer := r.acquire(producerKey{streamID: "/other", producerID: "p2"}); newcomer != nil {
		r.release(newcomer)
		r.release(entry)
		t.Fatal("capacity admission pruned pinned producer state")
	}
	r.release(entry)
}

// TestProducerRegistry_ForgetDropsStream covers per-stream cleanup.
func TestProducerRegistry_ForgetDropsStream(t *testing.T) {
	r := newProducerRegistry(16, defaultProducerStateTTL)
	for _, streamID := range []string{"/a", "/a", "/b"} {
		entry := r.acquire(producerKey{streamID: streamID, producerID: streamID + "-p"})
		entry.commit(0, 1)
		r.release(entry)
	}

	r.forget("/a")

	if got := r.len(); got != 1 {
		t.Fatalf("registry holds %d entries after forget, want 1", got)
	}
	entry := r.acquire(producerKey{streamID: "/b", producerID: "/b-p"})
	known := entry.state.known
	r.release(entry)
	if !known {
		t.Error("forget(\"/a\") dropped state for stream /b")
	}
}

// TestHandler_ReservedDSPath covers the reserved __ds namespace (spec Section 6):
// application streams must not be able to squat on it.
func TestHandler_ReservedDSPath(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, nil)

	paths := []string{"/__ds", "/__ds/subscriptions/abc", "/app/__ds/subscriptions/abc"}
	methods := []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodHead, http.MethodDelete}

	for _, path := range paths {
		for _, method := range methods {
			t.Run(method+" "+path, func(t *testing.T) {
				req := httptest.NewRequest(method, path, strings.NewReader("x"))
				req.Header.Set("Content-Type", "text/plain")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				if rec.Code != http.StatusNotFound {
					t.Errorf("status = %d, want 404", rec.Code)
				}
				if _, err := storage.Head(context.Background(), path); err == nil {
					t.Error("reserved path was created as a stream")
				}
			})
		}
	}

	// A stream whose name merely contains the reserved text is unaffected.
	req := httptest.NewRequest(http.MethodPut, "/my__ds-stream", nil)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201: only a whole __ds path segment is reserved", rec.Code)
	}
}

// TestWriteStorageError covers error mapping at the HTTP boundary: wrapped
// protocol errors keep their status, ErrClosed is retryable (503), and
// unclassified errors must not leak internal detail into the response body.
func TestWriteStorageError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantNoLeak  string
		wantMessage string
	}{
		{
			name:       "wrapped protocol error",
			err:        fmt.Errorf("badger read: %w", newError(codeGone, "offset expired")),
			wantStatus: http.StatusGone,
		},
		{
			name:       "wrapped sentinel",
			err:        fmt.Errorf("badger read: %w", ErrNotFound),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "closed storage is retryable",
			err:        fmt.Errorf("store: %w", ErrClosed),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:        "unclassified error is generic",
			err:         errors.New("open /var/lib/durablestream/data.db: permission denied"),
			wantStatus:  http.StatusInternalServerError,
			wantNoLeak:  "/var/lib/durablestream",
			wantMessage: "internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeStorageError(rec, tt.err)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			body := rec.Body.String()
			if tt.wantNoLeak != "" && strings.Contains(body, tt.wantNoLeak) {
				t.Errorf("response body leaks internal detail %q: %s", tt.wantNoLeak, body)
			}
			if tt.wantMessage != "" && !strings.Contains(body, tt.wantMessage) {
				t.Errorf("response body = %s, want it to contain %q", body, tt.wantMessage)
			}
		})
	}
}

// TestValidateOffset_RejectsSlash covers the spec addition of "/" to the set of
// characters an offset must not contain (Section 8).
func TestValidateOffset_RejectsSlash(t *testing.T) {
	for _, offset := range []string{"/", "abc/def", "0000_0000/"} {
		if err := validateOffset(offset); err == nil {
			t.Errorf("validateOffset(%q) = nil, want an error: offsets must not contain '/'", offset)
		}
	}
}

// TestHandler_SSE_ContentTypes covers SSE binary support (spec Section 5.8):
// every content type is served, and content types that are neither text/* nor
// application/json are base64-encoded and announced with
// Stream-SSE-Data-Encoding: base64.
func TestHandler_SSE_ContentTypes(t *testing.T) {
	tests := []struct {
		name         string
		contentType  string
		data         []byte
		wantEncoding string
		wantDataLine string
	}{
		{
			name:         "binary is base64 encoded",
			contentType:  "application/octet-stream",
			data:         []byte{0x00, 0x01, 0x02, 0xff},
			wantEncoding: "base64",
			wantDataLine: "data:AAEC/w==",
		},
		{
			name:         "unknown type is base64 encoded",
			contentType:  "application/protobuf",
			data:         []byte("hi"),
			wantEncoding: "base64",
			wantDataLine: "data:aGk=",
		},
		{
			name:         "text is sent verbatim",
			contentType:  "text/plain",
			data:         []byte("hello"),
			wantEncoding: "",
			wantDataLine: "data:hello",
		},
		{
			name:         "json is sent verbatim",
			contentType:  "application/json",
			data:         []byte(`{"a":1}`),
			wantEncoding: "",
			wantDataLine: `data:[{"a":1}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newTestStorage()
			handler := NewHandler(storage, &HandlerConfig{SSECloseAfter: 50 * time.Millisecond})

			if _, err := storage.Create(ctx, "/stream", StreamConfig{ContentType: tt.contentType}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			if _, err := storage.Append(ctx, "/stream", tt.data, ""); err != nil {
				t.Fatalf("Append: %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, "/stream?offset="+string(formatTestOffset(0))+"&live=sse", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (SSE serves all content types)", rec.Code)
			}
			if got := rec.Header().Get(protocol.HeaderStreamSSEDataEncoding); got != tt.wantEncoding {
				t.Errorf("%s = %q, want %q", protocol.HeaderStreamSSEDataEncoding, got, tt.wantEncoding)
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: data\n"+tt.wantDataLine+"\n\n") {
				t.Errorf("body missing data event %q:\n%s", tt.wantDataLine, body)
			}
			if tt.wantEncoding == "base64" {
				encoded := strings.TrimPrefix(tt.wantDataLine, "data:")
				decoded, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					t.Fatalf("data line is not standard base64: %v", err)
				}
				if !bytes.Equal(decoded, tt.data) {
					t.Errorf("decoded data = %v, want %v", decoded, tt.data)
				}
			}
			// Control events are never encoded (Section 5.8).
			if !strings.Contains(body, `event: control`+"\n"+`data:{"streamNextOffset":`) {
				t.Errorf("control event missing or encoded:\n%s", body)
			}
		})
	}
}

// Pretty-printed JSON contains literal line terminators as insignificant
// whitespace. SSE requires every physical payload line to carry a data: field;
// otherwise EventSource discards the continuation and delivers truncated JSON.
func TestHandler_SSE_MultilineJSONPrefixesEveryLine(t *testing.T) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{SSECloseAfter: 10 * time.Millisecond})
	if _, err := storage.Create(context.Background(), "/stream", StreamConfig{ContentType: "application/json"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := storage.Append(context.Background(), "/stream", []byte("{\n  \"a\": 1\n}"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/stream?offset=-1&live=sse", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	dataBlock := strings.SplitN(rec.Body.String(), "\n\n", 2)[0]
	lines := strings.Split(dataBlock, "\n")
	if len(lines) < 2 || lines[0] != "event: data" {
		t.Fatalf("first SSE block is not a data event: %q", dataBlock)
	}
	var payload strings.Builder
	for i, line := range lines[1:] {
		if !strings.HasPrefix(line, "data:") {
			t.Fatalf("payload line %q has no data: prefix in block %q", line, dataBlock)
		}
		value := strings.TrimPrefix(line, "data:")
		value = strings.TrimPrefix(value, " ")
		if i > 0 {
			payload.WriteByte('\n')
		}
		payload.WriteString(value)
	}

	var got []map[string]int
	if err := json.Unmarshal([]byte(payload.String()), &got); err != nil {
		t.Fatalf("SSE data payload %q is not complete JSON: %v", payload.String(), err)
	}
	if len(got) != 1 || got[0]["a"] != 1 {
		t.Fatalf("decoded payload = %#v, want [{a:1}]", got)
	}
}
