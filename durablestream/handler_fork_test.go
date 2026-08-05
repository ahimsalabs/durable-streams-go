package durablestream_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/memorystorage"
)

type forkReplayRaceStorage struct {
	durablestream.Storage
	forks  durablestream.ForkStorage
	source string
	target string
	once   sync.Once
	err    error
}

func (s *forkReplayRaceStorage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	triggered := false
	if streamID == s.source {
		s.once.Do(func() {
			triggered = true
			sourceInfo, err := s.Storage.Head(ctx, s.source)
			if err != nil {
				s.err = err
				return
			}
			_, _, err = s.forks.CreateFork(ctx, s.target, durablestream.ForkRequest{
				SourceStreamID:      s.source,
				SourceIncarnationID: sourceInfo.IncarnationID,
				Config:              durablestream.StreamConfig{ContentType: sourceInfo.ContentType},
			}, [][]byte{[]byte(" child")})
			if err == nil {
				err = s.Storage.Delete(ctx, s.source)
			}
			s.err = err
		})
	}
	if triggered && s.err != nil {
		return nil, s.err
	}
	return s.Storage.Head(ctx, streamID)
}

func (s *forkReplayRaceStorage) CreateFork(
	ctx context.Context,
	targetStreamID string,
	req durablestream.ForkRequest,
	initialMessages [][]byte,
) (bool, *durablestream.StreamInfo, error) {
	return s.forks.CreateFork(ctx, targetStreamID, req, initialMessages)
}

func forkResponse(
	t *testing.T,
	handler http.Handler,
	method, target, body string,
	headers map[string]string,
	wantStatus int,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d: %s", method, target, rec.Code, wantStatus, rec.Body.String())
	}
	return rec
}

func TestHandlerFork_CreateAndReadInheritedData(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	forkResponse(t, handler, http.MethodPut, "/source", "source", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusCreated)

	forkResponse(t, handler, http.MethodPut, "/fork", "", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/source",
	}, http.StatusCreated)
	forkResponse(t, handler, http.MethodPost, "/source", " source-only", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusNoContent)
	forkResponse(t, handler, http.MethodPost, "/fork", " fork-only", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusNoContent)

	read := forkResponse(t, handler, http.MethodGet, "/fork?offset=-1", "", nil, http.StatusOK)
	if got := read.Body.String(); got != "source fork-only" {
		t.Errorf("fork body = %q, want %q", got, "source fork-only")
	}
}

func TestHandlerFork_InheritsJSONBeforeParsingInitialBody(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	forkResponse(t, handler, http.MethodPut, "/json-source", `{"source":1}`, map[string]string{
		"Content-Type": "application/json",
	}, http.StatusCreated)

	created := forkResponse(t, handler, http.MethodPut, "/json-fork", `[{"fork":2},{"fork":3}]`, map[string]string{
		protocol.HeaderStreamForkedFrom: "/json-source",
	}, http.StatusCreated)
	if got := created.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("fork Content-Type = %q, want application/json", got)
	}

	read := forkResponse(t, handler, http.MethodGet, "/json-fork?offset=-1", "", nil, http.StatusOK)
	var got []map[string]int
	if err := json.Unmarshal(read.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode fork JSON %q: %v", read.Body.String(), err)
	}
	if len(got) != 3 || got[0]["source"] != 1 || got[1]["fork"] != 2 || got[2]["fork"] != 3 {
		t.Errorf("fork JSON = %v, want source followed by two initial fork messages", got)
	}
}

func TestHandlerFork_ExplicitSubOffsetAndInvalidHeaders(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	created := forkResponse(t, handler, http.MethodPut, "/sub-source", "", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusCreated)
	zero := created.Header().Get(protocol.HeaderStreamNextOffset)
	forkResponse(t, handler, http.MethodPost, "/sub-source", "abcdef", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusNoContent)

	forkResponse(t, handler, http.MethodPut, "/sub-fork", "X", map[string]string{
		"Content-Type":                     "text/plain",
		protocol.HeaderStreamForkedFrom:    "/sub-source",
		protocol.HeaderStreamForkOffset:    zero,
		protocol.HeaderStreamForkSubOffset: "3",
	}, http.StatusCreated)
	read := forkResponse(t, handler, http.MethodGet, "/sub-fork?offset=-1", "", nil, http.StatusOK)
	if got := read.Body.String(); got != "abcX" {
		t.Errorf("sub-offset fork body = %q, want %q", got, "abcX")
	}

	invalid := []struct {
		name    string
		headers map[string]string
	}{
		{
			name: "offset without source",
			headers: map[string]string{
				protocol.HeaderStreamForkOffset: zero,
			},
		},
		{
			name: "sub-offset without source",
			headers: map[string]string{
				protocol.HeaderStreamForkSubOffset: "0",
			},
		},
		{
			name: "malformed offset",
			headers: map[string]string{
				protocol.HeaderStreamForkedFrom: "/sub-source",
				protocol.HeaderStreamForkOffset: "bad offset",
			},
		},
		{
			name: "malformed sub-offset",
			headers: map[string]string{
				protocol.HeaderStreamForkedFrom:    "/sub-source",
				protocol.HeaderStreamForkOffset:    zero,
				protocol.HeaderStreamForkSubOffset: "05",
			},
		},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			forkResponse(t, handler, http.MethodPut, "/invalid-"+strings.ReplaceAll(test.name, " ", "-"), "", test.headers, http.StatusBadRequest)
		})
	}
}

func TestHandlerFork_CanCreateClosedTarget(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	forkResponse(t, handler, http.MethodPut, "/closed-source", "history", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusCreated)

	created := forkResponse(t, handler, http.MethodPut, "/closed-fork", "", map[string]string{
		protocol.HeaderStreamForkedFrom: "/closed-source",
		protocol.HeaderStreamClosed:     "true",
	}, http.StatusCreated)
	if got := created.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("create %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	tail := created.Header().Get(protocol.HeaderStreamNextOffset)

	read := forkResponse(t, handler, http.MethodGet, "/closed-fork?offset=-1", "", nil, http.StatusOK)
	if got := read.Body.String(); got != "history" {
		t.Errorf("closed fork body = %q, want history", got)
	}
	if got := read.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("read %s = %q, want true", protocol.HeaderStreamClosed, got)
	}

	rejected := forkResponse(t, handler, http.MethodPost, "/closed-fork", "late", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusConflict)
	if got := rejected.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("append conflict %s = %q, want true", protocol.HeaderStreamClosed, got)
	}
	if got := rejected.Header().Get(protocol.HeaderStreamNextOffset); got != tail {
		t.Errorf("append conflict tail = %q, want %q", got, tail)
	}
}

func TestHandlerFork_SoftDeletePreservesChildAndBlocksSource(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), nil)
	forkResponse(t, handler, http.MethodPut, "/soft-source", "preserved", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusCreated)
	forkHeaders := map[string]string{
		protocol.HeaderStreamForkedFrom: "/soft-source",
	}
	forkResponse(t, handler, http.MethodPut, "/soft-fork", " child", forkHeaders, http.StatusCreated)
	forkResponse(t, handler, http.MethodDelete, "/soft-source", "", nil, http.StatusNoContent)

	// The existing target is durable proof that the exact fork PUT committed.
	// Its retry remains idempotent even though direct source access is now 410.
	forkResponse(t, handler, http.MethodPut, "/soft-fork", " child", forkHeaders, http.StatusOK)

	for _, request := range []struct {
		method, target, body string
		headers              map[string]string
	}{
		{method: http.MethodGet, target: "/soft-source?offset=-1"},
		{method: http.MethodHead, target: "/soft-source"},
		{method: http.MethodPost, target: "/soft-source", body: "late", headers: map[string]string{"Content-Type": "text/plain"}},
		{method: http.MethodDelete, target: "/soft-source"},
	} {
		forkResponse(t, handler, request.method, request.target, request.body, request.headers, http.StatusGone)
	}

	forkResponse(t, handler, http.MethodPut, "/soft-source", "replacement", map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusConflict)
	forkResponse(t, handler, http.MethodPut, "/soft-fork-2", "", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/soft-source",
	}, http.StatusConflict)

	read := forkResponse(t, handler, http.MethodGet, "/soft-fork?offset=-1", "", nil, http.StatusOK)
	if got := read.Body.String(); got != "preserved child" {
		t.Errorf("child body after source soft-delete = %q, want preserved child", got)
	}
}

func TestHandlerFork_RetryWinsTargetSourceDeletionRace(t *testing.T) {
	base := memorystorage.New()
	storage := &forkReplayRaceStorage{
		Storage: base,
		forks:   base,
		source:  "/race-source",
		target:  "/race-fork",
	}
	handler := durablestream.NewHandler(storage, nil)
	created, err := base.Create(t.Context(), storage.source, durablestream.StreamConfig{ContentType: "text/plain"})
	if err != nil || !created {
		t.Fatalf("create source = %v, %v; want true, nil", created, err)
	}
	if _, err := base.Append(t.Context(), storage.source, []byte("preserved"), ""); err != nil {
		t.Fatalf("append source: %v", err)
	}

	// The storage hook commits the same fork and deletes its source between
	// the Handler's first target lookup and its source metadata lookup. The
	// in-flight request must recognize the committed target as an exact replay.
	forkResponse(t, handler, http.MethodPut, storage.target, " child", map[string]string{
		protocol.HeaderStreamForkedFrom: storage.source,
	}, http.StatusOK)
	if storage.err != nil {
		t.Fatalf("race setup failed: %v", storage.err)
	}

	read := forkResponse(t, handler, http.MethodGet, storage.target+"?offset=-1", "", nil, http.StatusOK)
	if got := read.Body.String(); got != "preserved child" {
		t.Fatalf("fork body = %q, want preserved child", got)
	}
}

func TestHandlerFork_CORSAllowsCreationHeaders(t *testing.T) {
	handler := durablestream.NewHandler(memorystorage.New(), &durablestream.HandlerConfig{EnableCORS: true})
	rec := forkResponse(t, handler, http.MethodOptions, "/fork", "", nil, http.StatusNoContent)

	allowed := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	for _, want := range []string{
		strings.ToLower(protocol.HeaderStreamForkedFrom),
		strings.ToLower(protocol.HeaderStreamForkOffset),
		strings.ToLower(protocol.HeaderStreamForkSubOffset),
	} {
		if !strings.Contains(allowed, want) {
			t.Errorf("Access-Control-Allow-Headers = %q, want it to include %q", allowed, want)
		}
	}
}

func TestHandlerFork_PathExtractionAndValidation(t *testing.T) {
	t.Run("default extractor supports a mounted handler", func(t *testing.T) {
		handler := http.StripPrefix("/streams", durablestream.NewHandler(memorystorage.New(), nil))
		forkResponse(t, handler, http.MethodPut, "/streams/source", "mounted", map[string]string{
			"Content-Type": "text/plain",
		}, http.StatusCreated)
		forkResponse(t, handler, http.MethodPut, "/streams/fork", "", map[string]string{
			protocol.HeaderStreamForkedFrom: "/streams/source",
		}, http.StatusCreated)
		read := forkResponse(t, handler, http.MethodGet, "/streams/fork?offset=-1", "", nil, http.StatusOK)
		if got := read.Body.String(); got != "mounted" {
			t.Errorf("mounted fork body = %q, want mounted", got)
		}

		for i, source := range []string{
			"/outside/source",
			"/streams-other/source",
			"https://evil.example/streams/source",
			"/streams/../source",
		} {
			forkResponse(t, handler, http.MethodPut, "/streams/rejected-"+string(rune('a'+i)), "", map[string]string{
				protocol.HeaderStreamForkedFrom: source,
			}, http.StatusBadRequest)
		}
	})

	t.Run("custom extractor maps non-suffix stream IDs", func(t *testing.T) {
		var seen []string
		handler := durablestream.NewHandler(memorystorage.New(), &durablestream.HandlerConfig{
			PathExtractor: func(r *http.Request) string {
				return "tenant:" + strings.TrimPrefix(r.URL.Path, "/custom/")
			},
			ForkPathExtractor: func(_ *http.Request, sourcePath string) (string, error) {
				seen = append(seen, sourcePath)
				return "tenant:" + strings.TrimPrefix(sourcePath, "/custom/"), nil
			},
		})
		forkResponse(t, handler, http.MethodPut, "/custom/source", "custom", map[string]string{
			"Content-Type": "text/plain",
		}, http.StatusCreated)
		forkResponse(t, handler, http.MethodPut, "/custom/fork", "", map[string]string{
			protocol.HeaderStreamForkedFrom: "/custom/source",
		}, http.StatusCreated)
		if len(seen) != 1 || seen[0] != "/custom/source" {
			t.Fatalf("ForkPathExtractor saw %v, want [/custom/source]", seen)
		}
		read := forkResponse(t, handler, http.MethodGet, "/custom/fork?offset=-1", "", nil, http.StatusOK)
		if got := read.Body.String(); got != "custom" {
			t.Errorf("custom-mapped fork body = %q, want custom", got)
		}

		forkResponse(t, handler, http.MethodPut, "/custom/rejected", "", map[string]string{
			protocol.HeaderStreamForkedFrom: "https://evil.example/custom/source",
		}, http.StatusBadRequest)
		if len(seen) != 1 {
			t.Errorf("unsafe source reached custom ForkPathExtractor: %v", seen)
		}
	})
}
