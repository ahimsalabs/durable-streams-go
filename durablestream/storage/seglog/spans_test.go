package seglog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	storagepkg "github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func TestReadSpans_PreservesReadOffsetsLimitsAndFallback(t *testing.T) {
	s := newSpanTestStorage(t)
	for _, payload := range [][]byte{[]byte("one"), []byte("twenty"), []byte("three")} {
		if _, err := s.Append(t.Context(), "s", payload, ""); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ReadSpans(t.Context(), "s", "0_1", 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSpans(got.Spans) })
	if got.NextOffset != storagepkg.FormatSimpleOffset(2) || got.TailOffset != storagepkg.FormatSimpleOffset(3) || len(got.Spans) != 1 {
		t.Fatalf("result = %+v, spans=%d", got, len(got.Spans))
	}
	var out bytes.Buffer
	if _, err := got.Spans[0].WriteTo(&out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "twenty" {
		t.Fatalf("payload = %q", out.String())
	}
}

func TestReadSpan_WriteToHonorsCancellationAndCloseIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	span := &ownedReadSpan{ctx: ctx, data: bytes.Repeat([]byte("x"), 128<<10)}
	w := &cancelWriter{cancel: cancel}
	_, err := span.WriteTo(w)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteTo error = %v, want context.Canceled", err)
	}
	if err := span.Close(); err != nil {
		t.Fatal(err)
	}
	if err := span.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

type cancelWriter struct{ cancel context.CancelFunc }

func (w *cancelWriter) Write(p []byte) (int, error) {
	w.cancel()
	return len(p), nil
}

func TestReadSpans_PinDefersSegmentUnlinkUntilClose(t *testing.T) {
	s := newSpanTestStorage(t)
	if _, err := s.Append(t.Context(), "s", []byte("sealed payload"), ""); err != nil {
		t.Fatal(err)
	}
	sealSpanTestStream(t, s, "s")
	state, _ := s.streams.Load("s")
	path := state.snapshot().sealed[0].path

	got, err := s.ReadSpans(t.Context(), "s", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Spans[0].(*fileReadSpan); !ok {
		t.Fatalf("span type = %T, want file-backed", got.Spans[0])
	}
	if err := s.fdCache.unlink(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned segment was unlinked: %v", err)
	}
	if err := got.Spans[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("segment after release: %v", err)
	}
}

func TestHandlerCatchup_FileSpanWireMatchesCopiedPath(t *testing.T) {
	s := newSpanTestStorage(t)
	for _, payload := range [][]byte{{0, 1, 2, 3}, []byte("sealed-binary\x00tail")} {
		if _, err := s.Append(t.Context(), "s", payload, ""); err != nil {
			t.Fatal(err)
		}
	}
	sealSpanTestStream(t, s, "s")

	serve := func(storage durablestream.Storage) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/s?offset=-1&cursor=wire", nil)
		request.Header.Set("If-None-Match", `"not-a-match"`)
		durablestream.NewHandler(storage, nil).ServeHTTP(recorder, request)
		return recorder
	}
	direct := serve(s)
	copied := serve(struct{ durablestream.Storage }{Storage: s})
	if direct.Code != copied.Code || !reflect.DeepEqual(direct.Header(), copied.Header()) || !bytes.Equal(direct.Body.Bytes(), copied.Body.Bytes()) {
		t.Fatalf("span response differs from copied response:\nspan: status=%d headers=%v body=%q\ncopy: status=%d headers=%v body=%q",
			direct.Code, direct.Header(), direct.Body.Bytes(), copied.Code, copied.Header(), copied.Body.Bytes())
	}
}

func newSpanTestStorage(t *testing.T) *Storage {
	t.Helper()
	opts := benchmarkOptions(t.TempDir())
	opts.Partitions = 1
	opts.SyncWrites = SyncWritesDisabled
	opts.GroupMaxBytes = 1
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	return s
}

func sealSpanTestStream(t interface {
	Helper()
	Fatal(args ...any)
}, s *Storage, id string,
) {
	t.Helper()
	state, _ := s.streams.Load(id)
	state.mu.Lock()
	state.forceSeal = true
	state.mu.Unlock()
	s.materializeRound(s.parts[0])
	if len(state.snapshot().sealed) == 0 {
		t.Fatal("stream did not seal")
	}
}

var _ io.Writer = (*cancelWriter)(nil)
