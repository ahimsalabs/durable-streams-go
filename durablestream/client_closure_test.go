package durablestream

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

func TestStreamWriter_CloseContext(t *testing.T) {
	t.Parallel()

	var got transport.AppendRequest
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain", NextOffset: "0_1"}, nil
		},
		appendFunc: func(_ context.Context, req transport.AppendRequest) (*transport.AppendResponse, error) {
			got = req
			return &transport.AppendResponse{NextOffset: "0_2", Closed: true}, nil
		},
	}

	writer, err := NewClientWithTransport(ft, nil).Writer(t.Context(), "/stream")
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	if err := writer.CloseContext(t.Context(), []byte("final")); err != nil {
		t.Fatalf("CloseContext() error = %v", err)
	}
	if !got.Close || got.Path != "/stream" || got.ContentType != "text/plain" || string(got.Data) != "final" {
		t.Fatalf("append request = %+v, want atomic final append", got)
	}
	if writer.Offset() != Offset("0_2") {
		t.Fatalf("writer offset = %q, want 0_2", writer.Offset())
	}
}

func TestStreamWriter_SendOptionsCanCloseWithSequence(t *testing.T) {
	t.Parallel()

	var got transport.AppendRequest
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		appendFunc: func(_ context.Context, req transport.AppendRequest) (*transport.AppendResponse, error) {
			got = req
			return &transport.AppendResponse{NextOffset: "0_0", Closed: true}, nil
		},
	}
	writer, err := NewClientWithTransport(ft, nil).Writer(t.Context(), "/stream")
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	if err := writer.SendContext(t.Context(), nil, &SendOptions{Seq: "last", Close: true}); err != nil {
		t.Fatalf("SendContext() error = %v", err)
	}
	if !got.Close || got.Seq != "last" || len(got.Data) != 0 {
		t.Fatalf("append request = %+v, want sequenced close-only request", got)
	}
}

func TestClientClosure_RequiresTransportConfirmation(t *testing.T) {
	t.Parallel()

	t.Run("writer", func(t *testing.T) {
		ft := &fakeTransport{
			headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
				return &transport.HeadResponse{ContentType: "text/plain", NextOffset: "0_1"}, nil
			},
			appendFunc: func(context.Context, transport.AppendRequest) (*transport.AppendResponse, error) {
				return &transport.AppendResponse{NextOffset: "0_2"}, nil
			},
		}
		writer, err := NewClientWithTransport(ft, nil).Writer(t.Context(), "/stream")
		if err != nil {
			t.Fatalf("Writer() error = %v", err)
		}
		if err := writer.CloseContext(t.Context(), []byte("final")); !errors.Is(err, ErrParseError) {
			t.Fatalf("CloseContext() error = %v, want ErrParseError", err)
		}
		if got := writer.Offset(); got != Offset("0_1") {
			t.Fatalf("writer offset after unconfirmed close = %q, want 0_1", got)
		}
	})

	t.Run("create", func(t *testing.T) {
		ft := &fakeTransport{
			createFunc: func(context.Context, transport.CreateRequest) (*transport.CreateResponse, error) {
				return &transport.CreateResponse{NextOffset: "0_2"}, nil
			},
		}
		_, err := NewClientWithTransport(ft, nil).Create(t.Context(), "/stream", &CreateOptions{Closed: true})
		if !errors.Is(err, ErrParseError) {
			t.Fatalf("Create() error = %v, want ErrParseError", err)
		}
	})
}

func TestStreamWriter_ClosedConflictUpdatesFinalOffset(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain", NextOffset: "0_1"}, nil
		},
		appendFunc: func(context.Context, transport.AppendRequest) (*transport.AppendResponse, error) {
			return nil, &transport.Error{
				Code: "STREAM_CLOSED", Message: "already closed", StatusCode: 409, FinalOffset: "0_9",
			}
		},
	}
	writer, err := NewClientWithTransport(ft, nil).Writer(t.Context(), "/stream")
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	err = writer.SendContext(t.Context(), []byte("late"), nil)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendContext() error = %v, want ErrStreamClosed", err)
	}
	var closedErr *StreamClosedError
	if !errors.As(err, &closedErr) || closedErr.FinalOffset != Offset("0_9") || closedErr.Path != "/stream" {
		t.Fatalf("SendContext() error = %#v, want typed final offset 0_9", err)
	}
	if got := writer.Offset(); got != Offset("0_9") {
		t.Fatalf("writer offset = %q, want final offset 0_9", got)
	}
}

func TestConvertTransportError_StreamClosed(t *testing.T) {
	t.Parallel()

	err := convertTransportErrorWithPath(&transport.Error{
		Code: "STREAM_CLOSED", Message: "already complete", StatusCode: 409, FinalOffset: "0_7",
	}, "/stream")
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("converted error = %v, want ErrStreamClosed", err)
	}
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrClosed) {
		t.Fatalf("ErrStreamClosed was conflated with another sentinel: %v", err)
	}
	var closedErr *StreamClosedError
	if !errors.As(err, &closedErr) || closedErr.FinalOffset != Offset("0_7") || closedErr.Path != "/stream" {
		t.Fatalf("converted error = %#v, want typed final offset 0_7", err)
	}
}

func TestReader_ClosedResultThenEOF(t *testing.T) {
	t.Parallel()

	var reads atomic.Int32
	ft := &fakeTransport{
		readFunc: func(context.Context, transport.ReadRequest) (*transport.ReadResponse, error) {
			reads.Add(1)
			return &transport.ReadResponse{
				Data: []byte("final"), NextOffset: "0_5", Closed: true,
			}, nil
		},
	}
	reader := NewClientWithTransport(ft, nil).Reader("/stream", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	if !result.Closed || !result.UpToDate || string(result.Data) != "final" {
		t.Fatalf("first Read() = %+v, want final data and EOF", result)
	}

	if _, err := reader.Read(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Read() error = %v, want io.EOF", err)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("transport reads = %d, want 1", got)
	}

	reader.Seek(ZeroOffset)
	if _, err := reader.Read(t.Context()); err != nil {
		t.Fatalf("Read() after Seek error = %v", err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("transport reads after Seek = %d, want 2", got)
	}
}

func TestReader_EmptyClosedJSONCatchUpThenEOF(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "application/json", NextOffset: "0_0"}, nil
		},
		readFunc: func(context.Context, transport.ReadRequest) (*transport.ReadResponse, error) {
			return &transport.ReadResponse{NextOffset: "0_0", UpToDate: true, Closed: true}, nil
		},
	}
	reader := NewClientWithTransport(ft, nil).Reader("/empty-json", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	if !result.Closed || !result.UpToDate || len(result.Data) != 0 {
		t.Fatalf("first Read() = %+v, want empty closed result", result)
	}
	if _, err := reader.Read(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Read() error = %v, want io.EOF", err)
	}
}

func TestReader_MessagesStopsCleanlyAtEOF(t *testing.T) {
	t.Parallel()

	var reads atomic.Int32
	ft := &fakeTransport{
		readFunc: func(context.Context, transport.ReadRequest) (*transport.ReadResponse, error) {
			reads.Add(1)
			return &transport.ReadResponse{
				Data: []byte("complete"), NextOffset: "0_8", UpToDate: true, Closed: true,
			}, nil
		},
	}
	reader := NewClientWithTransport(ft, nil).Reader("/stream", ZeroOffset)
	defer reader.Close()

	var messages []string
	for msg, err := range reader.Messages(t.Context()) {
		if err != nil {
			t.Fatalf("Messages() error = %v", err)
		}
		messages = append(messages, msg.String())
	}
	if len(messages) != 1 || messages[0] != "complete" {
		t.Fatalf("messages = %q, want [complete]", messages)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("transport reads = %d, want 1", got)
	}
}

func TestReader_SSEFinalControlStopsReconnect(t *testing.T) {
	t.Parallel()

	var opens atomic.Int32
	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, _ transport.SSERequest) (transport.EventStream, error) {
			opens.Add(1)
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "data", Data: []byte("final")},
					{Type: "control", NextOffset: "0_5", Closed: true, UpToDate: true},
				},
			}, nil
		},
	}
	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/stream", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !result.Closed || string(result.Data) != "final" || result.NextOffset != Offset("0_5") {
		t.Fatalf("Read() = %+v, want final SSE data and EOF", result)
	}
	if _, err := reader.Read(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Read() error = %v, want io.EOF", err)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("SSE opens = %d, want no reconnect after EOF", got)
	}
}

func TestReader_SSEEmptyFinalControl(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{
		headFunc: func(context.Context, transport.HeadRequest) (*transport.HeadResponse, error) {
			return &transport.HeadResponse{ContentType: "text/plain"}, nil
		},
		sseFunc: func(ctx context.Context, _ transport.SSERequest) (transport.EventStream, error) {
			return &scriptedEventStream{
				requestCtx: ctx,
				events: []*transport.Event{
					{Type: "control", NextOffset: "0_0", Closed: true},
				},
			}, nil
		},
	}
	reader := NewClientWithTransport(ft, &TransportClientConfig{ReadMode: ReadModeSSE}).Reader("/empty", ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !result.Closed || !result.UpToDate || len(result.Data) != 0 || result.NextOffset != Offset("0_0") {
		t.Fatalf("Read() = %+v, want empty EOF control", result)
	}
}
