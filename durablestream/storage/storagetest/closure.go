package storagetest

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func requireCloseStorage(t *testing.T, cfg Config, s durablestream.Storage) durablestream.AtomicCloseStorage {
	t.Helper()
	closer, ok := s.(durablestream.AtomicCloseStorage)
	if ok {
		return closer
	}
	if cfg.RequireAtomicClose {
		t.Fatalf("Storage %T does not implement durablestream.AtomicCloseStorage", s)
	}
	t.Skipf("Storage %T does not implement optional AtomicCloseStorage capability", s)
	return nil
}

func testClosedCreate(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	_ = requireCloseStorage(t, cfg, s)
	ctx := t.Context()

	const streamID = "closed-create"
	closedConfig := textConfig()
	closedConfig.Closed = true
	created, err := s.Create(ctx, streamID, closedConfig)
	if err != nil || !created {
		t.Fatalf("Create with Closed=true = (created %v, err %v), want (true, nil)", created, err)
	}

	if info := mustHead(t, s, streamID); !info.Closed {
		t.Error("Head after closed Create reported Closed=false")
	}
	read := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if !read.Closed {
		t.Error("Read after closed Create reported Closed=false")
	}
	if len(read.Messages) != 0 {
		t.Errorf("closed empty stream contains %q, want no messages", payloads(read))
	}

	// WaitForData must expose permanent EOF immediately, even when the stream
	// has never contained a message.
	waited := awaitWait(t, "waiting on a stream created closed", startWait(t, s, streamID, durablestream.ZeroOffset, 0))
	if waited.err != nil {
		t.Fatalf("WaitForData on a stream created closed: %v", waited.err)
	}
	if waited.res == nil || !waited.res.Closed || len(waited.res.Messages) != 0 {
		t.Errorf("WaitForData on a stream created closed = %+v, want an empty closed result", waited.res)
	}

	_, err = s.Append(ctx, streamID, []byte("too late"), "")
	assertErrorIs(t, "Append to a stream created closed", err, durablestream.ErrStreamClosed)
	if atomic, ok := s.(durablestream.AtomicBatchStorage); ok {
		_, err = atomic.AppendBatch(ctx, streamID, [][]byte{[]byte("also too late")}, "")
		assertErrorIs(t, "AppendBatch to a stream created closed", err, durablestream.ErrStreamClosed)
	}

	created, err = s.Create(ctx, streamID, closedConfig)
	if err != nil || created {
		t.Errorf("idempotent closed Create = (created %v, err %v), want (false, nil)", created, err)
	}
	created, err = s.Create(ctx, streamID, textConfig())
	assertErrorIs(t, "open Create over a closed stream", err, durablestream.ErrConflict)
	if created {
		t.Error("open Create over a closed stream reported created=true")
	}

	// Closure belongs to an incarnation, not the stream ID.
	mustDelete(t, s, streamID)
	mustCreate(t, s, streamID)
	if info := mustHead(t, s, streamID); info.Closed {
		t.Error("stream recreated after deleting a closed incarnation remained closed")
	}
	mustAppend(t, s, streamID, "fresh")

	// Atomic creation with initial messages must publish the body and EOF in a
	// single snapshot when that separate optional capability is available.
	if atomic, ok := s.(durablestream.AtomicBatchStorage); ok {
		const populatedID = "closed-create-populated"
		created, tail, createErr := atomic.CreateWithMessages(ctx, populatedID, closedConfig, [][]byte{[]byte("first"), []byte("second")})
		if createErr != nil || !created {
			t.Fatalf("closed CreateWithMessages = (created %v, err %v), want (true, nil)", created, createErr)
		}
		result := mustRead(t, s, populatedID, durablestream.ZeroOffset, 0)
		assertPayloads(t, "closed CreateWithMessages", payloads(result), []string{"first", "second"})
		if !result.Closed || !mustHead(t, s, populatedID).Closed {
			t.Error("closed CreateWithMessages did not expose Closed=true through Read and Head")
		}
		if result.TailOffset != tail {
			t.Errorf("closed CreateWithMessages tail = %q, Read tail = %q", tail, result.TailOffset)
		}
	}
}

func testAtomicCloseFinalBatch(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	closer := requireCloseStorage(t, cfg, s)
	ctx := t.Context()

	const streamID = "close-final-batch"
	mustCreate(t, s, streamID)
	if _, err := s.Append(ctx, streamID, []byte("initial"), "0001"); err != nil {
		t.Fatalf("initial Append: %v", err)
	}

	// Validation failure must leave both closure and its sequence uncommitted.
	_, err := closer.CloseStream(ctx, streamID, [][]byte{[]byte("valid"), nil}, "0002")
	assertErrorIs(t, "CloseStream with an empty message", err, durablestream.ErrBadRequest)
	if mustHead(t, s, streamID).Closed {
		t.Fatal("rejected CloseStream marked the stream closed")
	}
	if cfg.MaxMessageSize > 0 {
		oversized := bytes.Repeat([]byte("x"), cfg.MaxMessageSize+1)
		_, err = closer.CloseStream(ctx, streamID, [][]byte{oversized}, "0002")
		assertErrorIs(t, "CloseStream with an oversized message", err, durablestream.ErrPayloadTooLarge)
		if mustHead(t, s, streamID).Closed {
			t.Fatal("oversized CloseStream marked the stream closed")
		}
	}

	input := [][]byte{[]byte("final-one"), []byte("final-two")}
	tail, err := closer.CloseStream(ctx, streamID, input, "0002")
	if err != nil {
		t.Fatalf("CloseStream: %v", err)
	}
	for _, message := range input {
		message[0] = 'X'
	}

	result := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "Read after CloseStream", payloads(result), []string{"initial", "final-one", "final-two"})
	if !result.Closed {
		t.Error("Read after CloseStream reported Closed=false")
	}
	info := mustHead(t, s, streamID)
	if !info.Closed {
		t.Error("Head after CloseStream reported Closed=false")
	}
	if result.TailOffset != tail || info.NextOffset != tail {
		t.Errorf("CloseStream tail = %q, Read tail = %q, Head tail = %q", tail, result.TailOffset, info.NextOffset)
	}

	// Closed describes the stream snapshot even when a byte-limited read has
	// not reached that snapshot's tail. Protocol layers decide when to reveal
	// EOF to a paged client.
	partial := mustRead(t, s, streamID, durablestream.ZeroOffset, len("initial"))
	if !partial.Closed || len(partial.Messages) != 1 {
		t.Errorf("limited Read after close = %+v, want one message with Closed=true", partial)
	}

	_, err = s.Append(ctx, streamID, []byte("after EOF"), "0003")
	assertErrorIs(t, "Append after CloseStream", err, durablestream.ErrStreamClosed)
	if atomic, ok := s.(durablestream.AtomicBatchStorage); ok {
		_, err = atomic.AppendBatch(ctx, streamID, [][]byte{[]byte("after"), []byte("EOF")}, "0003")
		assertErrorIs(t, "AppendBatch after CloseStream", err, durablestream.ErrStreamClosed)
	}
	_, err = closer.CloseStream(ctx, streamID, [][]byte{[]byte("another final message")}, "0003")
	assertErrorIs(t, "CloseStream with data after EOF", err, durablestream.ErrStreamClosed)

	// A close-only replay is idempotent. It neither appends nor moves the tail,
	// and its sequence is irrelevant because no new mutation is committed.
	replayTail, err := closer.CloseStream(ctx, streamID, nil, "9999")
	if err != nil {
		t.Fatalf("idempotent close-only replay: %v", err)
	}
	if replayTail != tail {
		t.Errorf("close-only replay tail = %q, want unchanged tail %q", replayTail, tail)
	}
	after := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "after rejected and replayed closes", payloads(after), []string{"initial", "final-one", "final-two"})
}

func testCloseWakesWaiters(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	closer := requireCloseStorage(t, cfg, s)

	const emptyID = "close-wakes-empty"
	mustCreate(t, s, emptyID)
	emptyWait := startWait(t, s, emptyID, durablestream.ZeroOffset, 0)
	emptyTail, err := closer.CloseStream(t.Context(), emptyID, nil, "")
	if err != nil {
		t.Fatalf("close-only CloseStream: %v", err)
	}
	emptyResult := awaitWait(t, "close-only wakeup", emptyWait)
	if emptyResult.err != nil {
		t.Fatalf("WaitForData released by close-only mutation: %v", emptyResult.err)
	}
	if emptyResult.res == nil || !emptyResult.res.Closed || len(emptyResult.res.Messages) != 0 {
		t.Errorf("close-only WaitForData result = %+v, want an empty closed result", emptyResult.res)
	} else if emptyResult.res.TailOffset != emptyTail {
		t.Errorf("close-only tail = %q, waiter observed %q", emptyTail, emptyResult.res.TailOffset)
	}

	// A future waiter must also return EOF rather than block on the replacement
	// notification channel.
	future := awaitWait(t, "future wait on a closed stream", startWait(t, s, emptyID, emptyTail, 0))
	if future.err != nil || future.res == nil || !future.res.Closed {
		t.Errorf("future WaitForData on closed stream = (result %+v, err %v), want closed result", future.res, future.err)
	}

	const finalID = "close-wakes-final-data"
	mustCreate(t, s, finalID)
	oldTail := mustAppend(t, s, finalID, "before")
	finalWait := startWait(t, s, finalID, oldTail, 0)
	finalTail, err := closer.CloseStream(t.Context(), finalID, [][]byte{[]byte("final-a"), []byte("final-b")}, "")
	if err != nil {
		t.Fatalf("CloseStream with final data: %v", err)
	}
	finalResult := awaitWait(t, "final-data close wakeup", finalWait)
	if finalResult.err != nil {
		t.Fatalf("WaitForData released by final-data close: %v", finalResult.err)
	}
	assertPayloads(t, "final-data close wakeup", payloads(finalResult.res), []string{"final-a", "final-b"})
	if !finalResult.res.Closed || finalResult.res.TailOffset != finalTail {
		t.Errorf("final-data waiter = %+v, want Closed=true and tail %q", finalResult.res, finalTail)
	}
}

func testCloseSerializesWithAppend(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	closer := requireCloseStorage(t, cfg, s)

	const rounds = 20
	for round := range rounds {
		streamID := fmt.Sprintf("close-race-append-%02d", round)
		mustCreate(t, s, streamID)

		start := make(chan struct{})
		var (
			wg        sync.WaitGroup
			appendErr error
			closeTail durablestream.Offset
			closeErr  error
		)
		wg.Go(func() {
			<-start
			_, appendErr = s.Append(t.Context(), streamID, []byte("ordinary"), "")
		})
		wg.Go(func() {
			<-start
			closeTail, closeErr = closer.CloseStream(t.Context(), streamID, [][]byte{[]byte("final")}, "")
		})
		close(start)
		wg.Wait()

		if closeErr != nil {
			t.Fatalf("round %d CloseStream: %v", round, closeErr)
		}
		if appendErr != nil && !errors.Is(appendErr, durablestream.ErrStreamClosed) {
			t.Fatalf("round %d concurrent Append: %v, want nil or ErrStreamClosed", round, appendErr)
		}
		result := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
		if !result.Closed {
			t.Fatalf("round %d stream remained open after successful CloseStream", round)
		}
		if result.TailOffset != closeTail {
			t.Errorf("round %d CloseStream tail = %q, Read tail = %q", round, closeTail, result.TailOffset)
		}
		want := []string{"final"}
		if appendErr == nil {
			want = []string{"ordinary", "final"}
		}
		assertPayloads(t, fmt.Sprintf("round %d append/close race", round), payloads(result), want)
	}
}

func testClosureSurvivesReopen(t *testing.T, cfg Config) {
	t.Parallel()
	if cfg.Reopen == nil {
		t.Skip("storagetest: Config.Reopen not set; skipping closure durability subtest")
	}
	s := newStorage(t, cfg)
	closer := requireCloseStorage(t, cfg, s)
	const streamID = "closed-reopen"
	mustCreate(t, s, streamID)
	tail, err := closer.CloseStream(t.Context(), streamID, [][]byte{[]byte("final")}, "0001")
	if err != nil {
		t.Fatalf("CloseStream before reopen: %v", err)
	}

	s = reopen(t, cfg, s)
	closer = requireCloseStorage(t, cfg, s)
	info := mustHead(t, s, streamID)
	result := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if !info.Closed || !result.Closed {
		t.Errorf("closure after reopen = Head %v, Read %v; want both true", info.Closed, result.Closed)
	}
	assertPayloads(t, "closed data after reopen", payloads(result), []string{"final"})
	if info.NextOffset != tail || result.TailOffset != tail {
		t.Errorf("tail after reopen = Head %q, Read %q; want %q", info.NextOffset, result.TailOffset, tail)
	}
	_, err = s.Append(t.Context(), streamID, []byte("too late"), "0002")
	assertErrorIs(t, "Append to closed stream after reopen", err, durablestream.ErrStreamClosed)
	replayTail, err := closer.CloseStream(t.Context(), streamID, nil, "0002")
	if err != nil || replayTail != tail {
		t.Errorf("close-only replay after reopen = (tail %q, err %v), want (%q, nil)", replayTail, err, tail)
	}
}
