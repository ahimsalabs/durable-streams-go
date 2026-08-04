package storagetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func requireAtomicStorage(t *testing.T, cfg Config, s durablestream.Storage) durablestream.AtomicBatchStorage {
	t.Helper()
	atomic, ok := s.(durablestream.AtomicBatchStorage)
	if ok {
		return atomic
	}
	if cfg.RequireAtomicBatches {
		t.Fatalf("Storage %T does not implement durablestream.AtomicBatchStorage", s)
	}
	t.Skipf("Storage %T does not implement optional AtomicBatchStorage capability", s)
	return nil
}

func testAtomicCreateWithMessages(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	atomic := requireAtomicStorage(t, cfg, s)
	ctx := t.Context()

	const streamID = "atomic-create"
	input := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	created, tail, err := atomic.CreateWithMessages(ctx, streamID, textConfig(), input)
	if err != nil {
		t.Fatalf("CreateWithMessages: %v", err)
	}
	if !created {
		t.Fatal("CreateWithMessages reported created=false for a missing stream")
	}
	for _, message := range input {
		message[0] = 'X'
	}

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "atomic initial read", payloads(res), []string{"first", "second", "third"})
	assertOffsetsIncreasing(t, "atomic initial messages", res.Messages)
	if tail != res.TailOffset || tail != mustHead(t, s, streamID).NextOffset {
		t.Errorf("CreateWithMessages tail = %q, Read tail = %q, Head tail = %q", tail, res.TailOffset, mustHead(t, s, streamID).NextOffset)
	}

	// A replay is idempotent by configuration. Its body is deliberately
	// different: initial messages are ignored rather than appended a second time.
	created, replayTail, err := atomic.CreateWithMessages(ctx, streamID, textConfig(), [][]byte{[]byte("different")})
	if err != nil {
		t.Fatalf("idempotent CreateWithMessages replay: %v", err)
	}
	if created {
		t.Error("idempotent CreateWithMessages replay reported created=true")
	}
	if replayTail != tail {
		t.Errorf("replay tail = %q, want existing tail %q", replayTail, tail)
	}
	assertPayloads(t, "after idempotent replay", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"first", "second", "third"})

	// Even an idempotent replay validates its borrowed batch before deciding not
	// to append it. This keeps caller-visible validation independent of whether a
	// matching stream happened to win a concurrent create.
	_, _, err = atomic.CreateWithMessages(ctx, streamID, textConfig(), [][]byte{[]byte("valid"), nil})
	assertErrorIs(t, "idempotent CreateWithMessages with an empty item", err, durablestream.ErrBadRequest)
	assertPayloads(t, "after invalid idempotent replay", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"first", "second", "third"})

	_, _, err = atomic.CreateWithMessages(ctx, streamID, durablestream.StreamConfig{ContentType: "application/octet-stream"}, [][]byte{[]byte("ignored")})
	assertErrorIs(t, "CreateWithMessages with a different config", err, durablestream.ErrConflict)
	assertPayloads(t, "after conflicting create", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"first", "second", "third"})

	created, emptyTail, err := atomic.CreateWithMessages(ctx, "atomic-create-empty", textConfig(), nil)
	if err != nil || !created {
		t.Fatalf("empty CreateWithMessages = (created %v, err %v), want (true, nil)", created, err)
	}
	if info := mustHead(t, s, "atomic-create-empty"); emptyTail != info.NextOffset {
		t.Errorf("empty CreateWithMessages tail = %q, Head tail = %q", emptyTail, info.NextOffset)
	}

	// Validation is performed before the configuration becomes visible.
	_, _, err = atomic.CreateWithMessages(ctx, "atomic-create-invalid", textConfig(), [][]byte{[]byte("good"), nil, []byte("unreachable")})
	assertErrorIs(t, "CreateWithMessages with an empty item", err, durablestream.ErrBadRequest)
	if _, headErr := s.Head(ctx, "atomic-create-invalid"); !errors.Is(headErr, durablestream.ErrNotFound) {
		t.Errorf("Head after rejected CreateWithMessages = %v, want ErrNotFound", headErr)
	}
	if cfg.MaxMessageSize > 0 {
		oversized := make([]byte, cfg.MaxMessageSize+1)
		_, _, err = atomic.CreateWithMessages(ctx, "atomic-create-oversized", textConfig(), [][]byte{[]byte("prefix"), oversized})
		assertErrorIs(t, "CreateWithMessages with an oversized item", err, durablestream.ErrPayloadTooLarge)
		if _, headErr := s.Head(ctx, "atomic-create-oversized"); !errors.Is(headErr, durablestream.ErrNotFound) {
			t.Errorf("Head after oversized CreateWithMessages = %v, want ErrNotFound", headErr)
		}
	}

	const expiredID = "atomic-create-expired"
	expired := textConfig()
	expired.ExpiresAt = time.Now().Add(-time.Second)
	created, _, err = atomic.CreateWithMessages(ctx, expiredID, expired, [][]byte{[]byte("old")})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages for already-expired incarnation = (created %v, err %v), want (true, nil)", created, err)
	}
	created, replacementTail, err := atomic.CreateWithMessages(ctx, expiredID, textConfig(), [][]byte{[]byte("replacement-a"), []byte("replacement-b")})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages replacing expired incarnation = (created %v, err %v), want (true, nil)", created, err)
	}
	replacement := mustRead(t, s, expiredID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "expired atomic replacement", payloads(replacement), []string{"replacement-a", "replacement-b"})
	if replacement.TailOffset != replacementTail {
		t.Errorf("expired replacement Read tail = %q, CreateWithMessages returned %q", replacement.TailOffset, replacementTail)
	}
}

func testAtomicAppendBatch(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	atomic := requireAtomicStorage(t, cfg, s)
	const streamID = "atomic-append"
	mustCreate(t, s, streamID)

	input := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	tail, err := atomic.AppendBatch(t.Context(), streamID, input, "0001")
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	for _, message := range input {
		message[0] = 'X'
	}
	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "atomic append read", payloads(res), []string{"one", "two", "three"})
	if tail != res.TailOffset || tail != mustHead(t, s, streamID).NextOffset {
		t.Errorf("AppendBatch tail = %q, Read tail = %q, Head tail = %q", tail, res.TailOffset, mustHead(t, s, streamID).NextOffset)
	}

	assertUnchanged := func(what string, err, want error) {
		t.Helper()
		assertErrorIs(t, what, err, want)
		got := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
		assertPayloads(t, what, payloads(got), []string{"one", "two", "three"})
		if got.TailOffset != tail {
			t.Errorf("%s moved tail from %q to %q", what, tail, got.TailOffset)
		}
	}

	_, err = atomic.AppendBatch(t.Context(), streamID, [][]byte{[]byte("prefix"), nil, []byte("suffix")}, "0002")
	assertUnchanged("batch with an empty item", err, durablestream.ErrBadRequest)
	_, err = atomic.AppendBatch(t.Context(), streamID, nil, "0002")
	assertUnchanged("empty batch", err, durablestream.ErrBadRequest)
	_, err = atomic.AppendBatch(t.Context(), streamID, [][]byte{[]byte("duplicate")}, "0001")
	assertUnchanged("batch with a regressed sequence", err, durablestream.ErrConflict)

	if cfg.MaxMessageSize > 0 {
		oversized := make([]byte, cfg.MaxMessageSize+1)
		_, err = atomic.AppendBatch(t.Context(), streamID, [][]byte{[]byte("prefix"), oversized}, "0002")
		assertUnchanged("batch with an oversized item", err, durablestream.ErrPayloadTooLarge)
	}

	// Rejected batches must not consume their sequence value. The next valid
	// batch with the same next sequence still succeeds.
	finalTail, err := atomic.AppendBatch(t.Context(), streamID, [][]byte{[]byte("four"), []byte("five")}, "0002")
	if err != nil {
		t.Fatalf("AppendBatch after rejected batches: %v", err)
	}
	final := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "after valid next sequence", payloads(final), []string{"one", "two", "three", "four", "five"})
	if final.TailOffset != finalTail {
		t.Errorf("final Read tail = %q, AppendBatch returned %q", final.TailOffset, finalTail)
	}
}

func testAtomicAppendDoesNotInterleave(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	atomic := requireAtomicStorage(t, cfg, s)
	const streamID = "atomic-no-interleave"
	mustCreate(t, s, streamID)

	const (
		batches   = 12
		batchSize = 4
		singles   = 12
	)
	start := make(chan struct{})
	errs := make(chan error, batches+singles)
	var wg sync.WaitGroup
	for batch := range batches {
		wg.Go(func() {
			messages := make([][]byte, batchSize)
			for item := range batchSize {
				messages[item] = []byte(fmt.Sprintf("batch-%02d-%d", batch, item))
			}
			<-start
			_, err := atomic.AppendBatch(t.Context(), streamID, messages, "")
			errs <- err
		})
	}
	for single := range singles {
		wg.Go(func() {
			<-start
			_, err := s.Append(t.Context(), streamID, []byte(fmt.Sprintf("single-%02d", single)), "")
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}

	got := payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0))
	if want := batches*batchSize + singles; len(got) != want {
		t.Fatalf("concurrent appends stored %d messages, want %d", len(got), want)
	}
	positions := make(map[string]int, len(got))
	for i, payload := range got {
		if previous, duplicate := positions[payload]; duplicate {
			t.Fatalf("concurrent appends stored duplicate payload %q at positions %d and %d", payload, previous, i)
		}
		positions[payload] = i
	}
	for batch := range batches {
		firstPayload := fmt.Sprintf("batch-%02d-0", batch)
		first, ok := positions[firstPayload]
		if !ok {
			t.Errorf("concurrent appends lost payload %q", firstPayload)
			continue
		}
		for item := 1; item < batchSize; item++ {
			payload := fmt.Sprintf("batch-%02d-%d", batch, item)
			position, ok := positions[payload]
			if !ok {
				t.Errorf("concurrent appends lost payload %q", payload)
				break
			}
			if position != first+item {
				t.Errorf("batch %d was interleaved: %q", batch, got[first:min(first+batchSize+2, len(got))])
				break
			}
		}
	}
	for single := range singles {
		payload := fmt.Sprintf("single-%02d", single)
		if _, ok := positions[payload]; !ok {
			t.Errorf("concurrent appends lost payload %q", payload)
		}
	}
}

func testAtomicBatchRacesLifecycle(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	atomic := requireAtomicStorage(t, cfg, s)

	const iterations = 12
	for iteration := range iterations {
		streamID := fmt.Sprintf("atomic-lifecycle-%02d", iteration)
		mustCreate(t, s, streamID)
		oldIncarnation := mustHead(t, s, streamID).IncarnationID
		batch := [][]byte{
			[]byte(fmt.Sprintf("batch-%02d-0", iteration)),
			[]byte(fmt.Sprintf("batch-%02d-1", iteration)),
			[]byte(fmt.Sprintf("batch-%02d-2", iteration)),
		}

		start := make(chan struct{})
		appendDone := make(chan error, 1)
		lifecycleDone := make(chan error, 1)
		go func() {
			<-start
			_, err := atomic.AppendBatch(t.Context(), streamID, batch, "")
			appendDone <- err
		}()
		go func() {
			<-start
			if err := s.Delete(t.Context(), streamID); err != nil {
				lifecycleDone <- fmt.Errorf("delete: %w", err)
				return
			}
			created, _, err := atomic.CreateWithMessages(t.Context(), streamID, textConfig(), [][]byte{[]byte("replacement")})
			if err == nil && !created {
				err = errors.New("replacement reported created=false")
			}
			lifecycleDone <- err
		}()
		close(start)

		if err := <-lifecycleDone; err != nil {
			t.Fatalf("iteration %d lifecycle: %v", iteration, err)
		}
		if err := <-appendDone; err != nil && !errors.Is(err, durablestream.ErrNotFound) && !errors.Is(err, durablestream.ErrConflict) {
			t.Fatalf("iteration %d AppendBatch: %v", iteration, err)
		}

		info := mustHead(t, s, streamID)
		if oldIncarnation != "" && info.IncarnationID == oldIncarnation {
			t.Errorf("iteration %d replacement kept old incarnation ID %q", iteration, oldIncarnation)
		}
		got := payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0))
		wantBatch := []string{"replacement", string(batch[0]), string(batch[1]), string(batch[2])}
		if len(got) == 1 && got[0] == "replacement" {
			continue // The append linearized against the deleted incarnation.
		}
		assertPayloads(t, fmt.Sprintf("iteration %d replacement", iteration), got, wantBatch)
	}
}

func testAtomicBatchSurvivesReopen(t *testing.T, cfg Config) {
	t.Parallel()
	if cfg.Reopen == nil {
		t.Skip("storagetest: Config.Reopen not set; skipping durability subtest")
	}
	s := newStorage(t, cfg)
	atomic := requireAtomicStorage(t, cfg, s)
	const streamID = "atomic-reopen"

	created, createTail, err := atomic.CreateWithMessages(t.Context(), streamID, textConfig(), [][]byte{[]byte("one"), []byte("two")})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages = (created %v, err %v), want (true, nil)", created, err)
	}
	s = reopen(t, cfg, s)
	atomic = requireAtomicStorage(t, cfg, s)

	third, err := s.Append(t.Context(), streamID, []byte("three"), "")
	if err != nil {
		t.Fatalf("Append after reopening atomic create: %v", err)
	}
	if third.Compare(createTail) <= 0 {
		t.Fatalf("Append after reopen returned offset %q, want greater than initial tail %q", third, createTail)
	}
	batchTail, err := atomic.AppendBatch(t.Context(), streamID, [][]byte{[]byte("four"), []byte("five")}, "")
	if err != nil {
		t.Fatalf("AppendBatch after reopen: %v", err)
	}
	s = reopen(t, cfg, s)

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "atomic data after second reopen", payloads(res), []string{"one", "two", "three", "four", "five"})
	if res.TailOffset != batchTail || mustHead(t, s, streamID).NextOffset != batchTail {
		t.Errorf("tail after reopen = Read %q, Head %q; want %q", res.TailOffset, mustHead(t, s, streamID).NextOffset, batchTail)
	}
}
