package storagetest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// testMissingStream checks that every method reports ErrNotFound for a stream
// that was never created, so callers can use any of them as an existence check.
func testMissingStream(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const missing = "no-such-stream"

	// A live stream with a different ID must not make the missing one appear.
	mustCreate(t, s, "unrelated")
	mustAppend(t, s, "unrelated", "data")

	_, err := s.Append(t.Context(), missing, []byte("data"), "")
	assertErrorIs(t, "Append to a missing stream", err, durablestream.ErrNotFound)

	_, err = s.Read(t.Context(), missing, durablestream.ZeroOffset, 0)
	assertErrorIs(t, "Read of a missing stream", err, durablestream.ErrNotFound)

	_, err = s.Head(t.Context(), missing)
	assertErrorIs(t, "Head of a missing stream", err, durablestream.ErrNotFound)

	_, err = s.WaitForData(t.Context(), missing, durablestream.ZeroOffset, 0)
	assertErrorIs(t, "WaitForData on a missing stream", err, durablestream.ErrNotFound)

	assertErrorIs(t, "Delete of a missing stream", s.Delete(t.Context(), missing), durablestream.ErrNotFound)
}

// testExpiredStream checks that a stream past its expiry reads as absent.
func testExpiredStream(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "expired"
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	_, err := s.Head(t.Context(), streamID)
	assertErrorIs(t, "Head of an expired stream", err, durablestream.ErrNotFound)

	_, err = s.Read(t.Context(), streamID, durablestream.ZeroOffset, 0)
	assertErrorIs(t, "Read of an expired stream", err, durablestream.ErrNotFound)

	_, err = s.Append(t.Context(), streamID, []byte("data"), "")
	assertErrorIs(t, "Append to an expired stream", err, durablestream.ErrNotFound)

	var waitErr error
	runBounded(t, "WaitForData on an expired stream", func() {
		_, waitErr = s.WaitForData(t.Context(), streamID, durablestream.ZeroOffset, 0)
	})
	assertErrorIs(t, "WaitForData on an expired stream", waitErr, durablestream.ErrNotFound)

	// Delete still reclaims the expired record: it exists as far as storage is
	// concerned, it is just no longer readable.
	if err := s.Delete(t.Context(), streamID); err != nil {
		t.Errorf("Delete of an expired stream returned %v, want nil: the record is still there to reclaim", err)
	}
}

// testEmptyAppend checks that a zero-length append is rejected rather than
// silently creating a message with no bytes.
func testEmptyAppend(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "empty-append"
	mustCreate(t, s, streamID)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "nil data", data: nil},
		{name: "zero-length slice", data: []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Append(t.Context(), streamID, tt.data, "")
			assertErrorIs(t, "Append of empty data", err, durablestream.ErrBadRequest)
		})
	}

	if res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0); len(res.Messages) != 0 {
		t.Errorf("stream holds %d messages after only rejected appends, want 0", len(res.Messages))
	}
}

// testSequenceRegression checks deduplication: a sequence number that does not
// sort after the last accepted one is refused, and refusing it appends nothing.
func testSequenceRegression(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "seq"
	mustCreate(t, s, streamID)

	firstOffset, err := s.Append(t.Context(), streamID, []byte("first"), "0002")
	if err != nil {
		t.Fatalf("Append with seq 0002: %v", err)
	}

	tests := []struct {
		name string
		seq  string
	}{
		{name: "a sequence before the last accepted one", seq: "0001"},
		{name: "the same sequence again", seq: "0002"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Append(t.Context(), streamID, []byte("rejected"), tt.seq)
			assertErrorIs(t, "Append with seq "+tt.seq, err, durablestream.ErrConflict)
			assertErrorIs(t, "Append with seq "+tt.seq, err, durablestream.ErrSequenceConflict)
			var conflict *durablestream.SequenceConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("Append with seq %s returned %T, want *SequenceConflictError", tt.seq, err)
			}
			if conflict.LastSeq != "0002" {
				t.Errorf("SequenceConflictError.LastSeq = %q, want %q", conflict.LastSeq, "0002")
			}
			if !conflict.LastOffset.IsZero() && conflict.LastOffset != firstOffset {
				t.Errorf("SequenceConflictError.LastOffset = %q, want zero or %q", conflict.LastOffset, firstOffset)
			}
		})
	}

	if _, err := s.Append(t.Context(), streamID, []byte("second"), "0003"); err != nil {
		t.Fatalf("Append with seq 0003 after rejected replays: %v", err)
	}

	// The rejected appends must have left no trace.
	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "reading after rejected sequence replays", payloads(res), []string{"first", "second"})
	assertOffsetsIncreasing(t, "reading after rejected sequence replays", res.Messages)
}

// testCreateIdempotent checks that recreating a stream with an equivalent
// config is a no-op rather than an error, so a retried request is safe.
func testCreateIdempotent(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "create-idempotent"
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{ContentType: "text/plain"})
	mustAppend(t, s, streamID, "data")

	tests := []struct {
		name   string
		config durablestream.StreamConfig
	}{
		{name: "the identical config", config: durablestream.StreamConfig{ContentType: "text/plain"}},
		{name: "a content type differing only in case", config: durablestream.StreamConfig{ContentType: "TEXT/Plain"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created, err := s.Create(t.Context(), streamID, tt.config)
			if err != nil {
				t.Fatalf("re-creating a stream with %s: %v", tt.name, err)
			}
			if created {
				t.Errorf("re-creating an existing stream reported created=true, want false")
			}
		})
	}

	// An idempotent create must not have disturbed the stream.
	assertPayloads(t, "reading after idempotent creates", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"data"})
}

// testCreateConflict checks that creating over a live stream with a different
// config fails and changes nothing.
func testCreateConflict(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "create-conflict"
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{ContentType: "text/plain"})
	mustAppend(t, s, streamID, "data")

	tests := []struct {
		name   string
		config durablestream.StreamConfig
	}{
		{name: "a different content type", config: durablestream.StreamConfig{ContentType: "application/json"}},
		{name: "a different TTL", config: durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour}},
		{name: "a different privacy setting", config: durablestream.StreamConfig{ContentType: "text/plain", IsPrivate: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created, err := s.Create(t.Context(), streamID, tt.config)
			assertErrorIs(t, "Create over a live stream with "+tt.name, err, durablestream.ErrConflict)
			if created {
				t.Errorf("a conflicting Create reported created=true, want false")
			}
		})
	}

	info := mustHead(t, s, streamID)
	if info.ContentType != "text/plain" {
		t.Errorf("content type is %q after conflicting creates, want the original %q", info.ContentType, "text/plain")
	}
	assertPayloads(t, "reading after conflicting creates", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"data"})
}

// testMaxMessageSize checks the boundary of the implementation's message size
// limit, when it has one.
func testMaxMessageSize(t *testing.T, cfg Config) {
	t.Parallel()
	if cfg.MaxMessageSize <= 0 {
		t.Skip("storagetest: Config.MaxMessageSize not set; implementation accepts messages of any size")
	}
	s := newStorage(t, cfg)
	const streamID = "max-message-size"
	mustCreate(t, s, streamID)

	atLimit := bytes.Repeat([]byte("a"), cfg.MaxMessageSize)
	if _, err := s.Append(t.Context(), streamID, atLimit, ""); err != nil {
		t.Fatalf("Append of a message of exactly MaxMessageSize (%d bytes) failed: %v", cfg.MaxMessageSize, err)
	}

	tooBig := bytes.Repeat([]byte("b"), cfg.MaxMessageSize+1)
	_, err := s.Append(t.Context(), streamID, tooBig, "")
	assertErrorIs(t, "Append of a message one byte over MaxMessageSize", err, durablestream.ErrPayloadTooLarge)

	// The rejected append must not be stored, even partially.
	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if len(res.Messages) != 1 {
		t.Fatalf("stream holds %d messages after one accepted and one rejected append, want 1", len(res.Messages))
	}
	if !bytes.Equal(res.Messages[0].Data, atLimit) {
		t.Errorf("the stored message is %d bytes, want the %d-byte message that was accepted", len(res.Messages[0].Data), len(atLimit))
	}
}

// testReadCancelled checks that Read reports cancellation rather than serving
// the read anyway.
func testReadCancelled(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "read-cancelled"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "data")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var err error
	runBounded(t, "Read with a cancelled context", func() {
		_, err = s.Read(ctx, streamID, durablestream.ZeroOffset, 0)
	})
	assertErrorIs(t, "Read with a cancelled context", err, context.Canceled)
}

// testWaitCancelled checks that WaitForData returns promptly when its context is
// cancelled, both before the call and while it is blocked.
func testWaitCancelled(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-cancelled"
	mustCreate(t, s, streamID)

	t.Run("cancelled before the call", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var err error
		runBounded(t, "WaitForData with an already-cancelled context", func() {
			_, err = s.WaitForData(ctx, streamID, durablestream.ZeroOffset, 0)
		})
		assertErrorIs(t, "WaitForData with an already-cancelled context", err, context.Canceled)
	})

	t.Run("cancelled while blocked", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			close(started)
			_, err := s.WaitForData(ctx, streamID, durablestream.ZeroOffset, 0)
			done <- err
		}()

		<-started
		cancel()

		select {
		case err := <-done:
			assertErrorIs(t, "WaitForData cancelled while blocked", err, context.Canceled)
		case <-time.After(waitTimeout):
			t.Fatalf("WaitForData did not return within %s of its context being cancelled", waitTimeout)
		}
	})
}

// testWaitDeadline checks that a deadline releases a waiter with
// context.DeadlineExceeded, which is how the handler bounds a long poll.
func testWaitDeadline(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-deadline"
	mustCreate(t, s, streamID)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	var err error
	runBounded(t, "WaitForData with a deadline on an empty stream", func() {
		_, err = s.WaitForData(ctx, streamID, durablestream.ZeroOffset, 0)
	})
	assertErrorIs(t, "WaitForData with an expired deadline", err, context.DeadlineExceeded)
}

// testMutationsCancelled checks the weaker guarantee mutations carry: they must
// not hang once the context is cancelled, and whatever they report must match
// what the storage actually did. Implementations may either abort or complete a
// durable mutation whose context was cancelled, so the subtest accepts both and
// only insists the report is honest.
func testMutationsCancelled(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "mutations-cancelled"

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var (
		created    bool
		createErr  error
		appendErr  error
		deleteErr  error
		appendedOK bool
	)

	runBounded(t, "Create with a cancelled context", func() {
		created, createErr = s.Create(ctx, streamID, textConfig())
	})
	if createErr != nil {
		assertErrorIs(t, "Create with a cancelled context", createErr, context.Canceled)
	}
	if createErr == nil && !created {
		t.Errorf("Create of a new stream reported created=false with no error")
	}

	_, headErr := s.Head(t.Context(), streamID)
	streamExists := headErr == nil
	if createErr == nil && !streamExists {
		t.Errorf("Create reported success but Head then reported %v; a successful mutation must be visible", headErr)
	}
	if !streamExists {
		// The implementation refused the cancelled create, which is allowed.
		// Nothing further to check.
		return
	}

	runBounded(t, "Append with a cancelled context", func() {
		_, appendErr = s.Append(ctx, streamID, []byte("data"), "")
	})
	if appendErr != nil {
		assertErrorIs(t, "Append with a cancelled context", appendErr, context.Canceled)
	}
	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	appendedOK = len(res.Messages) == 1
	if appendErr == nil && !appendedOK {
		t.Errorf("Append reported success but the stream holds %d messages, want 1", len(res.Messages))
	}
	if appendErr != nil && len(res.Messages) != 0 {
		t.Errorf("Append reported error %v but the stream holds %d messages; a failed append must store nothing", appendErr, len(res.Messages))
	}

	runBounded(t, "Delete with a cancelled context", func() {
		deleteErr = s.Delete(ctx, streamID)
	})
	if deleteErr != nil {
		assertErrorIs(t, "Delete with a cancelled context", deleteErr, context.Canceled)
	}
	_, headErr = s.Head(t.Context(), streamID)
	if deleteErr == nil && headErr == nil {
		t.Errorf("Delete reported success but Head still finds the stream")
	}
}
