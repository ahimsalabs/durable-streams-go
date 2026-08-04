package storagetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func requireIncarnationID(t *testing.T, cfg Config, operation, id string) string {
	t.Helper()
	if id != "" {
		return id
	}
	if cfg.RequireIncarnationID {
		t.Fatalf("%s returned an empty IncarnationID", operation)
	}
	t.Skipf("%s returned an empty IncarnationID; implementation does not expose optional incarnation identity", operation)
	return ""
}

func testIncarnationIDStable(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "incarnation-stable"
	mustCreate(t, s, streamID)

	initial := requireIncarnationID(t, cfg, "Head after Create", mustHead(t, s, streamID).IncarnationID)
	emptyRead := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if emptyRead.IncarnationID != initial {
		t.Errorf("empty Read IncarnationID = %q, want Head value %q", emptyRead.IncarnationID, initial)
	}

	mustAppend(t, s, streamID, "data")
	populatedRead := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if populatedRead.IncarnationID != initial {
		t.Errorf("populated Read IncarnationID = %q, want original value %q", populatedRead.IncarnationID, initial)
	}
	if got := mustHead(t, s, streamID).IncarnationID; got != initial {
		t.Errorf("Head after Append IncarnationID = %q, want original value %q", got, initial)
	}

	if cfg.Reopen != nil {
		s = reopen(t, cfg, s)
		if got := mustHead(t, s, streamID).IncarnationID; got != initial {
			t.Errorf("Head after durable reopen IncarnationID = %q, want original value %q", got, initial)
		}
		if got := mustRead(t, s, streamID, durablestream.ZeroOffset, 0).IncarnationID; got != initial {
			t.Errorf("Read after durable reopen IncarnationID = %q, want original value %q", got, initial)
		}
	}
}

func testIncarnationIDChangesAfterRecreate(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "incarnation-recreate"
	mustCreate(t, s, streamID)
	oldID := requireIncarnationID(t, cfg, "Head before Delete", mustHead(t, s, streamID).IncarnationID)

	mustDelete(t, s, streamID)
	mustCreate(t, s, streamID)
	newID := requireIncarnationID(t, cfg, "Head after recreate", mustHead(t, s, streamID).IncarnationID)
	if newID == oldID {
		t.Fatalf("recreated stream reused IncarnationID %q", newID)
	}
	if got := mustRead(t, s, streamID, durablestream.ZeroOffset, 0).IncarnationID; got != newID {
		t.Errorf("Read of recreated stream IncarnationID = %q, want Head value %q", got, newID)
	}
}

// testConcurrentCreate checks that racing creates of the same stream settle on
// one winner: exactly one caller is told it created the stream, and nobody sees
// an error.
func testConcurrentCreate(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "concurrent-create"

	const creators = 8
	createdFlags := make([]bool, creators)
	errs := make([]error, creators)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range creators {
		wg.Go(func() {
			<-start
			createdFlags[i], errs[i] = s.Create(t.Context(), streamID, textConfig())
		})
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := range creators {
		if errs[i] != nil {
			t.Errorf("creator %d failed with %v; concurrent creates of an identical config must all succeed", i, errs[i])
		}
		if createdFlags[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Errorf("%d of %d concurrent creates reported created=true, want exactly 1", winners, creators)
	}

	// The stream must be usable and empty.
	if res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0); len(res.Messages) != 0 {
		t.Errorf("a freshly created stream holds %d messages, want 0", len(res.Messages))
	}
	mustAppend(t, s, streamID, "data")
}

// testExpiredStreamReplacement checks that an expired stream does not block the
// stream ID: Create takes it over, and the replacement starts clean.
func testExpiredStreamReplacement(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "expired-replacement"

	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	// A config that would conflict with a live stream is accepted here, because
	// the expired stream is being replaced rather than reused.
	replacement := durablestream.StreamConfig{ContentType: "application/json"}
	created, err := s.Create(t.Context(), streamID, replacement)
	if err != nil {
		t.Fatalf("re-creating an expired stream: %v", err)
	}
	if !created {
		t.Fatalf("re-creating an expired stream reported created=false, want true: the expired stream is replaced")
	}

	info := mustHead(t, s, streamID)
	if info.ContentType != "application/json" {
		t.Errorf("the replacement stream reports content type %q, want the new config's %q", info.ContentType, "application/json")
	}
	if res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0); len(res.Messages) != 0 {
		t.Errorf("the replacement stream holds %d messages, want 0", len(res.Messages))
	}
	mustAppend(t, s, streamID, "fresh")
	assertPayloads(t, "reading the replacement stream", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"fresh"})
}

// testDeleteRemovesStream checks that Delete is immediately visible to every
// other method.
func testDeleteRemovesStream(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "delete-removes"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "data")

	mustDelete(t, s, streamID)

	_, err := s.Head(t.Context(), streamID)
	assertErrorIs(t, "Head after Delete", err, durablestream.ErrNotFound)

	_, err = s.Read(t.Context(), streamID, durablestream.ZeroOffset, 0)
	assertErrorIs(t, "Read after Delete", err, durablestream.ErrNotFound)

	_, err = s.Append(t.Context(), streamID, []byte("more"), "")
	assertErrorIs(t, "Append after Delete", err, durablestream.ErrNotFound)

	assertErrorIs(t, "Delete of an already deleted stream", s.Delete(t.Context(), streamID), durablestream.ErrNotFound)
}

// testDeleteThenRecreate checks the isolation guarantee that lets an
// implementation reclaim deleted bytes lazily: a stream recreated with the same
// ID must never serve data, offsets or deduplication state from the incarnation
// it replaced. The rounds run back to back so a background purge is still in
// flight while the next incarnation is being written.
func testDeleteThenRecreate(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const (
		streamID = "delete-recreate"
		rounds   = 5
	)

	for round := range rounds {
		mustCreate(t, s, streamID)

		stale := fmt.Sprintf("stale-round-%d", round)
		mustAppend(t, s, streamID, stale)
		mustAppend(t, s, streamID, stale+"-second")

		mustDelete(t, s, streamID)
		mustCreate(t, s, streamID)

		// The new incarnation starts empty, at the zero tail.
		fresh := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
		if len(fresh.Messages) != 0 {
			t.Fatalf("round %d: a recreated stream holds %q, want no messages from the deleted incarnation", round, payloads(fresh))
		}
		if info := mustHead(t, s, streamID); info.NextOffset != fresh.TailOffset {
			t.Errorf("round %d: Head reports NextOffset %q but Read reports TailOffset %q", round, info.NextOffset, fresh.TailOffset)
		}

		// A deduplication sequence from the previous incarnation must not be
		// remembered: the same seq has to be accepted again.
		if _, err := s.Append(t.Context(), streamID, []byte("current"), "0001"); err != nil {
			t.Fatalf("round %d: appending with a seq reused from the deleted incarnation: %v", round, err)
		}
		assertPayloads(t, fmt.Sprintf("round %d: reading the recreated stream", round),
			payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"current"})

		mustDelete(t, s, streamID)
	}
}

// testDeleteRaceCreate checks that Delete and Create of the same stream ID never
// interleave into a hybrid: whatever survives the race is either no stream at
// all or a clean, empty one, never the deleted stream's data.
func testDeleteRaceCreate(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const (
		streamID = "delete-race-create"
		rounds   = 10
	)

	for round := range rounds {
		mustCreate(t, s, streamID)
		stale := fmt.Sprintf("stale-%d", round)
		mustAppend(t, s, streamID, stale)

		start := make(chan struct{})
		var (
			wg        sync.WaitGroup
			deleteErr error
			createErr error
		)
		wg.Go(func() {
			<-start
			deleteErr = s.Delete(t.Context(), streamID)
		})
		wg.Go(func() {
			<-start
			_, createErr = s.Create(t.Context(), streamID, textConfig())
		})
		close(start)
		wg.Wait()

		if deleteErr != nil && !errors.Is(deleteErr, durablestream.ErrNotFound) {
			t.Fatalf("round %d: Delete racing Create failed with %v", round, deleteErr)
		}
		if createErr != nil {
			t.Fatalf("round %d: Create racing Delete failed with %v", round, createErr)
		}

		res, err := s.Read(t.Context(), streamID, durablestream.ZeroOffset, 0)
		switch {
		case errors.Is(err, durablestream.ErrNotFound):
			// Delete won: nothing survives, which is a valid outcome.
		case err != nil:
			t.Fatalf("round %d: reading after the race: %v", round, err)
		default:
			// Create won: the surviving stream is a new, empty one.
			for _, got := range payloads(res) {
				if got == stale {
					t.Fatalf("round %d: the stream that survived Delete racing Create still serves the deleted data %q", round, stale)
				}
			}
			if len(res.Messages) != 0 {
				t.Fatalf("round %d: the stream that survived the race holds %q, want no messages", round, payloads(res))
			}
			mustDelete(t, s, streamID)
		}
	}
}

// testCloseIdempotent checks that Close can be called more than once, which
// matters because shutdown paths often close defensively.
func testCloseIdempotent(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	mustCreate(t, s, "close-idempotent")

	for i := range 3 {
		var err error
		runBounded(t, fmt.Sprintf("Close call %d", i+1), func() { err = s.Close() })
		if err != nil {
			t.Errorf("Close call %d returned %v, want nil", i+1, err)
		}
	}
}

// testOperationsAfterClose checks that a closed storage stays well behaved.
// Whether it rejects work with ErrClosed or keeps serving it is
// implementation-defined, but it must never panic, hang, or invent a different
// class of failure.
func testOperationsAfterClose(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "after-close"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "data")

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Any error here must be a sentinel a caller can classify. ErrNotFound is
	// allowed because an implementation may drop in-memory state on close.
	acceptable := func(t *testing.T, op string, err error) {
		t.Helper()
		if err == nil {
			return
		}
		if errors.Is(err, durablestream.ErrClosed) || errors.Is(err, durablestream.ErrNotFound) {
			return
		}
		t.Errorf("%s after Close returned %v, want nil or an error matching ErrClosed", op, err)
	}

	ctx := t.Context()

	runBounded(t, "Create after Close", func() {
		_, err := s.Create(ctx, "created-after-close", textConfig())
		acceptable(t, "Create", err)
	})
	runBounded(t, "Append after Close", func() {
		_, err := s.Append(ctx, streamID, []byte("more"), "")
		acceptable(t, "Append", err)
	})
	runBounded(t, "Read after Close", func() {
		_, err := s.Read(ctx, streamID, durablestream.ZeroOffset, 0)
		acceptable(t, "Read", err)
	})
	runBounded(t, "Head after Close", func() {
		_, err := s.Head(ctx, streamID)
		acceptable(t, "Head", err)
	})
	runBounded(t, "WaitForData after Close", func() {
		_, err := s.WaitForData(ctx, streamID, durablestream.ZeroOffset, 0)
		acceptable(t, "WaitForData", err)
	})
	runBounded(t, "Delete after Close", func() {
		acceptable(t, "Delete", s.Delete(ctx, streamID))
	})
}

// testDurabilityAcrossReopen checks that a restart preserves messages, their
// offsets, and stream metadata, and that appends resume after the old tail.
func testDurabilityAcrossReopen(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const (
		kept     = "durable-kept"
		alsoKept = "durable-also-kept"
	)
	mustCreateConfig(t, s, kept, durablestream.StreamConfig{ContentType: "application/json"})
	mustCreate(t, s, alsoKept)

	const count = 5
	want := make([]string, 0, count)
	offsets := make([]durablestream.Offset, 0, count)
	for i := range count {
		offsets = append(offsets, mustAppend(t, s, kept, message(i)))
		want = append(want, message(i))
	}
	mustAppend(t, s, alsoKept, "other")
	tailBefore := mustHead(t, s, kept).NextOffset

	s = reopen(t, cfg, s)

	info := mustHead(t, s, kept)
	if info.ContentType != "application/json" {
		t.Errorf("after reopening, content type is %q, want the configured %q", info.ContentType, "application/json")
	}
	if info.NextOffset != tailBefore {
		t.Errorf("after reopening, the tail offset is %q, want %q", info.NextOffset, tailBefore)
	}

	res := mustRead(t, s, kept, durablestream.ZeroOffset, 0)
	assertPayloads(t, "reading after a reopen", payloads(res), want)
	for i, m := range res.Messages {
		if m.Offset != offsets[i] {
			t.Errorf("after reopening, message %d has offset %q, want the original %q", i, m.Offset, offsets[i])
		}
	}
	assertPayloads(t, "reading the second stream after a reopen", payloads(mustRead(t, s, alsoKept, durablestream.ZeroOffset, 0)), []string{"other"})

	// Appends continue past the recovered tail rather than overwriting it.
	next := mustAppend(t, s, kept, "after-reopen")
	if next.Compare(tailBefore) <= 0 {
		t.Errorf("the first append after a reopen returned offset %q, which does not sort after the recovered tail %q", next, tailBefore)
	}
	assertPayloads(t, "reading after appending post-reopen", payloads(mustRead(t, s, kept, durablestream.ZeroOffset, 0)), append(want, "after-reopen"))
}

// testDeleteSurvivesReopen checks that a delete is durable, and that the deleted
// stream's data does not reappear under a stream later created with its ID.
func testDeleteSurvivesReopen(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "durable-deleted"

	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "stale")
	mustDelete(t, s, streamID)

	s = reopen(t, cfg, s)

	_, err := s.Head(t.Context(), streamID)
	assertErrorIs(t, "Head of a stream deleted before the reopen", err, durablestream.ErrNotFound)

	mustCreate(t, s, streamID)
	if res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0); len(res.Messages) != 0 {
		t.Errorf("a stream recreated after a reopen holds %q, want no messages from the deleted incarnation", payloads(res))
	}
	mustAppend(t, s, streamID, "fresh")
	assertPayloads(t, "reading the recreated stream after a reopen", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"fresh"})
}
