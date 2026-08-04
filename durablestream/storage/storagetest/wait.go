package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// waitOutcome is what a WaitForData call on another goroutine returned.
type waitOutcome struct {
	res *durablestream.ReadResult
	err error
}

// startWait calls WaitForData on its own goroutine and returns a channel that
// carries the outcome. It returns once the goroutine is running, which does not
// guarantee the call is already blocked — every property the suite checks holds
// either way, which is exactly the no-lost-wakeup guarantee.
func startWait(t *testing.T, s durablestream.Storage, streamID string, offset durablestream.Offset, limit int) <-chan waitOutcome {
	t.Helper()
	out := make(chan waitOutcome, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		res, err := s.WaitForData(t.Context(), streamID, offset, limit)
		out <- waitOutcome{res: res, err: err}
	}()
	<-started
	return out
}

// awaitWait collects a startWait outcome, failing the test if the waiter is
// never released.
func awaitWait(t *testing.T, what string, out <-chan waitOutcome) waitOutcome {
	t.Helper()
	select {
	case o := <-out:
		return o
	case <-time.After(waitTimeout):
		t.Fatalf("%s: WaitForData did not return within %s", what, waitTimeout)
		return waitOutcome{}
	}
}

// testWaitExistingData checks that WaitForData is a Read when data is already
// there, rather than waiting for the next append.
func testWaitExistingData(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-existing"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "first")
	last := mustAppend(t, s, streamID, "second")

	got := awaitWait(t, "waiting on a stream that already has data", startWait(t, s, streamID, durablestream.ZeroOffset, 0))
	if got.err != nil {
		t.Fatalf("WaitForData on a stream that already has data: %v", got.err)
	}
	assertPayloads(t, "WaitForData on a stream that already has data", payloads(got.res), []string{"first", "second"})
	if got.res.NextOffset != last {
		t.Errorf("NextOffset = %q, want the last message's offset %q", got.res.NextOffset, last)
	}
}

// testWaitWakesOnAppend checks that an append releases a waiter. The append is
// racing the waiter's decision to block, so the loop covers both interleavings:
// the message must be delivered whichever way the race lands.
func testWaitWakesOnAppend(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-append"
	mustCreate(t, s, streamID)

	offset := durablestream.ZeroOffset
	const rounds = 20
	for i := range rounds {
		payload := message(i)
		out := startWait(t, s, streamID, offset, 0)
		appended := mustAppend(t, s, streamID, payload)

		got := awaitWait(t, fmt.Sprintf("round %d", i), out)
		if got.err != nil {
			t.Fatalf("round %d: WaitForData: %v", i, got.err)
		}
		assertPayloads(t, fmt.Sprintf("round %d", i), payloads(got.res), []string{payload})
		if got.res.NextOffset != appended {
			t.Fatalf("round %d: NextOffset = %q, want the appended offset %q", i, got.res.NextOffset, appended)
		}
		offset = got.res.NextOffset
	}
}

// testWaitNoLostWakeups checks that a consumer looping on WaitForData sees every
// message a concurrent producer writes, in order and without polling. A lost
// wakeup shows up as the consumer stalling and the subtest timing out.
func testWaitNoLostWakeups(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-no-lost-wakeups"
	mustCreate(t, s, streamID)

	const count = 50
	produced := make(chan error, 1)
	go func() {
		for i := range count {
			if _, err := s.Append(t.Context(), streamID, []byte(message(i)), ""); err != nil {
				produced <- fmt.Errorf("append %d: %w", i, err)
				return
			}
		}
		produced <- nil
	}()

	consumed := make(chan []string, 1)
	go func() {
		var got []string
		offset := durablestream.ZeroOffset
		for len(got) < count {
			res, err := s.WaitForData(t.Context(), streamID, offset, 0)
			if err != nil {
				consumed <- got
				return
			}
			if len(res.Messages) == 0 {
				// WaitForData must not return empty-handed without an error;
				// stop rather than spin, and let the comparison below report it.
				consumed <- got
				return
			}
			for _, m := range res.Messages {
				got = append(got, string(m.Data))
			}
			offset = res.NextOffset
		}
		consumed <- got
	}()

	select {
	case err := <-produced:
		if err != nil {
			t.Fatalf("producer failed: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("producer did not finish within %s", waitTimeout)
	}

	var got []string
	select {
	case got = <-consumed:
	case <-time.After(waitTimeout):
		t.Fatalf("consumer did not receive all %d messages within %s: a wakeup was lost", count, waitTimeout)
	}

	want := make([]string, 0, count)
	for i := range count {
		want = append(want, message(i))
	}
	assertPayloads(t, "messages seen by a WaitForData consumer", got, want)
}

// testWaitWakesOnDelete checks that deleting a stream releases its waiters
// instead of leaving them blocked on a stream that no longer exists.
func testWaitWakesOnDelete(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-delete"
	mustCreate(t, s, streamID)

	out := startWait(t, s, streamID, durablestream.ZeroOffset, 0)
	mustDelete(t, s, streamID)

	got := awaitWait(t, "waiting on a stream that is deleted", out)
	assertErrorIs(t, "WaitForData on a stream deleted while waiting", got.err, durablestream.ErrNotFound)
}

// testWaitWakesOnExpiry checks that an otherwise idle waiter observes the
// stream becoming absent at its deadline. Requiring an unrelated append,
// delete, or caller deadline would let a WaitForData call outlive the stream
// incarnation it was bound to.
func testWaitWakesOnExpiry(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-expiry"
	expiresAt := time.Now().Add(250 * time.Millisecond)
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   expiresAt,
	})

	got := awaitWait(t, "waiting for a stream to expire", startWait(t, s, streamID, durablestream.ZeroOffset, 0))
	assertErrorIs(t, "WaitForData on a stream that expired while waiting", got.err, durablestream.ErrNotFound)
	if time.Now().Before(expiresAt) {
		t.Errorf("WaitForData returned before the stream's expiry %v", expiresAt)
	}
}

func testWaitTracksTouchedExpiry(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-touched-expiry"
	const ttl = 1500 * time.Millisecond
	initialExpiry := time.Now().Add(500 * time.Millisecond)
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         ttl,
		ExpiresAt:   initialExpiry,
	})

	out := startWait(t, s, streamID, durablestream.ZeroOffset, 0)
	// Give the waiter time to arm its original timer. Correctness does not rely
	// on this ordering: if Touch wins the race, the first read sees the renewal.
	time.Sleep(50 * time.Millisecond)
	if err := s.Touch(t.Context(), streamID); err != nil {
		t.Fatalf("Touch while WaitForData is blocked: %v", err)
	}
	renewedExpiry := mustHead(t, s, streamID).ExpiresAt
	if !renewedExpiry.After(initialExpiry) {
		t.Fatalf("Touch moved expiry to %v, want after initial deadline %v", renewedExpiry, initialExpiry)
	}

	// The old deadline must no longer release the waiter.
	untilPastOriginal := time.Until(initialExpiry.Add(100 * time.Millisecond))
	if untilPastOriginal > 0 {
		select {
		case got := <-out:
			t.Fatalf("WaitForData returned at the superseded expiry: result=%v error=%v", got.res, got.err)
		case <-time.After(untilPastOriginal):
		}
	}

	got := awaitWait(t, "waiting for the renewed expiry", out)
	assertErrorIs(t, "WaitForData after the renewed expiry", got.err, durablestream.ErrNotFound)
	if time.Now().Before(renewedExpiry) {
		t.Errorf("WaitForData returned before the renewed expiry %v", renewedExpiry)
	}
}

// testWaitWakesOnClose checks that Close releases waiters rather than leaking
// their goroutines for the life of the process.
func testWaitWakesOnClose(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-close"
	mustCreate(t, s, streamID)

	out := startWait(t, s, streamID, durablestream.ZeroOffset, 0)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := awaitWait(t, "waiting on a storage that is closed", out)
	assertErrorIs(t, "WaitForData on a storage closed while waiting", got.err, durablestream.ErrClosed)
}

// testWaitHonorsLimit checks that the byte limit applies to WaitForData exactly
// as it does to Read.
func testWaitHonorsLimit(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "wait-limit"
	mustCreate(t, s, streamID)
	for i := range 3 {
		mustAppend(t, s, streamID, fixedSizeMessage(i))
	}

	got := awaitWait(t, "waiting with a one-message limit", startWait(t, s, streamID, durablestream.ZeroOffset, fixedMessageSize))
	if got.err != nil {
		t.Fatalf("WaitForData: %v", got.err)
	}
	assertPayloads(t, "WaitForData with a one-message byte limit", payloads(got.res), []string{fixedSizeMessage(0)})

	// The rest is still reachable from the returned offset.
	rest := awaitWait(t, "waiting for the remainder", startWait(t, s, streamID, got.res.NextOffset, 0))
	if rest.err != nil {
		t.Fatalf("WaitForData for the remainder: %v", rest.err)
	}
	assertPayloads(t, "WaitForData for the remainder", payloads(rest.res), []string{fixedSizeMessage(1), fixedSizeMessage(2)})
}
