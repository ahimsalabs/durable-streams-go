package storagetest

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// The sliding-TTL subtests are the one part of the suite that has to watch a
// real clock: expiry is defined in wall-clock terms, and the Storage contract
// exposes no seam for injecting time. They are written so that only a delay of
// a full second in either direction can change an outcome.
//
// ttlWindow is the window every such subtest creates its stream with, and
// ttlMargin keeps observations away from the exact expiry boundary.
const (
	ttlWindow = 2 * time.Second
	ttlMargin = time.Second
)

// slidingConfig is the configuration a server creates a stream with from a
// Stream-TTL header: a window, plus the first deadline derived from it.
func slidingConfig(window time.Duration) durablestream.StreamConfig {
	return durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         window,
		ExpiresAt:   time.Now().Add(window),
	}
}

// assertGone checks that a stream reads as absent, which is how an expired
// stream must present itself.
func assertGone(t *testing.T, s durablestream.Storage, streamID, when string) {
	t.Helper()
	if _, err := s.Head(t.Context(), streamID); err == nil {
		t.Errorf("Head(%q) %s: stream is still live, want it expired", streamID, when)
	} else {
		assertErrorIs(t, "Head of an expired stream "+when, err, durablestream.ErrNotFound)
	}
}

// testCreateInitializesSlidingTTL verifies that TTL is a complete storage-level
// configuration: callers need not duplicate the handler's deadline arithmetic.
// It also checks that an idempotent replay does not slide the initialized window.
func testCreateInitializesSlidingTTL(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "create-initializes-ttl"
	streamCfg := durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         ttlWindow,
	}

	before := time.Now()
	mustCreateConfig(t, s, streamID, streamCfg)
	after := time.Now()
	info := mustHead(t, s, streamID)
	if info.ExpiresAt.IsZero() {
		t.Fatal("Create with a positive TTL left ExpiresAt zero")
	}
	if info.ExpiresAt.Before(before.Add(ttlWindow)) || info.ExpiresAt.After(after.Add(ttlWindow)) {
		t.Errorf("Create initialized ExpiresAt to %v, want now+TTL in [%v, %v]", info.ExpiresAt, before.Add(ttlWindow), after.Add(ttlWindow))
	}
	initialExpiry := info.ExpiresAt

	created, err := s.Create(t.Context(), streamID, streamCfg)
	if err != nil {
		t.Fatalf("idempotent Create with TTL and zero ExpiresAt: %v", err)
	}
	if created {
		t.Fatal("idempotent Create reported created=true")
	}
	if got := mustHead(t, s, streamID).ExpiresAt; !got.Equal(initialExpiry) {
		t.Errorf("idempotent Create moved initialized expiry from %v to %v", initialExpiry, got)
	}
}

// testTouchExtendsWindow checks the sliding half of a sliding TTL: a touched
// stream outlives its original deadline by a full window, and then expires.
func testTouchExtendsWindow(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "touch-extends"
	mustCreateConfig(t, s, streamID, slidingConfig(ttlWindow))

	// Touch half a window in, well before the stream could expire on its own.
	time.Sleep(ttlWindow / 2)
	touchBefore := time.Now()
	if err := s.Touch(t.Context(), streamID); err != nil {
		t.Fatalf("Touch(%q) on a live stream: unexpected error: %v", streamID, err)
	}
	touchAfter := time.Now()

	// The deadline must be the exact TTL window measured from the backend's
	// Touch instant. Bounding that instant by the call's start and end catches
	// implementations that add deadline slack while tolerating scheduling and
	// transaction latency.
	info := mustHead(t, s, streamID)
	if info.ExpiresAt.Before(touchBefore.Add(ttlWindow)) || info.ExpiresAt.After(touchAfter.Add(ttlWindow)) {
		t.Errorf("Head(%q).ExpiresAt is %v after Touch, want an exact now+TTL deadline in [%v, %v]", streamID, info.ExpiresAt, touchBefore.Add(ttlWindow), touchAfter.Add(ttlWindow))
	}

	// Past the original deadline, but still ttlMargin/2 before the exact
	// touched deadline.
	time.Sleep(ttlWindow/2 + ttlMargin/2)
	info = mustHead(t, s, streamID)
	if info.TTL != ttlWindow {
		t.Errorf("Head(%q).TTL is %v, want the configured window %v: Touch must move the deadline, not the window", streamID, info.TTL, ttlWindow)
	}

	// Now well past a whole window of idleness, so the extension must have run
	// out at exactly touch-time+TTL.
	time.Sleep(ttlWindow/2 + ttlMargin)
	assertGone(t, s, streamID, "a full window after the last Touch")
}

// testTouchLeavesAbsoluteExpiryAlone checks that Touch moves a sliding window
// only: a stream created with an absolute expiry and no TTL keeps its deadline
// no matter how much activity it sees.
func testTouchLeavesAbsoluteExpiryAlone(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "touch-absolute-expiry"
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(ttlWindow),
	})

	time.Sleep(ttlWindow / 2)
	if err := s.Touch(t.Context(), streamID); err != nil {
		t.Fatalf("Touch(%q) on a live stream with an absolute expiry: unexpected error: %v, want a no-op reporting nil", streamID, err)
	}

	time.Sleep(ttlWindow/2 + ttlMargin)
	assertGone(t, s, streamID, "at its absolute expiry despite a Touch")
}

// testNoImplicitWindowExtension checks the other half of the contract: reads and
// writes do not slide the window by themselves. Only Touch does, so that the
// caller decides which requests count as activity.
func testNoImplicitWindowExtension(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "no-implicit-extension"
	mustCreateConfig(t, s, streamID, slidingConfig(ttlWindow))

	// Exercise every operation that a caller might expect to renew the window.
	time.Sleep(ttlWindow / 2)
	mustAppend(t, s, streamID, "data")
	mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	mustHead(t, s, streamID)

	// The window still closes on the schedule set at creation.
	time.Sleep(ttlWindow/2 + ttlMargin)
	assertGone(t, s, streamID, "after reads and writes that were never touched")
}

// testTouchRacesLifecycle checks that Touch is safe against the operations that
// replace the record it updates. Run under -race, this is what catches a
// backend that moves a stream's expiry outside the lock protecting its config.
func testTouchRacesLifecycle(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "touch-races-lifecycle"
	const rounds = 50

	// Every operation here may legitimately lose the race and report the stream
	// absent, so the subtest asserts only that failures stay inside the
	// contract's sentinels; the race detector supplies the rest.
	touchErrs := make([]error, rounds)

	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			_, _ = s.Create(t.Context(), streamID, slidingConfig(time.Hour))
		}
	})
	wg.Go(func() {
		for range rounds {
			_ = s.Delete(t.Context(), streamID)
		}
	})
	wg.Go(func() {
		for i := range rounds {
			touchErrs[i] = s.Touch(t.Context(), streamID)
		}
	})
	wg.Go(func() {
		for range rounds {
			_, _ = s.Read(t.Context(), streamID, durablestream.ZeroOffset, 0)
		}
	})
	wg.Wait()

	for i, err := range touchErrs {
		if err != nil && !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Touch %d during concurrent create and delete returned %v, want nil or ErrNotFound", i, err)
		}
	}
}

// testTouchMissingStream checks that Touch reports an absent stream rather than
// creating or resurrecting one.
func testTouchMissingStream(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)

	err := s.Touch(t.Context(), "never-created")
	assertErrorIs(t, "Touch of a missing stream", err, durablestream.ErrNotFound)

	const expiredID = "touch-expired"
	mustCreateConfig(t, s, expiredID, durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         time.Hour,
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	err = s.Touch(t.Context(), expiredID)
	assertErrorIs(t, "Touch of an expired stream", err, durablestream.ErrNotFound)

	// The expired stream must still read as absent: a Touch that revived it
	// would contradict every other operation, and Create's right to replace it.
	assertGone(t, s, expiredID, "after a Touch that must not revive it")
}
