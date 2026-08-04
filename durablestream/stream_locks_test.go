package durablestream

import (
	"context"
	"testing"
	"time"
)

func TestKeyedMutexSerializesSameStreamAndReclaimsEntries(t *testing.T) {
	var locks keyedMutex
	unlockFirst := locks.lock("/same")

	sameStarted := make(chan struct{})
	sameAcquired := make(chan struct{})
	sameDone := make(chan struct{})
	go func() {
		close(sameStarted)
		unlock := locks.lock("/same")
		close(sameAcquired)
		unlock()
		close(sameDone)
	}()
	<-sameStarted

	// Wait until the second caller has registered its reference and is therefore
	// definitely blocked on the per-key mutex. A fixed sleep could pass without
	// ever scheduling the goroutine under test.
	deadline := time.Now().Add(time.Second)
	for {
		locks.mu.Lock()
		refs := locks.locks["/same"].refs
		locks.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second mutation never queued on the same stream")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-sameAcquired:
		t.Fatal("second mutation of the same stream acquired concurrently")
	default:
	}

	// A hash collision or global mutex must not serialize distinct stream IDs.
	differentAcquired := make(chan struct{})
	differentDone := make(chan struct{})
	go func() {
		unlock := locks.lock("/different")
		close(differentAcquired)
		unlock()
		close(differentDone)
	}()
	select {
	case <-differentAcquired:
	case <-time.After(time.Second):
		t.Fatal("mutation of a different stream was blocked")
	}
	<-differentDone

	unlockFirst()
	select {
	case <-sameDone:
	case <-time.After(time.Second):
		t.Fatal("same-stream waiter was not released")
	}

	locks.mu.Lock()
	defer locks.mu.Unlock()
	if got := len(locks.locks); got != 0 {
		t.Fatalf("keyed mutex retained %d idle entries, want 0", got)
	}
}

func TestKeyedMutexTokenInvalidatesOnLifecycleChangeAndReclaimsEntry(t *testing.T) {
	var locks keyedMutex

	unlockSnapshot := locks.lock("/stream")
	token := locks.pin("/stream")
	ctx, cancel := token.context(context.Background())
	unlockSnapshot()

	select {
	case <-ctx.Done():
		t.Fatal("fresh incarnation token was already canceled")
	default:
	}

	unlockMutation := locks.lock("/stream")
	locks.bump("/stream")
	unlockMutation()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("lifecycle mutation did not cancel the pinned context")
	}
	if !token.invalidated() {
		t.Fatal("token did not report lifecycle invalidation")
	}

	cancel()
	token.release()

	locks.mu.Lock()
	defer locks.mu.Unlock()
	if got := len(locks.locks); got != 0 {
		t.Fatalf("keyed mutex retained %d idle entries after token release, want 0", got)
	}
}
