package seglog

import (
	"sync/atomic"
	"testing"
	"time"
)

type checkpointBarrierResult struct {
	supported bool
	performed bool
	err       error
}

func TestCheckpointBarrier_RequestsDuringSyncShareNextEpoch(t *testing.T) {
	var barrier checkpointBarrier
	var calls atomic.Int64
	firstStarted, releaseFirst := make(chan struct{}), make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _, _ = barrier.run(func() (bool, error) {
			calls.Add(1)
			close(firstStarted)
			<-releaseFirst
			return true, nil
		})
	}()
	awaitSignal(t, firstStarted, "first checkpoint barrier")

	const followers = 4
	results := make(chan checkpointBarrierResult, followers)
	for range followers {
		go func() {
			supported, performed, err := barrier.run(func() (bool, error) {
				calls.Add(1)
				return true, nil
			})
			results <- checkpointBarrierResult{supported: supported, performed: performed, err: err}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		barrier.mu.Lock()
		ready := barrier.tail != nil && barrier.tail.epoch == 2 && barrier.tail.waiters == followers
		barrier.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("followers did not join the next checkpoint epoch")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first checkpoint barrier did not finish")
	}
	var performed int
	for range followers {
		select {
		case result := <-results:
			if result.err != nil || !result.supported {
				t.Errorf("coalesced checkpoint barrier = (%v, %v), want (true, nil)", result.supported, result.err)
			}
			if result.performed {
				performed++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("coalesced checkpoint barrier did not finish")
		}
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("filesystem barrier calls = %d, want 2 epochs", got)
	}
	if performed != 1 {
		t.Errorf("followers performing coalesced barrier = %d, want 1", performed)
	}
}
