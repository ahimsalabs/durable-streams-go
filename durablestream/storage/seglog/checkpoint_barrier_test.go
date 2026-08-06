package seglog

import (
	"sync/atomic"
	"testing"
	"time"
)

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
	results := make(chan bool, followers)
	for range followers {
		go func() {
			supported, _, err := barrier.run(func() (bool, error) {
				calls.Add(1)
				return true, nil
			})
			results <- supported && err == nil
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
	for range followers {
		select {
		case ok := <-results:
			if !ok {
				t.Error("coalesced checkpoint barrier returned an error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("coalesced checkpoint barrier did not finish")
		}
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("filesystem barrier calls = %d, want 2 epochs", got)
	}
}
