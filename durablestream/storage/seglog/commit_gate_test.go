package seglog

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

type admissionResult struct {
	admission commitAdmission
	err       error
}

func admitAsync(gate *commitGate) <-chan admissionResult {
	done := make(chan admissionResult, 1)
	go func() {
		admission, err := gate.admit()
		done <- admissionResult{admission: admission, err: err}
	}()
	return done
}

func awaitAdmission(t *testing.T, done <-chan admissionResult) commitAdmission {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("admit: %v", result.err)
		}
		return result.admission
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for commit admission")
		return commitAdmission{}
	}
}

func waitForQueued(t *testing.T, gate *commitGate, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		queued := gate.queued
		gate.mu.Unlock()
		if queued == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("commit gate did not reach %d queued waiters", want)
}

func TestCommitGate_SingleWaiterReleasesImmediately(t *testing.T) {
	gate := newCommitGate()
	admission := awaitAdmission(t, admitAsync(gate))
	admission.complete()
	if got := gate.completed.Load(); got != 1 {
		t.Errorf("completed waves = %d, want 1", got)
	}
}

func TestCommitGate_MultipleWaitersFormOneCohort(t *testing.T) {
	gate := newCommitGate()
	first, err := gate.admit()
	if err != nil {
		t.Fatal(err)
	}
	secondDone, thirdDone := admitAsync(gate), admitAsync(gate)
	waitForQueued(t, gate, 2)
	first.complete()

	second, third := awaitAdmission(t, secondDone), awaitAdmission(t, thirdDone)
	if second.wave != third.wave || second.wave != first.wave+1 {
		t.Errorf("cohort waves = (%d, %d), want %d", second.wave, third.wave, first.wave+1)
	}
	second.complete()
	third.complete()
	if got := gate.completed.Load(); got != 2 {
		t.Errorf("completed waves = %d, want 2", got)
	}
}

func TestCommitGate_FailingMemberDoesNotBlockNextWave(t *testing.T) {
	gate := newCommitGate()
	first, err := gate.admit()
	if err != nil {
		t.Fatal(err)
	}
	failingDone, peerDone := admitAsync(gate), admitAsync(gate)
	waitForQueued(t, gate, 2)
	first.complete()
	failing, peer := awaitAdmission(t, failingDone), awaitAdmission(t, peerDone)

	// A committer reports completion even when its own fdatasync fails.
	failing.complete()
	nextDone := admitAsync(gate)
	waitForQueued(t, gate, 1)
	select {
	case <-nextDone:
		t.Fatal("next wave admitted before every cohort member completed")
	default:
	}
	peer.complete()
	next := awaitAdmission(t, nextDone)
	next.complete()
}

func TestCommitGate_StorageCloseDrainsActiveCommit(t *testing.T) {
	opts := pipelineOptions(t.TempDir())
	storage, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	var cleanup sync.Once
	releaseSync := make(chan struct{})
	t.Cleanup(func() {
		cleanup.Do(func() { close(releaseSync) })
		_ = storage.Close()
	})
	if _, err := storage.Create(t.Context(), "close-gate", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	storage.parts[0].wal.blockNextSync(syncStarted, releaseSync)
	appendDone := appendAsync(storage, "close-gate", "value", "")
	awaitSignal(t, syncStarted, "active commit sync")
	closeDone := make(chan error, 1)
	go func() { closeDone <- storage.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before active commit drained: %v", err)
	default:
	}
	cleanup.Do(func() { close(releaseSync) })
	if result := awaitAppend(t, appendDone); result.err != nil {
		t.Fatalf("append: %v", result.err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain commit gate")
	}
	if _, err := storage.commitGate.admit(); !errors.Is(err, ErrClosed) {
		t.Errorf("admit after Close = %v, want ErrClosed", err)
	}
}
