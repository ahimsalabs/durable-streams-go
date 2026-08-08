package seglog

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type admissionResult struct {
	admission syncAdmission
	err       error
}

func admitAsync(limiter *syncLimiter) <-chan admissionResult {
	done := make(chan admissionResult, 1)
	go func() {
		admission, err := limiter.admit()
		done <- admissionResult{admission: admission, err: err}
	}()
	return done
}

func awaitAdmission(t *testing.T, done <-chan admissionResult) syncAdmission {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("admit: %v", result.err)
		}
		return result.admission
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for sync admission")
		return syncAdmission{}
	}
}

func TestSyncLimiter_AtMostLimitHoldersRunConcurrently(t *testing.T) {
	const (
		limit   = 3
		writers = 12
	)

	limiter := newSyncLimiter(limit)
	release := make(chan struct{})
	entered := make(chan struct{}, writers)
	var mu sync.Mutex
	active := 0
	maxActive := 0
	var workers sync.WaitGroup
	for range writers {
		workers.Go(func() {
			admission, err := limiter.admit()
			if err != nil {
				t.Errorf("admit: %v", err)
				return
			}
			mu.Lock()
			active++
			maxActive = max(maxActive, active)
			mu.Unlock()
			entered <- struct{}{}
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			admission.complete()
		})
	}
	for range limit {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for holders")
		}
	}
	select {
	case <-entered:
		t.Fatalf("more than %d holders entered before release", limit)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	workers.Wait()

	mu.Lock()
	defer mu.Unlock()
	if maxActive != limit {
		t.Errorf("maximum concurrent holders = %d, want %d", maxActive, limit)
	}
	if active != 0 {
		t.Errorf("active holders after completion = %d, want 0", active)
	}
	if got := limiter.completed.Load(); got != writers {
		t.Errorf("completed admissions = %d, want %d", got, writers)
	}
}

func TestSyncLimiter_AcquirerBeyondLimitBlocksUntilRelease(t *testing.T) {
	const limit = 2
	limiter := newSyncLimiter(limit)
	holders := make([]syncAdmission, limit)
	for i := range holders {
		admission, err := limiter.admit()
		if err != nil {
			t.Fatalf("admit holder %d: %v", i, err)
		}
		holders[i] = admission
	}

	waiting := admitAsync(limiter)
	select {
	case result := <-waiting:
		t.Fatalf("acquirer beyond limit returned before release: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}
	holders[0].complete()
	next := awaitAdmission(t, waiting)
	next.complete()
	holders[1].complete()
}

func TestSyncLimiter_CloseUnblocksWaitersAndRejectsAdmissions(t *testing.T) {
	limiter := newSyncLimiter(1)
	holder, err := limiter.admit()
	if err != nil {
		t.Fatal(err)
	}
	waiting := admitAsync(limiter)
	select {
	case result := <-waiting:
		t.Fatalf("waiter returned while slot held: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}

	closed := make(chan struct{})
	go func() {
		limiter.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close blocked on an in-flight holder")
	}
	select {
	case result := <-waiting:
		if !errors.Is(result.err, ErrClosed) {
			t.Errorf("blocked admit after close = %v, want ErrClosed", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not unblock waiter")
	}
	if _, err := limiter.admit(); !errors.Is(err, ErrClosed) {
		t.Errorf("admit after Close = %v, want ErrClosed", err)
	}
	// Releasing an admission after close remains valid and does not block close.
	holder.complete()
}

func TestSyncLimiter_ConcurrentWritersAllMakeProgress(t *testing.T) {
	const writers = 128

	limiter := newSyncLimiter(8)
	start := make(chan struct{})
	errs := make(chan error, writers)
	var workers sync.WaitGroup
	for range writers {
		workers.Go(func() {
			<-start
			admission, err := limiter.admit()
			if err == nil {
				admission.complete()
			}
			errs <- err
		})
	}
	close(start)

	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent writers did not all make progress")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("admit: %v", err)
		}
	}
	if got := limiter.completed.Load(); got != writers {
		t.Errorf("completed admissions = %d, want %d", got, writers)
	}
}

func TestSyncLimiter_ConcurrencyOneSerializesHolders(t *testing.T) {
	const writers = 16

	limiter := newSyncLimiter(1)
	var mu sync.Mutex
	active := 0
	overlapped := false
	var workers sync.WaitGroup
	for range writers {
		workers.Go(func() {
			admission, err := limiter.admit()
			if err != nil {
				t.Errorf("admit: %v", err)
				return
			}
			mu.Lock()
			active++
			if active > 1 {
				overlapped = true
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
			admission.complete()
		})
	}
	workers.Wait()
	mu.Lock()
	defer mu.Unlock()
	if overlapped {
		t.Error("holders overlapped with concurrency limit 1")
	}
}
