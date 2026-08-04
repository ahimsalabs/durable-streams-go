package badgerstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// waitTimeout bounds how long a test waits for a call that must not block.
// It only fires when the code under test is broken.
const waitTimeout = 30 * time.Second

// TestErrClosedWrapsDurablestreamErrClosed guards the error taxonomy: callers
// (and the HTTP error mapping) classify a closed store via the shared sentinel.
func TestErrClosedWrapsDurablestreamErrClosed(t *testing.T) {
	if !errors.Is(ErrClosed, durablestream.ErrClosed) {
		t.Fatalf("ErrClosed does not wrap durablestream.ErrClosed")
	}

	s := newTestStorage(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// newTestStorage's cleanup closes again; Close is idempotent.

	if _, err := s.Head(context.Background(), "stream"); !errors.Is(err, durablestream.ErrClosed) {
		t.Errorf("Head after close = %v, want durablestream.ErrClosed", err)
	}
}

// TestCloseReleasesWaitForDataWaiters ensures Close unblocks long-polling
// readers so a server shutdown can drain. Without the fix the waiter parks
// until its own deadline.
func TestCloseReleasesWaitForDataWaiters(t *testing.T) {
	s, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		GCInterval:      -1,
		CleanupInterval: -1,
		ReapInterval:    time.Hour,
	})
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	<-s.initialReapDone

	ctx := context.Background()
	if _, err := s.Create(ctx, "stream", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const waiters = 4
	errs := make(chan error, waiters)
	for range waiters {
		go func() {
			// No deadline: only Close can release this call.
			_, err := s.WaitForData(ctx, "stream", "", 0)
			errs <- err
		}()
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for i := range waiters {
		select {
		case err := <-errs:
			if !errors.Is(err, durablestream.ErrClosed) {
				t.Errorf("waiter %d: WaitForData = %v, want durablestream.ErrClosed", i, err)
			}
		case <-time.After(waitTimeout):
			t.Fatalf("waiter %d still blocked %s after Close", i, waitTimeout)
		}
	}
}

// TestWaitForDataStillWakesOnAppend guards against the shutdown wiring
// breaking normal notification delivery.
func TestWaitForDataStillWakesOnAppend(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "stream")

	type result struct {
		data []string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		res, err := s.WaitForData(ctx, "stream", "", 0)
		if err != nil {
			done <- result{err: err}
			return
		}
		var data []string
		for _, m := range res.Messages {
			data = append(data, string(m.Data))
		}
		done <- result{data: data}
	}()

	// Whether the waiter is already parked or has not read yet, the append
	// must satisfy it: parked waiters are woken, later readers see the data.
	mustAppend(t, s, "stream", "hello")

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("WaitForData: %v", got.err)
		}
		if len(got.data) == 0 || got.data[0] != "hello" {
			t.Fatalf("WaitForData returned %v, want first message \"hello\"", got.data)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("WaitForData still blocked %s after append", waitTimeout)
	}
}

// TestCloseDuringConcurrentOperations hammers Close against in-flight
// operations. Badger panics rather than returning an error when a closed
// database is used, so an operation that slips past the closed check and into
// a transaction takes the process down: any panic here fails the test.
func TestCloseDuringConcurrentOperations(t *testing.T) {
	const (
		rounds  = 10
		workers = 8
		ops     = 50
	)

	for range rounds {
		s, err := New(Options{
			InMemory:        true,
			Logger:          &quietLogger{},
			SLogger:         quietSLog(),
			GCInterval:      -1,
			CleanupInterval: -1,
			ReapInterval:    time.Hour,
		})
		if err != nil {
			t.Fatalf("new storage: %v", err)
		}
		<-s.initialReapDone

		ctx := context.Background()
		mustCreate(t, s, "stream")
		mustAppend(t, s, "stream", "seed")
		// A second, empty stream so the WaitForData workers actually park.
		// Close wakes them, and they loop straight back into a read — the
		// window that made the original panic so easy to hit.
		mustCreate(t, s, "empty")

		// Every operation must either succeed or report a closed storage.
		check := func(err error) {
			if err != nil && !errors.Is(err, durablestream.ErrClosed) {
				t.Errorf("operation during close = %v, want nil or durablestream.ErrClosed", err)
			}
		}

		// Each worker reports once it is inside its loop, so Close lands while
		// operations are genuinely in flight rather than before they start.
		ready := make(chan struct{}, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Go(func() {
				// Waiters report before their first call, since that call
				// parks until Close; everyone else reports after one op, by
				// which point they are looping over the database.
				signalAt := 1
				if i%4 == 3 {
					signalAt = 0
				}
				for op := range ops {
					if op == signalAt {
						ready <- struct{}{}
					}
					switch i % 4 {
					case 0:
						_, err := s.Read(ctx, "stream", "", 0)
						check(err)
					case 1:
						_, err := s.Append(ctx, "stream", []byte("data"), "")
						check(err)
					case 2:
						_, err := s.Head(ctx, "stream")
						check(err)
					default:
						_, err := s.WaitForData(ctx, "empty", "", 0)
						check(err)
					}
				}
			})
		}

		for range workers {
			<-ready
		}
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
		wg.Wait()
	}
}

func TestSyncWritesResolution(t *testing.T) {
	tests := []struct {
		name    string
		mode    SyncWrites
		onDisk  bool
		want    bool
		wantErr bool
	}{
		{name: "default on disk fsyncs", mode: SyncWritesDefault, onDisk: true, want: true},
		{name: "default in memory does not", mode: SyncWritesDefault, onDisk: false, want: false},
		{name: "enabled", mode: SyncWritesEnabled, onDisk: false, want: true},
		{name: "disabled on disk", mode: SyncWritesDisabled, onDisk: true, want: false},
		{name: "invalid", mode: SyncWrites(42), onDisk: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.mode.enabled(tt.onDisk)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error for an invalid SyncWrites value")
				}
				return
			}
			if err != nil {
				t.Fatalf("enabled: %v", err)
			}
			if got != tt.want {
				t.Errorf("enabled(%v) = %v, want %v", tt.onDisk, got, tt.want)
			}
		})
	}
}

// TestSyncWritesDiskRoundTrip checks that both sync modes open and serve
// traffic on a real directory.
func TestSyncWritesDiskRoundTrip(t *testing.T) {
	for _, mode := range []SyncWrites{SyncWritesDefault, SyncWritesEnabled, SyncWritesDisabled} {
		dir := t.TempDir()
		s, err := New(Options{
			Dir:             dir,
			Logger:          &quietLogger{},
			SLogger:         quietSLog(),
			MaxMessageSize:  1024,
			GCInterval:      -1,
			CleanupInterval: -1,
			ReapInterval:    time.Hour,
			SyncWrites:      mode,
		})
		if err != nil {
			t.Fatalf("SyncWrites=%d: new storage: %v", mode, err)
		}
		mustCreate(t, s, "stream")
		mustAppend(t, s, "stream", "durable")
		if got := readAll(t, s, "stream"); len(got) != 1 || got[0] != "durable" {
			t.Errorf("SyncWrites=%d: contents = %v, want [durable]", mode, got)
		}
		if err := s.Close(); err != nil {
			t.Errorf("SyncWrites=%d: close: %v", mode, err)
		}
	}
}

func TestNewRejectsInvalidSyncWrites(t *testing.T) {
	_, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		GCInterval:      -1,
		CleanupInterval: -1,
		SyncWrites:      SyncWrites(99),
	})
	if err == nil {
		t.Fatal("expected New to reject an invalid SyncWrites value")
	}
}
