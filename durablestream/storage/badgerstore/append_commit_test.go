package badgerstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

func TestAppendCommitterModeSelection(t *testing.T) {
	tests := []struct {
		name        string
		options     func(*testing.T) Options
		wantBatcher bool
	}{
		{
			name: "durable default",
			options: func(t *testing.T) Options {
				return Options{Dir: t.TempDir()}
			},
			wantBatcher: true,
		},
		{
			name: "durability disabled",
			options: func(t *testing.T) Options {
				return Options{Dir: t.TempDir(), SyncWrites: SyncWritesDisabled}
			},
		},
		{
			name: "in memory default",
			options: func(*testing.T) Options {
				return Options{InMemory: true}
			},
		},
		{
			name: "in memory explicit sync",
			options: func(*testing.T) Options {
				return Options{InMemory: true, SyncWrites: SyncWritesEnabled}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.options(t)
			opts.Logger = &quietLogger{}
			opts.SLogger = quietSLog()
			opts.GCInterval = -1
			opts.CleanupInterval = -1
			opts.ReapInterval = time.Hour
			s, err := New(opts)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			<-s.initialReapDone
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			if got := s.appendCommits != nil; got != tt.wantBatcher {
				t.Fatalf("append committer enabled = %v, want %v", got, tt.wantBatcher)
			}
		})
	}
}

func TestAppendCommitterGroupsIndependentStreams(t *testing.T) {
	s := newDiskStorage(t, t.TempDir())
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	const streamCount = 32
	streamIDs := make([]string, streamCount)
	gens := make([]generation, streamCount)
	for i := range streamCount {
		streamIDs[i] = fmt.Sprintf("stream-%02d", i)
		mustCreate(t, s, streamIDs[i])
		gens[i] = currentGeneration(t, s, streamIDs[i])
	}

	// Make this test insensitive to scheduler jitter. Production uses 200us;
	// the longer white-box deadline changes only how long the first request may
	// wait for its independent peers.
	s.appendCommits.config.maxWait = 100 * time.Millisecond
	before := s.appendCommits.transactionAttempts.Load()

	type outcome struct {
		offset durablestream.Offset
		err    error
	}
	outcomes := make([]outcome, streamCount)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range streamCount {
		wg.Go(func() {
			<-start
			outcomes[i].offset, outcomes[i].err = s.Append(t.Context(), streamIDs[i], []byte("value"), "")
		})
	}
	close(start)
	wg.Wait()

	if attempts := s.appendCommits.transactionAttempts.Load() - before; attempts != 1 {
		t.Fatalf("physical append transactions = %d, want 1", attempts)
	}

	var wantVersion uint64
	if err := s.db.View(func(txn *badger.Txn) error {
		for i := range streamCount {
			if outcomes[i].err != nil {
				return fmt.Errorf("append %q: %w", streamIDs[i], outcomes[i].err)
			}
			item, err := txn.Get(messageKey(streamIDs[i], gens[i], outcomes[i].offset))
			if err != nil {
				return err
			}
			if i == 0 {
				wantVersion = item.Version()
			} else if item.Version() != wantVersion {
				return fmt.Errorf("stream %q committed at version %d, want shared version %d",
					streamIDs[i], item.Version(), wantVersion)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify grouped transaction: %v", err)
	}
}

func TestAppendCommitterIsolatesSemanticFailure(t *testing.T) {
	s := newDiskStorage(t, t.TempDir())
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	mustCreate(t, s, "healthy")
	mustCreate(t, s, "regressing")
	if _, err := s.Append(t.Context(), "regressing", []byte("accepted"), "0002"); err != nil {
		t.Fatalf("seed sequence: %v", err)
	}

	s.appendCommits.config.maxWait = 100 * time.Millisecond
	before := s.appendCommits.transactionAttempts.Load()
	start := make(chan struct{})
	var (
		healthyErr error
		badErr     error
		wg         sync.WaitGroup
	)
	wg.Go(func() {
		<-start
		_, healthyErr = s.Append(t.Context(), "healthy", []byte("committed"), "")
	})
	wg.Go(func() {
		<-start
		_, badErr = s.Append(t.Context(), "regressing", []byte("rejected"), "0001")
	})
	close(start)
	wg.Wait()

	if healthyErr != nil {
		t.Fatalf("healthy append: %v", healthyErr)
	}
	if !errors.Is(badErr, durablestream.ErrConflict) {
		t.Fatalf("regressing append error = %v, want ErrConflict", badErr)
	}
	if attempts := s.appendCommits.transactionAttempts.Load() - before; attempts != 1 {
		t.Fatalf("physical append transactions = %d, want 1", attempts)
	}
	if got := readAll(t, s, "healthy"); len(got) != 1 || got[0] != "committed" {
		t.Fatalf("healthy messages = %q, want [committed]", got)
	}
	if got := readAll(t, s, "regressing"); len(got) != 1 || got[0] != "accepted" {
		t.Fatalf("regressing messages = %q, want [accepted]", got)
	}
}

func TestOffsetHighWaterCompatibility(t *testing.T) {
	t.Run("leased value leaves a safe gap", func(t *testing.T) {
		s := newTestStorage(t)
		mustCreate(t, s, "stream")
		gen := currentGeneration(t, s, "stream")
		var leased [8]byte
		binary.BigEndian.PutUint64(leased[:], 100)
		if err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(seqKey("stream", gen), leased[:])
		}); err != nil {
			t.Fatalf("write leased high-water: %v", err)
		}

		offset, err := s.Append(t.Context(), "stream", []byte("value"), "")
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if want := durablestream.Offset("0000000000000000_0000000000000101"); offset != want {
			t.Fatalf("offset = %q, want %q", offset, want)
		}
	})

	t.Run("missing value derives visible tail", func(t *testing.T) {
		s := newTestStorage(t)
		created, _, err := s.CreateWithMessages(t.Context(), "stream", durablestream.StreamConfig{
			ContentType: "text/plain",
		}, [][]byte{[]byte("initial")})
		if err != nil || !created {
			t.Fatalf("CreateWithMessages = (%v, %v), want (true, nil)", created, err)
		}
		gen := currentGeneration(t, s, "stream")
		if err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(seqKey("stream", gen))
		}); err != nil {
			t.Fatalf("delete high-water: %v", err)
		}

		offset, err := s.Append(t.Context(), "stream", []byte("next"), "")
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if want := durablestream.Offset("0000000000000000_0000000000000002"); offset != want {
			t.Fatalf("offset = %q, want %q", offset, want)
		}
	})

	t.Run("malformed value fails without visibility", func(t *testing.T) {
		s := newTestStorage(t)
		mustCreate(t, s, "stream")
		gen := currentGeneration(t, s, "stream")
		if err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(seqKey("stream", gen), []byte("short"))
		}); err != nil {
			t.Fatalf("write malformed high-water: %v", err)
		}

		if _, err := s.Append(t.Context(), "stream", []byte("value"), ""); err == nil {
			t.Fatal("Append succeeded with malformed offset high-water")
		}
		if got := readAll(t, s, "stream"); len(got) != 0 {
			t.Fatalf("messages after rejected append = %q, want none", got)
		}
	})
}

func TestCloseReleasesQueuedDurableAppends(t *testing.T) {
	dir := t.TempDir()
	s := newDiskStorage(t, dir)

	const streamCount = 64
	streamIDs := make([]string, streamCount)
	for i := range streamCount {
		streamIDs[i] = fmt.Sprintf("stream-%02d", i)
		mustCreate(t, s, streamIDs[i])
	}
	// Encourage a real queue at the instant Close races admission.
	s.appendCommits.config.maxWait = 100 * time.Millisecond

	type outcome struct {
		succeeded bool
		err       error
	}
	outcomes := make([]outcome, streamCount)
	start := make(chan struct{})
	ready := make(chan struct{}, streamCount)
	var wg sync.WaitGroup
	for i := range streamCount {
		wg.Go(func() {
			ready <- struct{}{}
			<-start
			_, outcomes[i].err = s.Append(t.Context(), streamIDs[i], []byte("value"), "")
			outcomes[i].succeeded = outcomes[i].err == nil
		})
	}
	for range streamCount {
		<-ready
	}
	close(start)

	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	wg.Wait()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("Close still blocked after %s", waitTimeout)
	}
	for i := range streamCount {
		if outcomes[i].err != nil && !errors.Is(outcomes[i].err, durablestream.ErrClosed) {
			t.Fatalf("Append %q during Close = %v, want nil or ErrClosed", streamIDs[i], outcomes[i].err)
		}
	}

	reopened := newDiskStorage(t, dir)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	})
	for i := range streamCount {
		messages := readAll(t, reopened, streamIDs[i])
		if outcomes[i].succeeded {
			if len(messages) != 1 || messages[0] != "value" {
				t.Fatalf("successful Append %q recovered messages %q, want [value]", streamIDs[i], messages)
			}
		} else if len(messages) != 0 {
			t.Fatalf("failed Append %q recovered messages %q, want none", streamIDs[i], messages)
		}
	}
}
