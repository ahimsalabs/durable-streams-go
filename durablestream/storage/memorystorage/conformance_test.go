package memorystorage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/storagetest"
)

// TestConformance runs the shared storage conformance suite. Reopen is left
// unset: in-memory storage keeps nothing across instances, so the durability
// subtests are skipped.
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Config{
		New: func(t *testing.T) durablestream.Storage {
			s := New()
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("closing storage: %v", err)
				}
			})
			return s
		},
		// MaxMessageSize is zero: this implementation imposes no limit.
		FutureOffset: "0000000000000000_0000000000009999",
		MalformedOffsets: []durablestream.Offset{
			"0000000000000000",
			"abc_def",
			"not-an-offset",
			"-2",
			"0000000000000000_-5",
		},
		RequireIncarnationID: true,
		RequireAtomicBatches: true,
		RequireAtomicClose:   true,
		RequireForks:         true,
	})
}

// Regression: Read used to reject a negative limit's stream by treating it as
// unlimited, which disagreed with the Storage contract and with badgerstore.
func TestReadRejectsNegativeLimit(t *testing.T) {
	s := New()
	if _, err := s.Create(t.Context(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Append(t.Context(), "test", []byte("data"), ""); err != nil {
		t.Fatalf("append: %v", err)
	}

	if _, err := s.Read(t.Context(), "test", durablestream.ZeroOffset, -1); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("Read with limit -1 returned %v, want an error matching ErrBadRequest", err)
	}
}

// Regression: Close used to be a no-op, leaving WaitForData callers blocked
// forever on a storage that had been shut down.
func TestCloseReleasesWaitForDataWaiters(t *testing.T) {
	s := New()
	if _, err := s.Create(t.Context(), "test", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := s.WaitForData(context.Background(), "test", durablestream.ZeroOffset, 0)
		done <- err
	}()

	<-started
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, durablestream.ErrClosed) {
			t.Errorf("WaitForData returned %v after Close, want an error matching ErrClosed", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("WaitForData was not released by Close")
	}
}
