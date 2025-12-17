package badgerstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// quietSLogger returns a silent slog.Logger for tests.
func quietSLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCleanupExpiredStreams(t *testing.T) {
	s := newTestStorage(t)

	// Create an expired stream
	_, _ = s.Create(context.Background(), "expired", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	// Create a valid stream
	_, _ = s.Create(context.Background(), "valid", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(time.Hour),
	})

	// Run cleanup
	s.cleanupExpiredStreams(context.Background())

	// Expired stream should be gone
	_, err := s.Head(context.Background(), "expired")
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected expired stream to be deleted, got: %v", err)
	}

	// Valid stream should still exist
	_, err = s.Head(context.Background(), "valid")
	if err != nil {
		t.Errorf("valid stream should still exist: %v", err)
	}
}

func TestBackgroundGoroutines(t *testing.T) {
	// Test that background goroutines start and stop properly
	s, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		SLogger:         quietSLogger(),
		GCInterval:      10 * time.Millisecond,
		CleanupInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// Let goroutines run a bit
	time.Sleep(50 * time.Millisecond)

	// Close should wait for goroutines to finish
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRunGC(t *testing.T) {
	s := newTestStorage(t)
	// RunGC should not error on empty/in-memory DB
	// (it may return ErrNoRewrite which is fine)
	_ = s.RunGC()
}

func TestDoubleClose(t *testing.T) {
	s, err := New(Options{InMemory: true, Logger: &quietLogger{}})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// First close should succeed
	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	// Second close should be a no-op (not panic)
	if err := s.Close(); err != nil {
		t.Fatalf("second close should not error: %v", err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	s, err := New(Options{InMemory: true, Logger: &quietLogger{}})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// All operations should return ErrClosed
	ctx := context.Background()

	_, err = s.Create(ctx, "test", durablestream.StreamConfig{ContentType: "text/plain"})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Create after close should return ErrClosed, got: %v", err)
	}

	_, err = s.Append(ctx, "test", []byte("data"), "")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Append after close should return ErrClosed, got: %v", err)
	}

	_, err = s.Read(ctx, "test", "", 0)
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Read after close should return ErrClosed, got: %v", err)
	}

	_, err = s.Head(ctx, "test")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Head after close should return ErrClosed, got: %v", err)
	}

	err = s.Delete(ctx, "test")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Delete after close should return ErrClosed, got: %v", err)
	}

	_, err = s.Subscribe(ctx, "test", "")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Subscribe after close should return ErrClosed, got: %v", err)
	}

	err = s.RunGC()
	if !errors.Is(err, ErrClosed) {
		t.Errorf("RunGC after close should return ErrClosed, got: %v", err)
	}
}

func TestCleanupInterruption(t *testing.T) {
	s := newTestStorage(t)

	// Create several expired streams
	for i := 0; i < 10; i++ {
		_, _ = s.Create(context.Background(), fmt.Sprintf("expired-%d", i), durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		})
	}

	// Create a context that we'll cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Cleanup should respect cancellation and return early
	s.cleanupExpiredStreams(ctx)

	// Some streams may not have been deleted due to cancellation
	// (this is expected behavior - cleanup is interruptible)
}

func TestCleanupWithMalformedConfig(t *testing.T) {
	s := newTestStorage(t)

	// Manually insert a malformed config directly into badger
	err := s.db.Update(func(txn *badger.Txn) error {
		// Insert invalid JSON as stream config
		return txn.Set([]byte("c:malformed"), []byte("not valid json{{{"))
	})
	if err != nil {
		t.Fatalf("failed to insert malformed config: %v", err)
	}

	// Create a valid expired stream
	_, _ = s.Create(context.Background(), "valid-expired", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	// Cleanup should skip malformed config and still delete valid expired stream
	s.cleanupExpiredStreams(context.Background())

	// Valid expired stream should be deleted
	_, err = s.Head(context.Background(), "valid-expired")
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected valid-expired to be deleted, got: %v", err)
	}

	// Malformed config should still exist (cleanup skips it)
	err = s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("c:malformed"))
		return err
	})
	if err != nil {
		t.Errorf("malformed config should still exist: %v", err)
	}
}

func TestShutdownTimeout(t *testing.T) {
	// Create storage with very short shutdown timeout and slow cleanup
	s, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		GCInterval:      -1,            // Disable GC
		CleanupInterval: time.Hour,     // Disable automatic cleanup
		ShutdownTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// Manually add a subscriber that will block
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _ = s.Create(context.Background(), "test", durablestream.StreamConfig{ContentType: "text/plain"})
	_, _ = s.Subscribe(ctx, "test", "")

	// Close should complete even if subscriber goroutine hasn't finished
	// (because we haven't cancelled the subscriber context yet)
	start := time.Now()
	err = s.Close()
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Close() error: %v", err)
	}

	// Should complete relatively quickly (within timeout + some buffer)
	if elapsed > 500*time.Millisecond {
		t.Errorf("Close() took too long: %v", elapsed)
	}
}

func TestCleanupWithStreamContainingMessages(t *testing.T) {
	s := newTestStorage(t)

	// Create an expired stream with messages
	_, _ = s.Create(context.Background(), "expired-with-data", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(-time.Hour),
	})

	// Add messages (bypassing expiry check by using raw db access)
	err := s.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("m:expired-with-data:%016X", i+1)
			if err := txn.Set([]byte(key), []byte("message data")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to add messages: %v", err)
	}

	// Run cleanup
	s.cleanupExpiredStreams(context.Background())

	// Stream and all messages should be deleted
	_, err = s.Head(context.Background(), "expired-with-data")
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expected stream to be deleted, got: %v", err)
	}

	// Verify no messages remain
	var messageCount int
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("m:expired-with-data:")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			messageCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view error: %v", err)
	}
	if messageCount > 0 {
		t.Errorf("expected 0 messages after cleanup, got %d", messageCount)
	}
}

func TestGCLoopInterruption(t *testing.T) {
	// Test that GC loop stops on shutdown
	s, err := New(Options{
		InMemory:        true,
		Logger:          &quietLogger{},
		SLogger:         quietSLogger(),
		GCInterval:      5 * time.Millisecond, // Very fast GC
		CleanupInterval: -1,
	})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// Let GC run a few times
	time.Sleep(20 * time.Millisecond)

	// Close should stop the GC loop
	start := time.Now()
	if err := s.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	elapsed := time.Since(start)

	// Should complete quickly
	if elapsed > time.Second {
		t.Errorf("Close() took too long: %v", elapsed)
	}
}

func TestCleanupNoExpiredStreams(t *testing.T) {
	s := newTestStorage(t)

	// Create only non-expired streams
	_, _ = s.Create(context.Background(), "active1", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(time.Hour),
	})
	_, _ = s.Create(context.Background(), "active2", durablestream.StreamConfig{
		ContentType: "text/plain",
		// No expiry
	})

	// Cleanup should complete without errors
	s.cleanupExpiredStreams(context.Background())

	// Both streams should still exist
	_, err := s.Head(context.Background(), "active1")
	if err != nil {
		t.Errorf("active1 should still exist: %v", err)
	}
	_, err = s.Head(context.Background(), "active2")
	if err != nil {
		t.Errorf("active2 should still exist: %v", err)
	}
}
