package badgerstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

// newDiskStorage opens a disk-backed store in dir and waits for its startup
// sweep. The caller is responsible for closing it.
func newDiskStorage(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := New(Options{
		Dir:             dir,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		MaxMessageSize:  1024,
		GCInterval:      -1,
		CleanupInterval: -1,
		ReapInterval:    time.Hour, // Only sweep on startup or on demand
	})
	if err != nil {
		t.Fatalf("open storage in %s: %v", dir, err)
	}
	<-s.initialReapDone
	return s
}

func mustCreate(t *testing.T, s *Storage, streamID string) {
	t.Helper()
	if _, err := s.Create(context.Background(), streamID, durablestream.StreamConfig{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("create %q: %v", streamID, err)
	}
}

func mustAppend(t *testing.T, s *Storage, streamID string, data string) {
	t.Helper()
	if _, err := s.Append(context.Background(), streamID, []byte(data), ""); err != nil {
		t.Fatalf("append to %q: %v", streamID, err)
	}
}

func readAll(t *testing.T, s *Storage, streamID string) []string {
	t.Helper()
	result, err := s.Read(context.Background(), streamID, "", 0)
	if err != nil {
		t.Fatalf("read %q: %v", streamID, err)
	}
	var out []string
	for _, m := range result.Messages {
		out = append(out, string(m.Data))
	}
	return out
}

// TestDeleteDoesNotDestroyRecreatedStream reproduces the delete/recreate race:
// a stream recreated while the previous incarnation's data is still being
// purged must keep every message it accepted.
func TestDeleteDoesNotDestroyRecreatedStream(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "stream")
	for range 100 {
		mustAppend(t, s, "stream", "old")
	}

	if err := s.Delete(ctx, "stream"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Recreate and write before the purge runs.
	mustCreate(t, s, "stream")
	mustAppend(t, s, "stream", "new-1")
	mustAppend(t, s, "stream", "new-2")

	// Now let the purge complete.
	s.reap(ctx, true)

	got := readAll(t, s, "stream")
	want := []string{"new-1", "new-2"}
	if len(got) != len(want) {
		t.Fatalf("after purge: got %d messages %v, want %v", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d = %q, want %q", i, got[i], want[i])
		}
	}

	// The old incarnation's data and tombstone are gone.
	if n := countKeys(t, s, []byte(prefixTombstone)); n != 0 {
		t.Errorf("tombstones remaining after reap: %d", n)
	}
	if n := countKeys(t, s, []byte(prefixMessage+"stream:")); n != len(want) {
		t.Errorf("message keys after reap: %d, want %d", n, len(want))
	}
}

// TestDeleteSucceedsWhenContextCancelled verifies that Delete does not report
// failure once the stream's removal has committed: the purge is the reaper's
// job, so a cancelled context cannot leave the caller believing the delete
// failed while the stream is in fact gone.
func TestDeleteSucceedsWhenContextCancelled(t *testing.T) {
	s := newTestStorage(t)

	mustCreate(t, s, "stream")
	for range 50 {
		mustAppend(t, s, "stream", "data")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Delete(ctx, "stream"); err != nil {
		t.Fatalf("delete with cancelled context: %v", err)
	}
	if _, err := s.Head(context.Background(), "stream"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("Head after delete = %v, want ErrNotFound", err)
	}
}

// TestOrphanedDataIsNotServedToNewStream reproduces the cross-tenant disclosure
// scenario: a purge interrupted after the config was removed leaves message
// keys behind, and a later stream with the same ID must never serve them.
func TestOrphanedDataIsNotServedToNewStream(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newDiskStorage(t, dir)
	mustCreate(t, s, "tenant-a")
	mustAppend(t, s, "tenant-a", "secret-1")
	mustAppend(t, s, "tenant-a", "secret-2")
	orphanPrefix := messagePrefix("tenant-a", currentGeneration(t, s, "tenant-a"))

	// Simulate a purge interrupted right after the config was removed: the
	// config is gone but the messages remain, with no tombstone to find them.
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(configKey("tenant-a"))
	}); err != nil {
		t.Fatalf("simulate interrupted purge: %v", err)
	}
	if n := countKeys(t, s, orphanPrefix); n != 2 {
		t.Fatalf("expected 2 orphaned messages, got %d", n)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: a different tenant takes the same stream ID.
	s2 := newDiskStorage(t, dir)
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	mustCreate(t, s2, "tenant-a")
	if got := readAll(t, s2, "tenant-a"); len(got) != 0 {
		t.Fatalf("new stream served orphaned data: %v", got)
	}
	mustAppend(t, s2, "tenant-a", "mine")
	if got := readAll(t, s2, "tenant-a"); len(got) != 1 || got[0] != "mine" {
		t.Fatalf("new stream contents = %v, want [mine]", got)
	}

	// The startup sweep reclaims the orphaned keys.
	s2.reap(ctx, true)
	if n := countKeys(t, s2, orphanPrefix); n != 0 {
		t.Errorf("orphaned messages remaining after sweep: %d", n)
	}
}

// TestReaperResumesPurgeAfterRestart verifies that a delete whose purge never
// ran is completed by the next process.
func TestReaperResumesPurgeAfterRestart(t *testing.T) {
	dir := t.TempDir()

	s := newDiskStorage(t, dir)
	mustCreate(t, s, "stream")
	for range 20 {
		mustAppend(t, s, "stream", "data")
	}
	gen := currentGeneration(t, s, "stream")

	// Delete, then stop the process before the reaper gets to the tombstone.
	// Stopping the reaper first (and waiting for it to exit) keeps it from
	// acting on the delete signal.
	s.shutdownCancel()
	s.wg.Wait()
	if err := s.Delete(context.Background(), "stream"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := newDiskStorage(t, dir)
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if n := countKeys(t, s2, messagePrefix("stream", gen)); n != 0 {
		t.Errorf("messages remaining after restart sweep: %d", n)
	}
	if n := countKeys(t, s2, batchPrefix("stream", gen)); n != 0 {
		t.Errorf("batch boundaries remaining after restart sweep: %d", n)
	}
	if n := countKeys(t, s2, []byte(prefixTombstone)); n != 0 {
		t.Errorf("tombstones remaining after restart sweep: %d", n)
	}
	if n := countKeys(t, s2, []byte(prefixSeq)); n != 0 {
		t.Errorf("sequence keys remaining after restart sweep: %d", n)
	}
}

// TestSweepKeepsLiveStreamData guards the orphan sweep against deleting data of
// streams it does not know about, including ones created while it runs.
func TestSweepKeepsLiveStreamData(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "live")
	mustAppend(t, s, "live", "keep-me")

	s.reap(ctx, true)

	if got := readAll(t, s, "live"); len(got) != 1 || got[0] != "keep-me" {
		t.Fatalf("live stream contents after sweep = %v, want [keep-me]", got)
	}
}

// TestExpiredStreamReplacementDropsOldGeneration covers the expired-stream
// replacement path: the displaced generation's data and sequence key must be
// reclaimed, and the replacement must start from offset 1 with no resurrected
// sequence key.
func TestExpiredStreamReplacementDropsOldGeneration(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	cfg := durablestream.StreamConfig{ContentType: "text/plain"}
	mustCreate(t, s, "stream")
	mustAppend(t, s, "stream", "old-1")
	mustAppend(t, s, "stream", "old-2")
	oldGen := currentGeneration(t, s, "stream")

	// Expire the stream deterministically by rewriting its record.
	expired := streamRecord{
		StreamConfig: durablestream.StreamConfig{
			ContentType: "text/plain",
			ExpiresAt:   time.Now().Add(-time.Hour),
		},
		FormatVersion: currentRecordFormatVersion,
		Gen:           oldGen,
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		encoded, err := json.Marshal(expired)
		if err != nil {
			return err
		}
		return txn.Set(configKey("stream"), encoded)
	}); err != nil {
		t.Fatalf("expire stream: %v", err)
	}

	created, err := s.Create(ctx, "stream", cfg)
	if err != nil {
		t.Fatalf("recreate expired stream: %v", err)
	}
	if !created {
		t.Fatal("recreating an expired stream should report created=true")
	}
	newGen := currentGeneration(t, s, "stream")
	if newGen == oldGen {
		t.Fatal("recreated stream reused the expired generation")
	}

	offset, err := s.Append(ctx, "stream", []byte("new-1"), "")
	if err != nil {
		t.Fatalf("append after recreate: %v", err)
	}
	if want := storage.FormatSimpleOffset(1); offset != want {
		t.Errorf("first offset after recreate = %s, want %s", offset, want)
	}

	s.reap(ctx, true)

	if n := countKeys(t, s, messagePrefix("stream", oldGen)); n != 0 {
		t.Errorf("old generation messages remaining: %d", n)
	}
	if n := countKeys(t, s, seqKey("stream", oldGen)); n != 0 {
		t.Errorf("old generation sequence key remaining: %d", n)
	}
	if got := readAll(t, s, "stream"); len(got) != 1 || got[0] != "new-1" {
		t.Errorf("recreated stream contents = %v, want [new-1]", got)
	}
}

// TestAppendAfterDeleteReturnsNotFound checks that an append racing a delete of
// the same stream ID fails rather than writing into a dead generation.
func TestAppendAfterDeleteReturnsNotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "stream")
	mustAppend(t, s, "stream", "one")

	if err := s.Delete(ctx, "stream"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Append(ctx, "stream", []byte("two"), ""); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("append after delete = %v, want ErrNotFound", err)
	}
}

// TestWaitStaysBoundToDeletedGeneration verifies that a waiter holding old
// generation state cannot attach to a replacement stream and return its data.
func TestWaitStaysBoundToDeletedGeneration(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "stream")
	oldGen := currentGeneration(t, s, "stream")
	oldState := s.notificationState("stream", oldGen)

	if err := s.Delete(ctx, "stream"); err != nil {
		t.Fatalf("delete old generation: %v", err)
	}
	mustCreate(t, s, "stream")
	mustAppend(t, s, "stream", "replacement-secret")

	res, err := s.waitForGeneration(ctx, "stream", oldGen, oldState, durablestream.ZeroOffset, 0)
	if !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("wait on deleted generation = (%v, %v), want ErrNotFound", res, err)
	}
}

// TestForgetOldGenerationKeepsReplacementState covers the post-commit race in
// which an old Delete finishes its in-memory cleanup after a replacement has
// already registered waiters. Cleanup must compare generation and pointer,
// never LoadAndDelete state by stream ID alone.
func TestForgetOldGenerationKeepsReplacementState(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mustCreate(t, s, "stream")
	oldGen := currentGeneration(t, s, "stream")
	if err := s.Delete(ctx, "stream"); err != nil {
		t.Fatalf("delete old generation: %v", err)
	}
	mustCreate(t, s, "stream")
	newGen := currentGeneration(t, s, "stream")
	newState := s.notificationState("stream", newGen)

	// Simulate delayed cleanup from the already-deleted generation.
	s.forgetStream("stream", oldGen)

	got, ok := s.streams.Load(streamStateKey("stream", newGen))
	if !ok || got != newState {
		t.Fatalf("old-generation cleanup removed replacement state: got (%p, %v), want %p", got, ok, newState)
	}
	newState.mu.RLock()
	deleted := newState.deleted
	newState.mu.RUnlock()
	if deleted {
		t.Fatal("old-generation cleanup marked replacement state deleted")
	}
}

// TestAppendToUnknownStreamDoesNotGrowState guards the generation cache
// against unbounded growth: a client hammering unknown stream IDs must not be
// able to allocate per-stream state.
func TestAppendToUnknownStreamDoesNotGrowState(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for i := range 100 {
		id := "ghost-" + strconv.Itoa(i)
		if _, err := s.Append(ctx, id, []byte("data"), ""); !errors.Is(err, durablestream.ErrNotFound) {
			t.Fatalf("append to %q = %v, want ErrNotFound", id, err)
		}
	}

	var tracked int
	s.streams.Range(func(string, *streamState) bool {
		tracked++
		return true
	})
	if tracked != 0 {
		t.Errorf("tracked %d streams after appends to unknown IDs, want 0", tracked)
	}
}

// TestNewRejectsLegacyFormatWithoutDeletingData checks upgrade safety. Merely
// opening a directory written before generation scoping must not start the
// startup reaper and erase the only copy of its durable stream data.
func TestNewRejectsLegacyFormatWithoutDeletingData(t *testing.T) {
	dir := t.TempDir()
	legacyConfig := []byte(`{"ContentType":"text/plain","TTL":0,"ExpiresAt":"0001-01-01T00:00:00Z","IsPrivate":false}`)
	legacyMessageKey := []byte(prefixMessage + "legacy:" + storage.FormatSimpleOffset(1).String())

	raw, err := badger.Open(badger.DefaultOptions(dir).WithLogger(&quietLogger{}).WithSyncWrites(true))
	if err != nil {
		t.Fatalf("open raw legacy database: %v", err)
	}
	if err := raw.Update(func(txn *badger.Txn) error {
		if err := txn.Set(configKey("legacy"), legacyConfig); err != nil {
			return err
		}
		if err := txn.Set(legacyMessageKey, []byte("old")); err != nil {
			return err
		}
		if err := txn.Set([]byte(prefixSeq+"legacy"), []byte("legacy-sequence")); err != nil {
			return err
		}
		return txn.Set(lastSeqKey("legacy"), []byte("0001"))
	}); err != nil {
		t.Fatalf("seed legacy database: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw legacy database: %v", err)
	}

	s, err := New(Options{
		Dir:             dir,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		MaxMessageSize:  1024,
		GCInterval:      -1,
		CleanupInterval: -1,
		ReapInterval:    time.Nanosecond,
	})
	if s != nil {
		_ = s.Close()
		t.Fatal("New returned a storage for a legacy directory, want nil")
	}
	if !errors.Is(err, ErrLegacyFormat) {
		t.Fatalf("New legacy directory error = %v, want ErrLegacyFormat", err)
	}

	// Reopen with Badger directly and verify every representative old-format key
	// still has its original value. This also proves New released the file lock.
	raw, err = badger.Open(badger.DefaultOptions(dir).WithLogger(&quietLogger{}))
	if err != nil {
		t.Fatalf("reopen raw database after rejected New: %v", err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close verification database: %v", err)
		}
	}()

	want := map[string]string{
		string(configKey("legacy")):  string(legacyConfig),
		string(legacyMessageKey):     "old",
		prefixSeq + "legacy":         "legacy-sequence",
		string(lastSeqKey("legacy")): "0001",
	}
	if err := raw.View(func(txn *badger.Txn) error {
		for key, wantValue := range want {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return fmt.Errorf("get preserved key %q: %w", key, err)
			}
			if err := item.Value(func(value []byte) error {
				if string(value) != wantValue {
					return fmt.Errorf("key %q value = %q, want %q", key, value, wantValue)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
