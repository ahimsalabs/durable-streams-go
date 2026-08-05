package badgerstore

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

func badgerForkRequest(t *testing.T, s *Storage, sourceID string) durablestream.ForkRequest {
	t.Helper()
	info, err := s.Head(t.Context(), sourceID)
	if err != nil {
		t.Fatalf("Head(%q): %v", sourceID, err)
	}
	return durablestream.ForkRequest{
		SourceStreamID:      sourceID,
		SourceIncarnationID: info.IncarnationID,
		Config:              durablestream.StreamConfig{ContentType: info.ContentType},
	}
}

func mustCreateBadgerFork(t *testing.T, s *Storage, targetID string, req durablestream.ForkRequest, messages ...string) *durablestream.StreamInfo {
	t.Helper()
	batch := make([][]byte, len(messages))
	for i, message := range messages {
		batch[i] = []byte(message)
	}
	created, info, err := s.CreateFork(t.Context(), targetID, req, batch)
	if err != nil || !created || info == nil {
		t.Fatalf("CreateFork(%q from %q) = (%v, %#v, %v), want (true, non-nil, nil)", targetID, req.SourceStreamID, created, info, err)
	}
	return info
}

func assertBadgerPayloads(t *testing.T, s *Storage, streamID string, want ...string) {
	t.Helper()
	got := readAll(t, s, streamID)
	if len(got) != len(want) {
		t.Fatalf("Read(%q) payloads = %q, want %q", streamID, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Read(%q) payloads = %q, want %q", streamID, got, want)
		}
	}
}

func TestForkLineageSurvivesReopenAndSoftDeleteCascade(t *testing.T) {
	dir := t.TempDir()
	s := newDiskStorage(t, dir)

	created, _, err := s.CreateWithMessages(t.Context(), "root", durablestream.StreamConfig{ContentType: "text/plain"}, [][]byte{[]byte("root")})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages(root) = (%v, %v), want (true, nil)", created, err)
	}
	childReq := badgerForkRequest(t, s, "root")
	mustCreateBadgerFork(t, s, "child", childReq, "child")
	grandReq := badgerForkRequest(t, s, "child")
	grandReq.Config.Closed = true
	mustCreateBadgerFork(t, s, "grandchild", grandReq, "grandchild")

	if err := s.Delete(t.Context(), "root"); err != nil {
		t.Fatalf("Delete(root): %v", err)
	}
	if err := s.Delete(t.Context(), "child"); err != nil {
		t.Fatalf("Delete(child): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}

	s = newDiskStorage(t, dir)
	for _, streamID := range []string{"root", "child"} {
		if _, err := s.Head(t.Context(), streamID); !errors.Is(err, durablestream.ErrSoftDeleted) {
			t.Errorf("Head(%q) after reopen = %v, want ErrSoftDeleted", streamID, err)
		}
	}
	assertBadgerPayloads(t, s, "grandchild", "root", "child", "grandchild")
	if _, err := s.Append(t.Context(), "grandchild", []byte("late"), ""); !errors.Is(err, durablestream.ErrStreamClosed) {
		t.Errorf("Append to closed grandchild after reopen = %v, want ErrStreamClosed", err)
	}

	if err := s.Delete(t.Context(), "grandchild"); err != nil {
		t.Fatalf("Delete(grandchild): %v", err)
	}
	for _, streamID := range []string{"root", "child", "grandchild"} {
		if _, err := s.Head(t.Context(), streamID); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Head(%q) after cascade = %v, want ErrNotFound", streamID, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after cascade: %v", err)
	}

	// The removal transaction, tombstones, and startup purge are independently
	// durable. A second reopen must not resurrect any member of the chain.
	s = newDiskStorage(t, dir)
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("final Close: %v", err)
		}
	}()
	for _, streamID := range []string{"root", "child", "grandchild"} {
		if _, err := s.Head(t.Context(), streamID); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Head(%q) after second reopen = %v, want ErrNotFound", streamID, err)
		}
	}
}

func TestForkJSONBatchBoundariesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s := newDiskStorage(t, dir)
	created, _, err := s.CreateWithMessages(t.Context(), "json", durablestream.StreamConfig{ContentType: "application/json"}, [][]byte{
		[]byte("1"), []byte("2"), []byte("3"),
	})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages(json) = (%v, %v), want (true, nil)", created, err)
	}
	if _, err := s.Append(t.Context(), "json", []byte("4"), ""); err != nil {
		t.Fatalf("Append separate JSON batch: %v", err)
	}
	req := badgerForkRequest(t, s, "json")
	if err := s.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}

	s = newDiskStorage(t, dir)
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	req.Offset = durablestream.ZeroOffset
	req.OffsetSet = true
	req.SubOffset = 2
	mustCreateBadgerFork(t, s, "prefix", req)
	assertBadgerPayloads(t, s, "prefix", "1", "2")

	// Starting inside a batch counts only the remaining flattened messages in
	// that same append. It may not spill into the separately appended "4".
	middle := req
	middle.Offset = storage.FormatSimpleOffset(1)
	middle.SubOffset = 2
	mustCreateBadgerFork(t, s, "middle", middle)
	assertBadgerPayloads(t, s, "middle", "1", "2", "3")

	over := middle
	over.SubOffset = 3
	if _, _, err := s.CreateFork(t.Context(), "over", over, nil); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Fatalf("CreateFork crossing JSON batch boundary = %v, want ErrBadRequest", err)
	}
}

func TestForkMissingBatchMetadataFailsAtomically(t *testing.T) {
	s := newTestStorage(t)
	created, _, err := s.CreateWithMessages(t.Context(), "json", durablestream.StreamConfig{ContentType: "application/json"}, [][]byte{
		[]byte("1"), []byte("2"),
	})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages(json) = (%v, %v), want (true, nil)", created, err)
	}
	gen := currentGeneration(t, s, "json")
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(batchKey("json", gen, storage.FormatSimpleOffset(1)))
	}); err != nil {
		t.Fatalf("remove batch metadata: %v", err)
	}

	req := badgerForkRequest(t, s, "json")
	req.Offset = durablestream.ZeroOffset
	req.OffsetSet = true
	req.SubOffset = 1
	if _, _, err := s.CreateFork(t.Context(), "target", req, nil); !errors.Is(err, ErrLegacyFormat) {
		t.Fatalf("CreateFork without batch metadata = %v, want ErrLegacyFormat", err)
	}
	if _, err := s.Head(t.Context(), "target"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("Head(target) after failed fork = %v, want ErrNotFound", err)
	}
	if err := s.db.View(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, "json")
		if err != nil {
			return err
		}
		if !found || rec.RefCount != 0 {
			return fmt.Errorf("source record after failed fork = (found %v, refs %d), want (true, 0)", found, rec.RefCount)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestForkIdempotencyIncludesOptionalFieldPresence(t *testing.T) {
	s := newTestStorage(t)
	if _, err := s.Create(t.Context(), "source", durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         time.Hour,
		ExpiresAt:   time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Create(source): %v", err)
	}

	omitted := badgerForkRequest(t, s, "source")
	mustCreateBadgerFork(t, s, "omitted", omitted)
	created, info, err := s.CreateFork(t.Context(), "omitted", omitted, nil)
	if err != nil || created || info == nil {
		t.Fatalf("equivalent replay = (%v, %#v, %v), want (false, non-nil, nil)", created, info, err)
	}

	explicitContentType := omitted
	explicitContentType.ContentTypeSet = true
	if _, _, err := s.CreateFork(t.Context(), "omitted", explicitContentType, nil); !errors.Is(err, durablestream.ErrConflict) {
		t.Errorf("replay changing ContentType presence = %v, want ErrConflict", err)
	}
	explicitTTL := omitted
	explicitTTL.TTLSet = true
	explicitTTL.Config.TTL = time.Hour
	if _, _, err := s.CreateFork(t.Context(), "omitted", explicitTTL, nil); !errors.Is(err, durablestream.ErrConflict) {
		t.Errorf("replay changing TTL presence = %v, want ErrConflict", err)
	}

	mustCreateBadgerFork(t, s, "explicit-ttl", explicitTTL)
	created, info, err = s.CreateFork(t.Context(), "explicit-ttl", explicitTTL, nil)
	if err != nil || created || info == nil {
		t.Fatalf("explicit TTL replay = (%v, %#v, %v), want (false, non-nil, nil)", created, info, err)
	}
	if _, _, err := s.CreateFork(t.Context(), "explicit-ttl", omitted, nil); !errors.Is(err, durablestream.ErrConflict) {
		t.Errorf("replay omitting original TTL = %v, want ErrConflict", err)
	}
}

func TestForkExpiryCleanupPreservesDescendantsAndReleasesReferences(t *testing.T) {
	t.Run("expired source is retained across reopen", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskStorage(t, dir)
		created, _, err := s.CreateWithMessages(t.Context(), "source", durablestream.StreamConfig{ContentType: "text/plain"}, [][]byte{[]byte("history")})
		if err != nil || !created {
			t.Fatalf("CreateWithMessages(source) = (%v, %v), want (true, nil)", created, err)
		}
		req := badgerForkRequest(t, s, "source")
		req.TTLSet = true // An explicit zero gives the child no source lifetime.
		req.Config.TTL = 0
		mustCreateBadgerFork(t, s, "child", req)
		lateForkReq := badgerForkRequest(t, s, "source")

		if err := s.update(func(txn *badger.Txn) error {
			rec, _, err := getRecord(txn, "source")
			if err != nil {
				return err
			}
			rec.ExpiresAt = time.Now().Add(-time.Hour)
			return setRecord(txn, "source", rec)
		}); err != nil {
			t.Fatalf("expire source: %v", err)
		}
		if _, err := s.Head(t.Context(), "source"); !errors.Is(err, durablestream.ErrSoftDeleted) {
			t.Errorf("Head(retained expiry before cleanup) = %v, want ErrSoftDeleted", err)
		}
		if _, err := s.Append(t.Context(), "source", []byte("late"), ""); !errors.Is(err, durablestream.ErrSoftDeleted) {
			t.Errorf("Append(retained expiry before cleanup) = %v, want ErrSoftDeleted", err)
		}
		if _, _, err := s.CreateFork(t.Context(), "late-child", lateForkReq, nil); !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("CreateFork(retained expiry source before cleanup) = %v, want ErrConflict", err)
		}
		s.cleanupExpiredStreams(t.Context())
		if _, err := s.Head(t.Context(), "source"); !errors.Is(err, durablestream.ErrSoftDeleted) {
			t.Errorf("Head(expired referenced source) = %v, want ErrSoftDeleted", err)
		}
		if _, err := s.Create(t.Context(), "source", durablestream.StreamConfig{ContentType: "text/plain"}); !errors.Is(err, durablestream.ErrConflict) {
			t.Errorf("Create at retained expired source path = %v, want ErrConflict", err)
		}
		assertBadgerPayloads(t, s, "child", "history")
		if err := s.Close(); err != nil {
			t.Fatalf("Close before reopen: %v", err)
		}

		s = newDiskStorage(t, dir)
		assertBadgerPayloads(t, s, "child", "history")
		if err := s.Delete(t.Context(), "child"); err != nil {
			t.Fatalf("Delete(child): %v", err)
		}
		if _, err := s.Head(t.Context(), "source"); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Head(source) after last child deletion = %v, want ErrNotFound", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close after cascade: %v", err)
		}
	})

	t.Run("expired child cleanup releases soft parent", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskStorage(t, dir)
		mustCreate(t, s, "source")
		req := badgerForkRequest(t, s, "source")
		req.ExpiresAtSet = true
		req.Config.ExpiresAt = time.Now().Add(-time.Hour)
		mustCreateBadgerFork(t, s, "expired-child", req)
		if err := s.Delete(t.Context(), "source"); err != nil {
			t.Fatalf("Delete(source): %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close before cleanup reopen: %v", err)
		}

		s = newDiskStorage(t, dir)
		s.cleanupExpiredStreams(t.Context())
		for _, streamID := range []string{"source", "expired-child"} {
			if _, err := s.Head(t.Context(), streamID); !errors.Is(err, durablestream.ErrNotFound) {
				t.Errorf("Head(%q) after expired-child cleanup = %v, want ErrNotFound", streamID, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

func TestConcurrentForkAndDeleteMaintainTopology(t *testing.T) {
	s := newTestStorage(t)
	const rounds = 24
	for round := range rounds {
		sourceID := fmt.Sprintf("source-%02d", round)
		targetID := fmt.Sprintf("target-%02d", round)
		created, _, err := s.CreateWithMessages(t.Context(), sourceID, durablestream.StreamConfig{ContentType: "text/plain"}, [][]byte{[]byte("history")})
		if err != nil || !created {
			t.Fatalf("round %d CreateWithMessages = (%v, %v), want (true, nil)", round, created, err)
		}
		req := badgerForkRequest(t, s, sourceID)

		start := make(chan struct{})
		var forkCreated bool
		var forkInfo *durablestream.StreamInfo
		var forkErr, deleteErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			forkCreated, forkInfo, forkErr = s.CreateFork(t.Context(), targetID, req, nil)
		})
		wg.Go(func() {
			<-start
			deleteErr = s.Delete(t.Context(), sourceID)
		})
		close(start)
		wg.Wait()

		checkNoRawConflict(t, "CreateFork racing Delete", forkErr)
		checkNoRawConflict(t, "Delete racing CreateFork", deleteErr)
		if deleteErr != nil {
			t.Fatalf("round %d Delete(source) = %v, want nil", round, deleteErr)
		}
		switch {
		case forkErr == nil:
			if !forkCreated || forkInfo == nil {
				t.Fatalf("round %d successful CreateFork = (%v, %#v), want (true, non-nil)", round, forkCreated, forkInfo)
			}
			if _, err := s.Head(t.Context(), sourceID); !errors.Is(err, durablestream.ErrSoftDeleted) {
				t.Errorf("round %d Head(source) = %v, want ErrSoftDeleted", round, err)
			}
			assertBadgerPayloads(t, s, targetID, "history")
			if err := s.Delete(t.Context(), targetID); err != nil {
				t.Fatalf("round %d Delete(target): %v", round, err)
			}
		case errors.Is(forkErr, durablestream.ErrNotFound):
			if forkCreated || forkInfo != nil {
				t.Fatalf("round %d failed CreateFork = (%v, %#v, %v), want no target", round, forkCreated, forkInfo, forkErr)
			}
			if _, err := s.Head(t.Context(), targetID); !errors.Is(err, durablestream.ErrNotFound) {
				t.Errorf("round %d Head(target) = %v, want ErrNotFound", round, err)
			}
		default:
			t.Fatalf("round %d CreateFork racing Delete = %v, want nil or ErrNotFound", round, forkErr)
		}
		if _, err := s.Head(t.Context(), sourceID); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("round %d Head(source) after cleanup = %v, want ErrNotFound", round, err)
		}
	}
}

func TestConcurrentLastChildDeletesCascadeOnce(t *testing.T) {
	s := newTestStorage(t)
	const children = 8
	mustCreate(t, s, "source")
	for i := range children {
		req := badgerForkRequest(t, s, "source")
		mustCreateBadgerFork(t, s, fmt.Sprintf("child-%d", i), req)
	}
	if err := s.Delete(t.Context(), "source"); err != nil {
		t.Fatalf("Delete(source): %v", err)
	}

	start := make(chan struct{})
	errs := make([]error, children)
	var wg sync.WaitGroup
	for i := range children {
		wg.Go(func() {
			<-start
			errs[i] = s.Delete(t.Context(), fmt.Sprintf("child-%d", i))
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		checkNoRawConflict(t, fmt.Sprintf("Delete(child-%d)", i), err)
		if err != nil {
			t.Errorf("Delete(child-%d) = %v, want nil", i, err)
		}
	}
	for i := -1; i < children; i++ {
		streamID := "source"
		if i >= 0 {
			streamID = fmt.Sprintf("child-%d", i)
		}
		if _, err := s.Head(t.Context(), streamID); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Head(%q) after concurrent cascade = %v, want ErrNotFound", streamID, err)
		}
	}
}
