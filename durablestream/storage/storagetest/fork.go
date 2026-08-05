package storagetest

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func newForkStorage(t *testing.T, cfg Config) (durablestream.Storage, durablestream.ForkStorage) {
	t.Helper()
	s := newStorage(t, cfg)
	forks, ok := s.(durablestream.ForkStorage)
	if !ok {
		if cfg.RequireForks {
			t.Fatal("storagetest: RequireForks is true, but Storage does not implement ForkStorage")
		}
		t.Skip("storagetest: Storage does not implement optional ForkStorage")
	}
	return s, forks
}

func forkRequest(t *testing.T, s durablestream.Storage, sourceID string) durablestream.ForkRequest {
	t.Helper()
	info := mustHead(t, s, sourceID)
	return durablestream.ForkRequest{
		SourceStreamID:      sourceID,
		SourceIncarnationID: info.IncarnationID,
		Config: durablestream.StreamConfig{
			ContentType: info.ContentType,
		},
	}
}

func mustFork(t *testing.T, forks durablestream.ForkStorage, targetID string, req durablestream.ForkRequest, messages ...string) *durablestream.StreamInfo {
	t.Helper()
	batch := make([][]byte, len(messages))
	for i, message := range messages {
		batch[i] = []byte(message)
	}
	created, info, err := forks.CreateFork(t.Context(), targetID, req, batch)
	if err != nil {
		t.Fatalf("CreateFork(%q from %q): unexpected error: %v", targetID, req.SourceStreamID, err)
	}
	if !created {
		t.Fatalf("CreateFork(%q from %q) reported created=false for a new target", targetID, req.SourceStreamID)
	}
	if info == nil {
		t.Fatalf("CreateFork(%q from %q) returned nil info on success", targetID, req.SourceStreamID)
	}
	return info
}

func testForkPrefixAndIsolation(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	mustCreate(t, s, "source")
	mustAppend(t, s, "source", "a")
	boundary := mustAppend(t, s, "source", "b")
	mustAppend(t, s, "source", "source-after-boundary")

	req := forkRequest(t, s, "source")
	req.Offset = boundary
	req.OffsetSet = true
	info := mustFork(t, forks, "target", req, "initial")
	if info.NextOffset.Compare(boundary) <= 0 {
		t.Errorf("fork with initial content has tail %q, want it after boundary %q", info.NextOffset, boundary)
	}
	assertPayloads(t, "new fork", payloads(mustRead(t, s, "target", durablestream.ZeroOffset, 0)), []string{"a", "b", "initial"})

	// Neither direction follows later writes from the other stream.
	mustAppend(t, s, "source", "later-source")
	mustAppend(t, s, "target", "later-target")
	assertPayloads(t, "fork after independent appends", payloads(mustRead(t, s, "target", durablestream.ZeroOffset, 0)), []string{"a", "b", "initial", "later-target"})
	assertPayloads(t, "source after independent appends", payloads(mustRead(t, s, "source", durablestream.ZeroOffset, 0)), []string{"a", "b", "source-after-boundary", "later-source"})

	// A fork of a fork sees the stitched logical stream and then diverges in the
	// same shared offset space.
	chainReq := forkRequest(t, s, "target")
	mustFork(t, forks, "grandchild", chainReq)
	assertPayloads(t, "recursive fork", payloads(mustRead(t, s, "grandchild", durablestream.ZeroOffset, 0)), []string{"a", "b", "initial", "later-target"})
}

func testForkIdempotency(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	mustCreate(t, s, "source")
	mustAppend(t, s, "source", "first")
	req := forkRequest(t, s, "source") // omitted offset defaults to the current tail

	const callers = 16
	start := make(chan struct{})
	created := make(chan bool, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			wasCreated, info, err := forks.CreateFork(t.Context(), "target", req, nil)
			if err == nil && info == nil {
				err = errors.New("nil StreamInfo on successful CreateFork")
			}
			if err != nil {
				errs <- err
				return
			}
			created <- wasCreated
		}()
	}
	close(start)
	wg.Wait()
	close(created)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent idempotent CreateFork: %v", err)
	}
	createdCount := 0
	for wasCreated := range created {
		if wasCreated {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("%d concurrent CreateFork calls reported created=true, want exactly 1", createdCount)
	}

	// Omitted-offset replay remains a replay rather than rebasing when the
	// source grows. Initial messages are validated but never replayed.
	mustAppend(t, s, "source", "second")
	wasCreated, info, err := forks.CreateFork(t.Context(), "target", req, [][]byte{[]byte("not replayed")})
	if err != nil || wasCreated || info == nil {
		t.Fatalf("idempotent default-offset replay = (%v, %#v, %v), want (false, non-nil, nil)", wasCreated, info, err)
	}
	assertPayloads(t, "default-offset fork after source growth", payloads(mustRead(t, s, "target", durablestream.ZeroOffset, 0)), []string{"first"})

	// Idempotent replays must not leak reference counts.
	mustDelete(t, s, "source")
	assertErrorIs(t, "Head of soft-deleted source", func() error { _, err := s.Head(t.Context(), "source"); return err }(), durablestream.ErrSoftDeleted)
	wasCreated, info, err = forks.CreateFork(t.Context(), "target", req, nil)
	if err != nil || wasCreated || info == nil {
		t.Fatalf("idempotent replay after source soft-delete = (%v, %#v, %v), want (false, non-nil, nil)", wasCreated, info, err)
	}
	mustDelete(t, s, "target")
	assertErrorIs(t, "Head after last reference is deleted", func() error { _, err := s.Head(t.Context(), "source"); return err }(), durablestream.ErrNotFound)
}

func testForkSubOffsets(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)

	mustCreate(t, s, "binary")
	mustAppend(t, s, "binary", "abcdef")
	binaryReq := forkRequest(t, s, "binary")
	binaryReq.Offset = durablestream.ZeroOffset
	binaryReq.OffsetSet = true
	binaryReq.SubOffset = 3
	mustFork(t, forks, "binary-prefix", binaryReq)
	res := mustRead(t, s, "binary-prefix", durablestream.ZeroOffset, 0)
	assertPayloads(t, "binary sub-offset", payloads(res), []string{"abc"})
	if len(res.Messages) != 1 || res.Messages[0].Offset.IsZero() {
		t.Errorf("binary prefix did not receive a server-minted message offset: %#v", res.Messages)
	}
	mustAppend(t, s, "binary-prefix", "own")
	assertPayloads(t, "binary prefix plus suffix", payloads(mustRead(t, s, "binary-prefix", durablestream.ZeroOffset, 0)), []string{"abc", "own"})

	fullReq := binaryReq
	fullReq.SubOffset = 6
	mustFork(t, forks, "binary-full-boundary", fullReq)
	assertPayloads(t, "binary boundary equality", payloads(mustRead(t, s, "binary-full-boundary", durablestream.ZeroOffset, 0)), []string{"abcdef"})
	overReq := binaryReq
	overReq.SubOffset = 7
	_, _, err := forks.CreateFork(t.Context(), "binary-overshoot", overReq, nil)
	assertErrorIs(t, "binary sub-offset overshoot", err, durablestream.ErrBadRequest)

	// Build a JSON source whose own initial messages are one atomic batch using
	// CreateFork itself, so this test does not require AtomicBatchStorage.
	mustCreateConfig(t, s, "json-root", durablestream.StreamConfig{ContentType: "application/json"})
	jsonBatchReq := forkRequest(t, s, "json-root")
	mustFork(t, forks, "json-source", jsonBatchReq, "1", "2")
	mustAppend(t, s, "json-source", "3") // a separate next batch

	jsonReq := forkRequest(t, s, "json-source")
	jsonReq.Offset = durablestream.ZeroOffset
	jsonReq.OffsetSet = true
	jsonReq.SubOffset = 2
	mustFork(t, forks, "json-prefix", jsonReq, "9")
	assertPayloads(t, "JSON sub-offset", payloads(mustRead(t, s, "json-prefix", durablestream.ZeroOffset, 0)), []string{"1", "2", "9"})

	jsonOver := jsonReq
	jsonOver.SubOffset = 3 // must not spill into the next Append batch
	_, _, err = forks.CreateFork(t.Context(), "json-overshoot", jsonOver, nil)
	assertErrorIs(t, "JSON sub-offset crossing a batch boundary", err, durablestream.ErrBadRequest)
}

func testForkWaitIsolation(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	mustCreate(t, s, "source")
	mustAppend(t, s, "source", "inherited")
	req := forkRequest(t, s, "source")
	info := mustFork(t, forks, "target", req)

	// Inherited data is already present and must never require a target append.
	inherited, err := s.WaitForData(t.Context(), "target", durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatalf("WaitForData for inherited data: %v", err)
	}
	assertPayloads(t, "WaitForData inherited range", payloads(inherited), []string{"inherited"})

	started := make(chan struct{})
	done := make(chan struct{})
	var waited *durablestream.ReadResult
	var waitErr error
	go func() {
		close(started)
		waited, waitErr = s.WaitForData(t.Context(), "target", info.NextOffset, 0)
		close(done)
	}()
	<-started
	mustAppend(t, s, "source", "source-after-fork")
	mustAppend(t, s, "target", "target-after-fork")
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatalf("WaitForData on fork did not wake for target append within %s", waitTimeout)
	}
	if waitErr != nil {
		t.Fatalf("WaitForData on fork: %v", waitErr)
	}
	assertPayloads(t, "WaitForData fork tail", payloads(waited), []string{"target-after-fork"})
}

func testForkSoftDeleteCascade(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	mustCreate(t, s, "source")
	mustAppend(t, s, "source", "root")
	sourceReq := forkRequest(t, s, "source")
	mustFork(t, forks, "child", sourceReq)
	mustAppend(t, s, "child", "child")
	childReq := forkRequest(t, s, "child")
	mustFork(t, forks, "grandchild", childReq)
	mustAppend(t, s, "grandchild", "grandchild")

	mustDelete(t, s, "source")
	assertPayloads(t, "descendant through a soft-deleted ancestor", payloads(mustRead(t, s, "grandchild", durablestream.ZeroOffset, 0)), []string{"root", "child", "grandchild"})

	_, err := s.Append(t.Context(), "source", []byte("x"), "")
	assertErrorIs(t, "Append to soft-deleted source", err, durablestream.ErrSoftDeleted)
	_, err = s.Read(t.Context(), "source", durablestream.ZeroOffset, 0)
	assertErrorIs(t, "Read of soft-deleted source", err, durablestream.ErrSoftDeleted)
	_, err = s.Head(t.Context(), "source")
	assertErrorIs(t, "Head of soft-deleted source", err, durablestream.ErrSoftDeleted)
	assertErrorIs(t, "Touch of soft-deleted source", s.Touch(t.Context(), "source"), durablestream.ErrSoftDeleted)
	_, err = s.WaitForData(t.Context(), "source", durablestream.ZeroOffset, 0)
	assertErrorIs(t, "WaitForData on soft-deleted source", err, durablestream.ErrSoftDeleted)
	assertErrorIs(t, "second Delete of soft-deleted source", s.Delete(t.Context(), "source"), durablestream.ErrSoftDeleted)
	_, err = s.Create(t.Context(), "source", textConfig())
	assertErrorIs(t, "Create at soft-deleted path", err, durablestream.ErrConflict)
	_, _, err = forks.CreateFork(t.Context(), "from-soft-source", sourceReq, nil)
	assertErrorIs(t, "CreateFork from soft-deleted source", err, durablestream.ErrConflict)
	if batches, ok := s.(durablestream.AtomicBatchStorage); ok {
		_, err = batches.AppendBatch(t.Context(), "source", [][]byte{[]byte("x")}, "")
		assertErrorIs(t, "AppendBatch to soft-deleted source", err, durablestream.ErrSoftDeleted)
	}
	if closer, ok := s.(durablestream.AtomicCloseStorage); ok {
		_, err = closer.CloseStream(t.Context(), "source", nil, "")
		assertErrorIs(t, "CloseStream on soft-deleted source", err, durablestream.ErrSoftDeleted)
	}

	// The middle node also becomes retained, then removing the leaf reclaims
	// both soft-deleted ancestors in one cascade.
	mustDelete(t, s, "child")
	assertPayloads(t, "leaf after two ancestors are soft-deleted", payloads(mustRead(t, s, "grandchild", durablestream.ZeroOffset, 0)), []string{"root", "child", "grandchild"})
	mustDelete(t, s, "grandchild")
	for _, id := range []string{"source", "child", "grandchild"} {
		_, err := s.Head(t.Context(), id)
		assertErrorIs(t, "Head after cascade for "+id, err, durablestream.ErrNotFound)
	}
	mustCreate(t, s, "source") // path is reusable only after the cascade
}

func testForkLifetimeInheritance(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	now := time.Now()
	mustCreateConfig(t, s, "ttl-source", durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         time.Hour,
		ExpiresAt:   now.Add(5 * time.Minute),
	})
	ttlSource := mustHead(t, s, "ttl-source")
	ttlReq := forkRequest(t, s, "ttl-source")
	ttlFork := mustFork(t, forks, "ttl-fork", ttlReq)
	if ttlFork.TTL != time.Hour {
		t.Errorf("inherited TTL = %s, want %s", ttlFork.TTL, time.Hour)
	}
	if !ttlFork.ExpiresAt.After(ttlSource.ExpiresAt) {
		t.Errorf("inherited TTL deadline = %s, want a fresh independent window after source deadline %s", ttlFork.ExpiresAt, ttlSource.ExpiresAt)
	}

	absolute := now.Add(2 * time.Hour).Round(0)
	mustCreateConfig(t, s, "absolute-source", durablestream.StreamConfig{ContentType: "text/plain", ExpiresAt: absolute})
	absReq := forkRequest(t, s, "absolute-source")
	absFork := mustFork(t, forks, "absolute-fork", absReq)
	if !absFork.ExpiresAt.Equal(absolute) || absFork.TTL != 0 {
		t.Errorf("inherited absolute lifetime = (TTL %s, expiry %s), want (0, %s)", absFork.TTL, absFork.ExpiresAt, absolute)
	}

	override := absReq
	override.TTLSet = true
	override.Config.TTL = 3 * time.Hour
	overrideFork := mustFork(t, forks, "override-fork", override)
	if overrideFork.TTL != 3*time.Hour || !overrideFork.ExpiresAt.After(absolute) {
		t.Errorf("explicit fork TTL = (TTL %s, expiry %s), want a fresh three-hour window", overrideFork.TTL, overrideFork.ExpiresAt)
	}

	// Expired descendants still release references when deleted, so a retained
	// source cannot leak solely because its final child reached expiry first.
	mustCreate(t, s, "expiry-source")
	expiredReq := forkRequest(t, s, "expiry-source")
	expiredReq.ExpiresAtSet = true
	expiredReq.Config.ExpiresAt = now.Add(-time.Hour)
	mustFork(t, forks, "expired-child", expiredReq)
	mustDelete(t, s, "expiry-source")
	mustDelete(t, s, "expired-child")
	_, err := s.Head(t.Context(), "expiry-source")
	assertErrorIs(t, "source after expired last child is deleted", err, durablestream.ErrNotFound)
}

func testForkTargetState(t *testing.T, cfg Config) {
	t.Parallel()
	s, forks := newForkStorage(t, cfg)
	mustCreateConfig(t, s, "closed-source", durablestream.StreamConfig{ContentType: "text/plain", Closed: true})
	openReq := forkRequest(t, s, "closed-source")
	openFork := mustFork(t, forks, "open-target", openReq)
	if openFork.Closed {
		t.Error("fork inherited source closure; target should be open by default")
	}
	mustAppend(t, s, "open-target", "allowed")

	closedReq := openReq
	closedReq.Config.Closed = true
	closedFork := mustFork(t, forks, "closed-target", closedReq, "final")
	if !closedFork.Closed {
		t.Error("CreateFork ignored target Config.Closed")
	}
	_, err := s.Append(t.Context(), "closed-target", []byte("rejected"), "")
	assertErrorIs(t, "Append to closed fork", err, durablestream.ErrStreamClosed)

	// Per-writer Stream-Seq state is fresh on the target.
	mustCreate(t, s, "seq-source")
	if _, err := s.Append(t.Context(), "seq-source", []byte("source"), "z"); err != nil {
		t.Fatalf("source append with seq: %v", err)
	}
	seqReq := forkRequest(t, s, "seq-source")
	mustFork(t, forks, "seq-target", seqReq)
	if _, err := s.Append(t.Context(), "seq-target", []byte("target"), "a"); err != nil {
		t.Errorf("fork inherited source Stream-Seq state: %v", err)
	}

	staleReq := seqReq
	staleReq.SourceIncarnationID = "not-the-current-incarnation"
	_, _, err = forks.CreateFork(t.Context(), "stale-target", staleReq, nil)
	assertErrorIs(t, "fork with stale source incarnation", err, durablestream.ErrConflict)
	_, err = s.Head(t.Context(), "stale-target")
	assertErrorIs(t, "target after fenced fork", err, durablestream.ErrNotFound)

	mismatchReq := seqReq
	mismatchReq.Config.ContentType = "application/json"
	_, _, err = forks.CreateFork(t.Context(), "mismatched-target", mismatchReq, nil)
	assertErrorIs(t, "resolved content type mismatch with omitted header", err, durablestream.ErrConflict)
}
