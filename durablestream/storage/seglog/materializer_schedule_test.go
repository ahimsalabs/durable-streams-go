package seglog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func schedulerOptions(t *testing.T) Options {
	t.Helper()
	opts := singlePartitionOptions(t.TempDir())
	opts.MaterializeBytes = 1 << 30
	opts.MaterializeMaxAge = time.Hour
	opts.CheckpointBytes = 1 << 30
	opts.CheckpointMaxAge = time.Hour
	opts.RetentionInterval = -1
	return opts
}

func TestMaterializer_BytePressureTriggersBeforeMaximumAge(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "byte-pressure", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := storage.Append(t.Context(), "byte-pressure", []byte("payload"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, "byte-pressure materialization", func() bool {
		return materializedThrough(storage, "byte-pressure") == 1
	})
}

func TestMaterializer_AgePressureTriggersBelowByteThreshold(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeMaxAge = 20 * time.Millisecond
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "age-pressure", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := storage.Append(t.Context(), "age-pressure", []byte("payload"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, "age-pressure materialization", func() bool {
		return materializedThrough(storage, "age-pressure") == 1
	})
}

func TestMaterializer_QuietPartitionDoesNotCheckpoint(t *testing.T) {
	opts := schedulerOptions(t)
	storage := openTest(t, opts)
	before := storage.Stats().CheckpointRounds
	time.Sleep(50 * time.Millisecond)
	if got := storage.Stats().CheckpointRounds; got != before {
		t.Errorf("CheckpointRounds = %d, want unchanged %d while idle", got, before)
	}
}

func TestMaterializer_CheckpointBytePressureTriggers(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	opts.CheckpointBytes = 1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "checkpoint-bytes", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, "mandatory initial checkpoint", func() bool { return storage.Stats().CheckpointRounds > 0 })
	initialRounds := storage.Stats().CheckpointRounds
	if _, err := storage.Append(t.Context(), "checkpoint-bytes", []byte("payload"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, "byte-pressure checkpoint", func() bool {
		stats := storage.Stats()
		return stats.CheckpointRounds > initialRounds && stats.MaterializedNotCheckpointedBytes == 0
	})
}

func TestMaterializer_CheckpointAgePressureTriggers(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	opts.CheckpointMaxAge = 300 * time.Millisecond
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "checkpoint-age", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, "initial checkpoint", func() bool { return storage.Stats().CheckpointRounds > 0 })
	initialRounds := storage.Stats().CheckpointRounds
	if _, err := storage.Append(t.Context(), "checkpoint-age", []byte("payload"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, "materialized uncheckpointed bytes", func() bool {
		return storage.Stats().MaterializedNotCheckpointedBytes > 0
	})
	waitFor(t, "age-pressure checkpoint", func() bool {
		stats := storage.Stats()
		return stats.CheckpointRounds > initialRounds && stats.MaterializedNotCheckpointedBytes == 0
	})
}

func TestMaterializer_CheckpointAgeStartsAtSuccessfulMaterialization(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	opts.CheckpointMaxAge = 200 * time.Millisecond
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "checkpoint-frontier-age", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, "mandatory initial checkpoint", func() bool { return storage.Stats().CheckpointRounds > 0 })
	time.Sleep(opts.CheckpointMaxAge + 50*time.Millisecond)
	initialRounds := storage.Stats().CheckpointRounds
	if _, err := storage.Append(t.Context(), "checkpoint-frontier-age", []byte("small"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, "small append materialization", func() bool {
		return storage.Stats().MaterializedNotCheckpointedBytes > 0
	})
	time.Sleep(50 * time.Millisecond)
	if got := storage.Stats().CheckpointRounds; got != initialRounds {
		t.Fatalf("CheckpointRounds = %d, want %d before frontier reaches max age", got, initialRounds)
	}
	waitFor(t, "frontier-age checkpoint", func() bool {
		return storage.Stats().CheckpointRounds > initialRounds
	})
}

func TestMaterializer_CheckpointFailureRetries(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	opts.CheckpointBytes = 1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "checkpoint-retry", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, "initial checkpoint", func() bool { return storage.Stats().CheckpointRounds > 0 })
	initialRounds := storage.Stats().CheckpointRounds

	p := storage.parts[0]
	positionBefore := storage.Stats().PerPartition[0].CheckpointReplayPosition
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	p.checkpointHookMu.Lock()
	p.checkpointWriteHook = func() error {
		close(hookStarted)
		<-releaseHook
		return errors.New("injected checkpoint write failure")
	}
	p.checkpointHookMu.Unlock()
	if _, err := storage.Append(t.Context(), "checkpoint-retry", []byte("retry"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case <-hookStarted:
	case <-time.After(time.Second):
		t.Fatal("checkpoint failure hook was not reached")
	}
	if p.pending == nil {
		t.Fatal("pending batch = nil during checkpoint write, want retained batch")
	}
	pendingPosition := WALPosition{SegmentSeq: p.pending.barrier.walSeq, Offset: p.pending.barrier.walOff}
	if pendingPosition == positionBefore || !positionAtOrAfter(pendingPosition, positionBefore) {
		t.Fatalf("pending frontier = %+v, want after checkpoint %+v", pendingPosition, positionBefore)
	}
	if got := storage.Stats().PerPartition[0].CheckpointReplayPosition; got != positionBefore {
		t.Fatalf("checkpoint position during failed write = %+v, want %+v", got, positionBefore)
	}
	close(releaseHook)
	waitFor(t, "checkpoint retry", func() bool {
		stats := storage.Stats()
		return stats.CheckpointRounds > initialRounds+1 && stats.MaterializedNotCheckpointedBytes == 0 &&
			positionAtOrAfter(stats.PerPartition[0].CheckpointReplayPosition, pendingPosition)
	})
}

func TestMaterializer_CheckpointExcludesPostBarrierAppend(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeMaxAge = -1
	opts.CheckpointMaxAge = -1
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20}
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "boundary", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Append(t.Context(), "boundary", []byte("initial"), ""); err != nil {
		t.Fatal(err)
	}
	if err := storage.materializeRoundResult(storage.parts[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Append(t.Context(), "boundary", []byte("before"), ""); err != nil {
		t.Fatal(err)
	}

	p := storage.parts[0]
	barrierCaptured := make(chan struct{})
	releaseMaterializer := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(releaseMaterializer)
		}
	})
	p.barrierHookMu.Lock()
	p.materializationBarrierHook = func() {
		close(barrierCaptured)
		<-releaseMaterializer
	}
	p.barrierHookMu.Unlock()
	roundDone := make(chan error, 1)
	go func() { roundDone <- storage.materializeRoundResult(p) }()
	select {
	case <-barrierCaptured:
	case <-time.After(time.Second):
		t.Fatal("materialization barrier was not captured")
	}
	if _, err := storage.Append(t.Context(), "boundary", []byte("after"), ""); err != nil {
		t.Fatal(err)
	}
	close(releaseMaterializer)
	released = true
	if err := <-roundDone; err != nil {
		t.Fatal(err)
	}

	st, _ := storage.streams.Load("boundary")
	snap := st.snapshot()
	if snap.through != 2 || snap.tail != 3 || len(snap.walTail) != 1 {
		t.Fatalf("first round state = through %d, tail %d, WAL tail %d; want 2, 3, 1", snap.through, snap.tail, len(snap.walTail))
	}
	assertSegment(t, st.activeSeg, 1, 2, []byte("initialbefore"))
	checkpoint, ok, err := loadCheckpoint(p.wal.dir)
	if err != nil || !ok {
		t.Fatalf("load first checkpoint = (%v, %v)", ok, err)
	}
	if got := checkpoint.Streams["boundary"].MaterializedThrough; got != 2 {
		t.Fatalf("first checkpoint materialized through = %d, want 2", got)
	}
	firstReplay := WALPosition{SegmentSeq: checkpoint.Replay.SegmentSeq, Offset: checkpoint.Replay.Offset}

	stopWithoutCheckpoint(t, storage)
	storage = openTest(t, opts)
	p = storage.parts[0]
	st, _ = storage.streams.Load("boundary")
	snap = st.snapshot()
	if snap.through != 2 || snap.tail != 3 || len(snap.walTail) != 1 {
		t.Fatalf("recovered state = through %d, tail %d, WAL tail %d; want 2, 3, 1", snap.through, snap.tail, len(snap.walTail))
	}
	if got := readAll(t, storage, "boundary"); !equalStrings(got, []string{"initial", "before", "after"}) {
		t.Fatalf("messages after first checkpoint recovery = %q", got)
	}
	if err := storage.materializeRoundResult(p); err != nil {
		t.Fatal(err)
	}
	checkpoint, ok, err = loadCheckpoint(p.wal.dir)
	if err != nil || !ok {
		t.Fatalf("load second checkpoint = (%v, %v)", ok, err)
	}
	if got := checkpoint.Streams["boundary"].MaterializedThrough; got != 3 {
		t.Fatalf("second checkpoint materialized through = %d, want 3", got)
	}
	secondReplay := WALPosition{SegmentSeq: checkpoint.Replay.SegmentSeq, Offset: checkpoint.Replay.Offset}
	if secondReplay == firstReplay || !positionAtOrAfter(secondReplay, firstReplay) {
		t.Fatalf("second replay position = %+v, want after %+v", secondReplay, firstReplay)
	}
	if got := readAll(t, storage, "boundary"); !equalStrings(got, []string{"initial", "before", "after"}) {
		t.Fatalf("messages after second round = %q", got)
	}
}

func TestCheckpointEntries_DeletedIncarnationDoesNotRemoveReplacement(t *testing.T) {
	oldInc, err := newIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	newInc, err := newIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	oldState := newStreamState("same", oldInc, 0, durablestream.StreamConfig{})
	oldSnapshot := oldState.materializationSnapshot()
	oldSnapshot.deleted = true
	base := map[string]streamCheckpointEntry{
		"same": {IncarnationID: newInc.String()},
	}
	entries := (&Storage{}).checkpointEntries(base, map[*streamState]*preparedStream{
		oldState: {st: oldState, snap: oldSnapshot},
	}, nil)
	if got := entries["same"].IncarnationID; got != newInc.String() {
		t.Fatalf("replacement incarnation = %q, want %q", got, newInc.String())
	}
}

func TestMaterializer_DeleteRecreateFrontierPreservesReplacement(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeMaxAge = -1
	opts.CheckpointMaxAge = -1
	opts.RetentionInterval = -1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "same", durablestream.StreamConfig{ContentType: "old/type"}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Append(t.Context(), "same", []byte("old-checkpointed"), ""); err != nil {
		t.Fatal(err)
	}
	if err := storage.materializeRoundResult(storage.parts[0]); err != nil {
		t.Fatal(err)
	}
	oldState, _ := storage.streams.Load("same")
	oldIncarnation := oldState.inc
	if _, err := storage.Append(t.Context(), "same", []byte("old-dirty"), ""); err != nil {
		t.Fatal(err)
	}
	if err := storage.Delete(t.Context(), "same"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Create(t.Context(), "same", durablestream.StreamConfig{ContentType: "new/type"}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Append(t.Context(), "same", []byte("replacement"), ""); err != nil {
		t.Fatal(err)
	}
	if err := storage.materializeRoundResult(storage.parts[0]); err != nil {
		t.Fatal(err)
	}
	newState, _ := storage.streams.Load("same")
	if newState.inc == oldIncarnation {
		t.Fatal("recreated stream retained the deleted incarnation")
	}
	checkpoint, ok, err := loadCheckpoint(storage.parts[0].wal.dir)
	if err != nil || !ok {
		t.Fatalf("load checkpoint = (%v, %v)", ok, err)
	}
	entry := checkpoint.Streams["same"]
	if entry.IncarnationID != newState.inc.String() || entry.ContentType != "new/type" || entry.MaterializedThrough != 1 {
		t.Fatalf("replacement checkpoint entry = %+v", entry)
	}

	stopWithoutCheckpoint(t, storage)
	reopened := openTest(t, opts)
	head, err := reopened.Head(t.Context(), "same")
	if err != nil {
		t.Fatal(err)
	}
	if head.IncarnationID != newState.inc.String() || head.ContentType != "new/type" {
		t.Fatalf("reopened replacement = %+v", head)
	}
	if got := readAll(t, reopened, "same"); !equalStrings(got, []string{"replacement"}) {
		t.Fatalf("reopened replacement messages = %q", got)
	}
}

func TestSegmentPolicy_AgeSealRetriesCheckpointFailure(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeMaxAge = -1
	opts.CheckpointMaxAge = -1
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20, MaxOpenAge: 500 * time.Millisecond}
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "age-retry", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Append(t.Context(), "age-retry", []byte("payload"), ""); err != nil {
		t.Fatal(err)
	}
	if err := storage.materializeRoundResult(storage.parts[0]); err != nil {
		t.Fatal(err)
	}
	st, _ := storage.streams.Load("age-retry")
	if st.snapshot().activeView.count != 1 {
		t.Fatal("initial payload was not materialized into an active segment")
	}
	initialRounds := storage.Stats().CheckpointRounds

	p := storage.parts[0]
	failureReached := make(chan struct{})
	p.checkpointHookMu.Lock()
	p.checkpointWriteHook = func() error {
		close(failureReached)
		return errors.New("injected age-seal checkpoint failure")
	}
	p.checkpointHookMu.Unlock()
	select {
	case <-failureReached:
	case <-time.After(2 * time.Second):
		t.Fatal("age seal did not reach the checkpoint failure")
	}
	waitFor(t, "age-seal checkpoint retry", func() bool {
		snap := st.snapshot()
		return len(snap.sealed) == 1 && snap.activeView.count == 0 && storage.Stats().CheckpointRounds >= initialRounds+2
	})
	if got := readAll(t, storage, "age-retry"); !equalStrings(got, []string{"payload"}) {
		t.Fatalf("messages after age-seal retry = %q", got)
	}
}

func TestMaterializer_RemovalOnlyWorkIsScheduled(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeBytes = 1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "live", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, "mandatory initial checkpoint", func() bool { return storage.Stats().CheckpointRounds > 0 })

	inc, err := newIncarnation()
	if err != nil {
		t.Fatalf("new incarnation: %v", err)
	}
	stale := newStreamState("stale", inc, 0, durablestream.StreamConfig{})
	dir := streamDir(storage.dir, stale.id, stale.inc)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create stale stream directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orphan"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("create stale stream file: %v", err)
	}
	storage.parts[0].markRemoval(stale)
	waitFor(t, "removal-only directory cleanup", func() bool {
		_, err := os.Stat(dir)
		return os.IsNotExist(err)
	})
}

func TestMaterializer_CloseStopsLongLivedTimer(t *testing.T) {
	opts := schedulerOptions(t)
	storage := openTest(t, opts)
	done := make(chan error, 1)
	go func() { done <- storage.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not stop idle materializer timer")
	}
}
