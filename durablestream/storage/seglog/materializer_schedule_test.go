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
