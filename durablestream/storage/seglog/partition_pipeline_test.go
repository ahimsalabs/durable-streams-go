package seglog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	storagepkg "github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

type pipelineAppendResult struct {
	offset durablestream.Offset
	err    error
}

func pipelineOptions(dir string) Options {
	opts := singlePartitionOptions(dir)
	opts.GroupMaxBytes = 1 // one mutation per group
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	return opts
}

func appendAsync(s *Storage, streamID, payload, seq string) <-chan pipelineAppendResult {
	done := make(chan pipelineAppendResult, 1)
	go func() {
		offset, err := s.Append(context.Background(), streamID, []byte(payload), seq)
		done <- pipelineAppendResult{offset: offset, err: err}
	}()
	return done
}

func awaitAppend(t *testing.T, done <-chan pipelineAppendResult) pipelineAppendResult {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pipelined append")
		return pipelineAppendResult{}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func closeTestGate(gate chan struct{}) {
	select {
	case <-gate:
	default:
		close(gate)
	}
}

func TestPartitionPipeline_StagesSequenceChainBeforePriorPublish(t *testing.T) {
	s := openTest(t, pipelineOptions(t.TempDir()))
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	p := s.parts[0]

	firstSyncStarted, releaseFirstSync := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseFirstSync) })
	p.wal.blockNextSync(firstSyncStarted, releaseFirstSync)
	first := appendAsync(s, "s", "first", "a")
	awaitSignal(t, firstSyncStarted, "first group sync")

	secondWriteStarted, releaseSecondWrite := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseSecondWrite) })
	p.wal.blockNextWrite(secondWriteStarted, releaseSecondWrite)
	second := appendAsync(s, "s", "second", "b")
	awaitSignal(t, secondWriteStarted, "second group write before first publish")

	secondSyncStarted, releaseSecondSync := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseSecondSync) })
	p.wal.blockNextSync(secondSyncStarted, releaseSecondSync)
	closeTestGate(releaseSecondWrite)
	stale := appendAsync(s, "s", "stale", "a")

	if got := readAll(t, s, "s"); len(got) != 0 {
		t.Fatalf("read before first sync = %q, want no published messages", got)
	}
	closeTestGate(releaseFirstSync)
	if got := awaitAppend(t, first); got.err != nil || got.offset != storagepkg.FormatSimpleOffset(1) {
		t.Fatalf("first append = (%q, %v), want (1, nil)", got.offset, got.err)
	}
	awaitSignal(t, secondSyncStarted, "second group sync")
	if got := readAll(t, s, "s"); !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("read while second sync is blocked = %q, want first group only", got)
	}

	closeTestGate(releaseSecondSync)
	if got := awaitAppend(t, second); got.err != nil || got.offset != storagepkg.FormatSimpleOffset(2) {
		t.Fatalf("second append = (%q, %v), want (2, nil)", got.offset, got.err)
	}
	if got := awaitAppend(t, stale); !errors.Is(got.err, durablestream.ErrConflict) {
		t.Fatalf("stale append error = %v, want ErrConflict", got.err)
	}
	if got := readAll(t, s, "s"); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("final read = %q", got)
	}
}

func TestPartitionPipeline_BarrierWaitsForPriorPublication(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, pipelineOptions(dir))
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	p := s.parts[0]

	syncStarted, releaseSync := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseSync) })
	p.wal.blockNextSync(syncStarted, releaseSync)
	appendDone := appendAsync(s, "s", "before-barrier", "")
	awaitSignal(t, syncStarted, "pre-barrier sync")
	barrierDone := make(chan result, 1)
	go func() {
		barrierDone <- p.submit(&request{op: opBarrier, done: make(chan result, 1)})
	}()
	select {
	case <-barrierDone:
		t.Fatal("barrier completed before the prior group published")
	default:
	}
	closeTestGate(releaseSync)
	if got := awaitAppend(t, appendDone); got.err != nil {
		t.Fatal(got.err)
	}

	var barrier result
	select {
	case barrier = <-barrierDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for barrier")
	}
	if barrier.err != nil {
		t.Fatal(barrier.err)
	}
	if barrier.walSeq != p.publishedSeq || barrier.walOff != p.publishedOff || barrier.nextTxn != p.publishedNextTx {
		t.Fatalf("barrier frontier = (%d,%d,%d), published = (%d,%d,%d)",
			barrier.walSeq, barrier.walOff, barrier.nextTxn,
			p.publishedSeq, p.publishedOff, p.publishedNextTx)
	}
	s.materializeRound(p)
	checkpoint, ok, err := loadCheckpoint(p.wal.dir)
	if err != nil || !ok {
		t.Fatalf("load materializer checkpoint = (%v, %v)", ok, err)
	}
	entry, ok := checkpoint.Streams["s"]
	if !ok || entry.MaterializedThrough < 1 {
		t.Fatalf("checkpoint stream entry = (%+v, %v), want append materialized", entry, ok)
	}
	if checkpoint.Replay.SegmentSeq != p.publishedSeq || checkpoint.Replay.Offset != p.publishedOff {
		t.Fatalf("checkpoint replay = (%d,%d), published = (%d,%d)",
			checkpoint.Replay.SegmentSeq, checkpoint.Replay.Offset, p.publishedSeq, p.publishedOff)
	}
}

func TestPartitionPipeline_SyncFailureFailsLaterGroupsAndIsolatesPartition(t *testing.T) {
	opts := pipelineOptions(t.TempDir())
	opts.Partitions = 2
	s := openTest(t, opts)
	ids := make([]string, 2)
	for i := 0; ids[0] == "" || ids[1] == ""; i++ {
		id := fmt.Sprintf("p-%d", i)
		ids[s.partitionFor(id).id] = id
	}
	for _, id := range ids {
		if _, err := s.Create(t.Context(), id, durablestream.StreamConfig{}); err != nil {
			t.Fatal(err)
		}
	}

	p := s.partitionFor(ids[0])
	firstSyncStarted, releaseFirstSync := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseFirstSync) })
	p.wal.blockNextSync(firstSyncStarted, releaseFirstSync)
	first := appendAsync(s, ids[0], "commits", "")
	awaitSignal(t, firstSyncStarted, "first sync")

	injected := errors.New("pipeline sync failure")
	p.wal.failNextSync(injected)
	failedWriteStarted, releaseFailedWrite := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeTestGate(releaseFailedWrite) })
	p.wal.blockNextWrite(failedWriteStarted, releaseFailedWrite)
	failed := appendAsync(s, ids[0], "fails-sync", "")
	awaitSignal(t, failedWriteStarted, "sync-failed group write")
	closeTestGate(releaseFailedWrite)
	later := appendAsync(s, ids[0], "fails-later", "")
	closeTestGate(releaseFirstSync)
	if got := awaitAppend(t, first); got.err != nil {
		t.Fatalf("prior group: %v", got.err)
	}
	if got := awaitAppend(t, failed); !errors.Is(got.err, injected) {
		t.Fatalf("sync-failed group error = %v, want injected error", got.err)
	}
	if got := awaitAppend(t, later); !errors.Is(got.err, injected) {
		t.Fatalf("later group error = %v, want latched error", got.err)
	}
	if _, err := s.Append(t.Context(), ids[0], []byte("also-fails"), ""); !errors.Is(err, injected) {
		t.Fatalf("post-failure append = %v, want latched error", err)
	}
	if _, err := s.Append(t.Context(), ids[1], []byte("other-partition"), ""); err != nil {
		t.Fatalf("other partition append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after sync fail-stop: %v", err)
	}
}
