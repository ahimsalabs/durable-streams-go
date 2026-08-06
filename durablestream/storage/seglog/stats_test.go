package seglog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestStats_AppendedBatchesCountCommittedGroupsAndOps(t *testing.T) {
	storage := openTest(t, singlePartitionOptions(t.TempDir()))
	if _, err := storage.Create(t.Context(), "stats", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	const appendedOps = 3
	for range appendedOps {
		if _, err := storage.AppendBatch(t.Context(), "stats", [][]byte{[]byte("one"), []byte("two")}, ""); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
	}

	stats := storage.Stats()
	if stats.GroupsCommitted <= 0 {
		t.Errorf("GroupsCommitted = %d, want > 0", stats.GroupsCommitted)
	}
	if stats.CommitWaves <= 0 {
		t.Errorf("CommitWaves = %d, want > 0", stats.CommitWaves)
	}
	if stats.OpsCommitted < appendedOps {
		t.Errorf("OpsCommitted = %d, want >= %d", stats.OpsCommitted, appendedOps)
	}
	var histogramTotal int64
	for _, groups := range stats.GroupSizeHist {
		histogramTotal += groups
	}
	if histogramTotal != stats.GroupsCommitted {
		t.Errorf("histogram total = %d, GroupsCommitted = %d", histogramTotal, stats.GroupsCommitted)
	}
}

func TestStats_BlockedFlushReportsPendingWALBytes(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.MaterializeInterval = -1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "pending-stats", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	storage.parts[0].wal.blockNextSync(started, release)
	appendResult := make(chan error, 1)
	go func() {
		_, err := storage.Append(t.Context(), "pending-stats", []byte("pending"), "")
		appendResult <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked WAL flush")
	}
	if got := storage.Stats().PendingWALBytes; got <= 0 {
		t.Errorf("PendingWALBytes = %d, want > 0 while fdatasync is blocked", got)
	}
	close(release)
	released = true
	if err := <-appendResult; err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := storage.Stats().PendingWALBytes; got != 0 {
		t.Errorf("PendingWALBytes = %d, want 0 after publication", got)
	}
}

func TestStats_FailedWALWriteOrSyncClearsPendingBytes(t *testing.T) {
	for _, test := range []struct {
		name   string
		inject func(*walWriter, error)
	}{
		{name: "write", inject: (*walWriter).failNextWrite},
		{name: "sync", inject: (*walWriter).failNextSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := singlePartitionOptions(t.TempDir())
			opts.MaterializeInterval = -1
			storage := openTest(t, opts)
			if _, err := storage.Create(t.Context(), "failed-stats", durablestream.StreamConfig{}); err != nil {
				t.Fatalf("Create: %v", err)
			}

			injected := errors.New("injected WAL failure")
			test.inject(storage.parts[0].wal, injected)
			if _, err := storage.Append(t.Context(), "failed-stats", []byte("not committed"), ""); !errors.Is(err, injected) {
				t.Fatalf("Append error = %v, want %v", err, injected)
			}
			if got := storage.Stats().PendingWALBytes; got != 0 {
				t.Errorf("PendingWALBytes = %d after failed %s, want 0", got, test.name)
			}
		})
	}
}

func TestPartitionStats_MaterializationKeepsPostBarrierOldestAge(t *testing.T) {
	var stats partitionStats
	first := time.Unix(100, 0)
	second := first.Add(time.Second)
	stats.recoverWALFrame(10, first.UnixNano())
	frontier := stats.captureMaterializationFrontier()
	stats.recoverWALFrame(20, second.UnixNano())
	stats.advanceMaterializationFrontier(frontier, WALPosition{SegmentSeq: 1, Offset: 10}, false)

	snapshot := stats.snapshot(second.Add(2*time.Second), walUsage{})
	if snapshot.OldestUnmaterializedAge != 2*time.Second {
		t.Errorf("OldestUnmaterializedAge = %v, want %v", snapshot.OldestUnmaterializedAge, 2*time.Second)
	}
	if snapshot.MaterializedNotCheckpointedBytes != 10 {
		t.Errorf("MaterializedNotCheckpointedBytes = %d, want 10", snapshot.MaterializedNotCheckpointedBytes)
	}
}

func TestStats_MaterializationAndCheckpointAdvanceFrontiers(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.MaterializeInterval = -1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "frontier-stats", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	storage.materializeRound(storage.parts[0]) // establish the first checkpoint
	firstCheckpoint := storage.Stats().PerPartition[0].CheckpointReplayPosition

	if _, err := storage.Append(t.Context(), "frontier-stats", []byte("not materialized"), ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	time.Sleep(time.Millisecond)
	committed := storage.Stats()
	if committed.OldestUnmaterializedAge <= 0 {
		t.Errorf("OldestUnmaterializedAge = %v, want > 0", committed.OldestUnmaterializedAge)
	}
	if committed.MaterializedNotCheckpointedBytes != 0 {
		t.Errorf("MaterializedNotCheckpointedBytes = %d, want 0 before materialization", committed.MaterializedNotCheckpointedBytes)
	}
	if committed.UnreclaimedWALBytes <= 0 {
		t.Errorf("UnreclaimedWALBytes = %d, want > 0", committed.UnreclaimedWALBytes)
	}
	if committed.CurrentWALSegmentUtilization <= 0 || committed.CurrentWALSegmentUtilization > 1 {
		t.Errorf("CurrentWALSegmentUtilization = %f, want in (0, 1]", committed.CurrentWALSegmentUtilization)
	}

	storage.materializeRound(storage.parts[0])
	materialized := storage.Stats()
	if materialized.OldestUnmaterializedAge != 0 {
		t.Errorf("OldestUnmaterializedAge = %v, want 0 after materialization", materialized.OldestUnmaterializedAge)
	}
	if materialized.MaterializedNotCheckpointedBytes <= 0 {
		t.Errorf("MaterializedNotCheckpointedBytes = %d, want > 0", materialized.MaterializedNotCheckpointedBytes)
	}
	if got := materialized.PerPartition[0].CheckpointReplayPosition; got != firstCheckpoint {
		t.Errorf("checkpoint position = %+v, want unchanged %+v", got, firstCheckpoint)
	}

	storage.parts[0].uncheckpointedSince = time.Time{}.Add(-time.Hour)
	storage.materializeRound(storage.parts[0])
	checkpointed := storage.Stats()
	if checkpointed.MaterializedNotCheckpointedBytes != 0 {
		t.Errorf("MaterializedNotCheckpointedBytes = %d, want 0 after checkpoint", checkpointed.MaterializedNotCheckpointedBytes)
	}
	if got := checkpointed.PerPartition[0].CheckpointReplayPosition; !positionAtOrAfter(got, firstCheckpoint) || got == firstCheckpoint {
		t.Errorf("checkpoint position = %+v, want after %+v", got, firstCheckpoint)
	}
}

func TestStats_CheckpointReclaimsWholeWALSegments(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.WALSegmentBytes = 8192
	opts.MaxMessageSize = 1024
	opts.MaterializeInterval = -1
	opts.CheckpointInterval = -1
	storage := openTest(t, opts)
	if _, err := storage.Create(t.Context(), "reclaim-stats", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	for range 20 {
		if _, err := storage.Append(t.Context(), "reclaim-stats", make([]byte, 700), ""); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	before := storage.Stats()
	if before.UnreclaimedWALBytes <= before.CurrentWALSegmentBytes {
		t.Fatalf("UnreclaimedWALBytes = %d, current segment = %d; want older retained segments", before.UnreclaimedWALBytes, before.CurrentWALSegmentBytes)
	}
	storage.materializeRound(storage.parts[0])
	after := storage.Stats()
	if after.UnreclaimedWALBytes >= before.UnreclaimedWALBytes {
		t.Errorf("UnreclaimedWALBytes = %d after checkpoint, want < %d", after.UnreclaimedWALBytes, before.UnreclaimedWALBytes)
	}
	if after.UnreclaimedWALBytes != after.CurrentWALSegmentBytes {
		t.Errorf("UnreclaimedWALBytes = %d, current segment = %d; want only active segment retained", after.UnreclaimedWALBytes, after.CurrentWALSegmentBytes)
	}
}

func TestDiskUsage_CountsDirectoryAndPerStreamFiles(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.CheckpointInterval = -1
	storage := openTest(t, opts)
	for _, streamID := range []string{"usage-a", "usage-b"} {
		if _, err := storage.Create(t.Context(), streamID, durablestream.StreamConfig{}); err != nil {
			t.Fatalf("Create(%q): %v", streamID, err)
		}
		if _, err := storage.Append(t.Context(), streamID, []byte("payload-"+streamID), ""); err != nil {
			t.Fatalf("Append(%q): %v", streamID, err)
		}
	}
	storage.materializeRound(storage.parts[0])

	usage, err := storage.DiskUsage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wantTotal := allocatedBytesUnder(t, dir)
	if usage.TotalBytes != wantTotal {
		t.Errorf("TotalBytes = %d, want directory allocation %d", usage.TotalBytes, wantTotal)
	}
	for _, streamID := range []string{"usage-a", "usage-b"} {
		state, ok := storage.streams.Load(streamID)
		if !ok {
			t.Fatalf("stream %q missing from catalog", streamID)
		}
		want := allocatedBytesUnder(t, streamDir(dir, streamID, state.inc))
		if got := usage.PerStreamBytes[streamID]; got != want || got == 0 {
			t.Errorf("PerStreamBytes[%q] = %d, want non-zero %d", streamID, got, want)
		}
	}

	usage.PerStreamBytes["usage-a"] = -1
	again, err := storage.DiskUsage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if again.PerStreamBytes["usage-a"] < 0 {
		t.Error("mutating returned PerStreamBytes changed storage-owned state")
	}
}

func allocatedBytesUnder(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	if err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += allocatedFileBytes(info)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return total
}

func positionAtOrAfter(got, want WALPosition) bool {
	return got.SegmentSeq > want.SegmentSeq ||
		(got.SegmentSeq == want.SegmentSeq && got.Offset >= want.Offset)
}
