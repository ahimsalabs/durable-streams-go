package seglog

import (
	"os"
	"path/filepath"
	"testing"

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
