package seglog

import (
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
