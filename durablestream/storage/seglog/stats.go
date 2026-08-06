package seglog

import "sync/atomic"

// PartitionStats is a snapshot of one partition's commit and materialization
// activity. GroupsCommitted and GroupSizeHist now count partition snapshots
// per flush wave; histogram buckets contain operations per snapshot.
type PartitionStats struct {
	GroupsCommitted      int64
	OpsCommitted         int64
	WALBytesWritten      int64
	CommitFdatasyncNanos int64
	GroupSizeHist        [10]int64
	CommitterIdleNanos   int64
	PublishNanos         int64
	MaterializerSyncs    int64
	SyncfsCalls          int64
	CheckpointRounds     int64
}

// Stats is an aggregate snapshot of all seglog partitions.
type Stats struct {
	PartitionStats
	CommitWaves  int64
	PerPartition []PartitionStats
}

type partitionStats struct {
	opsCommitted         atomic.Int64
	walBytesWritten      atomic.Int64
	commitFdatasyncNanos atomic.Int64
	groupSizeHist        [10]atomic.Int64
	committerIdleNanos   atomic.Int64
	publishNanos         atomic.Int64
	materializerSyncs    atomic.Int64
	syncfsCalls          atomic.Int64
	checkpointRounds     atomic.Int64
}

// Stats returns a point-in-time snapshot aggregated across all partitions.
func (s *Storage) Stats() Stats {
	stats := Stats{
		CommitWaves:  s.commitGate.completed.Load(),
		PerPartition: make([]PartitionStats, len(s.parts)),
	}
	for i, p := range s.parts {
		partition := p.stats.snapshot()
		stats.PerPartition[i] = partition
		stats.PartitionStats.add(partition)
	}
	return stats
}

func (s *partitionStats) snapshot() PartitionStats {
	stats := PartitionStats{
		OpsCommitted:         s.opsCommitted.Load(),
		WALBytesWritten:      s.walBytesWritten.Load(),
		CommitFdatasyncNanos: s.commitFdatasyncNanos.Load(),
		CommitterIdleNanos:   s.committerIdleNanos.Load(),
		PublishNanos:         s.publishNanos.Load(),
		MaterializerSyncs:    s.materializerSyncs.Load(),
		SyncfsCalls:          s.syncfsCalls.Load(),
		CheckpointRounds:     s.checkpointRounds.Load(),
	}
	for i := range stats.GroupSizeHist {
		groups := s.groupSizeHist[i].Load()
		stats.GroupSizeHist[i] = groups
		stats.GroupsCommitted += groups
	}
	return stats
}

func (s *PartitionStats) add(other PartitionStats) {
	s.GroupsCommitted += other.GroupsCommitted
	s.OpsCommitted += other.OpsCommitted
	s.WALBytesWritten += other.WALBytesWritten
	s.CommitFdatasyncNanos += other.CommitFdatasyncNanos
	s.CommitterIdleNanos += other.CommitterIdleNanos
	s.PublishNanos += other.PublishNanos
	s.MaterializerSyncs += other.MaterializerSyncs
	s.SyncfsCalls += other.SyncfsCalls
	s.CheckpointRounds += other.CheckpointRounds
	for i := range s.GroupSizeHist {
		s.GroupSizeHist[i] += other.GroupSizeHist[i]
	}
}

func groupSizeBucket(ops int64) int {
	switch {
	case ops <= 2:
		return int(ops - 1)
	case ops <= 4:
		return 2
	case ops <= 8:
		return 3
	case ops <= 16:
		return 4
	case ops <= 32:
		return 5
	case ops <= 64:
		return 6
	case ops <= 128:
		return 7
	case ops <= 256:
		return 8
	default:
		return 9
	}
}
