package seglog

import (
	"sync"
	"sync/atomic"
	"time"
)

// WALPosition identifies a byte boundary in one partition's WAL.
type WALPosition struct {
	SegmentSeq uint64
	Offset     int64
}

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

	// PendingWALBytes is the number of outstanding frame bytes not yet resolved
	// by publication after their required fdatasync or by a request failure. In
	// unsafe sync-off mode, publication is the boundary even though no
	// durability barrier runs.
	PendingWALBytes int64

	// UnmaterializedWALBytes is the number of committed WAL-frame bytes not
	// yet covered by a completed per-stream materialization barrier.
	UnmaterializedWALBytes int64

	// OldestUnmaterializedAge is the age of the oldest committed WAL frame not
	// yet covered by a completed per-stream materialization barrier.
	OldestUnmaterializedAge time.Duration

	// MaterializedNotCheckpointedBytes is the number of committed WAL frame
	// bytes between the latest completed materialization barrier and the
	// durable checkpoint frontier.
	MaterializedNotCheckpointedBytes int64

	// UnreclaimedWALBytes is the logical byte size, including segment headers,
	// of WAL segments that the writer still retains. Physical allocation can be
	// larger because WAL files grow in extents.
	UnreclaimedWALBytes int64

	// CurrentWALSegmentBytes and CurrentWALSegmentCapacityBytes describe the
	// active WAL segment, including its header. Both are zero before the first
	// frame creates a segment.
	CurrentWALSegmentBytes         int64
	CurrentWALSegmentCapacityBytes int64
	CurrentWALSegmentUtilization   float64

	// CheckpointReplayPosition is meaningful only in Stats.PerPartition. WAL
	// positions from different partitions cannot be aggregated.
	CheckpointReplayPosition WALPosition
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

	frontierMu               sync.Mutex
	pendingWALBytes          int64
	committedWALBytes        int64
	materializedWALBytes     int64
	checkpointedWALBytes     int64
	oldestUnmaterializedTS   int64
	postBarrierOldestTS      int64
	barrierActive            bool
	barrierCommittedWALBytes int64
	checkpointPosition       WALPosition
}

type statsFrontier struct {
	committedWALBytes int64
}

// Stats returns a point-in-time snapshot aggregated across all partitions.
func (s *Storage) Stats() Stats {
	now := time.Now()
	stats := Stats{
		CommitWaves:  s.commitGate.completed.Load(),
		PerPartition: make([]PartitionStats, len(s.parts)),
	}
	for i, p := range s.parts {
		partition := p.stats.snapshot(now, p.wal.usageSnapshot())
		stats.PerPartition[i] = partition
		stats.PartitionStats.add(partition)
	}
	stats.CurrentWALSegmentUtilization = utilization(
		stats.CurrentWALSegmentBytes,
		stats.CurrentWALSegmentCapacityBytes,
	)
	return stats
}

func (s *partitionStats) snapshot(now time.Time, wal walUsage) PartitionStats {
	s.frontierMu.Lock()
	oldestAge := time.Duration(0)
	oldestTS := s.oldestUnmaterializedTS
	if oldestTS == 0 {
		oldestTS = s.postBarrierOldestTS
	}
	if oldestTS > 0 && now.UnixNano() > oldestTS {
		oldestAge = time.Duration(now.UnixNano() - oldestTS)
	}
	pendingBytes := s.pendingWALBytes
	unmaterializedBytes := s.committedWALBytes - s.materializedWALBytes
	materializedBytes := s.materializedWALBytes - s.checkpointedWALBytes
	checkpointPosition := s.checkpointPosition
	s.frontierMu.Unlock()

	stats := PartitionStats{
		OpsCommitted:                     s.opsCommitted.Load(),
		WALBytesWritten:                  s.walBytesWritten.Load(),
		CommitFdatasyncNanos:             s.commitFdatasyncNanos.Load(),
		CommitterIdleNanos:               s.committerIdleNanos.Load(),
		PublishNanos:                     s.publishNanos.Load(),
		MaterializerSyncs:                s.materializerSyncs.Load(),
		SyncfsCalls:                      s.syncfsCalls.Load(),
		CheckpointRounds:                 s.checkpointRounds.Load(),
		PendingWALBytes:                  pendingBytes,
		UnmaterializedWALBytes:           unmaterializedBytes,
		OldestUnmaterializedAge:          oldestAge,
		MaterializedNotCheckpointedBytes: materializedBytes,
		UnreclaimedWALBytes:              wal.retainedBytes,
		CurrentWALSegmentBytes:           wal.activeBytes,
		CurrentWALSegmentCapacityBytes:   wal.segmentCapacityBytes,
		CheckpointReplayPosition:         checkpointPosition,
	}
	stats.CurrentWALSegmentUtilization = utilization(
		stats.CurrentWALSegmentBytes,
		stats.CurrentWALSegmentCapacityBytes,
	)
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
	s.PendingWALBytes += other.PendingWALBytes
	s.UnmaterializedWALBytes += other.UnmaterializedWALBytes
	s.OldestUnmaterializedAge = max(s.OldestUnmaterializedAge, other.OldestUnmaterializedAge)
	s.MaterializedNotCheckpointedBytes += other.MaterializedNotCheckpointedBytes
	s.UnreclaimedWALBytes += other.UnreclaimedWALBytes
	s.CurrentWALSegmentBytes += other.CurrentWALSegmentBytes
	s.CurrentWALSegmentCapacityBytes += other.CurrentWALSegmentCapacityBytes
	for i := range s.GroupSizeHist {
		s.GroupSizeHist[i] += other.GroupSizeHist[i]
	}
}

func (s *partitionStats) frontierPressure(now time.Time) (unmaterialized int64, oldestAge time.Duration, uncheckpointed int64) {
	s.frontierMu.Lock()
	defer s.frontierMu.Unlock()
	unmaterialized = s.committedWALBytes - s.materializedWALBytes
	uncheckpointed = s.materializedWALBytes - s.checkpointedWALBytes
	oldestTS := s.oldestUnmaterializedTS
	if oldestTS == 0 {
		oldestTS = s.postBarrierOldestTS
	}
	if oldestTS > 0 && now.UnixNano() > oldestTS {
		oldestAge = time.Duration(now.UnixNano() - oldestTS)
	}
	return unmaterialized, oldestAge, uncheckpointed
}

func (s *partitionStats) checkpointBytesAt(frontier statsFrontier) int64 {
	s.frontierMu.Lock()
	defer s.frontierMu.Unlock()
	return frontier.committedWALBytes - s.checkpointedWALBytes
}

func (s *partitionStats) addPendingWALBytes(bytes int64) {
	s.frontierMu.Lock()
	s.pendingWALBytes += bytes
	s.frontierMu.Unlock()
}

func (s *partitionStats) discardPendingWALBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	s.frontierMu.Lock()
	s.pendingWALBytes -= bytes
	s.frontierMu.Unlock()
}

func (s *partitionStats) publishWALFrame(bytes, ts int64) {
	if bytes <= 0 {
		return
	}
	s.frontierMu.Lock()
	s.pendingWALBytes -= bytes
	s.recordCommittedWALFrame(bytes, ts)
	s.frontierMu.Unlock()
}

func (s *partitionStats) recoverWALFrame(bytes, ts int64) {
	if bytes <= 0 {
		return
	}
	s.frontierMu.Lock()
	s.recordCommittedWALFrame(bytes, ts)
	s.frontierMu.Unlock()
}

func (s *partitionStats) recordCommittedWALFrame(bytes, ts int64) {
	s.committedWALBytes += bytes
	if s.barrierActive {
		if s.postBarrierOldestTS == 0 {
			s.postBarrierOldestTS = ts
		}
	} else if s.oldestUnmaterializedTS == 0 {
		s.oldestUnmaterializedTS = ts
	}
}

func (s *partitionStats) captureMaterializationFrontier() statsFrontier {
	s.frontierMu.Lock()
	s.barrierActive = true
	s.barrierCommittedWALBytes = s.committedWALBytes
	s.postBarrierOldestTS = 0
	frontier := statsFrontier{committedWALBytes: s.committedWALBytes}
	s.frontierMu.Unlock()
	return frontier
}

func (s *partitionStats) cancelMaterializationFrontier(frontier statsFrontier) {
	s.frontierMu.Lock()
	if s.barrierActive && frontier.committedWALBytes == s.barrierCommittedWALBytes {
		if s.oldestUnmaterializedTS == 0 {
			s.oldestUnmaterializedTS = s.postBarrierOldestTS
		}
		s.barrierActive = false
		s.postBarrierOldestTS = 0
	}
	s.frontierMu.Unlock()
}

func (s *partitionStats) advanceMaterializationFrontier(frontier statsFrontier, position WALPosition, checkpoint bool) {
	s.frontierMu.Lock()
	if s.barrierActive && frontier.committedWALBytes == s.barrierCommittedWALBytes {
		s.materializedWALBytes = frontier.committedWALBytes
		s.oldestUnmaterializedTS = s.postBarrierOldestTS
		s.barrierActive = false
		s.postBarrierOldestTS = 0
		if checkpoint {
			s.checkpointedWALBytes = frontier.committedWALBytes
			s.checkpointPosition = position
		}
	}
	s.frontierMu.Unlock()
}

func (s *partitionStats) initializeCheckpoint(position WALPosition) {
	s.frontierMu.Lock()
	s.checkpointPosition = position
	s.frontierMu.Unlock()
}

func utilization(bytes, capacity int64) float64 {
	if capacity == 0 {
		return 0
	}
	return float64(bytes) / float64(capacity)
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
