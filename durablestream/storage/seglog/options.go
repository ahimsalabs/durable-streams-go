package seglog

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Defaults for Options fields left at their zero values.
const (
	DefaultPartitions      = 32
	DefaultMaxMessageSize  = 10 << 20 // 10 MiB
	DefaultWALSegmentBytes = 256 << 20
	// DefaultGroupLinger remains for source compatibility. The write-at-arrival
	// pipeline does not read it; commit waves now self-clock.
	DefaultGroupLinger         = time.Duration(0)
	DefaultGroupMaxBytes       = 4 << 20
	DefaultQueueDepth          = 256
	DefaultShutdownTimeout     = 30 * time.Second
	DefaultMaterializeInterval = 25 * time.Millisecond
	DefaultCheckpointInterval  = 250 * time.Millisecond
	DefaultRetentionInterval   = 30 * time.Second
	DefaultStreamSegmentBytes  = 128 << 20
	// One hour avoids surprising segment churn for low-traffic streams while
	// still giving age retention a bounded opportunity to seal an idle tail.
	DefaultStreamSegmentAge = time.Hour
	DefaultFDCacheSize      = 384
)

// Retention limits the history retained for one stream. Zero values mean
// unlimited. Age is measured from the commit timestamp of each record.
type Retention struct {
	MaxBytes int64
	MaxAge   time.Duration
}

// SyncWrites selects whether acknowledged writes are fdatasync'd.
//
// It is a tri-state instead of a bool so the zero value can mean "the safe
// default" (sync enabled) while still allowing an explicit opt-out for
// throughput experiments where acknowledged writes may be lost on a crash.
type SyncWrites int

const (
	// SyncWritesDefault enables fdatasync (identical to SyncWritesEnabled).
	SyncWritesDefault SyncWrites = iota
	// SyncWritesEnabled fdatasyncs each commit group before acknowledging it.
	SyncWritesEnabled
	// SyncWritesDisabled skips fdatasync. Acknowledged writes can be lost on
	// a crash; recovery correctness (valid-prefix replay) is unaffected.
	SyncWritesDisabled
)

func (s SyncWrites) enabled() (bool, error) {
	switch s {
	case SyncWritesDefault, SyncWritesEnabled:
		return true, nil
	case SyncWritesDisabled:
		return false, nil
	default:
		return false, fmt.Errorf("seglog: invalid SyncWrites value %d", s)
	}
}

// Options configures a seglog Storage. The zero value of every field selects
// a documented default; fields that control background cadence accept -1 to
// disable the loop entirely (used by tests and conformance runs).
type Options struct {
	// Dir is the storage root. When empty, an ephemeral temporary directory
	// is created and removed on Close.
	Dir string

	// SLogger receives operational logs. Nil discards them.
	SLogger *slog.Logger

	// Partitions is the number of WAL partitions. It is fixed at first open:
	// the value is persisted in the FORMAT file and later opens must match.
	Partitions int

	// MaxMessageSize bounds one message's payload bytes; larger appends fail
	// with ErrPayloadTooLarge.
	MaxMessageSize int

	// WALSegmentBytes is the logical size of one WAL segment file. Disk space
	// is preallocated incrementally. It also bounds a single logical mutation:
	// a frame must fit one segment.
	WALSegmentBytes int64

	// GroupLinger is retained for source compatibility and is now a no-op.
	// Write-at-arrival commit waves self-clock without a staging linger.
	GroupLinger time.Duration

	// GroupMaxBytes is retained for source compatibility and is now a no-op.
	// WALSegmentBytes bounds each independently written frame.
	GroupMaxBytes int64

	// QueueDepth is each partition's request queue capacity. Submissions
	// beyond it block, which is the backpressure contract: callers are held
	// at the queue rather than growing unbounded memory.
	QueueDepth int

	// SyncWrites controls fdatasync on commit groups.
	SyncWrites SyncWrites

	// ShutdownTimeout bounds how long Close waits for workers to drain.
	ShutdownTimeout time.Duration

	// MaterializeInterval is how often each partition's materializer copies
	// committed WAL records into per-stream segments, writes a checkpoint,
	// advances the checkpoint, and reclaims fully-reflected WAL segments.
	// -1 disables materialization: reads stay WAL-resident and the WAL is
	// never reclaimed (useful for tests).
	MaterializeInterval time.Duration

	// CheckpointInterval is the minimum time between ordinary checkpoint
	// writes. Materialized segment state is still published every
	// MaterializeInterval. -1 checkpoints every materialization round, which
	// is useful for tests that require tight coupling.
	CheckpointInterval time.Duration

	// DefaultRetention is copied to each stream when it is created.
	DefaultRetention Retention

	// RetentionInterval is how often each partition's materializer evaluates
	// stream retention after its normal materialization round. -1 disables
	// retention sweeps.
	RetentionInterval time.Duration

	// StreamSegmentBytes is the size at which a stream's active segment is
	// sealed and a new one started.
	StreamSegmentBytes int64

	// StreamSegmentAge seals a non-empty, idle active segment after this age
	// so age retention can eventually remove it. -1 disables age sealing.
	StreamSegmentAge time.Duration

	// FDCacheSize bounds cached stream segment and sidecar descriptors. In-use
	// descriptors may temporarily exceed this bound until their pins release.
	FDCacheSize int
}

func (o Options) withDefaults() Options {
	if o.SLogger == nil {
		o.SLogger = slog.New(slog.DiscardHandler)
	}
	if o.Partitions == 0 {
		o.Partitions = DefaultPartitions
	}
	if o.MaxMessageSize == 0 {
		o.MaxMessageSize = DefaultMaxMessageSize
	}
	if o.WALSegmentBytes == 0 {
		o.WALSegmentBytes = DefaultWALSegmentBytes
	}
	if o.GroupMaxBytes == 0 {
		o.GroupMaxBytes = DefaultGroupMaxBytes
	}
	if o.QueueDepth == 0 {
		o.QueueDepth = DefaultQueueDepth
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	if o.MaterializeInterval == 0 {
		o.MaterializeInterval = DefaultMaterializeInterval
	}
	if o.CheckpointInterval == 0 {
		o.CheckpointInterval = DefaultCheckpointInterval
	}
	if o.RetentionInterval == 0 {
		o.RetentionInterval = DefaultRetentionInterval
	}
	if o.StreamSegmentBytes == 0 {
		o.StreamSegmentBytes = DefaultStreamSegmentBytes
	}
	if o.StreamSegmentAge == 0 {
		o.StreamSegmentAge = DefaultStreamSegmentAge
	}
	if o.FDCacheSize == 0 {
		o.FDCacheSize = DefaultFDCacheSize
	}
	return o
}

func (o Options) validate() error {
	var errs []error
	if o.Partitions < 1 {
		errs = append(errs, fmt.Errorf("option Partitions must be positive, got %d", o.Partitions))
	}
	if o.MaxMessageSize < 1 {
		errs = append(errs, fmt.Errorf("option MaxMessageSize must be positive, got %d", o.MaxMessageSize))
	}
	if o.WALSegmentBytes < walSegmentHeaderSize+minFrameSize {
		errs = append(errs, fmt.Errorf("option WALSegmentBytes %d is below the minimum %d",
			o.WALSegmentBytes, walSegmentHeaderSize+minFrameSize))
	}
	if int64(o.MaxMessageSize) > o.WALSegmentBytes-walSegmentHeaderSize {
		errs = append(errs, fmt.Errorf("option MaxMessageSize %d cannot fit one WAL segment of %d bytes",
			o.MaxMessageSize, o.WALSegmentBytes))
	}
	if o.GroupLinger < 0 {
		errs = append(errs, fmt.Errorf("option GroupLinger cannot be negative, got %v", o.GroupLinger))
	}
	if o.GroupMaxBytes < 1 {
		errs = append(errs, fmt.Errorf("option GroupMaxBytes must be positive, got %d", o.GroupMaxBytes))
	}
	if o.QueueDepth < 1 {
		errs = append(errs, fmt.Errorf("option QueueDepth must be positive, got %d", o.QueueDepth))
	}
	if _, err := o.SyncWrites.enabled(); err != nil {
		errs = append(errs, err)
	}
	if o.MaterializeInterval < 0 && o.MaterializeInterval != -1 {
		errs = append(errs, fmt.Errorf("option MaterializeInterval must be positive or -1, got %v", o.MaterializeInterval))
	}
	if o.CheckpointInterval < 0 && o.CheckpointInterval != -1 {
		errs = append(errs, fmt.Errorf("option CheckpointInterval must be positive or -1, got %v", o.CheckpointInterval))
	}
	if o.RetentionInterval < 0 && o.RetentionInterval != -1 {
		errs = append(errs, fmt.Errorf("option RetentionInterval must be positive or -1, got %v", o.RetentionInterval))
	}
	if o.DefaultRetention.MaxBytes < 0 {
		errs = append(errs, fmt.Errorf("option DefaultRetention.MaxBytes cannot be negative, got %d", o.DefaultRetention.MaxBytes))
	}
	if o.DefaultRetention.MaxAge < 0 {
		errs = append(errs, fmt.Errorf("option DefaultRetention.MaxAge cannot be negative, got %v", o.DefaultRetention.MaxAge))
	}
	if o.StreamSegmentBytes < segmentHeaderSize+1 {
		errs = append(errs, fmt.Errorf("option StreamSegmentBytes %d is below the minimum %d",
			o.StreamSegmentBytes, segmentHeaderSize+1))
	}
	if o.StreamSegmentAge < 0 && o.StreamSegmentAge != -1 {
		errs = append(errs, fmt.Errorf("option StreamSegmentAge must be positive or -1, got %v", o.StreamSegmentAge))
	}
	if o.FDCacheSize < 1 {
		errs = append(errs, fmt.Errorf("option FDCacheSize must be positive, got %d", o.FDCacheSize))
	}
	if len(errs) > 0 {
		return fmt.Errorf("seglog: invalid options: %w", errors.Join(errs...))
	}
	return nil
}
