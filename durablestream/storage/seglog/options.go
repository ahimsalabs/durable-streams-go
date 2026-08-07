package seglog

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// Defaults for Options fields left at their zero values.
const (
	DefaultPartitions        = 1
	DefaultMaxMessageSize    = 10 << 20 // 10 MiB
	DefaultWALSegmentBytes   = 256 << 20
	DefaultWALExtentBytes    = 16 << 20
	DefaultQueueDepth        = 256
	DefaultShutdownTimeout   = 30 * time.Second
	DefaultMaterializeBytes  = 4 << 20
	DefaultMaterializeMaxAge = 250 * time.Millisecond
	DefaultCheckpointBytes   = 32 << 20
	DefaultCheckpointMaxAge  = 3 * time.Second
	// Deprecated interval names remain aliases for source compatibility.
	DefaultMaterializeInterval    = DefaultMaterializeMaxAge
	DefaultCheckpointInterval     = DefaultCheckpointMaxAge
	DefaultRetentionInterval      = 30 * time.Second
	DefaultSegmentTargetBytes     = 128 << 20
	DefaultCompressionBlockBytes  = 1 << 20
	DefaultCompressionMaxBlockAge = 10 * time.Second
	// One hour avoids surprising segment churn for low-traffic streams while
	// still giving age retention a bounded opportunity to seal an idle tail.
	DefaultSegmentMaxOpenAge = time.Hour
	DefaultFDCacheSize       = 384
)

// Compression selects the encoding used for derived stream segments. WAL
// records are never compressed by seglog. The zero value preserves v2 segment
// files because applications can already supply compressed record payloads.
type Compression uint8

const (
	CompressionDisabled Compression = iota
	CompressionZstd
)

// Retention limits the history retained for one stream. Zero values mean
// unlimited. MaxBytes counts physical segment payload bytes; for compressed
// v3 segments this is compressed frame data. Age is measured from the commit
// timestamp of each record.
type Retention struct {
	MaxBytes int64
	MaxAge   time.Duration
}

// SegmentPolicy is the immutable rollover policy stored with each stream.
// TargetBytes counts physical payload bytes only. For compressed v3 segments,
// this is completed compressed frame data and rollover can overshoot by one
// normal compression block. MaxOpenAge is wall-clock time from creation of a
// non-empty active segment. Zero MaxOpenAge disables age rollover.
type SegmentPolicy struct {
	TargetBytes int64
	MaxOpenAge  time.Duration
}

// SegmentPolicySelector can override the default policy at stream creation.
// Returning nil selects Options.DefaultSegmentPolicy. Recovery never calls it.
type SegmentPolicySelector func(streamID string, config durablestream.StreamConfig) *SegmentPolicy

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

	// WALExtentBytes is the unit of physical WAL preallocation.
	WALExtentBytes int64

	// QueueDepth is each partition's request queue capacity. Submissions
	// beyond it block, which is the backpressure contract: callers are held
	// at the queue rather than growing unbounded memory.
	QueueDepth int

	// SyncWrites controls fdatasync on commit groups.
	SyncWrites SyncWrites

	// Compression selects optional block compression for materialized segment
	// payloads. CompressionZstd writes v3 segments as independent zstd frames;
	// CompressionBlockBytes is their target uncompressed size. A record is
	// never split, and an oversized record gets one dedicated frame. Zero
	// CompressionBlockBytes selects DefaultCompressionBlockBytes when enabled.
	// CompressionMaxBlockAge replaces MaterializeMaxAge for compression
	// accumulation while preserving MaterializeBytes as the WAL pressure limit.
	// Its zero value selects DefaultCompressionMaxBlockAge.
	Compression            Compression
	CompressionBlockBytes  int
	CompressionMaxBlockAge time.Duration

	// ShutdownTimeout bounds how long Close waits for workers to drain.
	ShutdownTimeout time.Duration

	// MaterializeBytes and MaterializeMaxAge bound the unmaterialized WAL
	// frontier. A partition materializes when either limit is reached. Their
	// zero values select DefaultMaterializeBytes and DefaultMaterializeMaxAge.
	// MaterializeMaxAge -1 disables these process-level triggers. The scheduler
	// still enforces persisted per-stream maximum open ages.
	MaterializeBytes  int64
	MaterializeMaxAge time.Duration

	// MaterializeInterval is deprecated. A nonzero value supplies
	// MaterializeMaxAge when that field is zero; -1 still disables the worker.
	MaterializeInterval time.Duration

	// CheckpointBytes and CheckpointMaxAge bound materialized data that is not
	// covered by a durable checkpoint. A checkpoint is written when either
	// limit is reached. Zero values select the documented defaults;
	// CheckpointMaxAge -1 checkpoints every materialization round.
	CheckpointBytes  int64
	CheckpointMaxAge time.Duration

	// CheckpointInterval is deprecated. A nonzero value supplies
	// CheckpointMaxAge when that field is zero.
	CheckpointInterval time.Duration

	// DefaultRetention is copied to each stream when it is created.
	DefaultRetention Retention

	// RetentionInterval is how often each partition's materializer evaluates
	// stream retention after its normal materialization round. -1 disables
	// retention sweeps.
	RetentionInterval time.Duration

	// DefaultSegmentPolicy is copied to each stream when it is created.
	DefaultSegmentPolicy SegmentPolicy

	// SelectSegmentPolicy optionally overrides the default for a create or
	// fork target. The resolved value is persisted with that target.
	SelectSegmentPolicy SegmentPolicySelector

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
	if o.WALExtentBytes == 0 {
		o.WALExtentBytes = min(DefaultWALExtentBytes, o.WALSegmentBytes)
	}
	if o.QueueDepth == 0 {
		o.QueueDepth = DefaultQueueDepth
	}
	if o.Compression == CompressionZstd && o.CompressionBlockBytes == 0 {
		o.CompressionBlockBytes = DefaultCompressionBlockBytes
	}
	if o.Compression == CompressionZstd && o.CompressionMaxBlockAge == 0 {
		o.CompressionMaxBlockAge = DefaultCompressionMaxBlockAge
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	if o.MaterializeBytes == 0 {
		o.MaterializeBytes = DefaultMaterializeBytes
	}
	if o.MaterializeMaxAge == 0 {
		if o.MaterializeInterval != 0 {
			o.MaterializeMaxAge = o.MaterializeInterval
		} else {
			o.MaterializeMaxAge = DefaultMaterializeMaxAge
		}
	}
	if o.CheckpointBytes == 0 {
		o.CheckpointBytes = DefaultCheckpointBytes
	}
	if o.CheckpointMaxAge == 0 {
		if o.CheckpointInterval != 0 {
			o.CheckpointMaxAge = o.CheckpointInterval
		} else {
			o.CheckpointMaxAge = DefaultCheckpointMaxAge
		}
	}
	if o.RetentionInterval == 0 {
		o.RetentionInterval = DefaultRetentionInterval
	}
	if o.DefaultSegmentPolicy == (SegmentPolicy{}) {
		o.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: DefaultSegmentTargetBytes, MaxOpenAge: DefaultSegmentMaxOpenAge}
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
	if o.WALExtentBytes < walSegmentHeaderSize || o.WALExtentBytes > o.WALSegmentBytes {
		errs = append(errs, fmt.Errorf("option WALExtentBytes must be in [%d, WALSegmentBytes], got %d", walSegmentHeaderSize, o.WALExtentBytes))
	}
	if int64(o.MaxMessageSize) > o.WALSegmentBytes-walSegmentHeaderSize {
		errs = append(errs, fmt.Errorf("option MaxMessageSize %d cannot fit one WAL segment of %d bytes",
			o.MaxMessageSize, o.WALSegmentBytes))
	}
	if o.QueueDepth < 1 {
		errs = append(errs, fmt.Errorf("option QueueDepth must be positive, got %d", o.QueueDepth))
	}
	if _, err := o.SyncWrites.enabled(); err != nil {
		errs = append(errs, err)
	}
	if o.Compression != CompressionDisabled && o.Compression != CompressionZstd {
		errs = append(errs, fmt.Errorf("option Compression has invalid value %d", o.Compression))
	}
	if o.Compression == CompressionDisabled && o.CompressionBlockBytes != 0 {
		errs = append(errs, errors.New("option CompressionBlockBytes requires CompressionZstd"))
	}
	if o.Compression == CompressionDisabled && o.CompressionMaxBlockAge != 0 {
		errs = append(errs, errors.New("option CompressionMaxBlockAge requires CompressionZstd"))
	}
	if o.Compression == CompressionZstd && o.CompressionBlockBytes < 1 {
		errs = append(errs, fmt.Errorf("option CompressionBlockBytes must be positive, got %d", o.CompressionBlockBytes))
	}
	if o.Compression == CompressionZstd && o.CompressionMaxBlockAge <= 0 {
		errs = append(errs, fmt.Errorf("option CompressionMaxBlockAge must be positive, got %v", o.CompressionMaxBlockAge))
	}
	if o.MaterializeBytes < 1 {
		errs = append(errs, fmt.Errorf("option MaterializeBytes must be positive, got %d", o.MaterializeBytes))
	}
	if o.MaterializeMaxAge < 0 && o.MaterializeMaxAge != -1 {
		errs = append(errs, fmt.Errorf("option MaterializeMaxAge must be positive or -1, got %v", o.MaterializeMaxAge))
	}
	if o.CheckpointBytes < 1 {
		errs = append(errs, fmt.Errorf("option CheckpointBytes must be positive, got %d", o.CheckpointBytes))
	}
	if o.CheckpointMaxAge < 0 && o.CheckpointMaxAge != -1 {
		errs = append(errs, fmt.Errorf("option CheckpointMaxAge must be positive or -1, got %v", o.CheckpointMaxAge))
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
	if o.DefaultSegmentPolicy.TargetBytes < 1 {
		errs = append(errs, fmt.Errorf("option DefaultSegmentPolicy.TargetBytes must be positive, got %d",
			o.DefaultSegmentPolicy.TargetBytes))
	}
	if o.DefaultSegmentPolicy.MaxOpenAge < 0 {
		errs = append(errs, fmt.Errorf("option DefaultSegmentPolicy.MaxOpenAge cannot be negative, got %v", o.DefaultSegmentPolicy.MaxOpenAge))
	}
	if o.FDCacheSize < 1 {
		errs = append(errs, fmt.Errorf("option FDCacheSize must be positive, got %d", o.FDCacheSize))
	}
	if len(errs) > 0 {
		return fmt.Errorf("seglog: invalid options: %w", errors.Join(errs...))
	}
	return nil
}
