package seglog

import (
	"testing"
	"time"
)

func TestOptions_AdaptiveScheduleDefaults(t *testing.T) {
	opts := (Options{}).withDefaults()
	if opts.Partitions != 1 {
		t.Errorf("Partitions = %d, want greenfield default 1", opts.Partitions)
	}
	if opts.MaterializeBytes != 4<<20 || opts.MaterializeMaxAge != 250*time.Millisecond {
		t.Errorf("materialize defaults = (%d, %v), want (4 MiB, 250ms)", opts.MaterializeBytes, opts.MaterializeMaxAge)
	}
	if opts.CheckpointBytes != 32<<20 || opts.CheckpointMaxAge != 3*time.Second {
		t.Errorf("checkpoint defaults = (%d, %v), want (32 MiB, 3s)", opts.CheckpointBytes, opts.CheckpointMaxAge)
	}
}

func TestOptions_DeprecatedIntervalsSupplyMaximumAges(t *testing.T) {
	opts := (Options{
		MaterializeInterval: 17 * time.Millisecond,
		CheckpointInterval:  23 * time.Millisecond,
	}).withDefaults()
	if opts.MaterializeMaxAge != 17*time.Millisecond {
		t.Errorf("MaterializeMaxAge = %v, want deprecated interval 17ms", opts.MaterializeMaxAge)
	}
	if opts.CheckpointMaxAge != 23*time.Millisecond {
		t.Errorf("CheckpointMaxAge = %v, want deprecated interval 23ms", opts.CheckpointMaxAge)
	}
}

func TestOptions_SyncConcurrencyDefaultsAndValidates(t *testing.T) {
	if got := (Options{}).withDefaults().SyncConcurrency; got != DefaultSyncConcurrency {
		t.Errorf("default SyncConcurrency = %d, want %d", got, DefaultSyncConcurrency)
	}
	if got := (Options{SyncConcurrency: 1}).withDefaults().SyncConcurrency; got != 1 {
		t.Errorf("explicit SyncConcurrency = %d, want 1", got)
	}
	if _, err := New(Options{SyncConcurrency: -1}); err == nil {
		t.Error("New accepted negative SyncConcurrency")
	}
}
