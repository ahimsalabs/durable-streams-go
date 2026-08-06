package seglog

import (
	"testing"
	"time"
)

func TestOptions_AdaptiveScheduleDefaults(t *testing.T) {
	opts := (Options{}).withDefaults()
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
