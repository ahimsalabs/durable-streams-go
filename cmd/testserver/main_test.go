package main

import (
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/seglog"
)

func TestParseByteSize_AcceptsHumanAndRawSizes(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{name: "binary MiB", raw: "64MiB", want: 64 << 20},
		{name: "case insensitive binary GiB", raw: "2gib", want: 2 << 30},
		{name: "decimal MB", raw: "10MB", want: 10_000_000},
		{name: "raw bytes", raw: "4096", want: 4096},
		{name: "negative rejected", raw: "-1MiB", wantErr: true},
		{name: "fraction rejected", raw: "1.5MiB", wantErr: true},
		{name: "overflow rejected", raw: "9223372036854775807GiB", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseByteSize(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseByteSize(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("parseByteSize(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseFlags_DefaultsAndSeglogOverrides(t *testing.T) {
	defaults, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.seglog.Partitions != seglog.DefaultPartitions ||
		defaults.seglog.WALSegmentBytes != seglog.DefaultWALSegmentBytes ||
		defaults.seglog.StreamSegmentBytes != seglog.DefaultStreamSegmentBytes ||
		defaults.seglog.MaterializeInterval != seglog.DefaultMaterializeInterval ||
		defaults.seglog.CheckpointInterval != seglog.DefaultCheckpointInterval {
		t.Errorf("seglog defaults = %+v", defaults.seglog)
	}

	cfg, err := parseFlags([]string{"-seglog-partitions=4", "-seglog-wal-segment-bytes=64MiB", "-seglog-stream-segment-bytes=32MiB", "-seglog-materialize-interval=50ms", "-seglog-checkpoint-interval=2s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.seglog.Partitions != 4 || cfg.seglog.WALSegmentBytes != 64<<20 ||
		cfg.seglog.StreamSegmentBytes != 32<<20 || cfg.seglog.MaterializeInterval != 50*time.Millisecond ||
		cfg.seglog.CheckpointInterval != 2*time.Second {
		t.Errorf("seglog overrides = %+v", cfg.seglog)
	}
}
