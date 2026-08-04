package main

import (
	"errors"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestReadModeForLive(t *testing.T) {
	tests := []struct {
		name     string
		live     any
		wantMode durablestream.ReadMode
		wantLive bool
	}{
		{name: "omitted", live: nil, wantMode: durablestream.ReadModeAuto, wantLive: false},
		{name: "false", live: false, wantMode: durablestream.ReadModeAuto, wantLive: false},
		{name: "true", live: true, wantMode: durablestream.ReadModeAuto, wantLive: true},
		{name: "long poll", live: "long-poll", wantMode: durablestream.ReadModeLongPoll, wantLive: true},
		{name: "SSE", live: "sse", wantMode: durablestream.ReadModeSSE, wantLive: true},
		{name: "unknown string", live: "unknown", wantMode: durablestream.ReadModeAuto, wantLive: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMode, gotLive := readModeForLive(tt.live)
			if gotMode != tt.wantMode || gotLive != tt.wantLive {
				t.Fatalf("readModeForLive(%v) = (%v, %v), want (%v, %v)",
					tt.live, gotMode, gotLive, tt.wantMode, tt.wantLive)
			}
		})
	}
}

func TestAppendErrorResultSequenceConflict(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		hasSequence bool
		wantCode    string
	}{
		{
			name:     "sequence conflict sentinel",
			err:      errors.Join(errors.New("append failed"), durablestream.ErrSequenceConflict),
			wantCode: "SEQUENCE_CONFLICT",
		},
		{
			name:        "legacy conflict with sequence",
			err:         errors.Join(errors.New("append failed"), durablestream.ErrConflict),
			hasSequence: true,
			wantCode:    "SEQUENCE_CONFLICT",
		},
		{
			name:     "plain conflict",
			err:      errors.Join(errors.New("append failed"), durablestream.ErrConflict),
			wantCode: "CONFLICT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appendErrorResult(tt.err, tt.hasSequence)
			if got.Status != 409 || got.ErrorCode != tt.wantCode {
				t.Fatalf("conflict = status %d, code %q; want 409, %s",
					got.Status, got.ErrorCode, tt.wantCode)
			}
		})
	}
}

func TestErrorResultSequenceConflict(t *testing.T) {
	err := errors.Join(errors.New("append failed"), durablestream.ErrSequenceConflict)

	got := errorResult("append", err)
	if got.Status != 409 || got.ErrorCode != "SEQUENCE_CONFLICT" {
		t.Fatalf("sequence conflict = status %d, code %q; want 409, SEQUENCE_CONFLICT",
			got.Status, got.ErrorCode)
	}
}
