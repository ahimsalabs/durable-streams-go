package seglog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// waitFor polls until cond is true or the deadline lapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func materializedThrough(s *Storage, id string) int64 {
	st, ok := s.streams.Load(id)
	if !ok {
		return -1
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.materializedThrough
}

func TestMaterializationServesReadsFromSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = 5 * time.Millisecond

	s := openTest(t, opts)
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := range 20 {
		msg := fmt.Sprintf("msg-%02d", i)
		want = append(want, msg)
		mustAppend(t, s, "s", msg)
	}
	waitFor(t, "materialization", func() bool { return materializedThrough(s, "s") == 20 })

	// The WAL tail must be pruned; reads now come from segments.
	st, _ := s.streams.Load("s")
	st.mu.RLock()
	walLen := len(st.walTail)
	st.mu.RUnlock()
	if walLen != 0 {
		t.Fatalf("walTail still has %d entries after materialization", walLen)
	}
	got := readAll(t, s, "s")
	if len(got) != len(want) {
		t.Fatalf("read %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q", i, got[i], want[i])
		}
	}

	// Byte-budget reads must behave identically from segments.
	res, err := s.Read(ctx, "s", "", len("msg-00")*3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 3 {
		t.Fatalf("limited read returned %d messages, want 3", len(res.Messages))
	}
}

func TestSealingRollsSegmentsAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = 5 * time.Millisecond
	opts.StreamSegmentBytes = 1024 // a few records per segment
	opts.SparseIndexBytes = 512

	s := openTest(t, opts)
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := range 40 {
		msg := fmt.Sprintf("payload-%03d-%s", i, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
		want = append(want, msg)
		mustAppend(t, s, "s", msg)
	}
	waitFor(t, "materialization", func() bool { return materializedThrough(s, "s") == 40 })

	st, _ := s.streams.Load("s")
	st.mu.RLock()
	sealedCount := len(st.sealed)
	st.mu.RUnlock()
	if sealedCount == 0 {
		t.Fatal("expected sealed segments after exceeding StreamSegmentBytes")
	}

	got := readAll(t, s, "s")
	if len(got) != len(want) {
		t.Fatalf("read %d messages, want %d", len(got), len(want))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	got = readAll(t, r, "s")
	if len(got) != len(want) {
		t.Fatalf("after reopen: read %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("after reopen: message %d = %q, want %q", i, got[i], want[i])
		}
	}
	// Appends continue at the correct index.
	off := mustAppend(t, r, "s", "after-reopen")
	if got := readAll(t, r, "s"); got[len(got)-1] != "after-reopen" || len(got) != 41 {
		t.Fatalf("append after reopen: %d messages, tail %q (offset %s)", len(got), got[len(got)-1], off)
	}
}

func TestWALReclaim(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = 5 * time.Millisecond
	opts.WALSegmentBytes = 8192 // force frequent rolls

	s := openTest(t, opts)
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 1500)
	for i := range 20 {
		if _, err := s.Append(ctx, "s", payload, ""); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Old WAL segments must eventually be reclaimed down to the active one.
	waitFor(t, "WAL reclaim", func() bool {
		paths := walSegments(t, dir)
		return len(paths) == 1
	})

	// Everything still reads correctly, from segments.
	res, err := s.Read(ctx, "s", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 20 {
		t.Fatalf("read %d messages, want 20", len(res.Messages))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen replays only the un-reclaimed suffix.
	r := openTest(t, opts)
	if got := readAll(t, r, "s"); len(got) != 20 {
		t.Fatalf("after reopen: read %d messages, want 20", len(got))
	}
}

// TestManifestAheadOfCheckpointReplay simulates a crash after manifests were
// flushed but before the checkpoint advanced: replay must apply frames
// idempotently over the manifest state without duplicating messages.
func TestManifestAheadOfCheckpointReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1 // manual materialization only

	s := openTest(t, opts)
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "s", "one")
	mustAppend(t, s, "s", "two")
	if _, err := s.Append(ctx, "s", []byte("three"), "seq-3"); err != nil {
		t.Fatal(err)
	}

	// Whitebox: flush segments and the manifest but never the checkpoint.
	st, _ := s.streams.Load("s")
	if err := s.materializeStream(s.parts[0], st); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	got := readAll(t, r, "s")
	if len(got) != 3 || got[0] != "one" || got[2] != "three" {
		t.Fatalf("after replay over manifest: %q", got)
	}
	// Metadata replay stayed idempotent too: seq floor intact.
	if _, err := r.Append(ctx, "s", []byte("x"), "seq-2"); err == nil {
		t.Fatal("stale seq accepted after manifest-ahead replay")
	}
	head, err := r.Head(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentType != "text/plain" {
		t.Fatalf("content type lost: %+v", head)
	}
}

// TestDeleteAfterMaterializationRemovesDir verifies a materialized stream's
// directory disappears after deletion.
func TestDeleteAfterMaterializationRemovesDir(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = 5 * time.Millisecond

	s := openTest(t, opts)
	if _, err := s.Create(ctx, "victim", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "victim", "data")
	waitFor(t, "materialization", func() bool { return materializedThrough(s, "victim") == 1 })

	st, _ := s.streams.Load("victim")
	sd := streamDir(dir, "victim", st.inc)
	if err := s.Delete(ctx, "victim"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "stream dir removal", func() bool {
		_, err := os.Stat(sd)
		return err != nil
	})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := openTest(t, opts)
	if _, err := r.Head(ctx, "victim"); err == nil {
		t.Fatal("deleted stream resurrected after reopen")
	}
}
