package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func retentionOptions(dir string) Options {
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = 5 * time.Millisecond
	opts.RetentionInterval = 5 * time.Millisecond
	opts.StreamSegmentBytes = 400
	opts.StreamSegmentAge = -1
	opts.SparseIndexBytes = 512
	return opts
}

func appendRetentionMessages(t *testing.T, s *Storage, id string, count int) {
	t.Helper()
	for i := range count {
		mustAppend(t, s, id, fmt.Sprintf("%03d-%s", i, strings.Repeat("x", 64)))
	}
}

func streamFloor(s *Storage, id string) int64 {
	st, ok := s.streams.Load(id)
	if !ok {
		return 0
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.floor
}

func streamSegmentPaths(t *testing.T, s *Storage, id string) []string {
	t.Helper()
	st, ok := s.streams.Load(id)
	if !ok {
		t.Fatalf("stream %q not found", id)
	}
	paths, err := filepath.Glob(filepath.Join(streamDir(s.dir, id, st.inc), "seg-*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestBytesRetention_TrimsWholeSegmentsAndPreservesTail(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, retentionOptions(dir))
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 30)
	waitFor(t, "sealed segments", func() bool { return len(streamSegmentPaths(t, s, "s")) >= 3 })
	before := len(streamSegmentPaths(t, s, "s"))
	if err := s.SetRetention(context.Background(), "s", Retention{MaxBytes: 400}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "bytes retention", func() bool {
		return streamFloor(s, "s") > 0 && len(streamSegmentPaths(t, s, "s")) < before
	})

	floor := streamFloor(s, "s")
	if _, err := s.Read(context.Background(), "s", "", 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("Read below floor: %v, want ErrGone", err)
	}
	res, err := s.Read(context.Background(), "s", storage.FormatSimpleOffset(floor), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) == 0 || res.Messages[0].Offset != storage.FormatSimpleOffset(floor+1) {
		t.Fatalf("retained read at floor %d: %+v", floor, res.Messages)
	}
	head, err := s.Head(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if head.NextOffset != storage.FormatSimpleOffset(30) {
		t.Fatalf("Head tail = %s, want %s", head.NextOffset, storage.FormatSimpleOffset(30))
	}
}

func TestAgeRetention_SealsIdleActiveAndTrimsOldPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	opts.StreamSegmentBytes = 512
	opts.StreamSegmentAge = 50 * time.Millisecond
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 20)
	if err := s.SetRetention(context.Background(), "s", Retention{MaxAge: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "age retention", func() bool { return streamFloor(s, "s") > 0 })
	floor := streamFloor(s, "s")
	if _, err := s.Read(context.Background(), "s", storage.FormatSimpleOffset(floor-1), 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("Read below age floor: %v, want ErrGone", err)
	}
	res, err := s.Read(context.Background(), "s", storage.FormatSimpleOffset(floor), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) == 0 {
		t.Fatal("age retention removed the newest record")
	}
}

func TestDefaultRetention_IsCopiedAtCreate(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	opts.DefaultRetention = Retention{MaxBytes: 400}
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 30)
	waitFor(t, "default retention", func() bool { return streamFloor(s, "s") > 0 })
}

func TestDefaultRetention_CreateWALPreservesPolicyAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	opts.DefaultRetention = Retention{MaxBytes: 1234, MaxAge: 5 * time.Minute}
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	opts.DefaultRetention = Retention{}
	r := openTest(t, opts)
	st, _ := r.streams.Load("s")
	if got := st.snapshot().retention; got != (Retention{MaxBytes: 1234, MaxAge: 5 * time.Minute}) {
		t.Fatalf("recovered retention = %+v", got)
	}
}

func TestRetentionFloor_SurvivesReopenAndAppendContinues(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 30)
	if err := s.SetRetention(context.Background(), "s", Retention{MaxBytes: 400}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retention floor", func() bool { return streamFloor(s, "s") > 0 })
	floor := streamFloor(s, "s")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	if _, err := r.Read(context.Background(), "s", storage.FormatSimpleOffset(floor-1), 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("Read below reopened floor: %v, want ErrGone", err)
	}
	st, _ := r.streams.Load("s")
	dir = streamDir(dir, "s", st.inc)
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(streamSegmentPaths(t, r, "s")); got != len(m.Sealed)+btoi(m.Active != nil) {
		t.Fatalf("segment files = %d, manifest references %d", got, len(m.Sealed)+btoi(m.Active != nil))
	}
	off := mustAppend(t, r, "s", "after-reopen")
	if off != storage.FormatSimpleOffset(31) {
		t.Fatalf("append offset = %s, want %s", off, storage.FormatSimpleOffset(31))
	}
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestWaitForData_BelowRetentionFloorReturnsGone(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, retentionOptions(dir))
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 20)
	if err := s.SetRetention(context.Background(), "s", Retention{MaxBytes: 300}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retention floor", func() bool { return streamFloor(s, "s") > 0 })
	if _, err := s.WaitForData(context.Background(), "s", storage.FormatSimpleOffset(streamFloor(s, "s")-1), 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("WaitForData below floor: %v, want ErrGone", err)
	}
}

func TestSetRetention_ValidatesLimitsAndStream(t *testing.T) {
	s := openTest(t, retentionOptions(t.TempDir()))
	if err := s.SetRetention(context.Background(), "missing", Retention{}); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("missing stream: %v, want ErrNotFound", err)
	}
	if err := s.SetRetention(context.Background(), "missing", Retention{MaxBytes: -1}); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("negative bytes: %v, want ErrBadRequest", err)
	}
	if err := s.SetRetention(context.Background(), "missing", Retention{MaxAge: -1}); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("negative age: %v, want ErrBadRequest", err)
	}
}

func TestRecovery_RemovesManifestOrphanSegment(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 20)
	if err := s.SetRetention(context.Background(), "s", Retention{MaxBytes: 300}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retention floor", func() bool { return streamFloor(s, "s") > 0 })
	st, _ := s.streams.Load("s")
	streamDir := streamDir(dir, "s", st.inc)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(streamDir, "seg-ffffffffffffffff.seg")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := openTest(t, opts)
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists after reopen: %v", err)
	}
	if _, err := r.Head(context.Background(), "s"); err != nil {
		t.Fatal(err)
	}
}

func TestTrimReplay_IsIdempotentWhenManifestRunsAhead(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "s", 20)
	st, _ := s.streams.Load("s")
	if err := s.materializeStream(s.parts[0], st); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetention(context.Background(), "s", Retention{MaxBytes: 300}); err != nil {
		t.Fatal(err)
	}
	s.retentionSweep(s.parts[0])
	floor := streamFloor(s, "s")
	if floor == 0 {
		t.Fatal("manual retention sweep did not trim")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := openTest(t, opts)
	if got := streamFloor(r, "s"); got != floor {
		t.Fatalf("replayed floor = %d, want %d", got, floor)
	}
	if _, err := r.Read(context.Background(), "s", storage.FormatSimpleOffset(floor-1), 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("Read below replayed floor: %v, want ErrGone", err)
	}
}

func TestMaterialization_CompletesReplayedTrimWhenSweepsDisabled(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	opts.WALSegmentBytes = 8192
	opts.StreamSegmentBytes = 1500
	s := openTest(t, opts)
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		mustAppend(t, s, "s", strings.Repeat("x", 1000))
	}
	st, _ := s.streams.Load("s")
	if err := s.materializeStream(s.parts[0], st); err != nil {
		t.Fatal(err)
	}
	snap := st.snapshot()
	if len(snap.sealed) < 2 {
		t.Fatalf("sealed segments = %d, want at least 2", len(snap.sealed))
	}
	floor := snap.sealed[0].lastIndex
	res := s.parts[0].submit(&request{op: opTrim, streamID: "s", floor: floor, done: make(chan result, 1)})
	if res.err != nil {
		t.Fatal(res.err)
	}
	// Deliberately skip the manifest rewrite and physical deletion, matching
	// a crash immediately after the trim frame commits.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	opts.MaterializeInterval = 5 * time.Millisecond
	r := openTest(t, opts)
	mustAppend(t, r, "s", "after-reopen")
	waitFor(t, "checkpoint advancement with retention sweeps disabled", func() bool {
		return len(walSegments(t, dir)) == 1
	})
	if _, err := r.Read(context.Background(), "s", storage.FormatSimpleOffset(floor-1), 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("Read below replayed floor: %v, want ErrGone", err)
	}
	m, err := loadManifest(streamDir(dir, "s", st.inc))
	if err != nil {
		t.Fatal(err)
	}
	if m.FloorIndex != floor || len(m.Sealed) == 0 || m.Sealed[0].FirstIndex != floor+1 {
		t.Fatalf("completed trim manifest = %+v", m)
	}
}
