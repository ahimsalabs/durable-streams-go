package seglog

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func openTest(t *testing.T, opts Options) *Storage {
	t.Helper()
	s, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func singlePartitionOptions(dir string) Options {
	return Options{
		Dir:             dir,
		Partitions:      1,
		MaxMessageSize:  2048,
		WALSegmentBytes: 1 << 20,
	}
}

func mustAppend(t *testing.T, s *Storage, id, data string) durablestream.Offset {
	t.Helper()
	off, err := s.Append(context.Background(), id, []byte(data), "")
	if err != nil {
		t.Fatalf("Append(%s, %q): %v", id, data, err)
	}
	return off
}

func readAll(t *testing.T, s *Storage, id string) []string {
	t.Helper()
	res, err := s.Read(context.Background(), id, "", 0)
	if err != nil {
		t.Fatalf("Read(%s): %v", id, err)
	}
	out := make([]string, len(res.Messages))
	for i, m := range res.Messages {
		out[i] = string(m.Data)
	}
	return out
}

func TestReopenPreservesState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := openTest(t, singlePartitionOptions(dir))
	cfg := durablestream.StreamConfig{ContentType: "text/plain", IsPrivate: true}
	if _, err := s.Create(ctx, "kept", cfg); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "kept", "one")
	mustAppend(t, s, "kept", "two")
	if _, err := s.CloseStream(ctx, "kept", [][]byte{[]byte("final")}, "seq-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "doomed", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "doomed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "seqd", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "seqd", []byte("m"), "s2"); err != nil {
		t.Fatal(err)
	}
	headBefore, err := s.Head(ctx, "kept")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, singlePartitionOptions(dir))
	head, err := r.Head(ctx, "kept")
	if err != nil {
		t.Fatalf("Head after reopen: %v", err)
	}
	if head.IncarnationID != headBefore.IncarnationID {
		t.Errorf("IncarnationID changed across reopen: %q != %q", head.IncarnationID, headBefore.IncarnationID)
	}
	if head.ContentType != "text/plain" || !head.IsPrivate || !head.Closed {
		t.Errorf("Head after reopen: %+v", head)
	}
	if head.NextOffset != headBefore.NextOffset {
		t.Errorf("tail changed across reopen: %q != %q", head.NextOffset, headBefore.NextOffset)
	}
	if got := readAll(t, r, "kept"); len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "final" {
		t.Errorf("messages after reopen: %q", got)
	}
	if _, err := r.Head(ctx, "doomed"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("deleted stream after reopen: %v, want ErrNotFound", err)
	}
	if _, err := r.CloseStream(ctx, "kept", nil, ""); err != nil {
		t.Errorf("idempotent close-only after reopen: %v", err)
	}
	// The dedup floor survives reopen: a stale sequence is rejected, the
	// next one is accepted.
	if _, err := r.Append(ctx, "seqd", []byte("m"), "s1"); !errors.Is(err, durablestream.ErrConflict) {
		t.Errorf("stale seq after reopen: %v, want ErrConflict", err)
	}
	if _, err := r.Append(ctx, "seqd", []byte("m"), "s3"); err != nil {
		t.Errorf("advancing seq after reopen: %v", err)
	}
}

func TestRecovery_OldCheckpointWithoutLastSeqOffsetLoadsWithUnknownOffset(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.CheckpointInterval = -1

	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "seq-checkpoint", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	acceptedOffset, err := s.Append(t.Context(), "seq-checkpoint", []byte("accepted"), "seq-0001")
	if err != nil {
		t.Fatal(err)
	}
	s.materializeRound(s.parts[0])
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	checkpointPath := filepath.Join(dir, "wal", "p0000", checkpointFileName)
	c, ok, err := loadCheckpoint(filepath.Dir(checkpointPath))
	if err != nil || !ok {
		t.Fatalf("load checkpoint = (%v, %v), want (true, nil)", ok, err)
	}
	entry := c.Streams["seq-checkpoint"]
	if entry.LastSeqOffset != acceptedOffset {
		t.Fatalf("checkpoint LastSeqOffset = %q, want %q", entry.LastSeqOffset, acceptedOffset)
	}
	entry.LastSeqOffset = ""
	c.Streams["seq-checkpoint"] = entry
	frames := scanFrames(t, walSegments(t, dir)[0])
	foundSeqFrame := false
	for _, frame := range frames {
		if frame.op == opAppend && frame.flags&flagHasSeq != 0 {
			// Exercise tolerant materialized-prefix replay: the checkpoint's
			// logical state already includes this frame, but recovery scans it
			// again. An old omitted offset must remain unknown.
			c.Replay.Offset = frame.start
			c.NextTxnID = frame.txnID
			foundSeqFrame = true
			break
		}
	}
	if !foundSeqFrame {
		t.Fatal("sequence append frame missing from WAL")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(checkpointPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	if got := mustHeadLastSeq(t, r, "seq-checkpoint"); got != "seq-0001" {
		t.Errorf("recovered LastSeq = %q, want %q", got, "seq-0001")
	}
	_, err = r.Append(t.Context(), "seq-checkpoint", []byte("duplicate"), "seq-0001")
	var conflict *durablestream.SequenceConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("duplicate error = %v, want *SequenceConflictError", err)
	}
	if !conflict.LastOffset.IsZero() {
		t.Errorf("old checkpoint conflict LastOffset = %q, want unknown", conflict.LastOffset)
	}

	nextOffset, err := r.Append(t.Context(), "seq-checkpoint", []byte("next"), "seq-0002")
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Append(t.Context(), "seq-checkpoint", []byte("duplicate"), "seq-0002")
	if !errors.As(err, &conflict) {
		t.Fatalf("new duplicate error = %v, want *SequenceConflictError", err)
	}
	if conflict.LastOffset != nextOffset {
		t.Errorf("new duplicate LastOffset = %q, want %q", conflict.LastOffset, nextOffset)
	}
}

func mustHeadLastSeq(t *testing.T, s *Storage, streamID string) string {
	t.Helper()
	info, err := s.Head(t.Context(), streamID)
	if err != nil {
		t.Fatal(err)
	}
	return info.LastSeq
}

// walSegments lists partition 0's segment paths in sequence order.
func walSegments(t *testing.T, dir string) []string {
	t.Helper()
	walDir := filepath.Join(dir, "wal", "p0000")
	seqs, err := listWALSegments(walDir)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(seqs))
	for i, seq := range seqs {
		paths[i] = walSegmentPath(walDir, seq)
	}
	return paths
}

// scanFrames decodes every valid frame of a segment file.
func scanFrames(t *testing.T, path string) []walFrame {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	scanner := newFrameScanner(f, info.Size())
	var frames []walFrame
	for {
		frame, err := scanner.next()
		if err != nil {
			return frames
		}
		frames = append(frames, frame)
	}
}

func TestRecoveryTruncatesTornFinalFrame(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := openTest(t, singlePartitionOptions(dir))
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "s", "kept")
	mustAppend(t, s, "s", "lost")
	stopWithoutCheckpoint(t, s)

	paths := walSegments(t, dir)
	if len(paths) != 1 {
		t.Fatalf("expected one WAL segment, found %d", len(paths))
	}
	frames := scanFrames(t, paths[0])
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames (create, append, append), found %d", len(frames))
	}
	last := frames[2]
	f, err := os.OpenFile(paths[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xde, 0xad}, last.start+frameHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, singlePartitionOptions(dir))
	if got := readAll(t, r, "s"); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("messages after torn-tail recovery: %q, want [kept]", got)
	}
	// The stream keeps working, and the truncated tail's index is reusable.
	mustAppend(t, r, "s", "after")
	if got := readAll(t, r, "s"); len(got) != 2 || got[1] != "after" {
		t.Fatalf("messages after post-recovery append: %q", got)
	}
}

func TestRecoveryFailsOnEarlierSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	opts := singlePartitionOptions(dir)
	opts.WALSegmentBytes = 8192 // 4096-byte capacity: a few appends roll segments
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 2000)
	for i := range 4 {
		if _, err := s.Append(ctx, "s", payload, ""); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	stopWithoutCheckpoint(t, s)

	paths := walSegments(t, dir)
	if len(paths) < 2 {
		t.Fatalf("expected multiple WAL segments, found %d", len(paths))
	}
	frames := scanFrames(t, paths[0])
	if len(frames) == 0 {
		t.Fatal("no frames in first segment")
	}
	f, err := os.OpenFile(paths[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, frames[0].start+frameHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := New(opts); !errors.Is(err, errCorrupt) {
		t.Fatalf("New on corrupt non-final segment: %v, want errCorrupt", err)
	}
}

func TestRecoveryReinitializesPartialFinalSegment(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := openTest(t, singlePartitionOptions(dir))
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "s", "one")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash during roll: the next segment exists with a partial
	// header and nothing else.
	walDir := filepath.Join(dir, "wal", "p0000")
	partial := walSegmentPath(walDir, 2)
	if err := os.WriteFile(partial, []byte("partial-header"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, singlePartitionOptions(dir))
	if got := readAll(t, r, "s"); len(got) != 1 || got[0] != "one" {
		t.Fatalf("messages after partial-segment recovery: %q", got)
	}
	mustAppend(t, r, "s", "two")
	if got := readAll(t, r, "s"); len(got) != 2 {
		t.Fatalf("messages after append: %q", got)
	}
}

func TestPartitionCountIsPersisted(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Partitions: 2, WALSegmentBytes: 1 << 20, MaxMessageSize: 1024}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Partitions = 3
	if _, err := New(opts); err == nil {
		t.Fatal("New with a different partition count should fail")
	}
}

func TestFormatV1IsRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte("seglog-format-v1\npartitions=1\nhash=xxh64\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(singlePartitionOptions(dir)); err == nil || !strings.Contains(err.Error(), "unsupported format") || !strings.Contains(err.Error(), "seglog-format-v1") {
		t.Fatalf("New v1 error = %v, want clear unsupported-format error", err)
	}
}

func TestRecoveryWithoutCheckpointReplaysCreateAndAppend(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "before-checkpoint", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, "before-checkpoint", "durable-in-wal")
	if _, err := os.Stat(filepath.Join(dir, "wal", "p0000", checkpointFileName)); !os.IsNotExist(err) {
		t.Fatalf("checkpoint exists before materialization: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	if got := readAll(t, r, "before-checkpoint"); len(got) != 1 || got[0] != "durable-in-wal" {
		t.Fatalf("whole-WAL recovery = %q, want [durable-in-wal]", got)
	}
}

func TestSecondOpenIsFenced(t *testing.T) {
	dir := t.TempDir()
	openTest(t, singlePartitionOptions(dir))
	if _, err := New(singlePartitionOptions(dir)); err == nil {
		t.Fatal("second New on a locked directory should fail")
	}
}

func TestEphemeralDirRemovedOnClose(t *testing.T) {
	s, err := New(Options{Partitions: 1, WALSegmentBytes: 1 << 20, MaxMessageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	dir := s.dir
	if _, err := s.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("ephemeral dir still exists: %v", err)
	}
}

func TestTouchDurableAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := openTest(t, singlePartitionOptions(dir))
	if _, err := s.Create(ctx, "s", durablestream.StreamConfig{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Head(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s.Touch(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	after, err := s.Head(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if !after.ExpiresAt.After(before.ExpiresAt) {
		t.Fatalf("Touch did not move expiry: %v -> %v", before.ExpiresAt, after.ExpiresAt)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, singlePartitionOptions(dir))
	reopened, err := r.Head(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.ExpiresAt.Equal(after.ExpiresAt) {
		t.Fatalf("Touch not durable: %v != %v", reopened.ExpiresAt, after.ExpiresAt)
	}
}

func TestTouchHead_ReturnsPreRenewalInfoAndDurablyRenews(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	s := openTest(t, opts)

	if _, err := s.Create(t.Context(), "sliding", durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         time.Hour,
		IsPrivate:   true,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Head(t.Context(), "sliding")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	got, err := s.TouchHead(t.Context(), "sliding")
	if err != nil {
		t.Fatal(err)
	}
	if !got.ExpiresAt.Equal(before.ExpiresAt) || got.ContentType != before.ContentType ||
		got.NextOffset != before.NextOffset || got.TTL != before.TTL || got.IsPrivate != before.IsPrivate ||
		got.Closed != before.Closed || got.IncarnationID != before.IncarnationID {
		t.Fatalf("TouchHead info = %+v, want pre-renewal %+v", got, before)
	}
	after, err := s.Head(t.Context(), "sliding")
	if err != nil {
		t.Fatal(err)
	}
	if !after.ExpiresAt.After(got.ExpiresAt) {
		t.Fatalf("TouchHead did not advance expiry: %v -> %v", got.ExpiresAt, after.ExpiresAt)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r := openTest(t, opts)
	reopened, err := r.Head(t.Context(), "sliding")
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.ExpiresAt.Equal(after.ExpiresAt) {
		t.Fatalf("TouchHead renewal not durable: %v != %v", reopened.ExpiresAt, after.ExpiresAt)
	}
}

func TestTouchHead_NoTTLMatchesHeadWithoutWritingFrame(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "static", durablestream.StreamConfig{ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	paths := walSegments(t, dir)
	beforeFrames := len(scanFrames(t, paths[0]))
	want, err := s.Head(t.Context(), "static")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.TouchHead(t.Context(), "static")
	if err != nil {
		t.Fatal(err)
	}
	if *got != *want {
		t.Fatalf("TouchHead info = %+v, want Head %+v", got, want)
	}
	afterFrames := len(scanFrames(t, paths[0]))
	if afterFrames != beforeFrames {
		t.Fatalf("TTL-less TouchHead wrote a WAL frame: before %d, after %d", beforeFrames, afterFrames)
	}
}

func TestTouchHead_ErrorsMatchHead(t *testing.T) {
	s := openTest(t, singlePartitionOptions(t.TempDir()))
	if _, err := s.TouchHead(t.Context(), "missing"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("missing stream error = %v, want ErrNotFound", err)
	}
	if _, err := s.Create(t.Context(), "expired", durablestream.StreamConfig{ExpiresAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TouchHead(t.Context(), "expired"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("expired stream error = %v, want ErrNotFound", err)
	}
}
