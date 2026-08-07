package seglog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// stopWithoutCheckpoint models process death while still closing descriptors
// so the test can safely mutate and copy the fixture.
func stopWithoutCheckpoint(t *testing.T, s *Storage) {
	t.Helper()
	for _, p := range s.parts {
		p.closeAdmission()
	}
	close(s.shutdownCh)
	s.workers.Wait()
	if err := s.releaseResources(); err != nil {
		t.Fatalf("release crash fixture: %v", err)
	}
	// Prevent a later cleanup from attempting the normal close path.
	s.closeOnce = sync.Once{}
	s.closeOnce.Do(func() {})
}

func crashOptions(dir string) Options {
	opts := singlePartitionOptions(dir)
	opts.MaterializeInterval = -1
	opts.RetentionInterval = -1
	return opts
}

func copyFixture(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "store")
	if err := filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(out, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		info, err := entry.Info()
		if err != nil {
			return err
		}
		file, err := os.OpenFile(out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(file, in)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}); err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
	return dst
}

func overwriteWithZeros(t *testing.T, path string, start, end int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(make([]byte, end-start), start); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertCrashPrefix(t *testing.T, dir string) {
	t.Helper()
	s, err := New(crashOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if got := readAll(t, s, "kept"); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("recovered messages = %q, want [one two]", got)
	}
	if _, err := s.Head(t.Context(), "doomed"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("later create survived: %v", err)
	}
	if off, err := s.Append(t.Context(), "kept", []byte("after"), "seq-3"); err != nil {
		t.Fatalf("fresh append: %v", err)
	} else if off != storage.FormatSimpleOffset(3) {
		t.Errorf("fresh append offset = %s, want 3", off)
	}
}

// Every observable suffix-zero tear in the bounded final frame must recover
// the same committed prefix. Additional cuts cover frame boundaries in the
// final commit-group suffix; sequential submissions make each group deterministic.
func TestCrashRecovery_TornTailAndFrameCorruptionPreservePrefix(t *testing.T) {
	fixture := t.TempDir()
	s, err := New(crashOptions(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), "kept", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "kept", []byte("one"), "seq-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "kept", []byte("two"), "seq-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), "doomed", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "doomed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CloseStream(t.Context(), "kept", [][]byte{[]byte("lost")}, "seq-3"); err != nil {
		t.Fatal(err)
	}
	stopWithoutCheckpoint(t, s)
	path := walSegments(t, fixture)[0]
	frames := scanFrames(t, path)
	if len(frames) != 6 {
		t.Fatalf("frames = %d, want 6", len(frames))
	}
	final := frames[len(frames)-1]
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cut := final.start; cut < final.end; cut++ {
		// An already-zero suffix is indistinguishable on disk from this
		// tear. Exercise every observable byte-boundary cut.
		if allZero(pristine[cut:final.end]) {
			continue
		}
		t.Run(fmt.Sprintf("suffix-zero-%d", cut), func(t *testing.T) {
			dir := copyFixture(t, fixture)
			mutant := walSegments(t, dir)[0]
			overwriteWithZeros(t, mutant, cut, final.end)
			assertCrashPrefix(t, dir)
		})
	}

	flips := []struct {
		name string
		off  int64
	}{
		{"header CRC", final.start + 4},
		{"payload", final.payloads[0].off},
		{"trailer", final.end - frameTrailerSize},
	}
	for _, tc := range flips {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyFixture(t, fixture)
			mutant := walSegments(t, dir)[0]
			f, err := os.OpenFile(mutant, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			var b [1]byte
			if _, err := f.ReadAt(b[:], tc.off); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 0x01
			if _, err := f.WriteAt(b[:], tc.off); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			assertCrashPrefix(t, dir)
		})
	}
}

func TestCrashOrdering_CommitMaterializeCheckpointThenReclaim(t *testing.T) {
	steps := []string{"WAL commit", "copy", "batch sync", "checkpoint", "publish", "reclaim"}
	for prefix := 1; prefix <= len(steps); prefix++ {
		t.Run("after "+steps[prefix-1], func(t *testing.T) {
			dir := t.TempDir()
			opts := crashOptions(dir)
			opts.WALSegmentBytes = 8192
			s, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
				t.Fatal(err)
			}
			for i := range 6 {
				mustAppend(t, s, "s", fmt.Sprintf("%d-%s", i, string(make([]byte, 1200))))
			}
			p := s.parts[0]
			barrier := p.submit(&request{op: opBarrier, captureDirty: true, done: make(chan result, 1)})
			prepared := make(map[*streamState]*preparedStream)
			if prefix >= 2 {
				for st, snap := range barrier.dirtySnapshots {
					draft, err := s.materializeStream(p, st, snap)
					if err != nil {
						t.Fatal(err)
					}
					prepared[st] = draft
				}
			}
			if prefix >= 3 {
				for _, draft := range prepared {
					syncDraftTouched(t, s, draft)
				}
			}
			if prefix >= 4 {
				if err := s.advanceCheckpoint(p, barrier, s.checkpointEntries(p.materializedEntries, prepared, barrier.removals)); err != nil {
					t.Fatal(err)
				}
			}
			if prefix >= 5 {
				for _, draft := range prepared {
					s.publishPrepared(draft)
				}
			}
			if prefix >= 6 {
				if err := p.wal.removeBefore(barrier.walSeq); err != nil {
					t.Fatal(err)
				}
			}
			s.releasePrepared(prepared)
			stopWithoutCheckpoint(t, s)
			r, err := New(opts)
			if err != nil {
				t.Fatalf("reopen after step prefix: %v", err)
			}
			t.Cleanup(func() { _ = r.Close() })
			if got := readAll(t, r, "s"); len(got) != 6 {
				t.Fatalf("messages after %s = %d, want 6", steps[prefix-1], len(got))
			}
		})
	}
}

func TestCrashOrdering_SealThenCheckpoint(t *testing.T) {
	for prefix, name := range []string{"batch sync", "checkpoint"} {
		t.Run("after "+name, func(t *testing.T) {
			dir := t.TempDir()
			opts := crashOptions(dir)
			s, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
				t.Fatal(err)
			}
			for i := range 4 {
				mustAppend(t, s, "s", fmt.Sprintf("message-%d", i))
			}
			st, _ := s.streams.Load("s")
			s.materializeRound(s.parts[0])
			st.forceSeal = true
			s.parts[0].markDirty(st)
			barrier := s.parts[0].submit(&request{op: opBarrier, captureDirty: true, done: make(chan result, 1)})
			draft, err := s.materializeStream(s.parts[0], st, barrier.dirtySnapshots[st])
			if err != nil {
				t.Fatal(err)
			}
			syncDraftTouched(t, s, draft)
			if prefix >= 1 {
				prepared := map[*streamState]*preparedStream{st: draft}
				if err := s.advanceCheckpoint(s.parts[0], barrier, s.checkpointEntries(s.parts[0].materializedEntries, prepared, nil)); err != nil {
					t.Fatal(err)
				}
			}
			s.releasePrepared(map[*streamState]*preparedStream{st: draft})
			stopWithoutCheckpoint(t, s)
			r, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			if got := readAll(t, r, "s"); len(got) != 4 {
				t.Fatalf("messages after %s = %d, want 4", name, len(got))
			}
		})
	}
}

func TestCrashOrdering_TrimFrameCheckpointThenUnlink(t *testing.T) {
	steps := []string{"trim frame", "checkpoint", "unlink"}
	for prefix := 1; prefix <= len(steps); prefix++ {
		t.Run("after "+steps[prefix-1], func(t *testing.T) {
			dir := t.TempDir()
			opts := crashOptions(dir)
			opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 300}
			s, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
				t.Fatal(err)
			}
			for i := range 12 {
				mustAppend(t, s, "s", fmt.Sprintf("%02d-%s", i, string(make([]byte, 180))))
			}
			st, _ := s.streams.Load("s")
			s.materializeRound(s.parts[0])
			snap := st.snapshot()
			if len(snap.sealed) < 2 {
				t.Fatalf("sealed segments = %d, want >= 2", len(snap.sealed))
			}
			streamPath := streamDir(dir, "s", st.inc)
			victim := filepath.Join(streamPath, snap.sealed[0].name)
			floor := snap.sealed[0].lastIndex
			res := s.parts[0].submit(&request{op: opTrim, streamID: "s", floor: floor, done: make(chan result, 1)})
			if res.err != nil {
				t.Fatal(res.err)
			}
			trimmedSnap := st.snapshot()
			if prefix >= 2 {
				retained := append([]*segmentFile(nil), trimmedSnap.sealed[1:]...)
				entries := s.checkpointEntries(s.parts[0].materializedEntries, nil, nil)
				entries["s"] = buildCheckpointEntry(trimmedSnap, retained, st.activeSeg, trimmedSnap.through)
				barrier := s.parts[0].submit(&request{op: opBarrier, done: make(chan result, 1)})
				if err := s.advanceCheckpoint(s.parts[0], barrier, entries); err != nil {
					t.Fatal(err)
				}
			}
			if prefix >= 3 {
				if err := os.Remove(victim); err != nil {
					t.Fatal(err)
				}
				if err := syncDir(streamPath); err != nil {
					t.Fatal(err)
				}
			}
			stopWithoutCheckpoint(t, s)
			r, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			if got := streamFloor(r, "s"); got != floor {
				t.Fatalf("replayed floor = %d, want %d", got, floor)
			}
			if _, err := r.Read(t.Context(), "s", storage.FormatSimpleOffset(floor-1), 0); !errors.Is(err, durablestream.ErrGone) {
				t.Fatalf("read below durable floor: %v, want ErrGone", err)
			}
			if prefix >= 2 {
				if _, err := os.Stat(victim); !os.IsNotExist(err) {
					t.Fatalf("checkpoint-excluded segment survived recovery: %v", err)
				}
			} else if _, err := os.Stat(victim); err != nil {
				t.Fatalf("segment unlinked before checkpoint: %v", err)
			}
		})
	}
}

func TestPartitionFailStop_LatchesWriteFailureAndIsolatesPartition(t *testing.T) {
	dir := t.TempDir()
	opts := crashOptions(dir)
	opts.Partitions = 2
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 2)
	for i := 0; ids[0] == "" || ids[1] == ""; i++ {
		id := fmt.Sprintf("p-%d", i)
		ids[s.partitionFor(id).id] = id
	}
	for _, id := range ids {
		if _, err := s.Create(t.Context(), id, durablestream.StreamConfig{}); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, s, id, "before")
	}
	injected := errors.New("test disk failure")
	s.partitionFor(ids[0]).wal.failNextWrite(injected)
	_, firstErr := s.Append(t.Context(), ids[0], []byte("fails"), "")
	if !errors.Is(firstErr, injected) {
		t.Fatalf("trigger error = %v, want injected failure", firstErr)
	}
	_, laterErr := s.Append(t.Context(), ids[0], []byte("also fails"), "")
	if !errors.Is(laterErr, injected) {
		t.Fatalf("latched error = %v, want same class", laterErr)
	}
	if _, err := s.Append(t.Context(), ids[1], []byte("works"), ""); err != nil {
		t.Fatalf("other partition append: %v", err)
	}
	if got := readAll(t, s, ids[0]); !reflect.DeepEqual(got, []string{"before"}) {
		t.Fatalf("failed partition read = %q", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after fail-stop: %v", err)
	}
}
