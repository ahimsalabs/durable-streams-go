package seglog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

type recordingWriterAt struct {
	data   []byte
	writes int
	short  bool
}

func (w *recordingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	w.writes++
	if w.short && len(p) > 0 {
		return len(p) - 1, nil
	}
	end := int(off) + len(p)
	if len(w.data) < end {
		w.data = append(w.data, make([]byte, end-len(w.data))...)
	}
	copy(w.data[off:], p)
	return len(p), nil
}

func TestSegmentWriteBuffer_CoalescesPayloadAndDenseIndex(t *testing.T) {
	var b segmentWriteBuffer
	b.reset()
	sf := &segmentFile{firstIndex: 1, lastIndex: 0, payloadEnd: segmentHeaderSize}
	payloads := [][]byte{[]byte("a"), []byte("bc"), []byte("def")}
	for i, payload := range payloads {
		rec := segmentRecord{index: int64(i + 1), batchFirst: 1, ts: int64(100 + i)}
		if err := b.append(sf, rec, payload); err != nil {
			t.Fatal(err)
		}
	}
	payloadFile, indexFile := &recordingWriterAt{}, &recordingWriterAt{}
	if err := b.flush(payloadFile, indexFile); err != nil {
		t.Fatal(err)
	}
	if payloadFile.writes != 1 || indexFile.writes != 1 {
		t.Fatalf("payload/index writes = %d/%d, want 1/1", payloadFile.writes, indexFile.writes)
	}
	if got := payloadFile.data[segmentHeaderSize:]; !bytes.Equal(got, []byte("abcdef")) {
		t.Fatalf("payload = %q, want abcdef", got)
	}
	wantEnds := []uint64{segmentHeaderSize + 1, segmentHeaderSize + 3, segmentHeaderSize + 6}
	for i, payload := range payloads {
		entry := indexFile.data[i*denseEntrySize : (i+1)*denseEntrySize]
		if got := binary.LittleEndian.Uint64(entry[:8]); got != wantEnds[i] {
			t.Errorf("entry %d end = %d, want %d", i, got, wantEnds[i])
		}
		if got := binary.LittleEndian.Uint32(entry[8:12]); got != crc32.Checksum(payload, crcTable) {
			t.Errorf("entry %d CRC = %08x", i, got)
		}
		if got := binary.LittleEndian.Uint32(entry[12:16]); got != uint32(i) {
			t.Errorf("entry %d batch delta = %d, want %d", i, got, i)
		}
	}
	if sf.payloadEnd != segmentHeaderSize+6 || sf.count != 3 || sf.lastIndex != 3 || sf.minTS != 100 || sf.maxTS != 102 {
		t.Errorf("segment geometry after flush = %+v", sf)
	}
}

func TestSegmentWriteBuffer_RejectsShortWrites(t *testing.T) {
	newBuffer := func(t *testing.T) segmentWriteBuffer {
		t.Helper()
		var b segmentWriteBuffer
		b.reset()
		sf := &segmentFile{firstIndex: 1, lastIndex: 0, payloadEnd: segmentHeaderSize}
		if err := b.append(sf, segmentRecord{index: 1, batchFirst: 1}, []byte("payload")); err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Run("payload", func(t *testing.T) {
		b := newBuffer(t)
		err := b.flush(&recordingWriterAt{short: true}, &recordingWriterAt{})
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("flush error = %v, want io.ErrShortWrite", err)
		}
	})
	t.Run("index", func(t *testing.T) {
		b := newBuffer(t)
		err := b.flush(&recordingWriterAt{}, &recordingWriterAt{short: true})
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("flush error = %v, want io.ErrShortWrite", err)
		}
	})
}

func TestSegmentWriteBuffer_RetryOverwritesUnpublishedSuffix(t *testing.T) {
	published := segmentFile{firstIndex: 1, lastIndex: 1, payloadEnd: segmentHeaderSize + 1, count: 1}
	payloadFile := &recordingWriterAt{data: append(make([]byte, segmentHeaderSize), 'a')}
	indexFile := &recordingWriterAt{data: make([]byte, denseEntrySize), short: true}
	stage := func(t *testing.T, sf *segmentFile) segmentWriteBuffer {
		t.Helper()
		var b segmentWriteBuffer
		b.reset()
		for i, payload := range [][]byte{[]byte("bc"), []byte("def")} {
			if err := b.append(sf, segmentRecord{index: int64(i + 2), batchFirst: 2}, payload); err != nil {
				t.Fatal(err)
			}
		}
		return b
	}
	failedDraft := published
	b := stage(t, &failedDraft)
	if err := b.flush(payloadFile, indexFile); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("first flush error = %v, want io.ErrShortWrite", err)
	}
	if published.payloadEnd != segmentHeaderSize+1 || published.count != 1 {
		t.Fatalf("published geometry changed after failed draft: %+v", published)
	}

	indexFile.short = false
	retryDraft := published
	b = stage(t, &retryDraft)
	if err := b.flush(payloadFile, indexFile); err != nil {
		t.Fatal(err)
	}
	if got := payloadFile.data[segmentHeaderSize:retryDraft.payloadEnd]; !bytes.Equal(got, []byte("abcdef")) {
		t.Fatalf("payload after retry = %q, want abcdef", got)
	}
	if retryDraft.count != 3 || retryDraft.lastIndex != 3 {
		t.Fatalf("retry geometry = %+v", retryDraft)
	}
}

func TestMaterializer_CoalescesAcrossFlushesAndRollovers(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.MaxMessageSize = 3 << 20
	opts.WALSegmentBytes = 8 << 20
	opts.WALExtentBytes = 4 << 20
	opts.MaterializeMaxAge = -1
	opts.RetentionInterval = -1
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 2 << 20}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{
		bytes.Repeat([]byte{'a'}, 600<<10),
		bytes.Repeat([]byte{'b'}, 424<<10),
		bytes.Repeat([]byte{'c'}, 600<<10),
		bytes.Repeat([]byte{'d'}, 500<<10),
		bytes.Repeat([]byte{'e'}, (2<<20)+1),
		[]byte("tail"),
	}
	if _, err := s.AppendBatch(t.Context(), "s", payloads, ""); err != nil {
		t.Fatal(err)
	}
	s.materializeRound(s.parts[0])
	st, _ := s.streams.Load("s")
	snap := st.snapshot()
	if len(snap.sealed) != 2 || snap.activeView.count != 1 {
		t.Fatalf("sealed segments = %d, active records = %d; want 2 and 1", len(snap.sealed), snap.activeView.count)
	}
	assertSegment(t, snap.sealed[0], 1, 4, bytes.Join(payloads[:4], nil))
	assertSegment(t, snap.sealed[1], 5, 5, payloads[4])
	assertSegment(t, st.activeSeg, 6, 6, payloads[5])
	for segmentNumber, sf := range append(append([]*segmentFile(nil), snap.sealed...), st.activeSeg) {
		var index []byte
		var err error
		if sf.sealed {
			raw, readErr := os.ReadFile(sf.path)
			err = readErr
			index = raw[sf.payloadEnd : sf.payloadEnd+sf.count*denseEntrySize]
		} else {
			index, err = os.ReadFile(sf.indexPath)
		}
		if err != nil {
			t.Fatal(err)
		}
		for i := range sf.count {
			entry := index[i*denseEntrySize : (i+1)*denseEntrySize]
			logicalIndex := sf.firstIndex + i
			if got, want := binary.LittleEndian.Uint32(entry[12:16]), uint32(logicalIndex-1); got != want {
				t.Errorf("segment %d record %d batch delta = %d, want %d", segmentNumber, logicalIndex, got, want)
			}
		}
	}
	want := make([]string, len(payloads))
	for i := range payloads {
		want[i] = string(payloads[i])
	}
	if got := readAll(t, s, "s"); !equalStrings(got, want) {
		t.Fatalf("read before reopen returned %d records, want %d", len(got), len(want))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := openTest(t, opts)
	if got := readAll(t, r, "s"); !equalStrings(got, want) {
		t.Fatalf("read after reopen returned %d records, want %d", len(got), len(want))
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func assertSegment(t *testing.T, sf *segmentFile, first, last int64, payload []byte) {
	t.Helper()
	if sf.firstIndex != first || sf.lastIndex != last {
		t.Errorf("segment indices = [%d,%d], want [%d,%d]", sf.firstIndex, sf.lastIndex, first, last)
	}
	raw, err := os.ReadFile(sf.path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if got := raw[segmentHeaderSize:sf.payloadEnd]; !bytes.Equal(got, payload) {
		t.Errorf("segment payload = %q, want %q", got, payload)
	}
}

func appendAndMaterialize(t *testing.T, s *Storage, id string, payload []byte) {
	t.Helper()
	if _, err := s.Append(t.Context(), id, payload, ""); err != nil {
		t.Fatalf("Append(%q): %v", payload, err)
	}
	s.materializeRound(s.parts[0])
}

func TestSegmentPolicy_TargetGeometrySealsOnlyBeforeFollowingRecord(t *testing.T) {
	tests := []struct {
		name   string
		first  []byte
		second []byte
		third  []byte
		target int64
	}{
		{name: "exact target remains active until next record", first: []byte("1234"), second: []byte("x"), target: 4},
		{name: "crossing record stays whole", first: []byte("123"), second: []byte("45"), third: []byte("x"), target: 4},
		{name: "oversized first record forms one-record segment", first: []byte("12345"), second: []byte("x"), target: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := schedulerOptions(t)
			opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: tt.target}
			s := openTest(t, opts)
			if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
				t.Fatal(err)
			}
			appendAndMaterialize(t, s, "s", tt.first)
			st, _ := s.streams.Load("s")
			before := st.snapshot()
			if len(before.sealed) != 0 || before.activeView.count != 1 {
				t.Fatalf("after first record: sealed=%d active count=%d, want 0 and 1", len(before.sealed), before.activeView.count)
			}
			assertSegment(t, st.activeSeg, 1, 1, tt.first)
			appendAndMaterialize(t, s, "s", tt.second)
			sealedPayload, sealedLast := tt.first, int64(1)
			if tt.third != nil {
				crossed := st.snapshot()
				if len(crossed.sealed) != 0 || crossed.activeView.count != 2 {
					t.Fatalf("crossing record split early: sealed=%d active count=%d", len(crossed.sealed), crossed.activeView.count)
				}
				sealedPayload = append(append([]byte(nil), tt.first...), tt.second...)
				sealedLast = 2
				assertSegment(t, st.activeSeg, 1, 2, sealedPayload)
				appendAndMaterialize(t, s, "s", tt.third)
			}
			after := st.snapshot()
			if len(after.sealed) != 1 {
				t.Fatalf("sealed segments = %d, want 1", len(after.sealed))
			}
			assertSegment(t, after.sealed[0], 1, sealedLast, sealedPayload)
			if tt.third == nil {
				assertSegment(t, st.activeSeg, 2, 2, tt.second)
			} else {
				assertSegment(t, st.activeSeg, 3, 3, tt.third)
			}
		})
	}
}

func TestSegmentPolicy_AgeSealDoesNotSealReplacementAfterTargetRollover(t *testing.T) {
	opts := schedulerOptions(t)
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 4}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendAndMaterialize(t, s, "s", []byte("1234"))
	st, _ := s.streams.Load("s")
	st.mu.Lock()
	st.forceSeal = true
	st.mu.Unlock()
	if _, err := s.Append(t.Context(), "s", []byte("next"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.materializeRoundResult(s.parts[0]); err != nil {
		t.Fatal(err)
	}
	snap := st.snapshot()
	if len(snap.sealed) != 1 || snap.activeView.count != 1 {
		t.Fatalf("sealed segments = %d, active records = %d; want 1 and 1", len(snap.sealed), snap.activeView.count)
	}
	assertSegment(t, snap.sealed[0], 1, 1, []byte("1234"))
	assertSegment(t, st.activeSeg, 2, 2, []byte("next"))
}

func TestSegmentPolicy_MaxOpenAgeSealsWithProcessSchedulesDisabled(t *testing.T) {
	opts := schedulerOptions(t)
	opts.MaterializeMaxAge = -1
	opts.RetentionInterval = -1
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20, MaxOpenAge: 300 * time.Millisecond}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendAndMaterialize(t, s, "s", []byte("payload"))
	waitFor(t, "age seal", func() bool {
		st, _ := s.streams.Load("s")
		snap := st.snapshot()
		return len(snap.sealed) == 1 && snap.activeView.count == 0
	})
}

func TestSegmentPolicy_AppendsDoNotResetActiveCreationAge(t *testing.T) {
	opts := schedulerOptions(t)
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20, MaxOpenAge: 700 * time.Millisecond}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendAndMaterialize(t, s, "s", []byte("first"))
	st, _ := s.streams.Load("s")
	created := st.snapshot().activeCreated
	for range 4 {
		time.Sleep(100 * time.Millisecond)
		appendAndMaterialize(t, s, "s", []byte("more"))
		if got := st.snapshot().activeCreated; got != created {
			t.Fatalf("active creation changed from %d to %d", created, got)
		}
	}
	waitFor(t, "original creation deadline seal", func() bool { return len(st.snapshot().sealed) == 1 })
}

func TestCreateMetaBound_IncreasesExactlyForSegmentPolicyFields(t *testing.T) {
	if segmentPolicyMetaBound != 91 {
		t.Fatalf("segment policy metadata bound = %d, want 91", segmentPolicyMetaBound)
	}
	meta, err := json.Marshal(createMeta{
		ContentType: "x",
		TTLNanos:    -1 << 63,
		ExpiresAt:   time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		IsPrivate:   true,
		Closed:      true,
		Retention:   &retentionMeta{MaxBytes: -1 << 63, MaxAgeNanos: -1 << 63},
		Policy:      &segmentPolicyMeta{TargetBytes: -1 << 63, MaxOpenAgeNanos: 1<<63 - 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) > metaBoundForCreate("x") {
		t.Fatalf("encoded create metadata = %d bytes, bound = %d", len(meta), metaBoundForCreate("x"))
	}
}

func TestSegmentAgeDeadline_PeekDoesNotInspectUnrelatedStream(t *testing.T) {
	s := &Storage{}
	p := newPartition(0, s, nil)
	due := newStreamState("due", incarnation{}, 0, durablestream.StreamConfig{})
	due.policy = SegmentPolicy{TargetBytes: 1, MaxOpenAge: time.Second}
	due.activeSeg = &segmentFile{count: 1, createdUnixNano: time.Now().Add(-time.Minute).UnixNano()}
	unrelated := newStreamState("unrelated", incarnation{}, 0, durablestream.StreamConfig{})
	unrelated.mu.Lock()
	defer unrelated.mu.Unlock()
	p.registerActiveDeadline(due, due.activeSeg.createdUnixNano, due.policy.MaxOpenAge, true)

	done := make(chan bool, 1)
	go func() {
		_, gotDue := s.segmentAgeDeadline(p, time.Now())
		done <- gotDue
	}()
	select {
	case gotDue := <-done:
		if !gotDue {
			t.Fatal("registered generation was not due")
		}
	case <-time.After(time.Second):
		t.Fatal("deadline peek blocked on an unrelated stream")
	}
}

func TestSegmentPolicySelector_AssignsIndependentRawAndMetaPolicies(t *testing.T) {
	raw := SegmentPolicy{TargetBytes: 1000, MaxOpenAge: time.Minute}
	meta := SegmentPolicy{TargetBytes: 100, MaxOpenAge: time.Second}
	opts := singlePartitionOptions(t.TempDir())
	opts.DefaultSegmentPolicy = raw
	opts.SelectSegmentPolicy = func(id string, _ durablestream.StreamConfig) *SegmentPolicy {
		if strings.HasSuffix(id, ".meta") {
			return &meta
		}
		return nil
	}
	s := openTest(t, opts)
	for _, id := range []string{"topic.raw", "topic.meta"} {
		if _, err := s.Create(t.Context(), id, durablestream.StreamConfig{}); err != nil {
			t.Fatalf("Create(%q): %v", id, err)
		}
	}
	gotRaw, _ := s.streams.Load("topic.raw")
	gotMeta, _ := s.streams.Load("topic.meta")
	if gotRaw.snapshot().policy != raw || gotMeta.snapshot().policy != meta {
		t.Errorf("policies = raw %+v, meta %+v; want raw %+v, meta %+v", gotRaw.snapshot().policy, gotMeta.snapshot().policy, raw, meta)
	}
}

func TestSegmentPolicy_CreateRetryComparesNewEffectivePolicy(t *testing.T) {
	selected := SegmentPolicy{TargetBytes: 100}
	opts := singlePartitionOptions(t.TempDir())
	opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy { return &selected }
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	selected.TargetBytes = 101
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); !errors.Is(err, durablestream.ErrConflict) {
		t.Fatalf("retry error = %v, want conflict", err)
	}
}

func TestSegmentPolicy_ForkTargetResolvesOwnPolicyAndRetryComparesIt(t *testing.T) {
	parentPolicy := SegmentPolicy{TargetBytes: 1000}
	childPolicy := SegmentPolicy{TargetBytes: 100}
	opts := singlePartitionOptions(t.TempDir())
	opts.DefaultSegmentPolicy = parentPolicy
	opts.SelectSegmentPolicy = func(id string, _ durablestream.StreamConfig) *SegmentPolicy {
		if id == "child" {
			return &childPolicy
		}
		return nil
	}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	req := durablestream.ForkRequest{SourceStreamID: "parent"}
	if created, _, err := s.CreateFork(t.Context(), "child", req, nil); err != nil || !created {
		t.Fatalf("CreateFork() = (%v, %v), want created", created, err)
	}
	child, _ := s.streams.Load("child")
	if got := child.snapshot().policy; got != childPolicy {
		t.Errorf("child policy = %+v, want %+v", got, childPolicy)
	}
	childPolicy.TargetBytes++
	if _, _, err := s.CreateFork(t.Context(), "child", req, nil); !errors.Is(err, durablestream.ErrConflict) {
		t.Fatalf("fork retry error = %v, want conflict", err)
	}
}

func TestSegmentPolicy_CheckpointRecoveryIgnoresChangedDefault(t *testing.T) {
	dir := t.TempDir()
	want := SegmentPolicy{TargetBytes: 321, MaxOpenAge: 2 * time.Second}
	opts := singlePartitionOptions(dir)
	opts.DefaultSegmentPolicy = want
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 999, MaxOpenAge: time.Hour}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	st, _ := s.streams.Load("s")
	if got := st.snapshot().policy; got != want {
		t.Errorf("recovered policy = %+v, want %+v", got, want)
	}
}

func TestSegmentPolicy_CheckpointRecoveryKeepsOriginalActiveDeadline(t *testing.T) {
	dir := t.TempDir()
	opts := schedulerOptions(t)
	opts.Dir = dir
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20, MaxOpenAge: 800 * time.Millisecond}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	appendAndMaterialize(t, s, "s", []byte("payload"))
	st, _ := s.streams.Load("s")
	created := st.snapshot().activeCreated
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 999}
	opts.MaterializeMaxAge, opts.RetentionInterval = -1, -1
	opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy {
		t.Fatal("selector called during checkpoint recovery")
		return nil
	}
	r := openTest(t, opts)
	recovered, _ := r.streams.Load("s")
	if got := recovered.snapshot().activeCreated; got != created {
		t.Fatalf("active creation = %d, want persisted %d", got, created)
	}
	waitFor(t, "persisted active deadline seal", func() bool { return len(recovered.snapshot().sealed) == 1 })
}

func TestSegmentPolicy_ZeroDefaultAndExplicitZeroAgeAreDistinct(t *testing.T) {
	got := (Options{}).withDefaults().DefaultSegmentPolicy
	if got != (SegmentPolicy{TargetBytes: DefaultSegmentTargetBytes, MaxOpenAge: DefaultSegmentMaxOpenAge}) {
		t.Errorf("zero-value policy defaults to %+v", got)
	}
	explicit := (Options{DefaultSegmentPolicy: SegmentPolicy{TargetBytes: 123, MaxOpenAge: 0}}).withDefaults().DefaultSegmentPolicy
	if explicit != (SegmentPolicy{TargetBytes: 123}) {
		t.Errorf("explicit zero age policy = %+v", explicit)
	}

	opts := singlePartitionOptions(t.TempDir())
	opts.DefaultSegmentPolicy = explicit
	opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy { return nil }
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "nil", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	st, _ := s.streams.Load("nil")
	if st.snapshot().policy != explicit {
		t.Errorf("nil selector policy = %+v, want %+v", st.snapshot().policy, explicit)
	}
	bad := SegmentPolicy{TargetBytes: 0}
	s.opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy { return &bad }
	if _, err := s.Create(t.Context(), "bad", durablestream.StreamConfig{}); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("invalid selected policy error = %v, want ErrBadRequest", err)
	}
}

func TestSegmentPolicy_UnchangedCreateAndForkRetriesAreIdempotent(t *testing.T) {
	policy := SegmentPolicy{TargetBytes: 100}
	opts := singlePartitionOptions(t.TempDir())
	opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy { return &policy }
	s := openTest(t, opts)
	if created, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{}); err != nil || !created {
		t.Fatalf("first Create = %v, %v", created, err)
	}
	if created, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{}); err != nil || created {
		t.Fatalf("retry Create = %v, %v", created, err)
	}
	req := durablestream.ForkRequest{SourceStreamID: "parent"}
	if created, _, err := s.CreateFork(t.Context(), "child", req, nil); err != nil || !created {
		t.Fatalf("first fork = %v, %v", created, err)
	}
	if created, _, err := s.CreateFork(t.Context(), "child", req, nil); err != nil || created {
		t.Fatalf("retry fork = %v, %v", created, err)
	}
}

func TestSegmentPolicy_WALOnlyRecoveryPreservesCreateAndForkPolicies(t *testing.T) {
	dir := t.TempDir()
	want := SegmentPolicy{TargetBytes: 333, MaxOpenAge: time.Minute}
	opts := singlePartitionOptions(dir)
	opts.MaterializeMaxAge, opts.RetentionInterval = -1, -1
	opts.DefaultSegmentPolicy = want
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateFork(t.Context(), "child", durablestream.ForkRequest{SourceStreamID: "parent"}, nil); err != nil {
		t.Fatal(err)
	}
	stopWithoutCheckpoint(t, s)
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 999}
	opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy {
		t.Fatal("selector called during recovery")
		return nil
	}
	r := openTest(t, opts)
	for _, id := range []string{"parent", "child"} {
		st, ok := r.streams.Load(id)
		if !ok || st.snapshot().policy != want {
			t.Errorf("recovered %s policy = %+v, want %+v", id, st.snapshot().policy, want)
		}
	}
}

func TestSegmentPolicy_DeadlineHeapHasOneEntryPerStreamGeneration(t *testing.T) {
	p := newPartition(0, &Storage{}, nil)
	st := newStreamState("s", incarnation{}, 0, durablestream.StreamConfig{})
	for i := range 1000 {
		p.registerActiveDeadline(st, int64(i+1), time.Second, true)
	}
	p.ageMu.Lock()
	defer p.ageMu.Unlock()
	if len(p.ageDeadlines) != 1 || len(p.ageEntries) != 1 {
		t.Errorf("deadline heap/map sizes = %d/%d, want 1/1", len(p.ageDeadlines), len(p.ageEntries))
	}
}

func TestSegmentPolicy_RemovalUnregistersActiveDeadline(t *testing.T) {
	p := newPartition(0, &Storage{}, nil)
	for i := range 1000 {
		st := newStreamState("s", incarnation{}, 0, durablestream.StreamConfig{})
		p.registerActiveDeadline(st, int64(i+1), time.Hour, true)
		p.markRemoval(st)
	}
	p.ageMu.Lock()
	defer p.ageMu.Unlock()
	if len(p.ageDeadlines) != 0 || len(p.ageEntries) != 0 {
		t.Errorf("deadline heap/map sizes after removals = %d/%d, want 0/0", len(p.ageDeadlines), len(p.ageEntries))
	}
}

func TestSegmentPolicy_CreateForkCallsSelectorWithoutTopologyLock(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	s.opts.SelectSegmentPolicy = func(string, durablestream.StreamConfig) *SegmentPolicy {
		if !s.topologyMu.TryLock() {
			t.Fatal("selector called while topologyMu held")
		}
		s.topologyMu.Unlock()
		return nil
	}
	if _, _, err := s.CreateFork(t.Context(), "child", durablestream.ForkRequest{SourceStreamID: "parent"}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentPolicy_RecoveryRejectsMissingOrInvalidMetadata(t *testing.T) {
	t.Run("checkpoint", func(t *testing.T) {
		for _, policy := range []segmentPolicyMeta{{}, {TargetBytes: -1}, {TargetBytes: 1, MaxOpenAgeNanos: -1}} {
			s := &Storage{}
			_, err := s.stateFromCheckpointEntry("s", t.TempDir(), streamCheckpointEntry{SegmentPolicy: policy})
			if !errors.Is(err, errCorrupt) {
				t.Errorf("policy %+v error = %v, want errCorrupt", policy, err)
			}
		}
	})

	t.Run("create WAL", func(t *testing.T) {
		for _, policy := range []*segmentPolicyMeta{nil, {}, {TargetBytes: -1}, {TargetBytes: 1, MaxOpenAgeNanos: -1}} {
			meta, err := json.Marshal(createMeta{Policy: policy})
			if err != nil {
				t.Fatal(err)
			}
			s := &Storage{}
			p := newPartition(0, s, nil)
			inc, err := newIncarnation()
			if err != nil {
				t.Fatal(err)
			}
			err = s.applyRecovered(p, &recoveryScan{}, 1, walFrame{op: opCreate, streamID: "s", inc: inc, meta: meta})
			if !errors.Is(err, errCorrupt) {
				t.Errorf("policy %+v error = %v, want errCorrupt", policy, err)
			}
		}
	})

	t.Run("fork WAL", func(t *testing.T) {
		valid := forkFrameMeta{
			Create: createMeta{Retention: &retentionMeta{}, Policy: &segmentPolicyMeta{TargetBytes: 1}},
			Fork:   forkMeta{Request: forkRequestMeta{SourceStreamID: "source"}, SourceID: "source", SourceIncarnationID: "inc", Boundary: 0},
		}
		for _, policy := range []*segmentPolicyMeta{nil, {}, {TargetBytes: -1}, {TargetBytes: 1, MaxOpenAgeNanos: -1}} {
			m := valid
			m.Create.Policy = policy
			err := validateRecoveredFork(walFrame{streamID: "fork", firstIndex: 1}, m)
			if !errors.Is(err, errCorrupt) {
				t.Errorf("policy %+v error = %v, want errCorrupt", policy, err)
			}
		}
	})
}
