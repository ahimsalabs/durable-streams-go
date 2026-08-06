package seglog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	storagepkg "github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func TestProducerGroup_MultipleStreamsAndBatches_PublishInOrderAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	s := openTest(t, opts)
	for _, id := range []string{"alpha", "beta"} {
		if _, err := s.Create(t.Context(), id, durablestream.StreamConfig{}); err != nil {
			t.Fatalf("Create(%q): %v", id, err)
		}
	}

	g := s.NewProducerGroup()
	if err := g.Append("alpha", []byte("one"), "0001"); err != nil {
		t.Fatalf("Append alpha one: %v", err)
	}
	if err := g.AppendBatch("beta", [][]byte{[]byte("two"), []byte("three")}, "0001"); err != nil {
		t.Fatalf("AppendBatch beta: %v", err)
	}
	if err := g.Append("alpha", []byte("four"), "0002"); err != nil {
		t.Fatalf("Append alpha four: %v", err)
	}
	results, err := g.Commit(t.Context())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("result count = %d, want 3", len(results))
	}
	for i, result := range results {
		if result.Err != nil {
			t.Errorf("result %d error = %v", i, result.Err)
		}
	}
	if results[0].Offset != storagepkg.FormatSimpleOffset(1) || results[1].Offset != storagepkg.FormatSimpleOffset(2) || results[2].Offset != storagepkg.FormatSimpleOffset(2) {
		t.Errorf("offsets = [%q %q %q], want [1 2 2]", results[0].Offset, results[1].Offset, results[2].Offset)
	}
	if got := readAll(t, s, "alpha"); !equalStrings(got, []string{"one", "four"}) {
		t.Fatalf("alpha payloads before reopen = %q", got)
	}
	if got := readAll(t, s, "beta"); !equalStrings(got, []string{"two", "three"}) {
		t.Fatalf("beta payloads before reopen = %q", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	r := openTest(t, opts)
	if head, err := r.Head(t.Context(), "alpha"); err != nil || head.NextOffset != storagepkg.FormatSimpleOffset(2) {
		t.Errorf("reopened alpha Head = %+v, %v; want next offset 2", head, err)
	}
	if head, err := r.Head(t.Context(), "beta"); err != nil || head.NextOffset != storagepkg.FormatSimpleOffset(2) {
		t.Errorf("reopened beta Head = %+v, %v; want next offset 2", head, err)
	}
	if got := readAll(t, r, "alpha"); !equalStrings(got, []string{"one", "four"}) {
		t.Fatalf("alpha payloads after reopen = %q", got)
	}
	if got := readAll(t, r, "beta"); !equalStrings(got, []string{"two", "three"}) {
		t.Fatalf("beta payloads after reopen = %q", got)
	}
}

func TestProducerGroup_CancellationBeforeAdmissionRejectsAndAfterAdmissionWaits(t *testing.T) {
	s := openTest(t, singlePartitionOptions(t.TempDir()))
	if _, err := s.Create(t.Context(), "stream", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	before := s.NewProducerGroup()
	_ = before.Append("stream", []byte("before"), "")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := before.Commit(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit with canceled context = %v, want context.Canceled", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	s.parts[0].wal.blockNextSync(started, release)
	after := s.NewProducerGroup()
	_ = after.Append("stream", []byte("after"), "")
	ctx, cancelAfter := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := after.Commit(ctx)
		done <- err
	}()
	awaitSignal(t, started, "producer group sync")
	cancelAfter()
	select {
	case err := <-done:
		t.Fatalf("Commit returned before admitted sync completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Commit after admission = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Commit did not return after sync release")
	}
}

func TestProducerGroup_MixedValidAndInvalid_PreservesIndependentResults(t *testing.T) {
	s := openTest(t, singlePartitionOptions(t.TempDir()))
	if _, err := s.Create(t.Context(), "stream", durablestream.StreamConfig{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	g := s.NewProducerGroup()
	for _, seq := range []string{"0002", "0001", "0003"} {
		if err := g.Append("stream", []byte(seq), seq); err != nil {
			t.Fatalf("Append(%q): %v", seq, err)
		}
	}
	results, err := g.Commit(t.Context())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if results[0].Err != nil || !errors.Is(results[1].Err, durablestream.ErrSequenceConflict) || results[2].Err != nil {
		t.Errorf("result errors = [%v %v %v], want [nil sequence-conflict nil]", results[0].Err, results[1].Err, results[2].Err)
	}
	if results[0].Offset != storagepkg.FormatSimpleOffset(1) || results[2].Offset != storagepkg.FormatSimpleOffset(2) {
		t.Errorf("valid offsets = [%q %q], want [1 2]", results[0].Offset, results[2].Offset)
	}
}

func TestProducerGroup_FrameLessAndLeadingInvalidStats(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.MaterializeMaxAge = -1
	opts.RetentionInterval = -1
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "stream", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "stream", []byte("initial"), "0002"); err != nil {
		t.Fatal(err)
	}
	s.materializeRound(s.parts[0])

	before := s.Stats().WALBytesWritten
	invalid := s.NewProducerGroup()
	_ = invalid.Append("stream", []byte("invalid"), "0001")
	results, err := invalid.Commit(t.Context())
	if err != nil || len(results) != 1 || !errors.Is(results[0].Err, durablestream.ErrSequenceConflict) {
		t.Fatalf("invalid Commit = %+v, %v", results, err)
	}
	if got := s.Stats().WALBytesWritten; got != before {
		t.Fatalf("WAL bytes after frame-less group = %d, want %d", got, before)
	}

	mixed := s.NewProducerGroup()
	_ = mixed.Append("stream", []byte("invalid"), "0001")
	_ = mixed.Append("stream", []byte("valid"), "0003")
	results, err = mixed.Commit(t.Context())
	if err != nil || !errors.Is(results[0].Err, durablestream.ErrSequenceConflict) || results[1].Err != nil {
		t.Fatalf("mixed Commit = %+v, %v", results, err)
	}
	stats := s.Stats()
	if stats.UnmaterializedWALBytes == 0 || stats.OldestUnmaterializedAge <= 0 {
		t.Fatalf("mixed group frontier = bytes %d, age %v; want positive", stats.UnmaterializedWALBytes, stats.OldestUnmaterializedAge)
	}
}

func TestProducerGroup_LifecycleAndCrossPartition_RejectedWithoutMutation(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.Partitions = 2
	s := openTest(t, opts)
	if _, err := s.NewProducerGroup().Commit(t.Context()); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("empty Commit error = %v, want ErrBadRequest", err)
	}

	ids := []string{"first", "second"}
	for streamHash(ids[0])%2 == streamHash(ids[1])%2 {
		ids[1] += "x"
	}
	for _, id := range ids {
		if _, err := s.Create(t.Context(), id, durablestream.StreamConfig{}); err != nil {
			t.Fatalf("Create(%q): %v", id, err)
		}
	}
	g := s.NewProducerGroup()
	if err := g.Append(ids[0], []byte("kept"), ""); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := g.Append(ids[1], []byte("rejected"), ""); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("cross-partition Append error = %v, want ErrBadRequest", err)
	}
	results, err := g.Commit(t.Context())
	if err != nil || len(results) != 1 || results[0].Err != nil {
		t.Fatalf("Commit retained operation = %+v, %v", results, err)
	}
	if err := g.Append(ids[0], []byte("late"), ""); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("Append after Commit error = %v, want ErrBadRequest", err)
	}
	if _, err := g.Commit(t.Context()); !errors.Is(err, durablestream.ErrBadRequest) {
		t.Errorf("second Commit error = %v, want ErrBadRequest", err)
	}
}

func TestProducerGroup_WALFailure_ClassifiesOnlyFrameBearingResultsUnknown(t *testing.T) {
	for _, test := range []struct {
		name   string
		inject func(*walWriter, error)
	}{
		{name: "write", inject: (*walWriter).failNextWrite},
		{name: "sync", inject: (*walWriter).failNextSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t, singlePartitionOptions(t.TempDir()))
			if _, err := s.Create(t.Context(), "stream", durablestream.StreamConfig{}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			g := s.NewProducerGroup()
			_ = g.Append("stream", []byte("valid"), "0002")
			_ = g.Append("stream", []byte("invalid"), "0001")
			injected := errors.New("injected")
			test.inject(s.parts[0].wal, injected)
			results, err := g.Commit(t.Context())
			if err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if !errors.Is(results[0].Err, ErrDurabilityUnknown) {
				t.Errorf("frame-bearing result error = %v, want ErrDurabilityUnknown", results[0].Err)
			}
			if !errors.Is(results[0].Err, injected) {
				t.Errorf("frame-bearing result error = %v, want injected cause", results[0].Err)
			}
			if !errors.Is(results[1].Err, durablestream.ErrSequenceConflict) || errors.Is(results[1].Err, ErrDurabilityUnknown) {
				t.Errorf("invalid result error = %v, want definitive sequence conflict", results[1].Err)
			}
		})
	}
}
