package seglog

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func TestFork_ReopenPreservesStitchedSoftDeletedLineage(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Partitions: 2, MaxMessageSize: 64 << 10, WALSegmentBytes: 1 << 20, MaterializeInterval: time.Millisecond}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), "source", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "source", []byte("parent"), ""); err != nil {
		t.Fatal(err)
	}
	source, err := s.Head(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateFork(t.Context(), "child", durablestream.ForkRequest{
		SourceStreamID: "source", SourceIncarnationID: source.IncarnationID,
		Config: durablestream.StreamConfig{ContentType: "text/plain"},
	}, [][]byte{[]byte("child")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Head(t.Context(), "source"); !errors.Is(err, durablestream.ErrSoftDeleted) {
		t.Errorf("Head(source) error = %v, want ErrSoftDeleted", err)
	}
	res, err := s.Read(t.Context(), "child", durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 2 || string(res.Messages[0].Data) != "parent" || string(res.Messages[1].Data) != "child" {
		t.Errorf("stitched messages = %#v, want parent, child", res.Messages)
	}
	if err := s.Delete(t.Context(), "child"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Head(t.Context(), "source"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Errorf("Head(source) after cascade error = %v, want ErrNotFound", err)
	}
}

func TestFork_SubOffsetPrefixesRemainChildLocalAcrossMaterializationAndReopen(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		batch       [][]byte
		sub         uint64
		want        []string
	}{
		{
			name:        "binary exact message length copies the complete next payload",
			contentType: "application/octet-stream",
			batch:       [][]byte{[]byte("abcd")},
			sub:         4,
			want:        []string{"anchor", "abcd", "initial"},
		},
		{
			name:        "JSON copies the requested prefix of only the next atomic batch",
			contentType: "application/json",
			batch:       [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)},
			sub:         2,
			want:        []string{"anchor", `{"n":1}`, `{"n":2}`, "initial"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, Partitions: 4, MaxMessageSize: 64 << 10, WALSegmentBytes: 1 << 20, MaterializeInterval: time.Millisecond}
			// The point of this test is a fork whose source lives in another
			// partition; derive a target ID that provably routes elsewhere so
			// the test does not depend on any particular hash function.
			target := crossPartitionID(t, "source-cross-partition", opts.Partitions)
			s, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.CreateWithMessages(t.Context(), "source-cross-partition", durablestream.StreamConfig{ContentType: tt.contentType}, [][]byte{[]byte("anchor")}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.AppendBatch(t.Context(), "source-cross-partition", tt.batch, ""); err != nil {
				t.Fatal(err)
			}
			head, err := s.Head(t.Context(), "source-cross-partition")
			if err != nil {
				t.Fatal(err)
			}
			created, _, err := s.CreateFork(t.Context(), target, durablestream.ForkRequest{
				SourceStreamID: "source-cross-partition", SourceIncarnationID: head.IncarnationID,
				Offset: storage.FormatSimpleOffset(1), OffsetSet: true, SubOffset: tt.sub,
				Config: durablestream.StreamConfig{ContentType: tt.contentType}, ContentTypeSet: true,
			}, [][]byte{[]byte("initial")})
			if err != nil || !created {
				t.Fatalf("CreateFork() = %v, %v, want created", created, err)
			}
			waitFor(t, "fork materialization", func() bool { return materializedThrough(s, target) >= int64(len(tt.want)) })
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			res, err := s.Read(t.Context(), target, durablestream.ZeroOffset, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(res.Messages))
			for i := range res.Messages {
				got[i] = string(res.Messages[i].Data)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("messages = %q, want %q", got, tt.want)
			}
			st, _ := s.streams.Load(target)
			if gotBatch, err := s.messageBatch(st, 2, 0); err != nil || gotBatch != 2 {
				t.Errorf("prefix batchFirst = %d, %v; want 2", gotBatch, err)
			}
			initial := int64(2) + st.fork.PrefixCount
			if gotBatch, err := s.messageBatch(st, initial, 0); err != nil || gotBatch != initial {
				t.Errorf("initial batchFirst = %d, %v; want %d", gotBatch, err, initial)
			}
		})
	}
}

func TestFork_DerivedRefCountsCascadeAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Partitions: 4, MaxMessageSize: 64 << 10, WALSegmentBytes: 1 << 20, MaterializeInterval: time.Millisecond}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), "parent", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "parent", []byte("root"), ""); err != nil {
		t.Fatal(err)
	}
	parentInfo, err := s.Head(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	request := durablestream.ForkRequest{SourceStreamID: "parent", SourceIncarnationID: parentInfo.IncarnationID, Config: durablestream.StreamConfig{ContentType: "text/plain"}}
	for _, child := range []string{"child-one", "child-two"} {
		if _, _, err := s.CreateFork(t.Context(), child, request, nil); err != nil {
			t.Fatalf("CreateFork(%s): %v", child, err)
		}
	}
	childInfo, err := s.Head(t.Context(), "child-two")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateFork(t.Context(), "grandchild", durablestream.ForkRequest{
		SourceStreamID: "child-two", SourceIncarnationID: childInfo.IncarnationID,
		Config: durablestream.StreamConfig{ContentType: "text/plain"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	parent, ok := s.streams.Load("parent")
	if !ok || parent.refCount.Load() != 2 {
		t.Fatalf("recovered parent refCount = %v, present %v; want 2", func() int64 {
			if !ok {
				return -1
			}
			return parent.refCount.Load()
		}(), ok)
	}
	if err := s.Delete(t.Context(), "child-one"); err != nil {
		t.Fatal(err)
	}
	if parent.refCount.Load() != 1 {
		t.Fatalf("parent refCount after first child deletion = %d, want 1", parent.refCount.Load())
	}
	if err := s.Delete(t.Context(), "child-two"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Head(t.Context(), "child-two"); !errors.Is(err, durablestream.ErrSoftDeleted) {
		t.Fatalf("Head(child-two) after retained delete = %v, want ErrSoftDeleted", err)
	}
	if err := s.Delete(t.Context(), "grandchild"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"parent", "child-one", "child-two", "grandchild"} {
		if _, err := s.Head(t.Context(), id); !errors.Is(err, durablestream.ErrNotFound) {
			t.Errorf("Head(%s) after final cascade = %v, want ErrNotFound", id, err)
		}
	}
}

func TestFork_RetentionAdvancesLogicalFloorButPinsPhysicalSegments(t *testing.T) {
	dir := t.TempDir()
	opts := retentionOptions(dir)
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "source", durablestream.StreamConfig{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	appendRetentionMessages(t, s, "source", 30)
	waitFor(t, "source segment sealing", func() bool { return len(streamSegmentPaths(t, s, "source")) >= 3 })
	beforePaths := streamSegmentPaths(t, s, "source")
	before := len(beforePaths)
	info, err := s.Head(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateFork(t.Context(), "child", durablestream.ForkRequest{
		SourceStreamID: "source", SourceIncarnationID: info.IncarnationID,
		Config: durablestream.StreamConfig{ContentType: "text/plain"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetention(t.Context(), "source", Retention{MaxBytes: 400}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "pinned source logical retention", func() bool { return streamFloor(s, "source") > 0 })
	for _, path := range beforePaths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("pinned source segment %s was unlinked: %v", path, err)
		}
	}
	if _, err := s.Read(t.Context(), "source", durablestream.ZeroOffset, 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("direct source read below floor = %v, want ErrGone", err)
	}
	res, err := s.Read(t.Context(), "child", durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatalf("child read through pinned source: %v", err)
	}
	if len(res.Messages) != 30 {
		t.Fatalf("child inherited messages = %d, want 30", len(res.Messages))
	}
	if err := s.Delete(t.Context(), "child"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "physical trim after final pin release", func() bool {
		return len(streamSegmentPaths(t, s, "source")) < before
	})
}
