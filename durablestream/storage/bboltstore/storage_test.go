package bboltstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func openTest(t *testing.T, opts Options) (*Storage, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "streams.db")
	s, err := New(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestAppendReadReopen(t *testing.T) {
	s, path := openTest(t, Options{})
	ctx := context.Background()
	if created, err := s.Create(ctx, "alpha", durablestream.StreamConfig{ContentType: "text/plain"}); !created || err != nil {
		t.Fatalf("create=%v err=%v", created, err)
	}
	if _, err := s.Append(ctx, "alpha", []byte("one"), "0001"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendBatch(ctx, "alpha", [][]byte{[]byte("two"), []byte("three")}, "0002"); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, "alpha", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 3 || string(r.Messages[1].Data) != "two" || r.TailOffset != storage.FormatSimpleOffset(3) {
		t.Fatalf("unexpected read: %+v", r)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err = s.Read(ctx, "alpha", storage.FormatSimpleOffset(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 2 || string(r.Messages[0].Data) != "two" {
		t.Fatalf("reopen read: %+v", r)
	}
}

func TestRetentionBytesReturnsGone(t *testing.T) {
	s, _ := openTest(t, Options{DefaultRetention: Retention{MaxBytes: 5}})
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Create(ctx, "r", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "r", []byte("abc"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "r", []byte("def"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, "r", "", 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("read start err=%v, want ErrGone", err)
	}
	r, err := s.Read(ctx, "r", storage.FormatSimpleOffset(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 1 || string(r.Messages[0].Data) != "def" {
		t.Fatalf("retained read: %+v", r)
	}
}

func TestRetentionAge(t *testing.T) {
	s, _ := openTest(t, Options{DefaultRetention: Retention{MaxAge: time.Millisecond}})
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Create(ctx, "age", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "age", []byte("old"), ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := s.Append(ctx, "age", []byte("new"), ""); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, "age", storage.FormatSimpleOffset(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 1 || string(r.Messages[0].Data) != "new" {
		t.Fatalf("age read: %+v", r)
	}
}

func TestWaitAndClose(t *testing.T) {
	s, _ := openTest(t, Options{})
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Create(ctx, "wait", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	result := make(chan *durablestream.ReadResult, 1)
	go func() {
		r, err := s.WaitForData(ctx, "wait", "", 0)
		if err != nil {
			t.Errorf("wait: %v", err)
			return
		}
		result <- r
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := s.Append(ctx, "wait", []byte("x"), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-result:
		if len(r.Messages) != 1 {
			t.Fatalf("wait result: %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("wait timed out")
	}
}

func TestCreateClosedWithMessages(t *testing.T) {
	s, _ := openTest(t, Options{})
	defer s.Close()
	created, tail, err := s.CreateWithMessages(context.Background(), "closed", durablestream.StreamConfig{Closed: true}, [][]byte{[]byte("final")})
	if err != nil || !created || tail != storage.FormatSimpleOffset(1) {
		t.Fatalf("create closed: created=%v tail=%s err=%v", created, tail, err)
	}
	if _, err := s.Append(context.Background(), "closed", []byte("late"), ""); !errors.Is(err, durablestream.ErrStreamClosed) {
		t.Fatalf("append after close: %v", err)
	}
}
