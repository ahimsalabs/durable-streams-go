package filestore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

func TestRoundTripAndRecovery(t *testing.T) {
	dir := t.TempDir()
	f, err := New(Options{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if created, err := f.Create(context.Background(), "orders/1", durablestream.StreamConfig{ContentType: "application/json"}); err != nil || !created {
		t.Fatalf("Create = %v, %v", created, err)
	}
	payload := []byte(`{"id":1}`)
	off, err := f.Append(context.Background(), "orders/1", payload, "1")
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X' // Append must own its input.
	result, err := f.Read(context.Background(), "orders/1", durablestream.ZeroOffset, 0)
	if err != nil || len(result.Messages) != 1 || string(result.Messages[0].Data) != `{"id":1}` {
		t.Fatalf("Read = %#v, %v", result, err)
	}
	if result.Messages[0].Offset != off {
		t.Fatalf("message offset = %q, want %q", result.Messages[0].Offset, off)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = New(Options{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	result, err = f.Read(context.Background(), "orders/1", durablestream.ZeroOffset, 0)
	if err != nil || len(result.Messages) != 1 || string(result.Messages[0].Data) != `{"id":1}` {
		t.Fatalf("recovered Read = %#v, %v", result, err)
	}
	if _, err := f.Append(context.Background(), "orders/1", []byte("duplicate"), "1"); !errors.Is(err, durablestream.ErrConflict) {
		t.Fatalf("sequence regression = %v", err)
	}
}

func TestCreateWithMessagesIsVisibleAsOneMutation(t *testing.T) {
	f, err := New(Options{Dir: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	created, tail, err := f.CreateWithMessages(context.Background(), "batch", durablestream.StreamConfig{}, [][]byte{[]byte("a"), []byte("b")})
	if err != nil || !created || tail != storage.FormatSimpleOffset(2) {
		t.Fatalf("CreateWithMessages = %v, %q, %v", created, tail, err)
	}
	result, err := f.Read(context.Background(), "batch", durablestream.ZeroOffset, 0)
	if err != nil || len(result.Messages) != 2 {
		t.Fatalf("initial Read = %#v, %v", result, err)
	}
	if created, tail, err := f.CreateWithMessages(context.Background(), "batch", durablestream.StreamConfig{}, [][]byte{[]byte("ignored")}); err != nil || created || tail != storage.FormatSimpleOffset(2) {
		t.Fatalf("idempotent CreateWithMessages = %v, %q, %v", created, tail, err)
	}
}

func TestRetentionByBytesReturnsGone(t *testing.T) {
	f, err := New(Options{Dir: t.TempDir(), MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Create(context.Background(), "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")} {
		if _, err := f.Append(context.Background(), "s", p, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Read(context.Background(), "s", durablestream.ZeroOffset, 0); !errors.Is(err, durablestream.ErrGone) {
		t.Fatalf("read before retention floor = %v", err)
	}
	result, err := f.Read(context.Background(), "s", storage.FormatSimpleOffset(2), 0)
	if err != nil || len(result.Messages) != 1 || string(result.Messages[0].Data) != "ccc" {
		t.Fatalf("retained Read = %#v, %v", result, err)
	}
}

func TestTouchAndWaitAndClose(t *testing.T) {
	f, err := New(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Create(context.Background(), "s", durablestream.StreamConfig{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	before, err := f.Head(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Touch(context.Background(), "s"); err != nil {
		t.Fatal(err)
	}
	after, err := f.Head(context.Background(), "s")
	if err != nil || !after.ExpiresAt.After(before.ExpiresAt) {
		t.Fatalf("Touch expiry = %v, before %v, err %v", after.ExpiresAt, before.ExpiresAt, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *durablestream.ReadResult, 1)
	go func() {
		result, _ := f.WaitForData(ctx, "s", durablestream.ZeroOffset, 0)
		done <- result
	}()
	if _, err := f.Append(context.Background(), "s", []byte("x"), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result == nil || len(result.Messages) != 1 {
			t.Fatalf("WaitForData = %#v", result)
		}
	case <-ctx.Done():
		t.Fatal("WaitForData did not wake")
	}
	if _, err := f.CloseStream(context.Background(), "s", nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(context.Background(), "s", []byte("late"), ""); !errors.Is(err, durablestream.ErrStreamClosed) {
		t.Fatalf("append after close = %v", err)
	}
}
