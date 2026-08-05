package pebblelog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestAppendReadBatchAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	created, err := l.Create(ctx, "orders/a", durablestream.StreamConfig{ContentType: "application/octet-stream"})
	if err != nil || !created {
		t.Fatalf("Create = %v, %v", created, err)
	}
	if _, err := l.AppendBatch(ctx, "orders/a", [][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	msgs, next, err := l.Read(ctx, "orders/a", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || string(msgs[0].Data) != "one" || string(msgs[1].Data) != "two" || next.String() != "0000000000000000_0000000000000002" {
		t.Fatalf("Read = %#v, next=%q", msgs, next)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	msgs, _, err = l.Read(ctx, "orders/a", "", 0)
	if err != nil || len(msgs) != 2 || string(msgs[1].Data) != "two" {
		t.Fatalf("reopen Read = %#v, %v", msgs, err)
	}
}

func TestRetentionReturnsGone(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Sync: true, MaxBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx := context.Background()
	if _, err := l.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendBatch(ctx, "s", [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}); err != nil {
		t.Fatal(err)
	}
	removed, err := l.Retain(ctx, "s", time.Now())
	if err != nil || removed != 2 {
		t.Fatalf("Retain removed=%d err=%v", removed, err)
	}
	if _, _, err := l.Read(ctx, "s", "", 0); !errors.Is(err, ErrGone) {
		t.Fatalf("Read before retained head err=%v, want ErrGone", err)
	}
	msgs, _, err := l.Read(ctx, "s", "0000000000000000_0000000000000002", 0)
	if err != nil || len(msgs) != 1 || string(msgs[0].Data) != "ccc" {
		t.Fatalf("Read retained = %#v, %v", msgs, err)
	}
}

func TestAgeRetention(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Sync: true, MaxAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx := context.Background()
	if _, err := l.Create(ctx, "s", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, "s", []byte("x")); err != nil {
		t.Fatal(err)
	}
	removed, err := l.Retain(ctx, "s", time.Now().Add(2*time.Minute))
	if err != nil || removed != 1 {
		t.Fatalf("Retain removed=%d err=%v", removed, err)
	}
	if _, _, err := l.Read(ctx, "s", "", 0); !errors.Is(err, ErrGone) {
		t.Fatalf("Read expired err=%v, want ErrGone", err)
	}
}
