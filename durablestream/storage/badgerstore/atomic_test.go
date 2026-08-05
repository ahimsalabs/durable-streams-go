package badgerstore

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// TestCreateWithMessagesInitializesHighWater guards the boundary between the
// atomic creation transaction and later appends. If the high-water starts at
// zero, the first later Append overwrites offset 1.
func TestCreateWithMessagesInitializesHighWater(t *testing.T) {
	s := newTestStorage(t)
	created, initialTail, err := s.CreateWithMessages(t.Context(), "stream", durablestream.StreamConfig{ContentType: "text/plain"}, [][]byte{
		[]byte("one"), []byte("two"), []byte("three"),
	})
	if err != nil || !created {
		t.Fatalf("CreateWithMessages = (created %v, err %v), want (true, nil)", created, err)
	}

	gen := currentGeneration(t, s, "stream")
	var persisted uint64
	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(seqKey("stream", gen))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			persisted = binary.BigEndian.Uint64(value)
			return nil
		})
	}); err != nil {
		t.Fatalf("read initialized high-water: %v", err)
	}
	if persisted != 3 {
		t.Fatalf("persisted offset high-water = %d, want 3", persisted)
	}

	next, err := s.Append(t.Context(), "stream", []byte("four"), "")
	if err != nil {
		t.Fatalf("Append after CreateWithMessages: %v", err)
	}
	if next.Compare(initialTail) <= 0 {
		t.Fatalf("Append offset %q does not follow initial tail %q", next, initialTail)
	}
	res, err := s.Read(t.Context(), "stream", durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	got := make([]string, len(res.Messages))
	for i, message := range res.Messages {
		got[i] = string(message.Data)
	}
	want := []string{"one", "two", "three", "four"}
	if len(got) != len(want) {
		t.Fatalf("messages = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("messages = %q, want %q", got, want)
		}
	}
}

// TestAtomicBatchesMapBadgerTransactionLimit covers batches made of many tiny
// JSON values. Their request body can be well below the HTTP byte limit while
// their generated keys exceed Badger's transaction entry limit. The backend
// must classify that as a payload rejection and preserve batch atomicity for
// both creation and append.
func TestAtomicBatchesMapBadgerTransactionLimit(t *testing.T) {
	s := newTestStorage(t)
	if _, err := s.Create(t.Context(), "stream", durablestream.StreamConfig{ContentType: "application/json"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	const messageCount = 110_000 // Badger v4's default limit is about 105k entries.
	messages := make([][]byte, messageCount)
	for i := range messages {
		messages[i] = []byte(`"x"`)
	}

	if _, err := s.AppendBatch(t.Context(), "stream", messages, ""); !errors.Is(err, durablestream.ErrPayloadTooLarge) {
		t.Fatalf("AppendBatch error = %v, want ErrPayloadTooLarge", err)
	}
	result, err := s.Read(t.Context(), "stream", durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(result.Messages) != 0 {
		t.Fatalf("failed batch committed %d messages, want 0", len(result.Messages))
	}

	created, _, err := s.CreateWithMessages(t.Context(), "new-stream", durablestream.StreamConfig{ContentType: "application/json"}, messages)
	if created || !errors.Is(err, durablestream.ErrPayloadTooLarge) {
		t.Fatalf("CreateWithMessages = (created %v, err %v), want (false, ErrPayloadTooLarge)", created, err)
	}
	if _, err := s.Head(t.Context(), "new-stream"); !errors.Is(err, durablestream.ErrNotFound) {
		t.Fatalf("Head after failed create = %v, want ErrNotFound", err)
	}
}
