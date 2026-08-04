package storagetest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// fixedSizeMessage returns a payload of exactly fixedMessageSize bytes, so byte
// budgets in the limit subtests are exact.
const fixedMessageSize = 10

func fixedSizeMessage(i int) string {
	return fmt.Sprintf("%010d", i)
}

// testStartSentinels checks that the two "start of stream" sentinels behave
// identically, and that neither is ever handed back to the caller.
func testStartSentinels(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "start-sentinels"
	mustCreate(t, s, streamID)

	// On an empty stream both sentinels must produce a usable, normalized
	// offset rather than echoing the sentinel back.
	for _, sentinel := range []durablestream.Offset{durablestream.ZeroOffset, "-1"} {
		res := mustRead(t, s, streamID, sentinel, 0)
		if len(res.Messages) != 0 {
			t.Errorf("Read(offset %q) on an empty stream returned %d messages, want 0", sentinel, len(res.Messages))
		}
		if res.NextOffset.IsZero() || res.NextOffset == "-1" {
			t.Errorf("Read(offset %q) returned NextOffset %q, want a normalized offset rather than a start sentinel", sentinel, res.NextOffset)
		}
		// The normalized offset must be stable: reading from it again returns
		// the same empty result rather than rewinding.
		again := mustRead(t, s, streamID, res.NextOffset, 0)
		if len(again.Messages) != 0 {
			t.Errorf("re-reading an empty stream from its own NextOffset %q returned %d messages, want 0", res.NextOffset, len(again.Messages))
		}
	}

	mustAppend(t, s, streamID, "first")
	mustAppend(t, s, streamID, "second")

	want := []string{"first", "second"}
	for _, sentinel := range []durablestream.Offset{durablestream.ZeroOffset, "-1"} {
		res := mustRead(t, s, streamID, sentinel, 0)
		assertPayloads(t, fmt.Sprintf("Read(offset %q)", sentinel), payloads(res), want)
	}
}

// testReadPastTail checks that a reader ahead of the writer is told to wait, not
// told the data is gone. ErrGone is reserved for retention and compaction.
func testReadPastTail(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "past-tail"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "first")
	tail := mustAppend(t, s, streamID, "second")

	t.Run("at the tail", func(t *testing.T) {
		res := mustRead(t, s, streamID, tail, 0)
		if len(res.Messages) != 0 {
			t.Errorf("reading from the tail offset %q returned %d messages, want 0", tail, len(res.Messages))
		}
		if res.NextOffset != tail {
			t.Errorf("reading from the tail offset %q returned NextOffset %q, want the requested offset so a poller can retry", tail, res.NextOffset)
		}
		if res.TailOffset != tail {
			t.Errorf("TailOffset = %q, want %q", res.TailOffset, tail)
		}
	})

	t.Run("far past the tail", func(t *testing.T) {
		if cfg.FutureOffset.IsZero() {
			t.Skip("Config.FutureOffset is not set")
		}
		// A well-formed offset the stream has not reached: the caller is ahead,
		// which is a wait condition rather than an error.
		ahead := cfg.FutureOffset
		res, err := s.Read(t.Context(), streamID, ahead, 0)
		if err != nil {
			t.Fatalf("Read past the tail returned error %v, want an empty result; ErrGone is only for retention or compaction", err)
		}
		if len(res.Messages) != 0 {
			t.Errorf("reading past the tail returned %d messages, want 0", len(res.Messages))
		}
		if res.NextOffset != ahead {
			t.Errorf("reading past the tail returned NextOffset %q, want the requested offset %q", res.NextOffset, ahead)
		}
		if res.TailOffset != tail {
			t.Errorf("TailOffset = %q while reading past the tail, want the real tail %q", res.TailOffset, tail)
		}
	})
}

// testLimitZero checks that a zero limit means unlimited.
func testLimitZero(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "limit-zero"
	mustCreate(t, s, streamID)

	const count = 12
	want := make([]string, 0, count)
	for i := range count {
		mustAppend(t, s, streamID, fixedSizeMessage(i))
		want = append(want, fixedSizeMessage(i))
	}

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "Read with limit 0", payloads(res), want)
}

// testLimitBudget checks that limit caps the total bytes returned, counted over
// whole messages.
func testLimitBudget(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "limit-budget"
	mustCreate(t, s, streamID)

	const count = 5
	for i := range count {
		mustAppend(t, s, streamID, fixedSizeMessage(i))
	}

	tests := []struct {
		name  string
		limit int
		want  []string
	}{
		{
			name:  "a limit below one message still returns one",
			limit: 1,
			want:  []string{fixedSizeMessage(0)},
		},
		{
			name:  "a limit of exactly one message returns one",
			limit: fixedMessageSize,
			want:  []string{fixedSizeMessage(0)},
		},
		{
			name:  "a limit between message boundaries stops at the last whole message that fits",
			limit: fixedMessageSize*2 + fixedMessageSize/2,
			want:  []string{fixedSizeMessage(0), fixedSizeMessage(1)},
		},
		{
			name:  "a limit of exactly three messages returns three",
			limit: fixedMessageSize * 3,
			want:  []string{fixedSizeMessage(0), fixedSizeMessage(1), fixedSizeMessage(2)},
		},
		{
			name:  "a limit beyond the stream returns everything",
			limit: fixedMessageSize * (count + 10),
			want:  []string{fixedSizeMessage(0), fixedSizeMessage(1), fixedSizeMessage(2), fixedSizeMessage(3), fixedSizeMessage(4)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mustRead(t, s, streamID, durablestream.ZeroOffset, tt.limit)
			assertPayloads(t, fmt.Sprintf("Read with limit %d", tt.limit), payloads(res), tt.want)
			if len(res.Messages) > 0 && res.NextOffset != res.Messages[len(res.Messages)-1].Offset {
				t.Errorf("NextOffset = %q, want the last returned message's offset %q", res.NextOffset, res.Messages[len(res.Messages)-1].Offset)
			}
		})
	}
}

// testLimitSingleOversizeMessage checks that a message larger than the limit is
// still delivered whole: messages are never split, so a small limit can never
// stall a reader.
func testLimitSingleOversizeMessage(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "limit-oversize"
	mustCreate(t, s, streamID)

	big := string(bytes.Repeat([]byte("x"), 4096))
	mustAppend(t, s, streamID, big)
	mustAppend(t, s, streamID, "after")

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 8)
	assertPayloads(t, "Read with a limit far below the first message", payloads(res), []string{big})

	// The reader can still make progress past it.
	rest := mustRead(t, s, streamID, res.NextOffset, 8)
	assertPayloads(t, "Read after the oversized message", payloads(rest), []string{"after"})
}

// testNegativeLimit checks that a negative limit is a client error rather than a
// synonym for unlimited.
func testNegativeLimit(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "negative-limit"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "data")

	for _, limit := range []int{-1, -4096} {
		_, err := s.Read(t.Context(), streamID, durablestream.ZeroOffset, limit)
		assertErrorIs(t, fmt.Sprintf("Read with limit %d", limit), err, durablestream.ErrBadRequest)

		_, err = s.WaitForData(t.Context(), streamID, durablestream.ZeroOffset, limit)
		assertErrorIs(t, fmt.Sprintf("WaitForData with limit %d", limit), err, durablestream.ErrBadRequest)
	}
}

// testReadReturnsCallerOwnedData checks that the caller can keep and mutate what
// Read gives it without corrupting the stream or another reader's copy.
func testReadReturnsCallerOwnedData(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "caller-owned"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "original")
	mustAppend(t, s, streamID, "second")

	first := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	second := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)

	// Scribble over everything the first read handed back.
	for i := range first.Messages {
		for j := range first.Messages[i].Data {
			first.Messages[i].Data[j] = '!'
		}
	}

	assertPayloads(t, "a read taken before another read was mutated", payloads(second), []string{"original", "second"})
	assertPayloads(t, "re-reading after mutating a previous result", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"original", "second"})

	// WaitForData returns the same kind of result and must own its data too.
	waited, err := s.WaitForData(t.Context(), streamID, durablestream.ZeroOffset, 0)
	if err != nil {
		t.Fatalf("WaitForData on a stream that already has data: %v", err)
	}
	for i := range waited.Messages {
		for j := range waited.Messages[i].Data {
			waited.Messages[i].Data[j] = '?'
		}
	}
	assertPayloads(t, "re-reading after mutating a WaitForData result", payloads(mustRead(t, s, streamID, durablestream.ZeroOffset, 0)), []string{"original", "second"})
}

// testAppendCopiesInput checks that the buffer handed to Append is only
// borrowed: a caller reusing its buffer must not rewrite stored messages.
func testAppendCopiesInput(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "append-copies"
	mustCreate(t, s, streamID)

	// One buffer, reused across appends the way a pooled writer would.
	buf := []byte("first ")
	if _, err := s.Append(t.Context(), streamID, buf, ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	copy(buf, "second")
	if _, err := s.Append(t.Context(), streamID, buf, ""); err != nil {
		t.Fatalf("Append: %v", err)
	}
	copy(buf, "xxxxxx")

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "reading after the append buffer was reused", payloads(res), []string{"first ", "second"})
}

// testMalformedOffset checks that unparseable offsets are rejected as client
// errors by every method that accepts one.
func testMalformedOffset(t *testing.T, cfg Config) {
	t.Parallel()
	if len(cfg.MalformedOffsets) == 0 {
		t.Skip("Config.MalformedOffsets is not set")
	}
	s := newStorage(t, cfg)
	const streamID = "malformed-offset"
	mustCreate(t, s, streamID)
	mustAppend(t, s, streamID, "data")

	for _, offset := range cfg.MalformedOffsets {
		t.Run(string(offset), func(t *testing.T) {
			_, err := s.Read(t.Context(), streamID, offset, 0)
			assertErrorIs(t, fmt.Sprintf("Read(offset %q)", offset), err, durablestream.ErrBadRequest)

			_, err = s.WaitForData(t.Context(), streamID, offset, 0)
			assertErrorIs(t, fmt.Sprintf("WaitForData(offset %q)", offset), err, durablestream.ErrBadRequest)
		})
	}
}
