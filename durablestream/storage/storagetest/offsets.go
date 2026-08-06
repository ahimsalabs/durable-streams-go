package storagetest

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// testOffsetOrdering checks the core offset invariants: strictly increasing
// within a stream, and ordered by plain byte-wise string comparison so a client
// can compare two offsets without parsing them.
func testOffsetOrdering(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "offset-ordering"
	mustCreate(t, s, streamID)

	const count = 25
	offsets := make([]durablestream.Offset, 0, count)
	for i := range count {
		off := mustAppend(t, s, streamID, message(i))
		if len(offsets) > 0 && off.Compare(offsets[len(offsets)-1]) <= 0 {
			t.Fatalf("append %d returned offset %q, which does not sort after the previous offset %q", i, off, offsets[len(offsets)-1])
		}
		offsets = append(offsets, off)
	}

	// Sorting the offsets as opaque strings must reproduce append order:
	// that is what "lexicographically sortable" buys a client.
	asStrings := make([]string, len(offsets))
	for i, off := range offsets {
		asStrings[i] = off.String()
	}
	if !slices.IsSorted(asStrings) {
		t.Errorf("offsets are not in lexicographic order when compared as strings: %q", asStrings)
	}
}

// testOffsetsRoundTrip checks that the offsets Read reports are the ones Append
// handed out, and that feeding an offset back to Read resumes immediately after
// the corresponding message.
func testOffsetsRoundTrip(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "offset-round-trip"
	mustCreate(t, s, streamID)

	const count = 5
	appended := make([]durablestream.Offset, 0, count)
	want := make([]string, 0, count)
	for i := range count {
		appended = append(appended, mustAppend(t, s, streamID, message(i)))
		want = append(want, message(i))
	}

	res := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	assertPayloads(t, "Read from the start", payloads(res), want)
	assertOffsetsIncreasing(t, "Read from the start", res.Messages)

	for i, m := range res.Messages {
		if m.Offset != appended[i] {
			t.Errorf("message %d has offset %q, want the offset %q that Append returned for it", i, m.Offset, appended[i])
		}
	}
	if res.NextOffset != appended[count-1] {
		t.Errorf("NextOffset = %q after reading everything, want the last message's offset %q", res.NextOffset, appended[count-1])
	}
	if res.TailOffset != appended[count-1] {
		t.Errorf("TailOffset = %q, want the last message's offset %q", res.TailOffset, appended[count-1])
	}

	// Resuming from message i's offset must return everything after it.
	for i, off := range appended {
		resumed := mustRead(t, s, streamID, off, 0)
		assertPayloads(t, fmt.Sprintf("Read resuming from message %d's offset", i), payloads(resumed), want[i+1:])
	}
}

// testConcurrentAppends checks that appends racing on one stream are serialized:
// every writer gets its own offset and every message shows up exactly once.
func testConcurrentAppends(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "concurrent-appends"
	mustCreate(t, s, streamID)

	const (
		writers          = 8
		appendsPerWriter = 20
	)

	type appended struct {
		payload string
		offset  durablestream.Offset
	}
	results := make([][]appended, writers)
	errs := make([]error, writers)

	// All writers block on start so their appends overlap.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			mine := make([]appended, 0, appendsPerWriter)
			<-start
			for i := range appendsPerWriter {
				payload := fmt.Sprintf("writer-%d-msg-%02d", w, i)
				off, err := s.Append(t.Context(), streamID, []byte(payload), "")
				if err != nil {
					errs[w] = fmt.Errorf("append %q: %w", payload, err)
					return
				}
				mine = append(mine, appended{payload: payload, offset: off})
			}
			results[w] = mine
		})
	}
	close(start)
	wg.Wait()

	for w, err := range errs {
		if err != nil {
			t.Fatalf("writer %d failed: %v", w, err)
		}
	}

	// Every returned offset must be unique across writers.
	owner := make(map[durablestream.Offset]string, writers*appendsPerWriter)
	for _, mine := range results {
		for _, a := range mine {
			if prev, dup := owner[a.offset]; dup {
				t.Errorf("Append returned offset %q for both %q and %q; concurrent appends must get distinct offsets", a.offset, prev, a.payload)
				continue
			}
			owner[a.offset] = a.payload
		}
		// A single writer's own appends must still be ordered.
		for i := 1; i < len(mine); i++ {
			if mine[i].offset.Compare(mine[i-1].offset) <= 0 {
				t.Errorf("one writer's append offsets went backward: %q followed %q", mine[i].offset, mine[i-1].offset)
			}
		}
	}

	// Every message must be readable exactly once, at the offset Append reported.
	stored := drain(t, s, streamID, 0)
	assertOffsetsIncreasing(t, "reading back concurrent appends", stored)
	if len(stored) != writers*appendsPerWriter {
		t.Errorf("stream holds %d messages, want %d", len(stored), writers*appendsPerWriter)
	}
	seen := make(map[string]int, len(stored))
	for _, m := range stored {
		seen[string(m.Data)]++
		if want, ok := owner[m.Offset]; ok && want != string(m.Data) {
			t.Errorf("offset %q holds %q, but Append reported that offset for %q", m.Offset, m.Data, want)
		}
	}
	for _, mine := range results {
		for _, a := range mine {
			if got := seen[a.payload]; got != 1 {
				t.Errorf("message %q appears %d times in the stream, want exactly 1", a.payload, got)
			}
		}
	}
}

// testHeadReportsTail checks that Head agrees with Read about where the stream
// ends, for both an empty and a non-empty stream.
func testHeadReportsTail(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "head-tail"
	mustCreateConfig(t, s, streamID, durablestream.StreamConfig{ContentType: "application/json"})

	empty := mustHead(t, s, streamID)
	if empty.ContentType != "application/json" {
		t.Errorf("Head reported content type %q, want the configured %q", empty.ContentType, "application/json")
	}
	emptyRead := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if empty.NextOffset != emptyRead.TailOffset {
		t.Errorf("Head reported NextOffset %q for an empty stream, want the TailOffset %q that Read reports", empty.NextOffset, emptyRead.TailOffset)
	}
	// Reading from the tail of an empty stream must yield nothing.
	atTail := mustRead(t, s, streamID, empty.NextOffset, 0)
	if len(atTail.Messages) != 0 {
		t.Errorf("reading an empty stream from its tail returned %d messages, want 0", len(atTail.Messages))
	}

	mustAppend(t, s, streamID, message(0))
	last := mustAppend(t, s, streamID, message(1))
	info := mustHead(t, s, streamID)
	if info.NextOffset != last {
		t.Errorf("Head reported NextOffset %q, want the last appended offset %q", info.NextOffset, last)
	}
	full := mustRead(t, s, streamID, durablestream.ZeroOffset, 0)
	if info.NextOffset != full.TailOffset {
		t.Errorf("Head reported NextOffset %q but Read reported TailOffset %q; they must agree", info.NextOffset, full.TailOffset)
	}
}

func testHeadReportsLastSeq(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const (
		streamID = "head-last-seq"
		seq      = "producer-0001"
	)
	mustCreate(t, s, streamID)
	if _, err := s.Append(t.Context(), streamID, []byte("data"), seq); err != nil {
		t.Fatalf("Append with sequence: %v", err)
	}
	if got := mustHead(t, s, streamID).LastSeq; got != seq {
		t.Errorf("Head LastSeq = %q, want %q", got, seq)
	}
	if cfg.Reopen == nil {
		return
	}
	s = reopen(t, cfg, s)
	if got := mustHead(t, s, streamID).LastSeq; got != seq {
		t.Errorf("Head LastSeq after reopen = %q, want %q", got, seq)
	}
}

// testPagingByNextOffset checks that a client that keeps feeding NextOffset back
// to Read sees every message once, with no gaps and no repeats, even when the
// byte limit forces many small pages.
func testPagingByNextOffset(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const streamID = "paging"
	mustCreate(t, s, streamID)

	const count = 30
	want := make([]string, 0, count)
	for i := range count {
		mustAppend(t, s, streamID, message(i))
		want = append(want, message(i))
	}

	// A limit of one byte forces a page per message: no message is ever split,
	// so each page holds exactly one.
	paged := drain(t, s, streamID, 1)
	assertPayloads(t, "paging with a one-byte limit", messagePayloads(paged), want)
	assertOffsetsIncreasing(t, "paging with a one-byte limit", paged)

	// A limit that fits a few messages must produce the same sequence.
	batched := drain(t, s, streamID, len(message(0))*3)
	assertPayloads(t, "paging with a three-message limit", messagePayloads(batched), want)
}

// testStreamIsolation checks that streams do not leak messages into each other.
func testStreamIsolation(t *testing.T, cfg Config) {
	t.Parallel()
	s := newStorage(t, cfg)
	const (
		first  = "isolation-a"
		second = "isolation-b"
	)
	mustCreate(t, s, first)
	mustCreate(t, s, second)

	mustAppend(t, s, first, "a1")
	mustAppend(t, s, second, "b1")
	mustAppend(t, s, first, "a2")

	assertPayloads(t, "reading the first stream", payloads(mustRead(t, s, first, durablestream.ZeroOffset, 0)), []string{"a1", "a2"})
	assertPayloads(t, "reading the second stream", payloads(mustRead(t, s, second, durablestream.ZeroOffset, 0)), []string{"b1"})

	// Deleting one stream must leave the other intact.
	mustDelete(t, s, first)
	assertPayloads(t, "reading the second stream after deleting the first", payloads(mustRead(t, s, second, durablestream.ZeroOffset, 0)), []string{"b1"})
}
