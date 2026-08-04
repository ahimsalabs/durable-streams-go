// Package storagetest provides a conformance suite for implementations of
// [durablestream.Storage].
//
// The suite encodes the contract documented on the Storage interface. An
// implementation that passes it behaves interchangeably with the backends in
// this repository, so a new backend should wire it up as its first test:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, storagetest.Config{
//			New: func(t *testing.T) durablestream.Storage {
//				s := mystore.New()
//				t.Cleanup(func() { _ = s.Close() })
//				return s
//			},
//		})
//	}
//
// The suite never coordinates with sleeps: goroutines synchronize through
// channels, and the timeouts it does use only bound how long a broken
// implementation takes to fail. The sliding-TTL subtests are the exception —
// expiry is defined against a real clock, and the Storage contract offers no
// seam for injecting one — so they wait out short windows with margins wide
// enough that only a delay of a full second changes an outcome. They add a few
// seconds to a suite run.
package storagetest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// Config describes the implementation under test.
type Config struct {
	// New returns a fresh, empty Storage. It is called at least once per
	// subtest and must register cleanup (closing the storage, removing
	// directories) with t.Cleanup. Storages returned for different subtests
	// must not share state.
	New func(t *testing.T) durablestream.Storage

	// Reopen closes s and returns a Storage backed by the same durable state,
	// as a process restart would. Leave it nil for implementations that keep
	// no state across instances; the durability subtests are skipped then.
	//
	// The returned storage must register its own cleanup with t.Cleanup.
	Reopen func(t *testing.T, s durablestream.Storage) durablestream.Storage

	// MaxMessageSize is the largest message the implementation accepts, in
	// bytes. Zero means unbounded, which skips the payload-size subtests.
	// Keep it small: the suite allocates buffers of this size.
	MaxMessageSize int

	// FutureOffset is a well-formed offset strictly beyond the offsets a fresh
	// stream will reach during the suite. The past-tail subtest is skipped when
	// it is empty because offset syntax is backend-defined.
	FutureOffset durablestream.Offset

	// MalformedOffsets contains offsets the implementation's grammar rejects.
	// The malformed-offset subtest is skipped when this is empty because no
	// malformed token is portable across all backend-defined grammars.
	MalformedOffsets []durablestream.Offset

	// RequireIncarnationID makes the optional incarnation-identity tests fail
	// rather than skip when Head or Read leaves IncarnationID empty. Set it for
	// implementations that advertise this capability; leave it false for legacy
	// custom Storage implementations.
	RequireIncarnationID bool

	// RequireAtomicBatches makes the optional AtomicBatchStorage tests fail
	// rather than skip when New returns a Storage without that capability. Set
	// it for implementations that advertise atomic create/append batches.
	RequireAtomicBatches bool
}

// Run executes the conformance suite against the implementation described by
// cfg. Each subtest gets its own storage from cfg.New.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.New == nil {
		t.Fatal("storagetest: Config.New must be set")
	}
	for _, s := range suites {
		t.Run(s.name, func(t *testing.T) { s.run(t, cfg) })
	}
}

type suite struct {
	name string
	run  func(*testing.T, Config)
}

// suites lists every conformance subtest. Names read as the behavior being
// checked, so a failure line states which part of the contract broke.
var suites = []suite{
	// Offsets.
	{"append offsets increase and sort lexicographically", testOffsetOrdering},
	{"read replays the offsets append returned", testOffsetsRoundTrip},
	{"concurrent appends get distinct offsets and land exactly once", testConcurrentAppends},
	{"head reports the tail offset", testHeadReportsTail},
	{"paging by next offset yields every message once", testPagingByNextOffset},
	{"streams are isolated from each other", testStreamIsolation},
	{"incarnation identity is stable across head and reads", testIncarnationIDStable},
	{"incarnation identity changes after delete and recreate", testIncarnationIDChangesAfterRecreate},

	// Read semantics.
	{"both start sentinels read from the beginning", testStartSentinels},
	{"reading at or past the tail returns no messages and no error", testReadPastTail},
	{"zero limit returns every message", testLimitZero},
	{"limit stops at the byte budget", testLimitBudget},
	{"a message larger than the limit is returned whole", testLimitSingleOversizeMessage},
	{"negative limit is rejected", testNegativeLimit},
	{"read returns caller-owned data", testReadReturnsCallerOwnedData},
	{"append copies the data it is given", testAppendCopiesInput},
	{"malformed offsets are rejected", testMalformedOffset},

	// Sentinel errors.
	{"operations on a missing stream report not found", testMissingStream},
	{"operations on an expired stream report not found", testExpiredStream},
	{"empty appends are rejected", testEmptyAppend},
	{"sequence regression is rejected and appends nothing", testSequenceRegression},
	{"create is idempotent for a matching config", testCreateIdempotent},
	{"create conflicts on a different config", testCreateConflict},
	{"oversized messages are rejected", testMaxMessageSize},

	// Sliding TTL.
	{"create initializes the first sliding ttl deadline", testCreateInitializesSlidingTTL},
	{"touch extends a sliding ttl window", testTouchExtendsWindow},
	{"touch leaves an absolute expiry alone", testTouchLeavesAbsoluteExpiryAlone},
	{"reads and writes do not extend the window on their own", testNoImplicitWindowExtension},
	{"touch reports a missing or expired stream", testTouchMissingStream},
	{"touch is safe against concurrent create and delete", testTouchRacesLifecycle},

	// Context handling.
	{"read honors a cancelled context", testReadCancelled},
	{"wait honors a cancelled context", testWaitCancelled},
	{"wait honors a deadline", testWaitDeadline},
	{"mutations with a cancelled context do not hang or lie", testMutationsCancelled},

	// WaitForData.
	{"wait returns data that already exists", testWaitExistingData},
	{"wait wakes on append", testWaitWakesOnAppend},
	{"wait loses no wakeups under a steady producer", testWaitNoLostWakeups},
	{"wait wakes on delete", testWaitWakesOnDelete},
	{"wait wakes when the current stream incarnation expires", testWaitWakesOnExpiry},
	{"wait tracks an expiry renewed by touch", testWaitTracksTouchedExpiry},
	{"wait wakes on close", testWaitWakesOnClose},
	{"wait honors the byte limit", testWaitHonorsLimit},

	// Lifecycle.
	{"exactly one concurrent create reports creation", testConcurrentCreate},
	{"create replaces an expired stream", testExpiredStreamReplacement},
	{"delete removes the stream", testDeleteRemovesStream},
	{"a recreated stream never shows the deleted stream's data", testDeleteThenRecreate},
	{"delete is atomic against a concurrent create", testDeleteRaceCreate},
	{"close is idempotent", testCloseIdempotent},
	{"operations after close fail cleanly", testOperationsAfterClose},

	// Optional atomic-batch capability.
	{"atomic create publishes every initial message exactly once", testAtomicCreateWithMessages},
	{"atomic append publishes all messages or none", testAtomicAppendBatch},
	{"atomic append batches do not interleave", testAtomicAppendDoesNotInterleave},
	{"atomic batches stay generation-safe during delete and recreate", testAtomicBatchRacesLifecycle},

	// Durability (skipped unless Config.Reopen is set).
	{"appended data survives a reopen", testDurabilityAcrossReopen},
	{"deleted streams stay deleted across a reopen", testDeleteSurvivesReopen},
	{"atomic batches and their next offset survive a reopen", testAtomicBatchSurvivesReopen},
}

// waitTimeout bounds how long a subtest waits for an operation that a correct
// implementation completes immediately. It only decides how quickly a broken
// implementation fails, so it is deliberately generous.
const waitTimeout = 30 * time.Second

// runBounded runs fn on its own goroutine and fails the test if it has not
// returned by waitTimeout, rather than hanging the package.
func runBounded(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatalf("%s did not return within %s", what, waitTimeout)
	}
}

// newStorage returns a fresh storage for a subtest.
func newStorage(t *testing.T, cfg Config) durablestream.Storage {
	t.Helper()
	s := cfg.New(t)
	if s == nil {
		t.Fatal("storagetest: Config.New returned nil")
	}
	return s
}

// reopen restarts the storage, skipping the subtest when the implementation
// keeps no durable state.
func reopen(t *testing.T, cfg Config, s durablestream.Storage) durablestream.Storage {
	t.Helper()
	if cfg.Reopen == nil {
		t.Skip("storagetest: Config.Reopen not set; skipping durability subtest")
	}
	reopened := cfg.Reopen(t, s)
	if reopened == nil {
		t.Fatal("storagetest: Config.Reopen returned nil")
	}
	return reopened
}

// textConfig is the stream configuration used by subtests that do not care
// about configuration.
func textConfig() durablestream.StreamConfig {
	return durablestream.StreamConfig{ContentType: "text/plain"}
}

func mustCreate(t *testing.T, s durablestream.Storage, streamID string) {
	t.Helper()
	mustCreateConfig(t, s, streamID, textConfig())
}

func mustCreateConfig(t *testing.T, s durablestream.Storage, streamID string, cfg durablestream.StreamConfig) {
	t.Helper()
	created, err := s.Create(t.Context(), streamID, cfg)
	if err != nil {
		t.Fatalf("Create(%q): unexpected error: %v", streamID, err)
	}
	if !created {
		t.Fatalf("Create(%q) reported created=false, want true for a stream that does not exist yet", streamID)
	}
}

func mustAppend(t *testing.T, s durablestream.Storage, streamID, data string) durablestream.Offset {
	t.Helper()
	off, err := s.Append(t.Context(), streamID, []byte(data), "")
	if err != nil {
		t.Fatalf("Append(%q, %q): unexpected error: %v", streamID, data, err)
	}
	if off.IsZero() || off == "-1" {
		t.Fatalf("Append(%q, %q) returned the start-of-stream sentinel %q, want a real offset", streamID, data, off)
	}
	return off
}

func mustRead(t *testing.T, s durablestream.Storage, streamID string, offset durablestream.Offset, limit int) *durablestream.ReadResult {
	t.Helper()
	res, err := s.Read(t.Context(), streamID, offset, limit)
	if err != nil {
		t.Fatalf("Read(%q, offset %q, limit %d): unexpected error: %v", streamID, offset, limit, err)
	}
	if res == nil {
		t.Fatalf("Read(%q, offset %q, limit %d) returned a nil result with a nil error", streamID, offset, limit)
	}
	return res
}

func mustHead(t *testing.T, s durablestream.Storage, streamID string) *durablestream.StreamInfo {
	t.Helper()
	info, err := s.Head(t.Context(), streamID)
	if err != nil {
		t.Fatalf("Head(%q): unexpected error: %v", streamID, err)
	}
	if info == nil {
		t.Fatalf("Head(%q) returned a nil result with a nil error", streamID)
	}
	return info
}

func mustDelete(t *testing.T, s durablestream.Storage, streamID string) {
	t.Helper()
	if err := s.Delete(t.Context(), streamID); err != nil {
		t.Fatalf("Delete(%q): unexpected error: %v", streamID, err)
	}
}

// payloads returns the message data as strings, for readable comparisons.
func payloads(res *durablestream.ReadResult) []string {
	out := make([]string, 0, len(res.Messages))
	for _, m := range res.Messages {
		out = append(out, string(m.Data))
	}
	return out
}

// messagePayloads is payloads for a bare message slice.
func messagePayloads(msgs []durablestream.StoredMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Data))
	}
	return out
}

// maxDrainPages bounds drain so a NextOffset that never advances fails the
// subtest instead of looping forever.
const maxDrainPages = 10_000

// drain reads a whole stream from the start by following NextOffset, and
// returns every message it saw.
func drain(t *testing.T, s durablestream.Storage, streamID string, limit int) []durablestream.StoredMessage {
	t.Helper()
	var all []durablestream.StoredMessage
	offset := durablestream.ZeroOffset
	for page := 0; ; page++ {
		if page == maxDrainPages {
			t.Fatalf("draining %q did not terminate within %d reads: NextOffset is not advancing", streamID, maxDrainPages)
		}
		res := mustRead(t, s, streamID, offset, limit)
		if len(res.Messages) == 0 {
			if res.NextOffset != offset && page > 0 {
				t.Errorf("Read(%q, offset %q) returned no messages but moved NextOffset to %q, want the requested offset", streamID, offset, res.NextOffset)
			}
			return all
		}
		if page > 0 && res.NextOffset.Compare(offset) <= 0 {
			t.Fatalf("Read(%q, offset %q) returned %d messages but NextOffset %q did not advance", streamID, offset, len(res.Messages), res.NextOffset)
		}
		all = append(all, res.Messages...)
		offset = res.NextOffset
	}
}

// assertOffsetsIncreasing checks that msgs carry strictly increasing,
// lexicographically ordered offsets. Gaps are allowed by the contract.
func assertOffsetsIncreasing(t *testing.T, what string, msgs []durablestream.StoredMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Offset.Compare(msgs[i-1].Offset) <= 0 {
			t.Errorf("%s: offset %q at index %d does not sort after %q at index %d; offsets must be strictly increasing and lexicographically sortable",
				what, msgs[i].Offset, i, msgs[i-1].Offset, i-1)
		}
	}
}

func assertErrorIs(t *testing.T, op string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Errorf("%s returned error %v, want one matching %v", op, err, want)
	}
}

func assertPayloads(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s returned %d messages %q, want %d messages %q", what, len(got), got, len(want), want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s: message %d is %q, want %q (full result %q)", what, i, got[i], want[i], got)
			return
		}
	}
}

// message builds a distinguishable payload of a known size.
func message(i int) string {
	return fmt.Sprintf("message-%04d", i)
}
