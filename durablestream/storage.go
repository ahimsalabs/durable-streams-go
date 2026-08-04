package durablestream

import (
	"context"
	"time"
)

// StreamConfig contains creation-time configuration.
type StreamConfig struct {
	ContentType string
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry; Create derives now+TTL when TTL is positive
	IsPrivate   bool          // If true, use Cache-Control: private (Section 10.1)
}

// StreamInfo contains metadata about a stream.
type StreamInfo struct {
	ContentType string
	NextOffset  Offset
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry
	IsPrivate   bool          // If true, use Cache-Control: private (Section 10.1)

	// IncarnationID is an opaque storage-internal token for this exact
	// incarnation of the stream. A non-empty value is immutable for the
	// incarnation's lifetime and differs from every incarnation created before
	// or after it under the same stream ID. Callers must not interpret it or
	// assume it is safe to place directly in a protocol header. An empty value
	// means the Storage implementation does not provide incarnation identity.
	IncarnationID string
}

// StoredMessage represents a single message in a stream.
// Each append operation creates one StoredMessage (or multiple if JSON array is flattened).
type StoredMessage struct {
	// Data holds the raw bytes of this message. Data returned by a Storage
	// belongs to the caller: implementations must return a copy, and callers
	// may retain or mutate it freely without affecting stored data.
	Data []byte

	// Offset is the position *after* this message: passing it back to Read or
	// WaitForData resumes with the following message.
	Offset Offset
}

// ReadResult contains messages from a storage read.
type ReadResult struct {
	// Messages holds the messages in ascending offset order. It is empty (not
	// an error) when the read starts at or past the tail.
	Messages []StoredMessage

	// NextOffset is the offset to pass to the next Read. It is the Offset of
	// the last returned message, or — when no messages were returned — the
	// requested offset, normalized away from the "" and "-1" start sentinels.
	// NextOffset never moves backward relative to the requested offset.
	NextOffset Offset

	// TailOffset is the offset after the last message currently in the stream,
	// so a caller is up to date exactly when NextOffset equals TailOffset.
	TailOffset Offset

	// IncarnationID identifies the exact stream incarnation read, with the same
	// opaque/optional semantics as [StreamInfo.IncarnationID]. When non-empty it
	// matches the value Head reports for that incarnation.
	IncarnationID string
}

// Storage defines the interface for stream persistence.
//
// # Contract
//
// The rules below apply to every implementation. The conformance suite in
// durablestream/storage/storagetest exercises them; new implementations should
// run it.
//
// Goroutine safety: all methods are safe to call concurrently from multiple
// goroutines, on the same stream or on different streams.
//
// Offsets: an offset is an opaque token (see [Offset]). Within one stream,
// offsets are strictly increasing and lexicographically sortable, so
// [Offset.Compare] orders any two offsets of a stream by position. Offsets MAY
// contain gaps: a failed Append can consume an offset that no message ever
// occupies, and readers must tolerate that. The empty offset and the string
// "-1" are the two "start of stream" sentinels accepted on input; they are
// never returned. An offset from one stream is meaningless in another,
// including in a stream recreated with the same ID after Delete.
//
// Ownership of byte slices: input slices ([]byte passed to Append) are only
// borrowed for the duration of the call, so implementations MUST copy any data
// they retain. Output slices (StoredMessage.Data) are owned by the caller, so
// implementations MUST NOT return memory that aliases their stored state.
//
// Context handling: Read and WaitForData MUST honor cancellation and return
// ctx.Err() promptly. Create, Append and Delete are durable mutations and MAY
// complete despite a cancelled context rather than report a committed write as
// failed; whether they check ctx is implementation-defined, but they MUST NOT
// block indefinitely after cancellation. Callers therefore cannot conclude from
// a ctx.Err() return that a mutation did not take effect.
//
// Sentinel errors: implementations return errors that satisfy errors.Is against
// the sentinels documented per method. They MAY wrap them with context.
// [ErrGone] is reserved for offsets dropped by retention or compaction; reading
// past the tail is not an error (see Read).
//
// Stream IDs: implementations MAY reject stream IDs they cannot represent with
// [ErrBadRequest] (badgerstore rejects the empty string and IDs containing
// ':'). Portable callers should use non-empty IDs without ':'.
//
// Incarnation identity: implementations SHOULD populate StreamInfo.IncarnationID
// and ReadResult.IncarnationID. Any non-empty value MUST remain stable for the
// exact incarnation, including across a durable backend's reopen, and MUST NOT
// be reused when an expired or deleted stream is recreated with the same ID.
// Empty preserves compatibility with Storage implementations that cannot expose
// an incarnation token.
//
// Expiry: a stream whose StreamConfig has elapsed its ExpiresAt behaves as if
// it does not exist — Append, Read, Head and WaitForData report [ErrNotFound] —
// and it may be replaced by Create.
//
// Sliding TTL: a stream created with a positive StreamConfig.TTL expires after
// being idle for that long. When ExpiresAt is zero, Create initializes the first
// deadline to its current time plus TTL. Every protocol read or write restarts
// the countdown (Section 5.1). Storage does not decide what counts as activity: Read, Append
// and WaitForData never move ExpiresAt on their own, and only an explicit
// [Storage.Touch] extends the window. That keeps the protocol rule — reads and
// writes reset the countdown, HEAD does not — in the one place that knows which
// request it is serving, and lets a caller extend the window for a request that
// performs no storage operation at all (a live read that only waits, or a
// close-only append).
type Storage interface {
	// Create creates a new stream. Returns (true, nil) if newly created.
	// Returns (false, nil) if a live stream already exists whose config
	// satisfies StreamConfig.Matches (idempotent replay).
	// Returns (false, error) wrapping ErrConflict if a live stream exists with
	// a different config.
	//
	// An existing stream that has expired is replaced: Create returns
	// (true, nil), the replacement starts empty, and no data from the previous
	// incarnation is ever visible through it. Replacement is atomic with
	// respect to concurrent Create and Delete of the same stream ID: exactly
	// one concurrent Create observes created=true, and a concurrent Delete
	// either removes the old incarnation or the new one, never leaves a hybrid.
	//
	// Errors: ErrConflict, ErrBadRequest (invalid stream ID), ErrClosed.
	Create(ctx context.Context, streamID string, cfg StreamConfig) (created bool, err error)

	// Append writes data to a stream and returns the offset after the appended
	// message. seq is an optional deduplication sequence number (Section 5.2);
	// when non-empty it must sort lexicographically after the last seq accepted
	// for the stream, otherwise Append fails with ErrConflict and writes
	// nothing.
	//
	// Append is atomic: on error nothing is appended and no partial message
	// becomes visible. An error MAY still consume an offset, leaving a gap in
	// the offset space; the returned offset of a successful Append is always
	// greater than that of every message already in the stream.
	//
	// Concurrent Appends to one stream are serialized: each receives a distinct
	// offset, and every message appears exactly once in subsequent reads.
	//
	// The data slice is only valid for the duration of the call; the caller may
	// reuse or modify it after Append returns, so implementations MUST copy any
	// data they retain.
	//
	// Empty data is rejected with ErrBadRequest. Implementations MAY impose a
	// maximum message size and reject larger messages with ErrPayloadTooLarge.
	//
	// A successful Append makes the message visible to Read and wakes every
	// WaitForData caller waiting at or before the new message.
	//
	// Errors: ErrNotFound (no such stream, or expired), ErrBadRequest (empty
	// data, invalid stream ID), ErrPayloadTooLarge, ErrConflict (seq
	// regression, or a lost write race the caller may retry), ErrClosed.
	Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error)

	// Read returns messages positioned strictly after offset, in ascending
	// offset order. The empty offset and "-1" both mean "start of stream".
	//
	// limit is the maximum total number of message bytes to return: 0 means
	// unlimited, and a negative limit is rejected with ErrBadRequest. Messages
	// are never split, so a single message larger than limit is returned whole
	// as the only message.
	//
	// Reading at or past the tail is not an error: Read returns an empty
	// Messages slice with NextOffset equal to the requested offset, so a poller
	// can call Read again with the same offset. ErrGone is returned only when
	// the offset predates the earliest retained position because of retention
	// or compaction, which no current implementation performs.
	//
	// Returned message data belongs to the caller and never aliases stored
	// state: mutating it cannot change what a later Read returns.
	//
	// Errors: ErrNotFound (no such stream, or expired), ErrBadRequest
	// (malformed offset, negative limit, invalid stream ID), ErrGone,
	// ErrClosed, ctx.Err().
	Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Head returns stream metadata without reading data (Section 5.5).
	// StreamInfo.NextOffset is the stream's tail offset, matching the
	// TailOffset a Read would report.
	//
	// Errors: ErrNotFound (no such stream, or expired — use this for existence
	// checks), ErrBadRequest (invalid stream ID), ErrClosed.
	Head(ctx context.Context, streamID string) (*StreamInfo, error)

	// Touch restarts a stream's sliding TTL window: the stream's expiry moves
	// to exactly now plus its StreamConfig.TTL. It is the caller's way of
	// saying "this stream saw activity"; see the Sliding TTL note above for why
	// storage does not infer that from Read and Append.
	//
	// Touch is a no-op, reporting nil, for a live stream whose TTL is zero: a
	// stream with an absolute Stream-Expires-At (or with no expiry at all) has
	// no sliding window to restart, and Touch must never move an absolute
	// deadline.
	//
	// Touch never resurrects a stream: an expired or missing stream reports
	// ErrNotFound and stays expired.
	//
	// Errors: ErrNotFound (no such stream, or expired), ErrBadRequest (invalid
	// stream ID), ErrClosed.
	Touch(ctx context.Context, streamID string) error

	// Delete removes a stream (Section 5.4). Returns ErrNotFound if the stream
	// does not exist. An expired stream is still deleted successfully: expiry
	// hides a stream from readers, but its record is there to reclaim.
	//
	// Delete is atomic with respect to a concurrent Create of the same stream
	// ID: once Delete returns nil the stream is gone, and a stream subsequently
	// created with that ID starts empty and never observes data, offsets or
	// deduplication state from the deleted incarnation — including when the
	// implementation reclaims the deleted bytes lazily in the background.
	//
	// Delete wakes every WaitForData caller on the stream, which then report
	// ErrNotFound.
	//
	// Errors: ErrNotFound, ErrBadRequest (invalid stream ID), ErrClosed.
	Delete(ctx context.Context, streamID string) error

	// WaitForData returns messages after offset, blocking until at least one is
	// available. It returns immediately if data already exists at offset, and
	// otherwise returns the same result a Read would once data arrives. offset
	// and limit have the same meaning as in Read.
	//
	// Wakeups are not lost: a waiter is released by any Append that commits
	// after the waiter's last unsuccessful read, so a caller that loops on
	// WaitForData observes every message without polling. Delete and Close also
	// release waiters.
	//
	// Errors: ctx.Err() on cancellation or deadline, ErrNotFound (stream absent,
	// expired, or deleted while waiting), ErrClosed (storage closed while
	// waiting), ErrBadRequest, ErrGone.
	WaitForData(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Close releases resources. It is idempotent: later calls are no-ops and
	// report no error, so only the first call's result is meaningful.
	//
	// Close releases every blocked WaitForData caller with an error satisfying
	// errors.Is(err, ErrClosed). Calls made after Close MUST NOT panic or
	// corrupt data; whether they are rejected with ErrClosed or still served is
	// implementation-defined (badgerstore rejects them; memorystorage continues
	// to serve reads and writes from memory), so callers must not depend on
	// either behavior.
	Close() error
}

// AtomicBatchStorage is an optional Storage capability for committing multiple
// messages as one mutation. Callers discover it with a type assertion; Storage
// implementations that do not provide it remain valid.
//
// Both methods borrow every message byte slice only for the duration of the
// call. A successful operation retains its own copy, and an error never makes a
// partial batch visible. Implementations may reserve offsets before a durable
// commit, so a failed operation may leave gaps just like [Storage.Append]. In
// addition to per-message limits, an implementation may enforce an aggregate
// batch or atomic-transaction capacity limit; exceeding either kind of limit
// reports ErrPayloadTooLarge without exposing a partial mutation.
type AtomicBatchStorage interface {
	Storage

	// CreateWithMessages creates a stream and its initial messages atomically.
	// Readers observe either no stream or the new stream with the entire batch;
	// they never observe the new configuration with only a prefix of messages.
	// Messages are stored in slice order and nextOffset is the offset after the
	// final message, or the new stream's zero offset when messages is empty.
	//
	// The Create idempotency rules still apply. If a live stream with matching
	// configuration already exists, the method returns created=false and its
	// current tail offset; messages is neither compared with existing data nor
	// appended again (although every message is still validated). A different
	// configuration reports ErrConflict. An expired stream is replaced atomically
	// and reports created=true.
	//
	// An empty messages slice is valid and is equivalent to Create. Individual
	// messages must be non-empty and must satisfy the implementation's per-message
	// and aggregate batch limits. The entire batch is validated before the stream
	// becomes visible.
	//
	// Errors are the union of Storage.Create and Storage.Append errors.
	CreateWithMessages(ctx context.Context, streamID string, cfg StreamConfig, messages [][]byte) (created bool, nextOffset Offset, err error)

	// AppendBatch appends every message as one atomic, ordered mutation and
	// returns the offset after the final message. Concurrent Append and
	// AppendBatch calls cannot interleave messages inside the batch.
	//
	// seq applies once to the whole batch: it is validated before commit and is
	// advanced only if every message commits. An empty batch, or a batch
	// containing an empty message, reports ErrBadRequest. Per-message and
	// aggregate batch limits are checked without committing a partial batch.
	//
	// Errors are the same as Storage.Append.
	AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (Offset, error)
}
