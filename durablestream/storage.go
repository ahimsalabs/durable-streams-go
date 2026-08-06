package durablestream

import (
	"context"
	"io"
	"time"
)

// StreamConfig contains creation-time configuration.
type StreamConfig struct {
	ContentType string
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry; Create derives now+TTL when TTL is positive
	IsPrivate   bool          // If true, use Cache-Control: private (Section 10.1)
	Closed      bool          // If true, the stream is created at permanent EOF
}

// StreamInfo contains metadata about a stream.
type StreamInfo struct {
	ContentType string
	NextOffset  Offset
	// LastSeq is the last non-empty producer sequence accepted for this stream.
	// It is empty when no sequence has been accepted. Persistent backends retain
	// it across reopen so producers can use Head to reseed after a restart.
	LastSeq   string
	TTL       time.Duration // Zero means no TTL
	ExpiresAt time.Time     // Zero means no expiry
	IsPrivate bool          // If true, use Cache-Control: private (Section 10.1)
	Closed    bool          // True after the stream has reached permanent EOF

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

	// Closed reports whether this stream incarnation has reached permanent EOF.
	// It describes the same atomic snapshot as Messages and TailOffset. A closed
	// stream remains readable, but no later message can appear after TailOffset.
	Closed bool
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
// never returned. An offset from one independent stream is meaningless in
// another, including in a stream recreated with the same ID after Delete. The
// intentional exception is a ForkStorage lineage: a fork preserves its
// source's offset space through the divergence boundary.
//
// Ownership of byte slices: input slices ([]byte passed to Append) are only
// borrowed for the duration of the call, so implementations MUST copy any data
// they retain. Output slices (StoredMessage.Data) are owned by the caller, so
// implementations MUST NOT return memory that aliases their stored state.
//
// Context handling: Read and WaitForData MUST honor cancellation and return
// ctx.Err() promptly. Create, Append, Delete, and CloseStream are durable
// mutations and MAY complete despite a cancelled context rather than report a
// committed write as failed; whether they check ctx is implementation-defined,
// but they MUST NOT block indefinitely after cancellation. Callers therefore
// cannot conclude from a ctx.Err() return that a mutation did not take effect.
//
// Sentinel errors: implementations return errors that satisfy errors.Is against
// the sentinels documented per method. They MAY wrap them with context.
// [ErrGone] is reserved for offsets dropped by retention or compaction; reading
// past the tail is not an error (see Read).
// [ErrStreamClosed] is the persistent EOF state of one stream and is distinct
// from [ErrClosed], which reports that the Storage itself has been shut down.
// [ErrSoftDeleted] is returned only by implementations that offer ForkStorage;
// it identifies a deleted, or optionally expired, path whose data is
// temporarily retained for descendants.
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
// Expiry: an unreferenced stream whose StreamConfig has elapsed its ExpiresAt
// behaves as if it does not exist — Append, Read, Head and WaitForData report
// [ErrNotFound] — and it may be replaced by Create. ForkStorage implementations
// must preserve an expired node's data while descendants still reference it,
// but may choose either lifecycle policy for its public path: retire the path
// immediately (ErrNotFound, and Create may reuse it), or retain the path like a
// soft deletion (direct operations report ErrSoftDeleted, and Create reports
// ErrConflict). The latter state ends when the final descendant reference is
// released. Callers must accept either policy for a referenced expired node.
//
// Sliding TTL: a stream created with a positive StreamConfig.TTL expires after
// being idle for that long. When ExpiresAt is zero, Create initializes the first
// deadline to its current time plus TTL. Every protocol read or write restarts
// the countdown (Section 5.1). Storage does not decide what counts as activity:
// Read, Append and WaitForData never move ExpiresAt on their own, and only an
// explicit [Storage.Touch] extends the window. That keeps the protocol rule —
// reads and writes reset the countdown, HEAD does not — in the one place that
// knows which request it is serving, and lets a caller extend the window for a
// request that performs no storage operation at all (a live read that only
// waits, or a close-only append).
type Storage interface {
	// Create creates a new stream. Returns (true, nil) if newly created.
	// Returns (false, nil) if a live stream already exists whose config
	// satisfies StreamConfig.Matches (idempotent replay).
	// Returns (false, error) wrapping ErrConflict if a live stream exists with
	// a different config.
	//
	// An existing unreferenced stream that has expired is replaced: Create
	// returns (true, nil), the replacement starts empty, and no data from the
	// previous incarnation is ever visible through it. A referenced expired
	// ForkStorage node may instead reserve its path and make Create report
	// ErrConflict until its descendants release it, as described under Expiry
	// above. Replacement, when allowed, is atomic with respect to concurrent
	// Create and Delete of the same stream ID: exactly one concurrent Create
	// observes created=true, and a concurrent Delete either removes the old
	// incarnation or the new one, never leaves a hybrid.
	//
	// Errors: ErrConflict, ErrBadRequest (invalid stream ID), ErrClosed.
	Create(ctx context.Context, streamID string, cfg StreamConfig) (created bool, err error)

	// Append writes data to a stream and returns the offset after the appended
	// message. seq is an optional deduplication sequence number (Section 5.2);
	// when non-empty it must sort lexicographically after the last seq accepted
	// for the stream, otherwise Append fails with ErrSequenceConflict and
	// ErrConflict and writes nothing. Callers can inspect SequenceConflictError
	// for the accepted sequence and, when known, its final offset.
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
	// Errors: ErrNotFound (no such stream, or an expiry whose path was retired),
	// ErrBadRequest (empty data, invalid stream ID), ErrPayloadTooLarge,
	// ErrConflict (seq regression, or a lost write race the caller may retry),
	// ErrStreamClosed (the stream has reached permanent EOF), ErrSoftDeleted,
	// ErrClosed (storage shutdown).
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
	// or compaction (seglog and bboltstore enforce retention floors; backends
	// without retention never return it).
	//
	// Returned message data belongs to the caller and never aliases stored
	// state: mutating it cannot change what a later Read returns.
	//
	// Errors: ErrNotFound (no such stream, or an expiry whose path was retired),
	// ErrBadRequest (malformed offset, negative limit, invalid stream ID),
	// ErrGone, ErrSoftDeleted, ErrClosed, ctx.Err().
	Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Head returns stream metadata without reading data (Section 5.5).
	// StreamInfo.NextOffset is the stream's tail offset, matching the
	// TailOffset a Read would report.
	//
	// Errors: ErrNotFound (no such stream, or an expiry whose path was retired),
	// ErrSoftDeleted, ErrBadRequest (invalid stream ID), ErrClosed.
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
	// Touch never resurrects a stream: a missing stream or an expiry whose path
	// was retired reports ErrNotFound; a referenced expiry retained by a
	// ForkStorage reports ErrSoftDeleted. Either stays expired.
	//
	// Errors: ErrNotFound (no such stream, or an expiry whose path was retired),
	// ErrSoftDeleted, ErrBadRequest (invalid stream ID), ErrClosed.
	Touch(ctx context.Context, streamID string) error

	// Delete logically removes a stream (Section 5.4). Returns ErrNotFound if the
	// stream does not exist. An expired, unreferenced record is still deleted
	// successfully: expiry hides it from readers, but its record is there to
	// reclaim. A referenced expiry whose ForkStorage retained its path instead
	// reports ErrSoftDeleted until the final descendant releases it.
	//
	// Delete is atomic with respect to a concurrent Create of the same stream
	// ID: once Delete returns nil the old incarnation is no longer directly
	// accessible. If its path is eligible for reuse, a stream subsequently
	// created with that ID starts empty and never observes data, offsets or
	// deduplication state from the deleted incarnation — including when the
	// implementation reclaims the deleted bytes lazily in the background.
	//
	// Delete wakes every WaitForData caller on the stream. They report
	// ErrSoftDeleted when the record is retained for forks, or ErrNotFound when
	// it was removed.
	//
	// Errors: ErrNotFound, ErrSoftDeleted, ErrBadRequest (invalid stream ID),
	// ErrClosed.
	Delete(ctx context.Context, streamID string) error

	// WaitForData returns messages after offset, blocking until at least one is
	// available or the stream reaches permanent EOF. It returns immediately if
	// data already exists at offset or if a Read at offset reports Closed, and
	// otherwise returns the same result a Read would once data or closure
	// arrives. offset and limit have the same meaning as in Read.
	//
	// Wakeups are not lost: a waiter is released by any Append or CloseStream
	// that commits after the waiter's last unsuccessful read, so a caller that
	// loops on WaitForData observes every message and permanent EOF without
	// polling. Delete and Storage.Close also release waiters.
	//
	// Errors: ctx.Err() on cancellation or deadline, ErrNotFound (stream absent,
	// retired after expiry, or deleted while waiting), ErrSoftDeleted, ErrClosed
	// (storage closed while waiting), ErrBadRequest, ErrGone.
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

// AtomicCloseStorage is the optional Storage capability for permanently
// closing a protocol stream. Protocol closure is durable per-stream state; it
// is unrelated to [Storage.Close], which shuts down a storage backend.
//
// Implementations expose closure through StreamInfo.Closed and
// ReadResult.Closed. After closure, ordinary Append calls (and AppendBatch,
// when implemented) must fail with ErrStreamClosed, while reads, metadata
// operations, Touch, and Delete continue to work normally. Create accepts a
// StreamConfig whose Closed field is true. An implementation that also offers
// AtomicBatchStorage accepts the field in CreateWithMessages, allowing an
// entire closed stream to become visible atomically at creation.
type AtomicCloseStorage interface {
	Storage

	// CloseStream atomically appends messages in order and marks the stream
	// closed. Readers observe either the state before the call or the complete
	// final batch together with Closed=true; they never observe a prefix of the
	// batch or final messages on a stream that still appears open. A successful
	// close wakes every WaitForData caller, including when messages is empty.
	//
	// An empty messages slice is a valid close-only mutation. Repeating a
	// close-only mutation on an already-closed stream is idempotent and returns
	// its unchanged tail offset. Supplying messages to an already-closed stream
	// fails with ErrStreamClosed and changes nothing.
	//
	// seq applies once to the whole closing mutation, with the same ordering and
	// atomicity rules as AppendBatch. A rejected close does not advance it. All
	// message slices are borrowed only for the call and must be copied before
	// CloseStream returns.
	//
	// Errors: ErrNotFound, ErrBadRequest, ErrPayloadTooLarge, ErrConflict,
	// ErrStreamClosed, ErrSoftDeleted, ErrClosed, and ctx.Err().
	CloseStream(ctx context.Context, streamID string, messages [][]byte, seq string) (Offset, error)
}

// TouchHeadStorage is the optional Storage capability for reading a stream's
// metadata snapshot and restarting its sliding TTL window as one operation.
// Every origin-reaching read and write performs [Storage.Head] followed by
// [Storage.Touch], so a backend that can do both in one transaction halves
// the per-request metadata work. Callers discover the capability with a type
// assertion and fall back to the two separate calls when it is absent.
type TouchHeadStorage interface {
	Storage

	// TouchHead behaves exactly like a successful Head immediately followed by
	// Touch, executed atomically against the stream's record: no concurrent
	// mutation can be observed between the snapshot and the renewal. The
	// returned StreamInfo carries the pre-renewal ExpiresAt, matching what the
	// separate Head call would have reported.
	//
	// Errors are the union of Storage.Head and Storage.Touch errors. On error
	// the TTL window may or may not have been restarted, exactly as when the
	// separate Touch call fails.
	TouchHead(ctx context.Context, streamID string) (*StreamInfo, error)
}

// SpanReadStorage is an optional Storage capability for reading message
// payloads as sequential binary ranges without requiring each payload to be
// copied into a StoredMessage. Callers discover it with a type assertion and
// fall back to [Storage.Read] when absent.
//
// ReadSpans has exactly the offset, limit, metadata, and error semantics of
// [Storage.Read]. Its ranges, in order, contain exactly the concatenation of
// the Data fields Read would return. A range may own copied memory or retain a
// backend resource. Every range must be closed; Close is idempotent. WriteTo
// honors the context passed to ReadSpans, including cancellation after the
// method returns.
type SpanReadStorage interface {
	Storage

	ReadSpans(ctx context.Context, streamID string, offset Offset, limit int) (*SpanReadResult, error)
}

// ReadSpan is one sequential payload range returned by SpanReadStorage.
// WriteTo may be called at most once and must not be called after Close.
type ReadSpan interface {
	io.WriterTo
	Close() error
}

// SpanReadResult is the span form of ReadResult. NextOffset, TailOffset,
// IncarnationID, and Closed have the same meaning as on [ReadResult].
type SpanReadResult struct {
	Spans         []ReadSpan
	NextOffset    Offset
	TailOffset    Offset
	IncarnationID string
	Closed        bool
}

// ForkRequest describes an atomic fork creation. It deliberately records
// whether optional protocol fields were present: omission has different
// inheritance and idempotency semantics from an explicitly supplied zero
// value.
//
// Config.ContentType carries the content type resolved by the caller even when
// ContentTypeSet is false. CreateFork verifies it against the current source
// incarnation before committing, then stores the source's canonical value. A
// caller that obtained SourceIncarnationID from Head should also supply it to
// fence deletion and recreation races; an empty value preserves compatibility
// with implementations that do not expose incarnation identity.
//
// Config.IsPrivate and Config.Closed always describe the new target and are
// never inherited. In particular, a closed source produces an open fork unless
// Config.Closed explicitly requests that the target itself be closed.
type ForkRequest struct {
	// SourceStreamID is the storage identifier of the immediate parent stream.
	SourceStreamID string

	// SourceIncarnationID, when non-empty, must equal the current source
	// incarnation. A mismatch reports ErrConflict rather than forking data that
	// the caller did not inspect.
	SourceIncarnationID string

	// Offset is the divergence anchor. OffsetSet distinguishes an explicit
	// offset (including a start sentinel) from omission. When OffsetSet is false,
	// CreateFork resolves the anchor to the source's current tail. Replaying the
	// same omitted-offset request against an existing target remains idempotent;
	// it never rebases the target after the source grows.
	Offset    Offset
	OffsetSet bool

	// SubOffset refines the anchor within the next source data boundary. Zero is
	// equivalent to omitting the protocol header. For JSON it counts flattened
	// messages in the next atomic batch; for other content types it counts bytes
	// in the next message.
	SubOffset uint64

	// Config holds target configuration. ContentType, TTL, and ExpiresAt are
	// interpreted together with their corresponding Set fields below.
	Config StreamConfig

	// ContentTypeSet reports whether the target content type was explicit. If
	// true it must match the source. If false the source content type is
	// inherited, while Config.ContentType still carries the caller's resolved
	// expectation for race detection.
	ContentTypeSet bool

	// TTLSet and ExpiresAtSet distinguish explicit target lifetime policy from
	// inheritance. At most one may be true. When both are false, a source TTL is
	// copied as a fresh independent sliding window and a source absolute expiry
	// is copied as the same absolute deadline.
	TTLSet       bool
	ExpiresAtSet bool
}

// ForkStorage is the optional Storage capability for creating streams that
// share an immutable prefix with another stream. Callers discover it with a
// type assertion; implementations without fork topology remain valid Storage
// implementations.
//
// A fork uses the source's offset space. It observes source data only through
// the resolved fork boundary, then its own initial messages and later appends;
// source appends after creation are never visible through the fork. Forks may
// themselves be forked, and reads transparently stitch the resulting chain.
// Producer and Stream-Seq state are target-local and are never inherited.
//
// Deleting a stream with direct child forks soft-deletes it. Its data remains
// available only through descendants, while direct Append, Read, Head, Touch,
// Delete, WaitForData, AppendBatch, and CloseStream operations report
// ErrSoftDeleted. Ordinary Create at its path, creating another fork at its
// path, and using it as a new fork source report ErrConflict. Deleting the last
// child permits recursive reclamation of soft-deleted ancestors.
//
// Expiry also preserves every referenced node's data for descendants. An
// implementation may either detach the expired node from its public path and
// allow an independent replacement there, or retain the path in the same
// externally inaccessible state as a soft deletion. In the retained form,
// direct operations report ErrSoftDeleted and creation at that path reports
// ErrConflict until reference release permits reclamation. This choice is an
// implementation policy; it must not affect reads through existing descendants.
type ForkStorage interface {
	Storage

	// CreateFork atomically creates targetStreamID, its source reference, and
	// all initial messages. On success info is non-nil and describes the exact
	// target snapshot, including its effective inherited configuration and tail.
	//
	// An existing live fork created by an equivalent ForkRequest is an
	// idempotent success with created=false; messages are validated but neither
	// compared nor appended again. This existing-target check is resolved before
	// source visibility, so a retry remains successful if that already-linked
	// source has since been soft-deleted. Any different target configuration, a
	// regular stream or soft-deleted stream at the target, a soft-deleted source
	// for a new target, or a SourceIncarnationID mismatch reports ErrConflict.
	//
	// An offset beyond the source tail, a sub-offset beyond the next applicable
	// message/batch boundary, invalid mutually exclusive lifetime fields, or an
	// empty message reports ErrBadRequest. A missing source, or one whose expired
	// path was retired, reports ErrNotFound; an expired source retained like a
	// soft deletion reports ErrConflict. Input message slices are borrowed only
	// for the call, and no partial target or reference is visible on error.
	CreateFork(ctx context.Context, targetStreamID string, req ForkRequest, messages [][]byte) (created bool, info *StreamInfo, err error)
}
