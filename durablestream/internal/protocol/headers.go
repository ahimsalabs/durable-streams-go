// Package protocol contains internal HTTP protocol constants and utilities.
package protocol

// HTTP header names defined by the Durable Streams Protocol.
// See PROTOCOL.md Section 13: IANA Considerations.
const (
	// HeaderStreamTTL sets or returns the relative time-to-live for a stream in seconds.
	HeaderStreamTTL = "Stream-TTL"

	// HeaderStreamExpiresAt sets or returns the absolute expiry time for a stream.
	HeaderStreamExpiresAt = "Stream-Expires-At"

	// HeaderStreamSeq is a monotonic, lexicographic writer sequence number for coordination.
	HeaderStreamSeq = "Stream-Seq"

	// HeaderStreamCursor is an opaque cursor for CDN collapsing optimization.
	HeaderStreamCursor = "Stream-Cursor"

	// HeaderStreamNextOffset is the next offset to read from after the current response.
	HeaderStreamNextOffset = "Stream-Next-Offset"

	// HeaderStreamUpToDate indicates the response includes all data available in the stream.
	HeaderStreamUpToDate = "Stream-Up-To-Date"

	// HeaderStreamClosed requests closure or indicates that a stream has reached permanent EOF.
	HeaderStreamClosed = "Stream-Closed"

	// HeaderStreamForkedFrom names the source stream when creating a fork.
	HeaderStreamForkedFrom = "Stream-Forked-From"

	// HeaderStreamForkOffset identifies the source position where a fork diverges.
	HeaderStreamForkOffset = "Stream-Fork-Offset"

	// HeaderStreamForkSubOffset refines a fork boundary within the next data batch.
	HeaderStreamForkSubOffset = "Stream-Fork-Sub-Offset"

	// HeaderStreamSSEDataEncoding declares the encoding applied to SSE data events.
	// Per Section 5.8, servers set this to "base64" for streams whose content type is
	// neither text/* nor application/json.
	HeaderStreamSSEDataEncoding = "Stream-SSE-Data-Encoding"

	// SSEDataEncodingBase64 is the only defined value for HeaderStreamSSEDataEncoding.
	SSEDataEncodingBase64 = "base64"

	// HeaderStreamPrivate indicates the stream contains user-specific or confidential data.
	// Per Section 10.1, private streams use Cache-Control: private instead of public.
	HeaderStreamPrivate = "Stream-Private"
)

// Idempotent Producer headers (PROTOCOL.md Section 5.2.1).
const (
	// HeaderProducerID identifies the logical producer.
	HeaderProducerID = "Producer-Id"

	// HeaderProducerEpoch is the client-declared epoch for session management.
	HeaderProducerEpoch = "Producer-Epoch"

	// HeaderProducerSeq is the monotonically increasing sequence number per epoch.
	HeaderProducerSeq = "Producer-Seq"

	// HeaderProducerExpectedSeq indicates the expected sequence on 409 Conflict.
	HeaderProducerExpectedSeq = "Producer-Expected-Seq"

	// HeaderProducerReceivedSeq indicates the received sequence on 409 Conflict.
	HeaderProducerReceivedSeq = "Producer-Received-Seq"
)

// Query parameter names used in stream operations.
const (
	QueryOffset   = "offset"
	QueryLive     = "live"
	QueryCursor   = "cursor"
	QuerySnapshot = "snapshot"
)

// Valid values for the "live" query parameter.
const (
	LiveModeLongPoll = "long-poll"
	LiveModeSSE      = "sse"
)

// Offset sentinel values (PROTOCOL.md Section 8).
const (
	// OffsetStart is the special offset value that indicates the beginning of the stream.
	// Per spec Section 8: semantically equivalent to omitting the offset parameter.
	OffsetStart = "-1"

	// OffsetNow is the special offset value that indicates the current tail position.
	// Per spec Section 8: allows clients to skip historical data and read only future data.
	OffsetNow = "now"
)
