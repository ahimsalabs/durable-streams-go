// Package protocol contains internal HTTP protocol constants and utilities.
package protocol

// HTTP header names defined by the Durable Streams Protocol.
// See PROTOCOL.md Section 11: IANA Considerations.
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
)

// Query parameter names used in stream operations.
const (
	QueryOffset = "offset"
	QueryLive   = "live"
	QueryCursor = "cursor"
)

// Valid values for the "live" query parameter.
const (
	LiveModeLongPoll = "long-poll"
	LiveModeSSE      = "sse"
)
