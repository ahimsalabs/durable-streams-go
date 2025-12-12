// Package transport defines the interface for durable stream network operations.
//
// The Transport interface abstracts the wire protocol, allowing different
// implementations (HTTP, WebSocket) and middleware composition (auth, logging, retry).
//
// See PROTOCOL.md Section 5: HTTP Operations for the full specification.
package transport

import (
	"context"
	"time"
)

// Transport defines the interface for durable stream network operations.
//
// Implementations handle the wire protocol; Client/Reader orchestrate usage
// based on configured ReadMode.
//
// All path parameters are relative paths that the transport combines with
// its configured base URL.
type Transport interface {
	// Read performs a catch-up read (Section 5.5: Read Stream - Catch-up).
	// Returns immediately with available data from the given offset.
	Read(ctx context.Context, req ReadRequest) (*ReadResponse, error)

	// LongPoll performs a long-poll read (Section 5.6: Read Stream - Live Long-poll).
	// Waits up to the configured timeout for new data if none is available.
	LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error)

	// SSE opens a Server-Sent Events stream (Section 5.7: Read Stream - Live SSE).
	// Returns an EventStream for consuming events. Caller must Close() when done.
	// Only valid for streams with text/* or application/json content types.
	SSE(ctx context.Context, req SSERequest) (EventStream, error)

	// Append adds data to a stream (Section 5.2: Append to Stream).
	// Data must not be empty. Content-Type must match the stream's type.
	Append(ctx context.Context, req AppendRequest) (*AppendResponse, error)

	// Create creates a new stream (Section 5.1: Create Stream).
	// Returns conflict error if stream exists with different configuration.
	Create(ctx context.Context, req CreateRequest) (*CreateResponse, error)

	// Delete removes a stream (Section 5.3: Delete Stream).
	Delete(ctx context.Context, req DeleteRequest) error

	// Head retrieves stream metadata (Section 5.4: Stream Metadata).
	// Returns the canonical way to find tail offset, TTL, and expiry.
	Head(ctx context.Context, req HeadRequest) (*HeadResponse, error)
}

// EventStream represents a persistent streaming connection (Section 5.7: SSE).
// Future implementations may support WebSocket.
type EventStream interface {
	// Next blocks until the next event arrives.
	// Returns io.EOF when the stream is closed normally.
	// Per Section 5.7: Server SHOULD close connections ~60s for CDN collapsing.
	Next(ctx context.Context) (*Event, error)

	// Close terminates the stream and releases resources.
	Close() error
}

// ReadMode specifies how live reads are handled after catch-up.
type ReadMode int

const (
	// ReadModeAuto catches up, then uses long-poll for live updates.
	// This is the default mode matching typical usage patterns.
	ReadModeAuto ReadMode = iota

	// ReadModeLongPoll uses long-polling for live updates (Section 5.6).
	// More robust across proxies and CDNs.
	ReadModeLongPoll

	// ReadModeSSE uses Server-Sent Events for live updates (Section 5.7).
	// Lower latency but requires text/* or application/json content type.
	ReadModeSSE
)

// ReadRequest is the request for a catch-up read (Section 5.5).
type ReadRequest struct {
	Path   string // Stream path (relative to base URL)
	Offset string // Starting offset; empty means stream start (Section 6)
}

// LongPollRequest is the request for a long-poll read (Section 5.6).
type LongPollRequest struct {
	Path    string        // Stream path
	Offset  string        // Starting offset (required for long-poll)
	Cursor  string        // Echo of Stream-Cursor for CDN collapsing (Section 8)
	Timeout time.Duration // Hint for server wait time
}

// SSERequest is the request to open an SSE stream (Section 5.7).
type SSERequest struct {
	Path   string // Stream path
	Offset string // Starting offset (required for SSE)
}

// ReadResponse is the response from Read or LongPoll operations.
type ReadResponse struct {
	// Data is the raw response bytes.
	// For JSON streams, this is a JSON array of messages (Section 7.1).
	Data []byte

	// NextOffset is the offset for subsequent reads (Section 6).
	// Per spec: "Clients MUST use the Stream-Next-Offset value returned
	// in responses for subsequent read requests."
	NextOffset string

	// Cursor is the opaque cursor for CDN collapsing (Section 8).
	// Should be echoed in subsequent long-poll requests.
	Cursor string

	// UpToDate is true when response includes all available data (Section 5.5).
	// Per spec: "Clients MAY use this header to determine when they have
	// caught up and can transition to live tailing mode."
	UpToDate bool
}

// Event represents an SSE event (Section 5.7).
type Event struct {
	// Type is "data" or "control" per Section 5.7.
	Type string

	// Data is the event payload.
	// For "data" events: the stream data.
	// For "control" events: JSON with streamNextOffset and optional streamCursor.
	Data []byte

	// NextOffset is extracted from control events for convenience.
	NextOffset string

	// Cursor is extracted from control events for convenience.
	Cursor string
}

// AppendRequest is the request to append data (Section 5.2).
type AppendRequest struct {
	Path string // Stream path
	Data []byte // Data to append (must not be empty per Section 5.2)

	// ContentType must match the stream's content type.
	// Per spec: "Servers MUST return 400 Bad Request on mismatch."
	ContentType string

	// Seq is an optional monotonic sequence number for writer coordination.
	// Per Section 5.2: "If provided and less than or equal to the last
	// appended sequence, the server MUST return 409 Conflict."
	Seq string
}

// AppendResponse is the response from an Append operation.
type AppendResponse struct {
	// NextOffset is the new tail offset after the append (Section 5.2).
	NextOffset string
}

// CreateRequest is the request to create a stream (Section 5.1).
type CreateRequest struct {
	Path string // Stream path

	// ContentType sets the stream's content type.
	// Per spec: "If omitted, the server MAY default to application/octet-stream."
	ContentType string

	// TTL sets relative time-to-live from creation.
	// Zero means no TTL. Mutually exclusive with ExpiresAt (Section 5.1).
	TTL time.Duration

	// ExpiresAt sets absolute expiry time.
	// Zero means no expiry. Mutually exclusive with TTL (Section 5.1).
	ExpiresAt time.Time

	// InitialData is optional initial stream content.
	InitialData []byte
}

// CreateResponse is the response from a Create operation.
type CreateResponse struct {
	// NextOffset is the tail offset after any initial content (Section 5.1).
	NextOffset string
}

// DeleteRequest is the request to delete a stream (Section 5.3).
type DeleteRequest struct {
	Path string // Stream path
}

// HeadRequest is the request for stream metadata (Section 5.4).
type HeadRequest struct {
	Path string // Stream path
}

// HeadResponse is the response from a Head operation (Section 5.4).
type HeadResponse struct {
	// ContentType is the stream's content type.
	ContentType string

	// NextOffset is the tail offset (next offset after current end).
	NextOffset string

	// TTL is the remaining time-to-live, if applicable.
	TTL time.Duration

	// ExpiresAt is the absolute expiry time, if applicable.
	ExpiresAt time.Time
}

// Middleware wraps a Transport with additional behavior.
type Middleware func(Transport) Transport
