package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// Re-export transport types for convenience.
type (
	// HeaderProvider is a function that provides HTTP headers per-request.
	// Re-exported from transport package.
	HeaderProvider = transport.HeaderProvider
)

// ReadMode specifies how live reads are handled after catch-up.
// See transport.ReadMode for detailed documentation.
type ReadMode = transport.ReadMode

// Read mode constants.
const (
	// ReadModeAuto catches up, then uses long-poll for live updates (default).
	ReadModeAuto = transport.ReadModeAuto

	// ReadModeLongPoll uses long-polling for live updates (Section 5.7).
	ReadModeLongPoll = transport.ReadModeLongPoll

	// ReadModeSSE uses Server-Sent Events for live updates (Section 5.8).
	ReadModeSSE = transport.ReadModeSSE
)

// ClientConfig configures a Client created via NewClient.
//
// For custom transports (testing, middleware), use NewClientWithTransport instead.
//
// # Zero Values
//
// Zero values are replaced with defaults:
//   - Timeout: 30s (if zero or negative)
//   - ReadMode: ReadModeAuto (if zero)
//   - HTTPClient: a package-owned client with bounded dial and idle settings (if nil)
//   - MaxResponseSize: 64 MiB (if zero or negative)
//   - MaxSSEEventSize: 16 MiB (if zero or negative)
//   - Headers: none (if nil)
type ClientConfig struct {
	// HTTPClient is the underlying HTTP client.
	//
	// Default: a package-owned client with bounded dial, TLS handshake, and
	// idle-connection settings. Do not set Client.Timeout on a custom client:
	// it applies to the whole request including body reads and would abort
	// long-poll and SSE reads. Use Timeout below, which is applied per
	// operation via the request context.
	HTTPClient *http.Client

	// Headers provides headers to include in all requests.
	// Called per-request to allow dynamic values (e.g., auth tokens).
	// This is the primary customization point for authentication.
	Headers HeaderProvider

	// Timeout bounds each non-streaming operation (Create, Head, Delete, Send,
	// StreamWriter.Close, and catch-up reads). It is applied as a context
	// deadline, so it does not affect long-poll or SSE reads, which have their
	// own deadlines.
	// Zero or negative values default to 30s.
	Timeout time.Duration

	// MaxResponseSize bounds the body of a single non-streaming response.
	// Responses larger than this fail with transport.ErrResponseTooLarge.
	// Zero or negative values default to 64 MiB.
	MaxResponseSize int64

	// MaxSSEEventSize bounds the bytes consumed by a single SSE event.
	// Events larger than this fail with transport.ErrResponseTooLarge.
	// Zero or negative values default to 16 MiB.
	MaxSSEEventSize int

	// ReadMode specifies how live reads are handled after catch-up (Section 5.7-5.8).
	// Zero value defaults to ReadModeAuto (catch-up then long-poll).
	ReadMode ReadMode
}

// Client provides methods to interact with durable streams.
// See PROTOCOL.md Section 5: HTTP Operations.
type Client struct {
	transport transport.Transport
	readMode  ReadMode
	timeout   time.Duration
}

// NewClient creates a new stream client for the given base URL.
// Pass nil for cfg to use defaults.
//
// The client automatically retries transient failures (5xx errors, rate limits)
// with exponential backoff for retry-safe operations. Bare appends made by
// StreamWriter are intentionally not retried because the server cannot
// deduplicate them; transport-level appends carrying idempotent producer headers
// are retryable. Empty close-only requests are also retried because protocol
// closure is idempotent, while append-and-close requests without producer
// headers are not. Deletes are not retried because the path could be recreated
// between attempts, causing a retry to delete the replacement. For custom retry
// behavior or to disable retry, use NewClientWithTransport.
//
// For custom transports (testing, middleware composition), use NewClientWithTransport.
func NewClient(baseURL string, cfg *ClientConfig) *Client {
	c := &Client{
		timeout:  30 * time.Second,
		readMode: ReadModeAuto,
	}

	var httpCfg *transport.HTTPConfig
	if cfg != nil {
		if cfg.Timeout > 0 {
			c.timeout = cfg.Timeout
		}
		c.readMode = cfg.ReadMode

		httpCfg = &transport.HTTPConfig{
			Client:          cfg.HTTPClient,
			Headers:         cfg.Headers,
			MaxResponseSize: cfg.MaxResponseSize,
			MaxSSEEventSize: cfg.MaxSSEEventSize,
		}
	}

	// Wrap transport with retry middleware for transient failures (5xx, 429)
	httpTransport := transport.NewHTTPTransport(baseURL, httpCfg)
	c.transport = transport.WithRetry(transport.DefaultRetryOptions())(httpTransport)

	return c
}

// NewClientWithTransport creates a client with a custom transport.
//
// Use this for:
//   - Testing with mock transports
//   - Middleware composition (logging, retry)
//   - Custom transport implementations
//
// Example with middleware:
//
//	t := transport.NewHTTPTransport(url, &transport.HTTPConfig{Headers: myHeaders})
//	t = transport.WithRetry(transport.DefaultRetryOptions())(t)
//	client := durablestream.NewClientWithTransport(t, nil)
func NewClientWithTransport(t transport.Transport, cfg *TransportClientConfig) *Client {
	c := &Client{
		transport: t,
		timeout:   30 * time.Second,
		readMode:  ReadModeAuto,
	}

	if cfg != nil {
		if cfg.Timeout > 0 {
			c.timeout = cfg.Timeout
		}
		c.readMode = cfg.ReadMode
	}

	return c
}

// TransportClientConfig configures a Client created via NewClientWithTransport.
type TransportClientConfig struct {
	// Timeout bounds each non-streaming operation, applied as a context
	// deadline. Zero or negative values default to 30s.
	Timeout time.Duration

	// ReadMode specifies how live reads are handled after catch-up.
	// Zero value defaults to ReadModeAuto.
	ReadMode ReadMode
}

// StreamData contains the result of a stream read operation.
type StreamData struct {
	Data       []byte // Raw response bytes (empty on 204 timeout)
	NextOffset Offset // Next offset to read from
	Cursor     string // Opaque cursor for long-poll
	UpToDate   bool   // True if caught up to tail
	Closed     bool   // True at permanent EOF; unlike UpToDate, no more data can arrive
}

// Message represents a single message from a stream.
// Use Decode() to unmarshal JSON, Bytes() for raw access, or String() for text.
type Message struct {
	data []byte
}

// Bytes returns the raw message bytes.
func (m Message) Bytes() []byte {
	return m.data
}

// Decode unmarshals the message as JSON into v.
func (m Message) Decode(v any) error {
	return json.Unmarshal(m.data, v)
}

// String returns the message as a string.
func (m Message) String() string {
	return string(m.data)
}

// CreateOptions specifies options for creating a stream (Section 5.1).
type CreateOptions struct {
	// ContentType sets the content type for the stream.
	// Default: "application/octet-stream"
	ContentType string

	// TTL sets a relative time-to-live for the stream. It must be a
	// non-negative whole number of seconds. Zero means no TTL. Mutually
	// exclusive with ExpiresAt.
	TTL time.Duration

	// ExpiresAt sets an absolute expiry time for the stream.
	// Zero means no expiry. Mutually exclusive with TTL.
	ExpiresAt time.Time

	// InitialData sets the initial stream data.
	InitialData []byte

	// Closed creates the stream in its terminal state. InitialData, if any, is
	// its complete and final content.
	Closed bool
}

// withTimeout applies the client's configured operation timeout to ctx.
//
// It is used for the non-streaming operations only. Long-poll and SSE reads
// carry their own, much longer deadlines and must not inherit this one.
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.timeout)
}

// Create creates a new stream with the given options (Section 5.1: Create Stream).
// Pass nil for opts to use defaults.
func (c *Client) Create(ctx context.Context, path string, opts *CreateOptions) (*StreamInfo, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	req := transport.CreateRequest{Path: path}

	contentType := "application/octet-stream"
	if opts != nil {
		if opts.ContentType != "" {
			contentType = opts.ContentType
		}
		req.ContentType = opts.ContentType
		req.TTL = opts.TTL
		req.ExpiresAt = opts.ExpiresAt
		req.InitialData = opts.InitialData
		req.Closed = opts.Closed
	}

	resp, err := c.transport.Create(ctx, req)
	if err != nil {
		return nil, convertTransportErrorWithPath(err, path)
	}
	if req.Closed && !resp.Closed {
		return nil, wrapSentinelWithPath("create response did not confirm stream closure", ErrParseError, path)
	}

	return &StreamInfo{
		ContentType: contentType,
		NextOffset:  Offset(resp.NextOffset),
		Closed:      resp.Closed,
	}, nil
}

// Head queries stream metadata without transferring data (Section 5.5: Stream Metadata).
func (c *Client) Head(ctx context.Context, path string) (*StreamInfo, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.transport.Head(ctx, transport.HeadRequest{Path: path})
	if err != nil {
		return nil, convertTransportErrorWithPath(err, path)
	}

	return &StreamInfo{
		ContentType: resp.ContentType,
		NextOffset:  Offset(resp.NextOffset),
		TTL:         resp.TTL,
		ExpiresAt:   resp.ExpiresAt,
		Closed:      resp.Closed,
	}, nil
}

// Delete removes a stream (Section 5.4: Delete Stream).
func (c *Client) Delete(ctx context.Context, path string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	return convertTransportErrorWithPath(c.transport.Delete(ctx, transport.DeleteRequest{Path: path}), path)
}

// StreamWriter provides efficient append operations by caching stream metadata.
// Create via Client.Writer(). The writer holds no resources requiring cleanup.
//
// A StreamWriter is not safe for concurrent use. Give each goroutine its own
// writer, or serialize access with a mutex: concurrent Send calls would race on
// the cached offset and interleave appends in an unspecified order.
//
// See PROTOCOL.md Sections 5.2-5.3: Append and Close Stream.
type StreamWriter struct {
	client      *Client
	ctx         context.Context
	path        string
	contentType string
	offset      Offset
}

// Writer creates a StreamWriter for append operations.
// The writer caches stream metadata (content-type) to avoid per-append overhead.
//
// Send and SendJSON retain ctx for backward compatibility, including its
// cancellation and values (for example values consumed by a HeaderProvider).
// SendContext and SendJSONContext use the context supplied to that call instead.
// Every append is also bounded by ClientConfig.Timeout.
func (c *Client) Writer(ctx context.Context, path string) (*StreamWriter, error) {
	info, err := c.Head(ctx, path)
	if err != nil {
		return nil, err
	}

	return &StreamWriter{
		client:      c,
		ctx:         ctx,
		path:        path,
		contentType: info.ContentType,
		offset:      info.NextOffset,
	}, nil
}

// SendOptions specifies options for Send and SendJSON operations.
type SendOptions struct {
	// Seq is an optional monotonic sequence number for writer coordination.
	// If provided and less than or equal to the last sequence, returns ErrConflict.
	Seq string

	// Close atomically closes the stream after this append. An empty data slice
	// is valid when Close is true. Use this form instead of StreamWriter.Close
	// when the closing mutation also needs Seq.
	Close bool
}

// Send appends raw bytes to the stream (Section 5.2: Append to Stream).
// It uses the context passed to [Client.Writer] and is also bounded by the
// client's configured timeout. Use [StreamWriter.SendContext] to supply a
// different context for one append.
func (w *StreamWriter) Send(data []byte, opts *SendOptions) error {
	ctx := w.ctx
	if ctx == nil {
		// Keep manually constructed writers inside this package usable in tests.
		ctx = context.Background()
	}
	return w.SendContext(ctx, data, opts)
}

// SendContext appends raw bytes to the stream using ctx.
//
// The append is bounded by the client's configured timeout in addition to any
// deadline on ctx.
func (w *StreamWriter) SendContext(ctx context.Context, data []byte, opts *SendOptions) error {
	return w.appendContext(ctx, data, opts, opts != nil && opts.Close)
}

// Close atomically appends finalData, if non-empty, and permanently closes the
// stream. It uses the context passed to [Client.Writer]. A close with no final
// data is idempotent and may be retried. A close with final data is sent only
// once because retrying an ambiguous response could append it twice. Therefore,
// an error from a close with final data does not prove that the server failed to
// commit it; callers needing safe retries must use idempotent producer headers.
func (w *StreamWriter) Close(finalData []byte) error {
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return w.CloseContext(ctx, finalData)
}

// CloseContext atomically appends finalData, if non-empty, and permanently
// closes the stream using ctx. The operation is bounded by the client's
// configured timeout.
func (w *StreamWriter) CloseContext(ctx context.Context, finalData []byte) error {
	return w.appendContext(ctx, finalData, nil, true)
}

func (w *StreamWriter) appendContext(ctx context.Context, data []byte, opts *SendOptions, closeStream bool) error {
	var seq string
	if opts != nil {
		seq = opts.Seq
	}

	ctx, cancel := w.client.withTimeout(ctx)
	defer cancel()

	resp, err := w.client.transport.Append(ctx, transport.AppendRequest{
		Path:        w.path,
		Data:        data,
		ContentType: w.contentType,
		Seq:         seq,
		Close:       closeStream,
	})
	if err != nil {
		var transportErr *transport.Error
		if errors.As(err, &transportErr) &&
			(transportErr.Code == "STREAM_CLOSED" || transportErr.Code == "stream_closed") &&
			transportErr.FinalOffset != "" {
			w.offset = Offset(transportErr.FinalOffset)
		}
		return convertTransportErrorWithPath(err, w.path)
	}
	if closeStream && !resp.Closed {
		return wrapSentinelWithPath("append response did not confirm stream closure", ErrParseError, w.path)
	}

	w.offset = Offset(resp.NextOffset)
	return nil
}

// SendJSON marshals v as JSON and appends it using the context passed to
// [Client.Writer]. Use [StreamWriter.SendJSONContext] to supply a different
// context for one append.
func (w *StreamWriter) SendJSON(v any, opts *SendOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.Send(data, opts)
}

// SendJSONContext marshals v as JSON and appends it to the stream using ctx.
func (w *StreamWriter) SendJSONContext(ctx context.Context, v any, opts *SendOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.SendContext(ctx, data, opts)
}

// Offset returns the current tail offset after the last successful append.
func (w *StreamWriter) Offset() Offset {
	return w.offset
}

// Reader creates a new Reader for continuous reading from a stream.
// The Reader inherits the client's ReadMode for live tailing behavior.
//
// When offset is "now" and the read mode is LongPoll or Auto, the reader
// skips catch-up and goes directly to long-poll mode. Per PROTOCOL.md Section 8:
// "Servers MUST immediately begin waiting for new data (no initial empty response)"
//
// For SSE mode, the reader always uses SSE directly (no catch-up phase) because
// SSE streams deliver both historical and live data via SSE events.
func (c *Client) Reader(path string, offset Offset) *Reader {
	// Per protocol spec Section 8, for offset=now with long-poll:
	// "Servers MUST immediately begin waiting for new data (no initial empty response)"
	// Skip catch-up phase for long-poll compatible modes with offset=now
	catching := true
	if offset == Offset("now") && (c.readMode == ReadModeAuto || c.readMode == ReadModeLongPoll) {
		catching = false
	}
	// SSE mode delivers all data (historical and live) via SSE events
	if c.readMode == ReadModeSSE {
		catching = false
	}

	return &Reader{
		client:   c,
		path:     path,
		offset:   offset,
		readMode: c.readMode,
		catching: catching,
	}
}

// convertTransportError converts transport package errors to durablestream errors.
// It wraps sentinel errors with the original message so callers can inspect details.
func convertTransportError(err error) error {
	return convertTransportErrorWithPath(err, "")
}

// convertTransportErrorWithPath converts transport errors and includes the stream path
// in the error message for better debugging context.
func convertTransportErrorWithPath(err error, path string) error {
	if err == nil {
		return nil
	}

	// Check if it's a transport error with a code. Middleware is allowed to wrap
	// transport errors, so use errors.As rather than a bare type assertion.
	var tErr *transport.Error
	if errors.As(err, &tErr) {
		// Check both uppercase (from HTTP status mapping) and lowercase (from JSON response)
		// Wrap with original message so details are preserved for inspection
		switch tErr.Code {
		case "NOT_FOUND", "not_found":
			return wrapSentinelWithPath(tErr.Message, ErrNotFound, path)
		case "SEQUENCE_CONFLICT", "sequence_conflict":
			return wrapSentinelWithPath(tErr.Message, ErrSequenceConflict, path)
		case "CONFLICT", "conflict":
			return wrapSentinelWithPath(tErr.Message, ErrConflict, path)
		case "STREAM_CLOSED", "stream_closed":
			return &StreamClosedError{
				Path:        path,
				FinalOffset: Offset(tErr.FinalOffset),
				Message:     tErr.Message,
			}
		case "GONE", "gone":
			return wrapSentinelWithPath(tErr.Message, ErrGone, path)
		case "BAD_REQUEST", "bad_request":
			return wrapSentinelWithPath(tErr.Message, ErrBadRequest, path)
		case "PAYLOAD_TOO_LARGE", "payload_too_large":
			return wrapSentinelWithPath(tErr.Message, ErrPayloadTooLarge, path)
		case "PARSE_ERROR", "parse_error":
			return wrapSentinelWithPath(tErr.Message, ErrParseError, path)
		case "RATE_LIMITED", "too_many_requests":
			if path != "" {
				return newError(codeTooManyRequests, fmt.Sprintf("[%s] %s", path, tErr.Message))
			}
			return newError(codeTooManyRequests, tErr.Message)
		default:
			// Return the transport error as-is
			return err
		}
	}

	return err
}

// wrapSentinel wraps a sentinel error with a message.
// If message is empty, returns the sentinel directly.
func wrapSentinel(msg string, sentinel error) error {
	return wrapSentinelWithPath(msg, sentinel, "")
}

// wrapSentinelWithPath wraps a sentinel error with the stream path and message.
// The path is included in brackets for easy identification in error messages.
func wrapSentinelWithPath(msg string, sentinel error, path string) error {
	if path == "" {
		if msg == "" {
			return sentinel
		}
		return fmt.Errorf("%s: %w", msg, sentinel)
	}
	if msg == "" {
		return fmt.Errorf("[%s]: %w", path, sentinel)
	}
	return fmt.Errorf("[%s] %s: %w", path, msg, sentinel)
}
