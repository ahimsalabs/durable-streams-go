package durablestream

import (
	"context"
	"encoding/json"
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

	// ReadModeLongPoll uses long-polling for live updates (Section 5.6).
	ReadModeLongPoll = transport.ReadModeLongPoll

	// ReadModeSSE uses Server-Sent Events for live updates (Section 5.7).
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
//   - HTTPClient: http.DefaultClient (if nil)
//   - Headers: none (if nil)
type ClientConfig struct {
	// HTTPClient is the underlying HTTP client.
	// Default: http.DefaultClient.
	HTTPClient *http.Client

	// Headers provides headers to include in all requests.
	// Called per-request to allow dynamic values (e.g., auth tokens).
	// This is the primary customization point for authentication.
	Headers HeaderProvider

	// Timeout is the default timeout for all operations (Create, Head, Delete, etc).
	// Zero or negative values default to 30s.
	Timeout time.Duration

	// ReadMode specifies how live reads are handled after catch-up (Section 5.6-5.7).
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
			Client:  cfg.HTTPClient,
			Headers: cfg.Headers,
		}
	}
	c.transport = transport.NewHTTPTransport(baseURL, httpCfg)

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
	// Timeout is the default timeout for all operations.
	// Zero or negative values default to 30s.
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

	// TTL sets a relative time-to-live for the stream.
	// Zero means no TTL. Mutually exclusive with ExpiresAt.
	TTL time.Duration

	// ExpiresAt sets an absolute expiry time for the stream.
	// Zero means no expiry. Mutually exclusive with TTL.
	ExpiresAt time.Time

	// InitialData sets the initial stream data.
	InitialData []byte
}

// Create creates a new stream with the given options (Section 5.1: Create Stream).
// Pass nil for opts to use defaults.
func (c *Client) Create(ctx context.Context, path string, opts *CreateOptions) (*StreamInfo, error) {
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
	}

	resp, err := c.transport.Create(ctx, req)
	if err != nil {
		return nil, convertTransportError(err)
	}

	return &StreamInfo{
		ContentType: contentType,
		NextOffset:  Offset(resp.NextOffset),
	}, nil
}

// Head queries stream metadata without transferring data (Section 5.4: Stream Metadata).
func (c *Client) Head(ctx context.Context, path string) (*StreamInfo, error) {
	resp, err := c.transport.Head(ctx, transport.HeadRequest{Path: path})
	if err != nil {
		return nil, convertTransportError(err)
	}

	return &StreamInfo{
		ContentType: resp.ContentType,
		NextOffset:  Offset(resp.NextOffset),
		TTL:         resp.TTL,
		ExpiresAt:   resp.ExpiresAt,
	}, nil
}

// Delete removes a stream (Section 5.3: Delete Stream).
func (c *Client) Delete(ctx context.Context, path string) error {
	return convertTransportError(c.transport.Delete(ctx, transport.DeleteRequest{Path: path}))
}

// StreamWriter provides efficient append operations by caching stream metadata.
// Create via Client.Writer(). The writer holds no resources requiring cleanup.
// See PROTOCOL.md Section 5.2: Append to Stream.
type StreamWriter struct {
	client      *Client
	ctx         context.Context
	path        string
	contentType string
	offset      Offset
}

// Writer creates a StreamWriter for append operations.
// The writer caches stream metadata (content-type) to avoid per-append overhead.
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
}

// Send appends raw bytes to the stream (Section 5.2: Append to Stream).
func (w *StreamWriter) Send(data []byte, opts *SendOptions) error {
	var seq string
	if opts != nil {
		seq = opts.Seq
	}

	resp, err := w.client.transport.Append(w.ctx, transport.AppendRequest{
		Path:        w.path,
		Data:        data,
		ContentType: w.contentType,
		Seq:         seq,
	})
	if err != nil {
		return convertTransportError(err)
	}

	w.offset = Offset(resp.NextOffset)
	return nil
}

// SendJSON marshals v as JSON and appends to the stream.
func (w *StreamWriter) SendJSON(v any, opts *SendOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.Send(data, opts)
}

// Offset returns the current tail offset after the last successful append.
func (w *StreamWriter) Offset() Offset {
	return w.offset
}

// Reader creates a new Reader for continuous reading from a stream.
// The Reader inherits the client's ReadMode for live tailing behavior.
func (c *Client) Reader(path string, offset Offset) *Reader {
	return &Reader{
		client:   c,
		path:     path,
		offset:   offset,
		readMode: c.readMode,
		catching: true, // Start in catch-up phase
	}
}

// convertTransportError converts transport package errors to durablestream errors.
func convertTransportError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's a transport error with a code
	if tErr, ok := err.(*transport.Error); ok {
		// Check both uppercase (from HTTP status mapping) and lowercase (from JSON response)
		switch tErr.Code {
		case "NOT_FOUND", "not_found":
			return ErrNotFound
		case "CONFLICT", "conflict":
			return ErrConflict
		case "GONE", "gone":
			return ErrGone
		case "BAD_REQUEST", "bad_request":
			return ErrBadRequest
		case "RATE_LIMITED", "too_many_requests":
			return newError(codeTooManyRequests, tErr.Message)
		default:
			// Return the transport error as-is
			return err
		}
	}

	return err
}
