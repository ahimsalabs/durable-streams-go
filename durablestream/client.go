package durablestream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// HTTPClient is the underlying HTTP client. Default: http.DefaultClient.
	HTTPClient *http.Client

	// Timeout is the default timeout for operations. Default: 30s.
	Timeout time.Duration

	// LongPollTimeout is the timeout for long-poll operations. Default: 60s.
	LongPollTimeout time.Duration
}

// Client provides methods to interact with durable streams over HTTP.
type Client struct {
	httpClient      *http.Client
	baseURL         string
	timeout         time.Duration
	longPollTimeout time.Duration
}

// NewClient creates a new stream client for the given base URL.
// Pass nil for cfg to use defaults.
func NewClient(baseURL string, cfg *ClientConfig) *Client {
	c := &Client{
		baseURL:         strings.TrimRight(baseURL, "/"),
		httpClient:      http.DefaultClient,
		timeout:         30 * time.Second,
		longPollTimeout: 60 * time.Second,
	}

	if cfg != nil {
		if cfg.HTTPClient != nil {
			c.httpClient = cfg.HTTPClient
		}
		if cfg.Timeout > 0 {
			c.timeout = cfg.Timeout
		}
		if cfg.LongPollTimeout > 0 {
			c.longPollTimeout = cfg.LongPollTimeout
		}
	}

	return c
}

// StreamData contains the result of a stream read operation.
type StreamData struct {
	Data       []byte // Raw response bytes (always populated)
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

// CreateOptions specifies options for creating a stream.
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

// Create creates a new stream with the given options.
// Pass nil for opts to use defaults.
func (c *Client) Create(ctx context.Context, path string, opts *CreateOptions) (*StreamInfo, error) {
	streamURL := c.buildURL(path)

	var body io.Reader
	if opts != nil && opts.InitialData != nil {
		body = bytes.NewReader(opts.InitialData)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, streamURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers from options
	if opts != nil {
		if opts.ContentType != "" {
			req.Header.Set("Content-Type", opts.ContentType)
		}
		if opts.TTL > 0 {
			req.Header.Set(protocol.HeaderStreamTTL, strconv.FormatInt(int64(opts.TTL.Seconds()), 10))
		}
		if !opts.ExpiresAt.IsZero() {
			req.Header.Set(protocol.HeaderStreamExpiresAt, opts.ExpiresAt.Format(time.RFC3339))
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkErrorResponse(resp); err != nil {
		return nil, err
	}

	return c.parseStreamInfo(resp.Header)
}

// Head queries stream metadata without transferring data.
func (c *Client) Head(ctx context.Context, path string) (*StreamInfo, error) {
	streamURL := c.buildURL(path)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, streamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("head request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("head request: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkErrorResponse(resp); err != nil {
		return nil, err
	}

	return c.parseStreamInfo(resp.Header)
}

// Delete removes a stream.
func (c *Client) Delete(ctx context.Context, path string) error {
	streamURL := c.buildURL(path)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, streamURL, nil)
	if err != nil {
		return fmt.Errorf("delete request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete request: %w", err)
	}
	defer resp.Body.Close()

	return c.checkErrorResponse(resp)
}

// StreamWriter provides efficient append operations by caching stream metadata.
// Create via Client.Writer(). The writer holds no resources requiring cleanup.
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

// Send appends a message to the stream.
// Returns error if the stream is closed or the append fails.
func (w *StreamWriter) Send(data []byte) error {
	return w.SendWithSeq("", data)
}

// SendWithSeq appends a message with sequence coordination.
// If seq is provided and less than or equal to the last sequence, returns ErrConflict.
func (w *StreamWriter) SendWithSeq(seq string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty append not allowed")
	}

	streamURL := w.client.buildURL(w.path)

	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, streamURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("append request: %w", err)
	}

	req.Header.Set("Content-Type", w.contentType)
	if seq != "" {
		req.Header.Set(protocol.HeaderStreamSeq, seq)
	}

	resp, err := w.client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("append request: %w", err)
	}
	defer resp.Body.Close()

	if err := w.client.checkErrorResponse(resp); err != nil {
		return err
	}

	w.offset = Offset(resp.Header.Get(protocol.HeaderStreamNextOffset))
	return nil
}

// Offset returns the current tail offset after the last successful append.
func (w *StreamWriter) Offset() Offset {
	return w.offset
}

// Reader creates a new Reader for continuous reading from a stream.
func (c *Client) Reader(path string, offset Offset) *Reader {
	return &Reader{
		client: c,
		path:   path,
		offset: offset,
		mode:   modeCatchUp,
	}
}

// buildURL constructs the full URL for a stream path.
func (c *Client) buildURL(path string) string {
	if c.baseURL == "" {
		return path
	}
	return c.baseURL + "/" + strings.TrimLeft(path, "/")
}

// checkErrorResponse checks for error responses and returns appropriate errors.
func (c *Client) checkErrorResponse(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Try to parse JSON error response
	var streamErr protoError
	if body, err := io.ReadAll(resp.Body); err == nil && len(body) > 0 {
		if json.Unmarshal(body, &streamErr) == nil && streamErr.Code != "" {
			return &streamErr
		}
	}

	// Map HTTP status to error code
	code := httpStatusToErrorCode(resp.StatusCode)
	return newError(code, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status))
}

// parseStreamInfo extracts stream metadata from response headers.
func (c *Client) parseStreamInfo(headers http.Header) (*StreamInfo, error) {
	info := &StreamInfo{
		ContentType: headers.Get("Content-Type"),
		NextOffset:  Offset(headers.Get(protocol.HeaderStreamNextOffset)),
	}

	if ttlStr := headers.Get(protocol.HeaderStreamTTL); ttlStr != "" {
		ttlSecs, err := strconv.ParseInt(ttlStr, 10, 64)
		if err == nil {
			info.TTL = time.Duration(ttlSecs) * time.Second
		}
	}

	if expiresStr := headers.Get(protocol.HeaderStreamExpiresAt); expiresStr != "" {
		expiresAt, err := time.Parse(time.RFC3339, expiresStr)
		if err == nil {
			info.ExpiresAt = expiresAt
		}
	}

	return info, nil
}
