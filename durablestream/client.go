package durablestream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

// Client provides methods to interact with durable streams over HTTP.
type Client struct {
	httpClient      *http.Client
	baseURL         string
	timeout         time.Duration
	longPollTimeout time.Duration
}

// NewClient creates a new stream client with default settings.
func NewClient() *Client {
	return &Client{
		httpClient:      http.DefaultClient,
		baseURL:         "",
		timeout:         30 * time.Second,
		longPollTimeout: 60 * time.Second,
	}
}

// BaseURL sets the base URL for stream operations and returns the client for chaining.
func (c *Client) BaseURL(url string) *Client {
	c.baseURL = strings.TrimRight(url, "/")
	return c
}

// HTTPClient sets the underlying HTTP client and returns the client for chaining.
func (c *Client) HTTPClient(hc *http.Client) *Client {
	c.httpClient = hc
	return c
}

// Timeout sets the default timeout for operations and returns the client for chaining.
func (c *Client) Timeout(d time.Duration) *Client {
	c.timeout = d
	return c
}

// LongPollTimeout sets the timeout for long-poll operations and returns the client for chaining.
func (c *Client) LongPollTimeout(d time.Duration) *Client {
	c.longPollTimeout = d
	return c
}

// ClientReadResult contains the result of a read operation.
type ClientReadResult struct {
	Data       []byte            // Raw bytes (non-JSON mode)
	Messages   []json.RawMessage // Individual messages (JSON mode)
	NextOffset Offset            // Next offset to read from
	Cursor     string            // Opaque cursor for long-poll
	UpToDate   bool              // True if caught up to tail
}

// CreateOptions specifies options for creating a stream.
type CreateOptions struct {
	// ContentType sets the content type for the stream.
	// Default: "application/octet-stream"
	ContentType string

	// TTL sets a relative time-to-live for the stream.
	// Mutually exclusive with ExpiresAt.
	TTL *time.Duration

	// ExpiresAt sets an absolute expiry time for the stream.
	// Mutually exclusive with TTL.
	ExpiresAt *time.Time

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
		if opts.TTL != nil {
			req.Header.Set(protocol.HeaderStreamTTL, strconv.FormatInt(int64(opts.TTL.Seconds()), 10))
		}
		if opts.ExpiresAt != nil {
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

// AppendOptions specifies options for appending to a stream.
type AppendOptions struct {
	// Seq sets the sequence number for writer coordination.
	// If provided and less than or equal to the last sequence, returns ErrConflict.
	Seq string
}

// Append appends data to a stream and returns the new tail offset.
// Pass nil for opts to use defaults.
func (c *Client) Append(ctx context.Context, path string, data []byte, opts *AppendOptions) (Offset, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty append not allowed")
	}

	streamURL := c.buildURL(path)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, streamURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("append request: %w", err)
	}

	// Detect content type from stream info
	info, err := c.Head(ctx, path)
	if err != nil {
		return "", fmt.Errorf("get stream info: %w", err)
	}
	req.Header.Set("Content-Type", info.ContentType)

	// Set sequence number if provided
	if opts != nil && opts.Seq != "" {
		req.Header.Set(protocol.HeaderStreamSeq, opts.Seq)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("append request: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkErrorResponse(resp); err != nil {
		return "", err
	}

	nextOffset := resp.Header.Get(protocol.HeaderStreamNextOffset)
	return Offset(nextOffset), nil
}

// ReadOptions specifies options for reading from a stream.
type ReadOptions struct {
	// Offset specifies where to start reading.
	// Default: ZeroOffset (stream start)
	Offset Offset
}

// Read performs a catch-up read from the stream.
// Pass nil for opts to read from the start.
func (c *Client) Read(ctx context.Context, path string, opts *ReadOptions) (*ClientReadResult, error) {
	streamURL := c.buildURL(path)

	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}

	q := u.Query()
	if opts != nil && !opts.Offset.IsZero() {
		q.Set(protocol.QueryOffset, opts.Offset.String())
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkErrorResponse(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	result := &ClientReadResult{
		NextOffset: Offset(resp.Header.Get(protocol.HeaderStreamNextOffset)),
		Cursor:     resp.Header.Get(protocol.HeaderStreamCursor),
		UpToDate:   resp.Header.Get(protocol.HeaderStreamUpToDate) == "true",
	}

	contentType := resp.Header.Get("Content-Type")
	if protocol.IsJSONContentType(contentType) {
		var messages []json.RawMessage
		if len(body) > 0 {
			if err := json.Unmarshal(body, &messages); err != nil {
				return nil, fmt.Errorf("parse JSON response: %w", err)
			}
		}
		result.Messages = messages
	} else {
		result.Data = body
	}

	return result, nil
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

// StreamWriter provides zero-allocation batch append operations.
// Use Writer() to create a StreamWriter for high-throughput scenarios.
type StreamWriter struct {
	client      *Client
	ctx         context.Context
	path        string
	contentType string
	offset      Offset
	closed      bool
}

// Writer creates a new StreamWriter for batch append operations.
// The writer should be closed when done to release resources.
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
func (w *StreamWriter) SendWithSeq(seq string, data []byte) error {
	if w.closed {
		return ErrClosed
	}

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

// Close closes the writer.
func (w *StreamWriter) Close() error {
	w.closed = true
	return nil
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
			ttl := time.Duration(ttlSecs) * time.Second
			info.TTL = &ttl
		}
	}

	if expiresStr := headers.Get(protocol.HeaderStreamExpiresAt); expiresStr != "" {
		expiresAt, err := time.Parse(time.RFC3339, expiresStr)
		if err == nil {
			info.ExpiresAt = &expiresAt
		}
	}

	return info, nil
}
