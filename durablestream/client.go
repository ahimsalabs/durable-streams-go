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

// StreamData contains the result of a stream read operation.
type StreamData struct {
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
