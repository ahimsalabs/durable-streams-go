package transport

import (
	"bufio"
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
)

// HTTP header names defined by the Durable Streams Protocol.
// See PROTOCOL.md Section 11: IANA Considerations.
const (
	headerStreamTTL        = "Stream-TTL"
	headerStreamExpiresAt  = "Stream-Expires-At"
	headerStreamSeq        = "Stream-Seq"
	headerStreamCursor     = "Stream-Cursor"
	headerStreamNextOffset = "Stream-Next-Offset"
	headerStreamUpToDate   = "Stream-Up-To-Date"
)

// Query parameter names used in stream operations.
const (
	queryOffset = "offset"
	queryLive   = "live"
	queryCursor = "cursor"
)

// Valid values for the "live" query parameter.
const (
	liveModeLongPoll = "long-poll"
	liveModeSSE      = "sse"
)

// HTTPTransport implements Transport using HTTP.
// This is the default transport for the Durable Streams Protocol.
type HTTPTransport struct {
	baseURL         string
	client          *http.Client
	longPollTimeout time.Duration
	headers         HeaderProvider
}

// HeaderProvider returns headers to include in requests.
// Called for each request, allowing dynamic values like refreshed auth tokens.
type HeaderProvider func(ctx context.Context) (http.Header, error)

// HTTPConfig configures an HTTPTransport.
type HTTPConfig struct {
	// Client is the underlying HTTP client. Default: http.DefaultClient.
	Client *http.Client

	// LongPollTimeout is the timeout for long-poll operations (Section 5.6).
	// Default: 60s.
	LongPollTimeout time.Duration

	// Headers provides headers to include in all requests.
	// Called per-request to allow dynamic values (e.g., auth tokens).
	// If nil, no additional headers are added.
	Headers HeaderProvider
}

// NewHTTPTransport creates a new HTTP transport for the given base URL.
// Pass nil for cfg to use defaults.
func NewHTTPTransport(baseURL string, cfg *HTTPConfig) *HTTPTransport {
	t := &HTTPTransport{
		baseURL:         strings.TrimRight(baseURL, "/"),
		client:          http.DefaultClient,
		longPollTimeout: 60 * time.Second,
	}

	if cfg != nil {
		if cfg.Client != nil {
			t.client = cfg.Client
		}
		if cfg.LongPollTimeout > 0 {
			t.longPollTimeout = cfg.LongPollTimeout
		}
		t.headers = cfg.Headers
	}

	return t
}

// applyHeaders applies configured headers to a request.
func (t *HTTPTransport) applyHeaders(ctx context.Context, req *http.Request) error {
	if t.headers == nil {
		return nil
	}
	headers, err := t.headers(ctx)
	if err != nil {
		return fmt.Errorf("get headers: %w", err)
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return nil
}

// Read performs a catch-up read (Section 5.5: Read Stream - Catch-up).
func (t *HTTPTransport) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	q := u.Query()
	if req.Offset != "" {
		q.Set(queryOffset, req.Offset)
	}
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return &ReadResponse{
		Data:       body,
		NextOffset: resp.Header.Get(headerStreamNextOffset),
		Cursor:     resp.Header.Get(headerStreamCursor),
		UpToDate:   resp.Header.Get(headerStreamUpToDate) == "true",
	}, nil
}

// LongPoll performs a long-poll read (Section 5.6: Read Stream - Live Long-poll).
func (t *HTTPTransport) LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	q := u.Query()
	q.Set(queryOffset, req.Offset)
	q.Set(queryLive, liveModeLongPoll)
	if req.Cursor != "" {
		// Echo cursor for CDN collapsing (Section 8)
		q.Set(queryCursor, req.Cursor)
	}
	u.RawQuery = q.Encode()

	// Apply timeout for long-poll
	timeout := t.longPollTimeout
	if req.Timeout > 0 {
		timeout = req.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Handle 204 No Content (timeout with no new data) - Section 5.6
	if resp.StatusCode == http.StatusNoContent {
		return &ReadResponse{
			NextOffset: resp.Header.Get(headerStreamNextOffset),
			UpToDate:   true,
		}, nil
	}

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return &ReadResponse{
		Data:       body,
		NextOffset: resp.Header.Get(headerStreamNextOffset),
		Cursor:     resp.Header.Get(headerStreamCursor),
		UpToDate:   resp.Header.Get(headerStreamUpToDate) == "true",
	}, nil
}

// SSE opens a Server-Sent Events stream (Section 5.7: Read Stream - Live SSE).
func (t *HTTPTransport) SSE(ctx context.Context, req SSERequest) (EventStream, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	q := u.Query()
	q.Set(queryOffset, req.Offset)
	q.Set(queryLive, liveModeSSE)
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set Accept header for SSE (Section 5.7)
	httpReq.Header.Set("Accept", "text/event-stream")

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	// Check for errors (don't close body - we use it for streaming)
	if err := checkErrorResponse(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}

	// Verify content type (Section 5.7)
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected content type: %s", resp.Header.Get("Content-Type"))
	}

	return &httpEventStream{
		reader:   bufio.NewReader(resp.Body),
		response: resp,
	}, nil
}

// Append adds data to a stream (Section 5.2: Append to Stream).
func (t *HTTPTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	// Reject empty body (Section 5.2)
	if len(req.Data) == 0 {
		return nil, &Error{Code: "BAD_REQUEST", Message: "empty append not allowed", StatusCode: 400}
	}

	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(req.Data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if req.ContentType != "" {
		httpReq.Header.Set("Content-Type", req.ContentType)
	}
	if req.Seq != "" {
		// Writer coordination (Section 5.2)
		httpReq.Header.Set(headerStreamSeq, req.Seq)
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}

	return &AppendResponse{
		NextOffset: resp.Header.Get(headerStreamNextOffset),
	}, nil
}

// Create creates a new stream (Section 5.1: Create Stream).
func (t *HTTPTransport) Create(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	var body io.Reader
	if req.InitialData != nil {
		body = bytes.NewReader(req.InitialData)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if req.ContentType != "" {
		httpReq.Header.Set("Content-Type", req.ContentType)
	}
	if req.TTL > 0 {
		httpReq.Header.Set(headerStreamTTL, strconv.FormatInt(int64(req.TTL.Seconds()), 10))
	}
	if !req.ExpiresAt.IsZero() {
		httpReq.Header.Set(headerStreamExpiresAt, req.ExpiresAt.Format(time.RFC3339))
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}

	return &CreateResponse{
		NextOffset: resp.Header.Get(headerStreamNextOffset),
	}, nil
}

// Delete removes a stream (Section 5.3: Delete Stream).
func (t *HTTPTransport) Delete(ctx context.Context, req DeleteRequest) error {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return fmt.Errorf("build URL: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	return checkErrorResponse(resp)
}

// Head retrieves stream metadata (Section 5.4: Stream Metadata).
func (t *HTTPTransport) Head(ctx context.Context, req HeadRequest) (*HeadResponse, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if err := t.applyHeaders(ctx, httpReq); err != nil {
		return nil, err
	}

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}

	result := &HeadResponse{
		ContentType: resp.Header.Get("Content-Type"),
		NextOffset:  resp.Header.Get(headerStreamNextOffset),
	}

	if ttlStr := resp.Header.Get(headerStreamTTL); ttlStr != "" {
		ttlSecs, err := strconv.ParseInt(ttlStr, 10, 64)
		if err == nil {
			result.TTL = time.Duration(ttlSecs) * time.Second
		}
	}

	if expiresStr := resp.Header.Get(headerStreamExpiresAt); expiresStr != "" {
		expiresAt, err := time.Parse(time.RFC3339, expiresStr)
		if err == nil {
			result.ExpiresAt = expiresAt
		}
	}

	return result, nil
}

// buildURL constructs the full URL for a stream path.
func (t *HTTPTransport) buildURL(path string) (*url.URL, error) {
	var fullURL string
	if t.baseURL == "" {
		fullURL = path
	} else {
		fullURL = t.baseURL + "/" + strings.TrimLeft(path, "/")
	}
	return url.Parse(fullURL)
}

// checkErrorResponse checks for error responses and returns appropriate errors.
func checkErrorResponse(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Try to parse JSON error response
	var errResp struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if body, err := io.ReadAll(resp.Body); err == nil && len(body) > 0 {
		if json.Unmarshal(body, &errResp) == nil && errResp.Code != "" {
			return &Error{
				Code:       errResp.Code,
				Message:    errResp.Message,
				StatusCode: resp.StatusCode,
			}
		}
	}

	// Map HTTP status to error
	return &Error{
		Code:       httpStatusToCode(resp.StatusCode),
		Message:    fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status),
		StatusCode: resp.StatusCode,
	}
}

// httpStatusToCode maps HTTP status codes to error codes.
func httpStatusToCode(status int) string {
	switch status {
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusGone:
		return "GONE"
	case http.StatusTooManyRequests:
		return "RATE_LIMITED"
	case http.StatusUnauthorized:
		return "UNAUTHORIZED"
	case http.StatusForbidden:
		return "FORBIDDEN"
	default:
		return "UNKNOWN"
	}
}

// Error represents a transport error.
type Error struct {
	Code       string
	Message    string
	StatusCode int
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// httpEventStream implements EventStream for SSE connections.
type httpEventStream struct {
	reader   *bufio.Reader
	response *http.Response
}

// Next reads the next SSE event (Section 5.7).
//
// SSE format per PROTOCOL.md Section 5.7:
//
//	event: data
//	data: [
//	data: {"k":"v"},
//	data: {"k":"w"}
//	data: ]
//
//	event: control
//	data: {"streamNextOffset":"123456_789","streamCursor":"abc"}
func (s *httpEventStream) Next(ctx context.Context) (*Event, error) {
	// Check context before blocking read
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var eventType string
	var dataLines []string

	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read SSE line: %w", err)
		}

		// Trim newline characters
		line = strings.TrimRight(line, "\r\n")

		// Empty line indicates end of event
		if line == "" {
			if eventType != "" {
				return s.buildEvent(eventType, dataLines)
			}
			// Empty event, continue reading
			continue
		}

		// Parse field
		// Per SSE spec: lines starting with ":" are comments, unknown fields are ignored
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(line[5:]))
		}
		// Comment lines (":") and unknown field types are ignored per SSE spec
	}
}

// buildEvent constructs an Event from parsed SSE data.
func (s *httpEventStream) buildEvent(eventType string, dataLines []string) (*Event, error) {
	data := buildSSEData(dataLines)

	event := &Event{
		Type: eventType,
		Data: data,
	}

	// Extract offset and cursor from control events (Section 5.7)
	if eventType == "control" {
		var control struct {
			StreamNextOffset string `json:"streamNextOffset"`
			StreamCursor     string `json:"streamCursor,omitempty"`
		}
		if err := json.Unmarshal(data, &control); err == nil {
			event.NextOffset = control.StreamNextOffset
			event.Cursor = control.StreamCursor
		}
	}

	return event, nil
}

// buildSSEData combines multiple data lines into a single value.
// For multi-line data fields, the SSE spec says to join with newlines.
func buildSSEData(lines []string) []byte {
	if len(lines) == 0 {
		return []byte("{}")
	}

	if len(lines) == 1 {
		return []byte(lines[0])
	}

	// Join multiple lines with newlines (per SSE spec)
	var buf bytes.Buffer
	for i, line := range lines {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(line)
	}

	return buf.Bytes()
}

// Close closes the SSE connection.
func (s *httpEventStream) Close() error {
	if s.response != nil && s.response.Body != nil {
		return s.response.Body.Close()
	}
	return nil
}
