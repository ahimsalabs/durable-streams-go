package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HTTP header names defined by the Durable Streams Protocol.
// See PROTOCOL.md Section 13.2: IANA Considerations — HTTP Headers.
const (
	headerStreamTTL        = "Stream-TTL"
	headerStreamExpiresAt  = "Stream-Expires-At"
	headerStreamSeq        = "Stream-Seq"
	headerStreamCursor     = "Stream-Cursor"
	headerStreamNextOffset = "Stream-Next-Offset"
	headerStreamUpToDate   = "Stream-Up-To-Date"

	// Idempotent producer headers (Section 5.2.1)
	headerProducerID          = "Producer-Id"
	headerProducerEpoch       = "Producer-Epoch"
	headerProducerSeq         = "Producer-Seq"
	headerProducerExpectedSeq = "Producer-Expected-Seq"
	headerProducerReceivedSeq = "Producer-Received-Seq"

	// SSE data encoding (Section 5.8). Present with value "base64" when the
	// server base64-encodes data events for binary content types.
	headerSSEDataEncoding = "Stream-SSE-Data-Encoding"
)

// sseEncodingBase64 is the only defined value for headerSSEDataEncoding (Section 5.8).
const sseEncodingBase64 = "base64"

// Response size limits. These bound memory consumed by a single response so a
// broken or hostile server cannot exhaust client memory.
const (
	// defaultMaxResponseSize bounds bodies of Read, LongPoll, and other
	// non-streaming responses. Override via HTTPConfig.MaxResponseSize.
	defaultMaxResponseSize = 64 << 20 // 64 MiB

	// defaultMaxSSEEventSize bounds the bytes consumed by a single SSE event
	// (all of its lines combined). Override via HTTPConfig.MaxSSEEventSize.
	// The default server accepts messages up to 10 MiB and emits at least one
	// complete message per event. Binary messages expand by 4/3 in base64, so
	// leave enough room for that valid wire representation plus SSE framing.
	defaultMaxSSEEventSize = 16 << 20 // 16 MiB

	// maxErrorBodySize bounds how much of an error response body is read into
	// the error message. Error bodies are diagnostic only.
	maxErrorBodySize = 4 << 10 // 4 KiB

	// sseReadBufferSize is the buffered reader size for SSE connections. Lines
	// longer than this are accumulated across reads up to the event size limit.
	sseReadBufferSize = 64 << 10 // 64 KiB

	// Producer epochs and sequence numbers are limited to integers that can be
	// represented exactly by interoperable JSON implementations (Section 5.2.1).
	maxProducerHeaderValue = int64(1<<53 - 1)
)

// ErrResponseTooLarge is returned when a response body or SSE event exceeds the
// configured size limit. Check with errors.Is.
var ErrResponseTooLarge = errors.New("response exceeds size limit")

// defaultHTTPClient returns the process-wide client used when HTTPConfig.Client
// is nil. It deliberately sets no Client.Timeout: long-poll and SSE requests are
// long-lived by design, and a client-level timeout would abort them. Per-request
// deadlines come from the context instead.
var defaultHTTPClient = sync.OnceValue(func() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			// Supplying DialContext disables HTTP/2's conservative auto-enable
			// path unless this is set explicitly.
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// No ResponseHeaderTimeout: a long-poll server legitimately holds a
			// request open for its full wait before writing response headers.
		},
	}
})

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
	maxResponseSize int64
	maxSSEEventSize int
}

// HeaderProvider returns headers to include in requests.
// Called for each request, allowing dynamic values like refreshed auth tokens.
type HeaderProvider func(ctx context.Context) (http.Header, error)

// HTTPConfig configures an HTTPTransport.
type HTTPConfig struct {
	// Client is the underlying HTTP client.
	//
	// Default: a package-owned client with bounded dial, TLS handshake, and
	// idle-connection settings and no Client.Timeout. Do not set Client.Timeout
	// on a custom client: it applies to the whole request including body reads
	// and would abort long-poll and SSE reads. Use per-request context
	// deadlines instead.
	Client *http.Client

	// LongPollTimeout is the timeout for long-poll operations (Section 5.7).
	// Default: 60s.
	LongPollTimeout time.Duration

	// Headers provides headers to include in all requests.
	// Called per-request to allow dynamic values (e.g., auth tokens).
	// If nil, no additional headers are added.
	Headers HeaderProvider

	// MaxResponseSize bounds the body of a single non-streaming response.
	// Responses larger than this fail with ErrResponseTooLarge.
	// Zero or negative values default to 64 MiB.
	MaxResponseSize int64

	// MaxSSEEventSize bounds the bytes consumed by a single SSE event.
	// Events larger than this fail with ErrResponseTooLarge.
	// Zero or negative values default to 16 MiB.
	MaxSSEEventSize int
}

// NewHTTPTransport creates a new HTTP transport for the given base URL.
// Pass nil for cfg to use defaults.
func NewHTTPTransport(baseURL string, cfg *HTTPConfig) *HTTPTransport {
	t := &HTTPTransport{
		baseURL:         strings.TrimRight(baseURL, "/"),
		client:          defaultHTTPClient(),
		longPollTimeout: 60 * time.Second,
		maxResponseSize: defaultMaxResponseSize,
		maxSSEEventSize: defaultMaxSSEEventSize,
	}

	if cfg != nil {
		if cfg.Client != nil {
			t.client = cfg.Client
		}
		if cfg.LongPollTimeout > 0 {
			t.longPollTimeout = cfg.LongPollTimeout
		}
		if cfg.MaxResponseSize > 0 {
			t.maxResponseSize = cfg.MaxResponseSize
		}
		if cfg.MaxSSEEventSize > 0 {
			t.maxSSEEventSize = cfg.MaxSSEEventSize
		}
		t.headers = cfg.Headers
	}

	return t
}

// readResponseBody reads a response body, failing if it exceeds limit.
func readResponseBody(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes: %w", limit, ErrResponseTooLarge)
	}
	return body, nil
}

// responseParseError reports a successful response that does not conform to
// the protocol. StatusCode retains the HTTP status for diagnostics even though
// the transport error is about the response rather than the operation.
func responseParseError(resp *http.Response, format string, args ...any) *Error {
	return &Error{
		Code:       "PARSE_ERROR",
		Message:    fmt.Sprintf(format, args...),
		StatusCode: resp.StatusCode,
	}
}

// requiredResponseHeader returns a required, single-valued response header.
func requiredResponseHeader(resp *http.Response, name string) (string, error) {
	values := resp.Header.Values(name)
	if len(values) == 0 {
		return "", responseParseError(resp, "response missing required %s header", name)
	}
	if len(values) != 1 {
		return "", responseParseError(resp, "response contains multiple %s headers", name)
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return "", responseParseError(resp, "response contains an empty %s header", name)
	}
	return value, nil
}

// optionalResponseHeader returns an optional, single-valued response header.
// A present empty or repeated header is malformed rather than equivalent to
// omission.
func optionalResponseHeader(resp *http.Response, name string) (string, bool, error) {
	values := resp.Header.Values(name)
	if len(values) == 0 {
		return "", false, nil
	}
	if len(values) != 1 {
		return "", false, responseParseError(resp, "response contains multiple %s headers", name)
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return "", false, responseParseError(resp, "response contains an empty %s header", name)
	}
	return value, true, nil
}

// producerResponseHeader parses a producer integer response header. When
// required is false, an absent header is valid, but a present malformed value
// is still a protocol error.
func producerResponseHeader(resp *http.Response, name string, required bool) (int, bool, error) {
	values := resp.Header.Values(name)
	if len(values) == 0 {
		if required {
			return 0, false, responseParseError(resp, "response missing required %s header", name)
		}
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, false, responseParseError(resp, "response contains multiple %s headers", name)
	}

	raw := strings.TrimSpace(values[0])
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || value > maxProducerHeaderValue || int64(int(value)) != value {
		return 0, false, responseParseError(
			resp,
			"response contains invalid %s header %q; want an integer from 0 to %d",
			name,
			raw,
			maxProducerHeaderValue,
		)
	}
	return int(value), true, nil
}

func hasResponseHeader(resp *http.Response, name string) bool {
	return len(resp.Header.Values(name)) > 0
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

// Read performs a catch-up read (Section 5.6: Read Stream - Catch-up).
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
	nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
	if err != nil {
		return nil, err
	}

	body, err := readResponseBody(resp.Body, t.maxResponseSize)
	if err != nil {
		return nil, err
	}

	return &ReadResponse{
		Data:       body,
		NextOffset: nextOffset,
		Cursor:     resp.Header.Get(headerStreamCursor),
		UpToDate:   resp.Header.Get(headerStreamUpToDate) == "true",
	}, nil
}

// LongPoll performs a long-poll read (Section 5.7: Read Stream - Live Long-poll).
func (t *HTTPTransport) LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	q := u.Query()
	q.Set(queryOffset, req.Offset)
	q.Set(queryLive, liveModeLongPoll)
	if req.Cursor != "" {
		// Echo cursor for CDN collapsing (Section 10.1)
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

	// Handle 204 No Content (timeout with no new data) - Section 5.7
	if resp.StatusCode == http.StatusNoContent {
		nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
		if err != nil {
			return nil, err
		}
		return &ReadResponse{
			NextOffset: nextOffset,
			Cursor:     resp.Header.Get(headerStreamCursor),
			UpToDate:   true,
		}, nil
	}

	if err := checkErrorResponse(resp); err != nil {
		return nil, err
	}
	nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
	if err != nil {
		return nil, err
	}

	body, err := readResponseBody(resp.Body, t.maxResponseSize)
	if err != nil {
		return nil, err
	}

	return &ReadResponse{
		Data:       body,
		NextOffset: nextOffset,
		Cursor:     resp.Header.Get(headerStreamCursor),
		UpToDate:   resp.Header.Get(headerStreamUpToDate) == "true",
	}, nil
}

// SSE opens a Server-Sent Events stream (Section 5.8: Read Stream - Live SSE).
func (t *HTTPTransport) SSE(ctx context.Context, req SSERequest) (EventStream, error) {
	u, err := t.buildURL(req.Path)
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	q := u.Query()
	q.Set(queryOffset, req.Offset)
	q.Set(queryLive, liveModeSSE)
	if req.Cursor != "" {
		// Echo the last cursor when reconnecting for CDN collapsing (Section 10.1).
		q.Set(queryCursor, req.Cursor)
	}
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set Accept header for SSE (Section 5.8)
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

	// Verify content type (Section 5.8). MIME types are case-insensitive and an
	// SSE response may carry parameters such as charset=utf-8.
	contentType := resp.Header.Get("Content-Type")
	mediaType, _, mediaTypeErr := mime.ParseMediaType(contentType)
	if mediaTypeErr != nil || !strings.EqualFold(mediaType, "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected content type: %s", contentType)
	}

	// Binary streams arrive base64-encoded (Section 5.8).
	encoding := strings.TrimSpace(resp.Header.Get(headerSSEDataEncoding))
	base64Data := strings.EqualFold(encoding, sseEncodingBase64)
	if encoding != "" && !base64Data {
		resp.Body.Close()
		return nil, fmt.Errorf("unsupported %s: %s", headerSSEDataEncoding, encoding)
	}

	return &httpEventStream{
		reader:       bufio.NewReaderSize(resp.Body, sseReadBufferSize),
		response:     resp,
		base64Data:   base64Data,
		maxEventSize: t.maxSSEEventSize,
	}, nil
}

// Append adds data to a stream (Section 5.2: Append to Stream).
func (t *HTTPTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	// Reject empty body (Section 5.2)
	if len(req.Data) == 0 {
		return nil, &Error{
			Code:       "BAD_REQUEST",
			Message:    "empty append not allowed",
			StatusCode: http.StatusBadRequest,
		}
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

	// Idempotent producer headers (Section 5.2.1)
	if req.HasProducerHeaders {
		httpReq.Header.Set(headerProducerID, req.ProducerID)
		httpReq.Header.Set(headerProducerEpoch, strconv.Itoa(req.ProducerEpoch))
		httpReq.Header.Set(headerProducerSeq, strconv.Itoa(req.ProducerSeq))
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
		// Producer-specific 403 and 409 responses carry required reconciliation
		// metadata. Generic authorization and conflict errors do not.
		var tErr *Error
		if errors.As(err, &tErr) {
			if resp.StatusCode == http.StatusForbidden {
				detail := strings.ToLower(tErr.Code + " " + tErr.Message)
				staleEpoch := hasResponseHeader(resp, headerProducerEpoch) ||
					strings.Contains(detail, "stale_epoch") ||
					strings.Contains(detail, "stale producer epoch")
				if staleEpoch {
					epoch, _, parseErr := producerResponseHeader(resp, headerProducerEpoch, true)
					if parseErr != nil {
						return nil, parseErr
					}
					tErr.ProducerEpoch = epoch
				}
			}
			if resp.StatusCode == http.StatusConflict {
				detail := strings.ToLower(tErr.Code + " " + tErr.Message)
				sequenceGap := hasResponseHeader(resp, headerProducerExpectedSeq) ||
					hasResponseHeader(resp, headerProducerReceivedSeq) ||
					strings.Contains(detail, "sequence_gap") ||
					strings.Contains(detail, "sequence gap")
				if sequenceGap {
					expected, _, parseErr := producerResponseHeader(resp, headerProducerExpectedSeq, true)
					if parseErr != nil {
						return nil, parseErr
					}
					received, _, parseErr := producerResponseHeader(resp, headerProducerReceivedSeq, true)
					if parseErr != nil {
						return nil, parseErr
					}
					tErr.ProducerExpectedSeq = expected
					tErr.ProducerReceivedSeq = received
				}
			}
		}
		return nil, err
	}

	nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
	if err != nil {
		return nil, err
	}
	epoch, _, err := producerResponseHeader(resp, headerProducerEpoch, req.HasProducerHeaders)
	if err != nil {
		return nil, err
	}
	seq, _, err := producerResponseHeader(resp, headerProducerSeq, req.HasProducerHeaders)
	if err != nil {
		return nil, err
	}

	// 204 means "duplicate, already applied" only for idempotent producers
	// (Section 5.2.1). Servers also answer 204 to ordinary appends that carry no
	// producer headers, where it carries no deduplication meaning.
	result := &AppendResponse{
		NextOffset:    nextOffset,
		Duplicate:     req.HasProducerHeaders && resp.StatusCode == http.StatusNoContent,
		StatusCode:    resp.StatusCode,
		ProducerEpoch: epoch,
		ProducerSeq:   seq,
	}

	return result, nil
}

// Create creates a new stream (Section 5.1: Create Stream).
func (t *HTTPTransport) Create(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
	if req.TTL < 0 || req.TTL%time.Second != 0 {
		return nil, &Error{
			Code:       "BAD_REQUEST",
			Message:    "TTL must be a non-negative whole number of seconds",
			StatusCode: http.StatusBadRequest,
		}
	}
	if req.TTL > 0 && !req.ExpiresAt.IsZero() {
		return nil, &Error{
			Code:       "BAD_REQUEST",
			Message:    "TTL and ExpiresAt are mutually exclusive",
			StatusCode: http.StatusBadRequest,
		}
	}

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
		httpReq.Header.Set(headerStreamTTL, strconv.FormatInt(int64(req.TTL/time.Second), 10))
	}
	if !req.ExpiresAt.IsZero() {
		httpReq.Header.Set(headerStreamExpiresAt, req.ExpiresAt.Format(time.RFC3339Nano))
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
	nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
	if err != nil {
		return nil, err
	}

	return &CreateResponse{
		NextOffset: nextOffset,
	}, nil
}

// Delete removes a stream (Section 5.4: Delete Stream).
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

// Head retrieves stream metadata (Section 5.5: Stream Metadata).
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
	nextOffset, err := requiredResponseHeader(resp, headerStreamNextOffset)
	if err != nil {
		return nil, err
	}
	contentType, err := requiredResponseHeader(resp, "Content-Type")
	if err != nil {
		return nil, err
	}

	result := &HeadResponse{
		ContentType: contentType,
		NextOffset:  nextOffset,
	}

	if ttlStr, ok, err := optionalResponseHeader(resp, headerStreamTTL); err != nil {
		return nil, err
	} else if ok {
		ttlSecs, err := strconv.ParseInt(ttlStr, 10, 64)
		const maxTTLSeconds = (1<<63 - 1) / int64(time.Second)
		if err != nil || ttlSecs < 0 || ttlSecs > maxTTLSeconds ||
			(ttlStr != "0" && (ttlStr[0] < '1' || ttlStr[0] > '9')) {
			return nil, responseParseError(
				resp,
				"response contains invalid %s header %q; want a non-negative whole number of seconds",
				headerStreamTTL,
				ttlStr,
			)
		}
		result.TTL = time.Duration(ttlSecs) * time.Second
	}

	if expiresStr, ok, err := optionalResponseHeader(resp, headerStreamExpiresAt); err != nil {
		return nil, err
	} else if ok {
		expiresAt, err := time.Parse(time.RFC3339Nano, expiresStr)
		if err != nil {
			return nil, responseParseError(
				resp,
				"response contains invalid %s header %q: %v",
				headerStreamExpiresAt,
				expiresStr,
				err,
			)
		}
		result.ExpiresAt = expiresAt
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

	// Error bodies are diagnostic only; read a bounded prefix.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	bodyStr := string(body)

	// Try to parse JSON error response
	var errResp struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if len(body) > 0 && json.Unmarshal(body, &errResp) == nil && errResp.Code != "" {
		return &Error{
			Code:       errResp.Code,
			Message:    errResp.Message,
			StatusCode: resp.StatusCode,
		}
	}

	// Check for sequence conflict in plain text body (server may return plain text)
	if resp.StatusCode == http.StatusConflict {
		lowerBody := strings.ToLower(bodyStr)
		if strings.Contains(lowerBody, "sequence") {
			return &Error{
				Code:       "SEQUENCE_CONFLICT",
				Message:    bodyStr,
				StatusCode: resp.StatusCode,
			}
		}
	}

	// Map HTTP status to error, include body text as message if available
	msg := bodyStr
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}
	return &Error{
		Code:       httpStatusToCode(resp.StatusCode),
		Message:    msg,
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
	case http.StatusRequestEntityTooLarge:
		return "PAYLOAD_TOO_LARGE"
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

	// ProducerEpoch is set for 403 (stale epoch) errors (Section 5.2.1).
	// Contains the server's current epoch for the producer.
	ProducerEpoch int

	// ProducerExpectedSeq is set for 409 (sequence gap) errors (Section 5.2.1).
	ProducerExpectedSeq int

	// ProducerReceivedSeq is set for 409 (sequence gap) errors (Section 5.2.1).
	ProducerReceivedSeq int
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// httpEventStream implements EventStream for SSE connections.
//
// A stream is not safe for concurrent Next calls. Close may be called
// concurrently with Next: it closes the underlying body, which unblocks a
// pending read.
type httpEventStream struct {
	reader       *bufio.Reader
	response     *http.Response
	base64Data   bool // server sent Stream-SSE-Data-Encoding: base64 (Section 5.8)
	maxEventSize int  // bytes allowed for a single event; <= 0 means default
	skipLF       bool // previous line ended in CR; ignore an immediately following LF

	closeOnce sync.Once
	closeErr  error
}

// Next reads the next SSE event (Section 5.8).
//
// SSE format per PROTOCOL.md Section 5.8:
//
//	event: data
//	data: [
//	data: {"k":"v"},
//	data: {"k":"w"}
//	data: ]
//
//	event: control
//	data: {"streamNextOffset":"123456_789","streamCursor":"abc"}
//
// Next blocks until the next event arrives, the event exceeds the configured
// size limit (ErrResponseTooLarge), the connection ends, or ctx is done.
//
// Cancelling ctx unblocks a pending read by closing the underlying connection,
// so the stream is unusable afterwards; the error returned is ctx.Err().
func (s *httpEventStream) Next(ctx context.Context) (*Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Watch for cancellation while blocked on the network read. Closing the body
	// is the only way to unblock it. The watcher is bounded by this call: the
	// deferred close of stop always runs, and Wait joins the goroutine.
	if done := ctx.Done(); done != nil {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			select {
			case <-done:
				s.Close()
			case <-stop:
			}
		})
		defer func() {
			close(stop)
			wg.Wait()
		}()
	}

	event, err := s.readEvent()
	if err != nil {
		// A cancelled context manifests as a read error on the closed body.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return event, nil
}

// readEvent reads lines until an event terminator, bounding total event size.
func (s *httpEventStream) readEvent() (*Event, error) {
	limit := s.maxEventSize
	if limit <= 0 {
		limit = defaultMaxSSEEventSize
	}

	var eventType string
	var dataLines []string
	used := 0

	for {
		line, err := s.readLine(&used, limit)
		if err != nil {
			return nil, err
		}

		// Empty line indicates end of event
		if line == "" {
			if eventType != "" {
				return s.buildEvent(eventType, dataLines)
			}
			if len(dataLines) > 0 {
				return nil, &Error{
					Code:    "PARSE_ERROR",
					Message: "SSE event missing required event type",
				}
			}
			// Empty event: reset the budget and keep reading.
			used = 0
			dataLines = nil
			continue
		}

		// Parse field
		// Per SSE spec: lines starting with ":" are comments, unknown fields are ignored
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			// Per SSE spec: strip exactly one leading space if present
			value := line[5:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
			dataLines = append(dataLines, value)
		}
		// Comment lines (":") and unknown field types are ignored per SSE spec
	}
}

// readLine reads one line terminated by CR, LF, or CRLF, without returning the
// terminator. It adds the bytes consumed to *used and fails once the running
// total exceeds limit, so neither a single enormous line nor an endless stream
// of lines can exhaust memory.
func (s *httpEventStream) readLine(used *int, limit int) (string, error) {
	var buf []byte
	for {
		b, err := s.reader.ReadByte()
		if err != nil {
			return "", fmt.Errorf("read SSE line: %w", err)
		}
		*used = *used + 1
		if *used > limit {
			return "", fmt.Errorf("SSE event exceeds %d bytes: %w", limit, ErrResponseTooLarge)
		}

		// A CR terminates a line immediately. Defer swallowing its optional LF
		// until the next read so a CR-only stream need not wait for another byte.
		if s.skipLF {
			s.skipLF = false
			if b == '\n' {
				continue
			}
		}

		switch b {
		case '\n':
			return string(buf), nil
		case '\r':
			s.skipLF = true
			return string(buf), nil
		default:
			buf = append(buf, b)
		}
	}
}

// buildEvent constructs an Event from parsed SSE data.
func (s *httpEventStream) buildEvent(eventType string, dataLines []string) (*Event, error) {
	if eventType != "data" && eventType != "control" {
		return nil, &Error{
			Code:    "PARSE_ERROR",
			Message: fmt.Sprintf("unknown SSE event type %q", eventType),
		}
	}

	// Base64 applies to data events only; control events stay JSON (Section 5.8).
	if s.base64Data && eventType != "control" {
		decoded, err := decodeBase64SSEData(dataLines)
		if err != nil {
			return nil, err
		}
		return &Event{Type: eventType, Data: decoded}, nil
	}

	data := buildSSEData(dataLines)

	event := &Event{
		Type: eventType,
		Data: data,
	}

	// Extract offset, cursor, and upToDate from control events (Section 5.8)
	// Control events MUST have valid JSON with required fields - return error if malformed
	if eventType == "control" {
		// Control events MUST have data - empty data is an error
		trimmedData := bytes.TrimSpace(data)
		if len(trimmedData) == 0 || string(trimmedData) == "{}" {
			return nil, &Error{
				Code:    "PARSE_ERROR",
				Message: "empty control event data",
			}
		}

		var control struct {
			StreamNextOffset string `json:"streamNextOffset"`
			StreamCursor     string `json:"streamCursor,omitempty"`
			UpToDate         bool   `json:"upToDate,omitempty"`
		}
		if err := json.Unmarshal(data, &control); err != nil {
			return nil, &Error{
				Code:    "PARSE_ERROR",
				Message: fmt.Sprintf("malformed control event JSON: %v", err),
			}
		}

		// streamNextOffset is required for control events
		if control.StreamNextOffset == "" {
			return nil, &Error{
				Code:    "PARSE_ERROR",
				Message: "control event missing required streamNextOffset field",
			}
		}

		event.NextOffset = control.StreamNextOffset
		event.Cursor = control.StreamCursor
		event.UpToDate = control.UpToDate
	}

	return event, nil
}

// buildSSEData combines multiple data lines into a single value.
// For multi-line data fields, the SSE spec says to join with newlines.
func buildSSEData(lines []string) []byte {
	if len(lines) == 0 {
		return nil
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

// decodeBase64SSEData decodes the base64 payload of a data event (Section 5.8).
// Servers may split base64 text across several data lines, so the lines are
// concatenated and all CR/LF removed before decoding with the standard RFC 4648
// alphabet.
func decodeBase64SSEData(lines []string) ([]byte, error) {
	joined := strings.Join(lines, "")
	// Per Section 5.8 clients MUST remove \n and \r inserted between lines.
	joined = strings.NewReplacer("\n", "", "\r", "").Replace(joined)

	if joined == "" {
		// A zero-length payload is encoded as the empty string.
		return nil, nil
	}
	if len(joined)%4 != 0 {
		return nil, &Error{
			Code:    "PARSE_ERROR",
			Message: fmt.Sprintf("base64 SSE data length %d is not a multiple of 4", len(joined)),
		}
	}

	decoded, err := base64.StdEncoding.DecodeString(joined)
	if err != nil {
		return nil, &Error{
			Code:    "PARSE_ERROR",
			Message: fmt.Sprintf("decode base64 SSE data: %v", err),
		}
	}
	return decoded, nil
}

// Close closes the SSE connection. It is safe to call more than once and
// concurrently with Next.
func (s *httpEventStream) Close() error {
	s.closeOnce.Do(func() {
		if s.response != nil && s.response.Body != nil {
			s.closeErr = s.response.Body.Close()
		}
	})
	return s.closeErr
}
