package durablestream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

// readMode represents the reading mode for continuous stream reading.
type readMode int

const (
	// modeCatchUp reads available data without waiting.
	modeCatchUp readMode = iota
	// modeLongPoll waits for new data using long-polling.
	modeLongPoll
	// modeSSE uses Server-Sent Events for streaming.
	modeSSE
)

// Reader provides continuous stream reading with mode switching.
// It manages offset tracking and mode transitions for efficient streaming.
type Reader struct {
	client *Client
	path   string
	offset Offset
	cursor string
	mode   readMode
	closed bool

	// SSE connection state
	sseConn *sseConnection
}

// LongPoll switches the reader to long-polling mode.
// Returns the reader for chaining.
func (r *Reader) LongPoll() *Reader {
	if r.mode == modeSSE && r.sseConn != nil {
		r.sseConn.Close()
		r.sseConn = nil
	}
	r.mode = modeLongPoll
	return r
}

// SSE switches the reader to Server-Sent Events mode.
// Returns the reader for chaining.
func (r *Reader) SSE() *Reader {
	if r.mode == modeSSE && r.sseConn != nil {
		r.sseConn.Close()
		r.sseConn = nil
	}
	r.mode = modeSSE
	return r
}

// Read performs a single read operation based on the current mode.
// Returns the read result or an error.
func (r *Reader) Read(ctx context.Context) (*StreamData, error) {
	if r.closed {
		return nil, ErrClosed
	}

	switch r.mode {
	case modeCatchUp:
		return r.readCatchUp(ctx)
	case modeLongPoll:
		return r.readLongPoll(ctx)
	case modeSSE:
		return r.readSSE(ctx)
	default:
		return nil, fmt.Errorf("unknown read mode: %d", r.mode)
	}
}

// readCatchUp performs a catch-up read without waiting.
func (r *Reader) readCatchUp(ctx context.Context) (*StreamData, error) {
	result, err := r.doRead(ctx, r.offset)
	if err != nil {
		return nil, err
	}

	// Update state
	r.offset = result.NextOffset
	r.cursor = result.Cursor

	return result, nil
}

// doRead performs a basic HTTP GET read.
func (r *Reader) doRead(ctx context.Context, offset Offset) (*StreamData, error) {
	streamURL := r.client.buildURL(r.path)

	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}

	q := u.Query()
	if !offset.IsZero() {
		q.Set(protocol.QueryOffset, offset.String())
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}

	resp, err := r.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}
	defer resp.Body.Close()

	if err := r.client.checkErrorResponse(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	result := &StreamData{
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

// readLongPoll performs a long-poll read, waiting for new data.
// Section 5.6: Read Stream - Live (Long-poll)
func (r *Reader) readLongPoll(ctx context.Context) (*StreamData, error) {
	streamURL := r.client.buildURL(r.path)

	// Build URL with query parameters
	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set(protocol.QueryOffset, r.offset.String())
	q.Set(protocol.QueryLive, protocol.LiveModeLongPoll)
	if r.cursor != "" {
		q.Set(protocol.QueryCursor, r.cursor)
	}
	u.RawQuery = q.Encode()

	// Create request with long-poll timeout
	ctx, cancel := context.WithTimeout(ctx, r.client.longPollTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := r.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("long-poll request: %w", err)
	}
	defer resp.Body.Close()

	// Handle 204 No Content (timeout with no new data)
	if resp.StatusCode == http.StatusNoContent {
		// Update offset if provided
		if nextOffset := resp.Header.Get(protocol.HeaderStreamNextOffset); nextOffset != "" {
			r.offset = Offset(nextOffset)
		}
		return &StreamData{
			NextOffset: r.offset,
			UpToDate:   true,
		}, nil
	}

	// Check for errors
	if err := r.client.checkErrorResponse(resp); err != nil {
		return nil, err
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	// Parse response
	result := &StreamData{
		NextOffset: Offset(resp.Header.Get(protocol.HeaderStreamNextOffset)),
		Cursor:     resp.Header.Get(protocol.HeaderStreamCursor),
		UpToDate:   resp.Header.Get(protocol.HeaderStreamUpToDate) == "true",
	}

	// Check if JSON mode
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

	// Update state
	r.offset = result.NextOffset
	r.cursor = result.Cursor

	return result, nil
}

// readSSE performs a read using Server-Sent Events.
// Section 5.7: Read Stream - Live (SSE)
func (r *Reader) readSSE(ctx context.Context) (*StreamData, error) {
	// Establish SSE connection if needed
	if r.sseConn == nil {
		if err := r.connectSSE(ctx); err != nil {
			return nil, err
		}
	}

	// Read next event from SSE connection
	event, err := r.sseConn.readEvent()
	if err != nil {
		// Close connection on error and return
		r.sseConn.Close()
		r.sseConn = nil
		return nil, fmt.Errorf("read SSE event: %w", err)
	}

	// Handle control events
	if event.Type == "control" {
		var control sseControlEvent
		if err := json.Unmarshal(event.Data, &control); err != nil {
			return nil, fmt.Errorf("parse control event: %w", err)
		}

		// Update state from control event
		if control.StreamNextOffset != "" {
			r.offset = Offset(control.StreamNextOffset)
		}
		if control.StreamCursor != "" {
			r.cursor = control.StreamCursor
		}

		// Control events don't contain data, read next event
		return r.readSSE(ctx)
	}

	// Handle data events
	if event.Type == "data" {
		result := &StreamData{
			NextOffset: r.offset,
			Cursor:     r.cursor,
			UpToDate:   true,
		}

		// Try to parse as JSON message(s)
		var messages []json.RawMessage
		if err := json.Unmarshal(event.Data, &messages); err == nil {
			result.Messages = messages
		} else {
			// Not a JSON array, treat as single message
			result.Messages = []json.RawMessage{event.Data}
		}

		return result, nil
	}

	// Unknown event type
	return nil, fmt.Errorf("unknown SSE event type: %s", event.Type)
}

// connectSSE establishes an SSE connection.
func (r *Reader) connectSSE(ctx context.Context) error {
	streamURL := r.client.buildURL(r.path)

	// Build URL with query parameters
	u, err := url.Parse(streamURL)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set(protocol.QueryOffset, r.offset.String())
	q.Set(protocol.QueryLive, protocol.LiveModeSSE)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}

	// Set Accept header for SSE
	req.Header.Set("Accept", "text/event-stream")

	resp, err := r.client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE request: %w", err)
	}

	// Check for errors (don't close body, we'll use it for streaming)
	if err := r.client.checkErrorResponse(resp); err != nil {
		resp.Body.Close()
		return err
	}

	// Verify content type
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		resp.Body.Close()
		return fmt.Errorf("unexpected content type: %s", resp.Header.Get("Content-Type"))
	}

	// Create SSE connection
	r.sseConn = newSSEConnection(resp)
	return nil
}

// Offset returns the current offset position of the reader.
func (r *Reader) Offset() Offset {
	return r.offset
}

// Seek repositions the reader to the given offset.
// The next Read will start from this offset.
// Returns the reader for chaining.
func (r *Reader) Seek(offset Offset) *Reader {
	// Close any existing SSE connection since we're changing position
	if r.sseConn != nil {
		r.sseConn.Close()
		r.sseConn = nil
	}
	r.offset = offset
	r.cursor = "" // Clear cursor when seeking
	return r
}

// SeekTail repositions the reader to the current tail of the stream.
// After seeking to tail, subsequent reads will only return new data.
// This is useful for "live tail" scenarios where you only want new messages.
func (r *Reader) SeekTail(ctx context.Context) error {
	info, err := r.client.Head(ctx, r.path)
	if err != nil {
		return err
	}
	r.Seek(info.NextOffset)
	return nil
}

// Close closes the reader and releases any resources.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	if r.sseConn != nil {
		r.sseConn.Close()
		r.sseConn = nil
	}

	return nil
}

// Messages returns an iterator for reading JSON messages from the stream.
// This enables use with Go 1.22+ range-over-func:
//
//	for msg, err := range reader.Messages(ctx) {
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // Process msg
//	}
func (r *Reader) Messages(ctx context.Context) iter.Seq2[json.RawMessage, error] {
	return func(yield func(json.RawMessage, error) bool) {
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			result, err := r.Read(ctx)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			// Yield each message
			for _, msg := range result.Messages {
				if !yield(msg, nil) {
					return
				}
			}

			// If we're up to date in catch-up mode, switch to long-poll
			if result.UpToDate && r.mode == modeCatchUp {
				r.LongPoll()
			}
		}
	}
}

// Bytes returns an iterator for reading raw bytes from the stream.
// This enables use with Go 1.22+ range-over-func:
//
//	for data, err := range reader.Bytes(ctx) {
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // Process data
//	}
func (r *Reader) Bytes(ctx context.Context) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			result, err := r.Read(ctx)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			// Yield data if available
			if len(result.Data) > 0 {
				if !yield(result.Data, nil) {
					return
				}
			}

			// If we're up to date in catch-up mode, switch to long-poll
			if result.UpToDate && r.mode == modeCatchUp {
				r.LongPoll()
			}
		}
	}
}
