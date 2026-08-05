package durablestream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// Reader provides continuous stream reading with automatic mode transitions.
// It manages offset tracking and switches from catch-up to live mode based
// on the client's configured ReadMode.
//
// A Reader is not safe for concurrent use: Read, Seek, SeekTail, and Messages
// must be called from one goroutine at a time. Close is the exception — it may
// be called concurrently with a blocked Read to abort it.
// A Reader must not be copied after first use.
//
// See PROTOCOL.md Section 5.6-5.8 for read operation details.
type Reader struct {
	client      *Client
	path        string
	offset      Offset
	cursor      string
	readMode    ReadMode
	contentType string // cached content type for JSON validation
	catching    bool   // true while in catch-up phase

	// mu guards closed, eof, the active Read cancellation, and the SSE connection
	// state so Close can abort a blocked Read in every mode.
	// The remaining fields belong to the single reading goroutine.
	mu         sync.Mutex
	closed     bool
	eof        bool
	activeRead *readerRead
	sseStream  transport.EventStream
	sseCancel  *readerCancel

	// SSE connection state (Section 5.8)
	sseUpToDate bool // cached upToDate from last control event
	sseClosed   bool // cached streamClosed from last control event
}

// readerRead tracks the cancellation for one in-flight Read call. Reader is
// single-reader by contract, so at most one is active at a time.
type readerRead struct {
	cancel context.CancelCauseFunc
}

// readerCancel keeps the function itself behind a pointer so Reader retains
// its historical comparable type. Reader values still must not be copied after
// first use; comparability is preserved only for source compatibility.
type readerCancel struct {
	cancel context.CancelFunc
}

// beginRead registers a reader-owned child context so Close can abort any
// blocked HEAD, catch-up, long-poll, or SSE operation.
func (r *Reader) beginRead(ctx context.Context) (context.Context, func() bool, error) {
	readCtx, cancel := context.WithCancelCause(ctx)
	call := &readerRead{cancel: cancel}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel(ErrClosed)
		return nil, nil, ErrClosed
	}
	if r.eof {
		r.mu.Unlock()
		cancel(io.EOF)
		return nil, nil, io.EOF
	}
	r.activeRead = call
	r.mu.Unlock()

	finish := func() bool {
		r.mu.Lock()
		closed := r.closed
		if r.activeRead == call {
			r.activeRead = nil
		}
		r.mu.Unlock()
		cancel(context.Canceled)
		return closed
	}
	return readCtx, finish, nil
}

// setStream stores stream as the active SSE connection. If the reader was closed
// concurrently it closes stream instead and returns false.
func (r *Reader) setStream(stream transport.EventStream) bool {
	r.mu.Lock()
	if r.closed || r.eof {
		r.mu.Unlock()
		stream.Close()
		return false
	}
	r.sseStream = stream
	r.mu.Unlock()
	return true
}

// markEOF records the durable protocol EOF and releases any SSE connection.
// The Read that discovers EOF still returns its StreamData; later Reads return
// io.EOF without issuing another network request.
func (r *Reader) markEOF() {
	r.mu.Lock()
	r.eof = true
	stream := r.sseStream
	cancel := r.sseCancel
	r.sseStream = nil
	r.sseCancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel.cancel()
	}
	if stream != nil {
		_ = stream.Close()
	}
}

// durableEOF distinguishes the protocol's permanent stream closure from an
// io.EOF returned when an open SSE connection is rotated or disconnected.
func (r *Reader) durableEOF() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.eof
}

// clearStream closes the active SSE connection, if any.
func (r *Reader) clearStream() {
	r.mu.Lock()
	stream := r.sseStream
	cancel := r.sseCancel
	r.sseStream = nil
	r.sseCancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel.cancel()
	}
	if stream != nil {
		stream.Close()
	}
}

// Read performs a single read operation based on the current state.
//
// The read that discovers permanent stream closure returns a result with
// StreamData.Closed set, including any final data. Later calls return io.EOF
// without another request. Seek clears that EOF state and permits replay.
//
// During catch-up phase, uses basic GET requests (Section 5.6).
// After UpToDate, switches to live mode based on ReadMode:
//   - ReadModeAuto/ReadModeLongPoll: long-poll requests (Section 5.7)
//   - ReadModeSSE: Server-Sent Events stream (Section 5.8)
func (r *Reader) Read(ctx context.Context) (result *StreamData, err error) {
	ctx, finish, err := r.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Close owns the lifetime of every in-flight Read. Normalize transport
		// cancellation (or even a late success from a context-ignoring custom
		// transport) to the documented Reader error.
		if finish() {
			result = nil
			err = ErrClosed
		}
	}()

	// Determine which operation to perform
	if r.catching {
		return r.readCatchUp(ctx)
	}

	switch r.readMode {
	case ReadModeAuto, ReadModeLongPoll:
		return r.readLongPoll(ctx)
	case ReadModeSSE:
		return r.readSSE(ctx)
	default:
		return r.readLongPoll(ctx)
	}
}

// readCatchUp performs a catch-up read (Section 5.6: Read Stream - Catch-up).
func (r *Reader) readCatchUp(ctx context.Context) (*StreamData, error) {
	// Fetch content type on first read if not already cached
	if r.contentType == "" {
		info, err := r.client.Head(ctx, r.path)
		if err != nil {
			return nil, err
		}
		r.contentType = info.ContentType
	}

	// Catch-up reads return promptly; bound them by the client timeout so a
	// stalled server cannot block the reader forever.
	readCtx, cancel := r.client.withTimeout(ctx)
	defer cancel()

	resp, err := r.client.transport.Read(readCtx, transport.ReadRequest{
		Path:   r.path,
		Offset: r.offset.String(),
	})
	if err != nil {
		return nil, convertTransportErrorWithPath(err, r.path)
	}

	// Validate JSON response BEFORE updating state.
	// If validation fails, we don't advance the offset, allowing the caller
	// to retry from the same position after the fault is cleared.
	if (len(resp.Data) > 0 || !resp.Closed) && isJSONContentType(r.contentType) {
		if err := validateJSONArray(resp.Data); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParseError, err)
		}
	}

	// Update state after successful validation
	r.offset = Offset(resp.NextOffset)
	if resp.Cursor != "" {
		r.cursor = resp.Cursor
	}

	// Check if we should transition to live mode
	if resp.UpToDate || resp.Closed {
		r.catching = false
	}
	if resp.Closed {
		r.markEOF()
	}

	return &StreamData{
		Data:       resp.Data,
		NextOffset: Offset(resp.NextOffset),
		Cursor:     resp.Cursor,
		UpToDate:   resp.UpToDate || resp.Closed,
		Closed:     resp.Closed,
	}, nil
}

// readLongPoll performs a long-poll read (Section 5.7: Read Stream - Live Long-poll).
func (r *Reader) readLongPoll(ctx context.Context) (*StreamData, error) {
	// Fetch content type if not already cached (might happen if reader started in long-poll mode)
	if r.contentType == "" {
		info, err := r.client.Head(ctx, r.path)
		if err != nil {
			return nil, err
		}
		r.contentType = info.ContentType
	}

	resp, err := r.client.transport.LongPoll(ctx, transport.LongPollRequest{
		Path:   r.path,
		Offset: r.offset.String(),
		Cursor: r.cursor,
	})
	if err != nil {
		return nil, convertTransportErrorWithPath(err, r.path)
	}

	// Validate JSON response BEFORE updating state.
	// If validation fails, we don't advance the offset, allowing the caller
	// to retry from the same position after the fault is cleared.
	if len(resp.Data) > 0 && isJSONContentType(r.contentType) {
		if err := validateJSONArray(resp.Data); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParseError, err)
		}
	}

	// Update state after successful validation
	r.offset = Offset(resp.NextOffset)
	if resp.Cursor != "" {
		r.cursor = resp.Cursor
	}
	if resp.Closed {
		r.markEOF()
	}

	return &StreamData{
		Data:       resp.Data,
		NextOffset: Offset(resp.NextOffset),
		Cursor:     r.cursor,
		UpToDate:   resp.UpToDate || resp.Closed,
		Closed:     resp.Closed,
	}, nil
}

// readSSE performs a read using Server-Sent Events (Section 5.8: Read Stream - Live SSE).
func (r *Reader) readSSE(ctx context.Context) (*StreamData, error) {
	// Fetch content type if not already cached
	if r.contentType == "" {
		info, err := r.client.Head(ctx, r.path)
		if err != nil {
			return nil, err
		}
		r.contentType = info.ContentType
	}

	// Consecutive control events carry no data, so keep reading until a data
	// event arrives. This is a loop rather than recursion: a server emitting
	// control events indefinitely would otherwise grow the stack without bound.
	for {
		stream, err := r.ensureSSEStream(ctx)
		if err != nil {
			return nil, err
		}

		event, err := stream.Next(ctx)
		if err != nil {
			r.clearStream()
			return nil, sseReadError(err)
		}

		switch event.Type {
		case "control":
			// A final control event is the SSE EOF signal. Return it once so
			// callers can observe closure; later Read calls return io.EOF.
			r.applyControlEvent(event)
			if event.Closed {
				result := r.sseData(nil)
				r.markEOF()
				return result, nil
			}
			continue

		case "data":
			// Validate JSON response for JSON streams
			if len(event.Data) > 0 && isJSONContentType(r.contentType) {
				if err := validateJSON(event.Data); err != nil {
					// The data event has been consumed but its trailing control
					// event has not. Reconnect from the last confirmed offset so a
					// manual retry cannot consume that control event and silently
					// advance past the malformed data.
					r.clearStream()
					return nil, fmt.Errorf("%w: %v", ErrParseError, err)
				}
			}

			// Per the SSE protocol, every data event is followed by a control
			// event that confirms the new offset. We must read it to:
			// 1. Get the correct offset for this data
			// 2. Detect malformed control events (see conformance tests)
			nextEvent, err := stream.Next(ctx)
			if err != nil {
				r.clearStream()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				// The data has no confirmed offset. Do not return it: reconnecting
				// from the last confirmed offset will safely redeliver the event.
				return nil, sseReadError(err)
			}

			// A second data event would make the first event's offset ambiguous.
			switch nextEvent.Type {
			case "control":
				r.applyControlEvent(nextEvent)
			case "data":
				r.clearStream()
				return nil, fmt.Errorf("%w: data event is not followed by a control event", ErrParseError)
			default:
				r.clearStream()
				return nil, fmt.Errorf("%w: unknown SSE event type %q", ErrParseError, nextEvent.Type)
			}

			result := r.sseData(event)
			if nextEvent.Closed {
				r.markEOF()
			}
			return result, nil

		default:
			r.clearStream()
			return nil, fmt.Errorf("%w: unknown SSE event type %q", ErrParseError, event.Type)
		}
	}
}

// ensureSSEStream returns the active SSE connection, opening one if needed.
func (r *Reader) ensureSSEStream(ctx context.Context) (transport.EventStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if stream := r.sseStream; stream != nil {
		r.mu.Unlock()
		return stream, nil
	}

	// The HTTP request context controls the entire lifetime of its response
	// body. A Read context, however, belongs to only this call and callers may
	// cancel it as soon as Read returns. Retain its values but give the SSE
	// connection a reader-owned lifetime; the AfterFunc below still propagates
	// cancellation while the opening handshake is in progress.
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	r.sseCancel = &readerCancel{cancel: streamCancel}
	r.mu.Unlock()

	// SSE requires an offset parameter (Section 5.8).
	// Default to -1 (stream beginning) if no offset specified.
	offset := r.offset.String()
	if offset == "" {
		offset = "-1"
	}
	stopOpeningCancel := context.AfterFunc(ctx, streamCancel)
	stream, err := r.client.transport.SSE(streamCtx, transport.SSERequest{
		Path:   r.path,
		Offset: offset,
		Cursor: r.cursor,
	})
	stopped := stopOpeningCancel()
	if err != nil {
		streamCancel()
		r.mu.Lock()
		r.sseCancel = nil
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return nil, ErrClosed
		}
		return nil, convertTransportErrorWithPath(err, r.path)
	}
	// If cancellation won the race with a transport that nevertheless returned
	// a stream, do not retain a connection whose request context is already
	// dead.
	if !stopped && ctx.Err() != nil {
		streamCancel()
		stream.Close()
		r.mu.Lock()
		r.sseCancel = nil
		r.mu.Unlock()
		return nil, ctx.Err()
	}
	if !r.setStream(stream) {
		streamCancel()
		r.mu.Lock()
		r.sseCancel = nil
		r.mu.Unlock()
		return nil, ErrClosed
	}
	return stream, nil
}

// applyControlEvent updates reader position from a control event (Section 5.8).
func (r *Reader) applyControlEvent(event *transport.Event) {
	if event.NextOffset != "" {
		r.offset = Offset(event.NextOffset)
	}
	if event.Cursor != "" {
		r.cursor = event.Cursor
	}
	r.sseUpToDate = event.UpToDate || event.Closed
	r.sseClosed = event.Closed
}

// sseData builds the result for a data event at the reader's current position.
func (r *Reader) sseData(event *transport.Event) *StreamData {
	var data []byte
	if event != nil {
		data = event.Data
	}
	return &StreamData{
		Data:       data,
		NextOffset: r.offset,
		Cursor:     r.cursor,
		UpToDate:   r.sseUpToDate,
		Closed:     r.sseClosed,
	}
}

// sseReadError maps an EventStream error to a durablestream error.
func sseReadError(err error) error {
	var tErr *transport.Error
	if errors.As(err, &tErr) && tErr.Code == "PARSE_ERROR" {
		return fmt.Errorf("%w: %v", ErrParseError, tErr.Message)
	}
	return fmt.Errorf("read SSE event: %w", err)
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
	r.clearStream()
	r.offset = offset
	r.cursor = ""     // Clear cursor when seeking
	r.catching = true // Reset to catch-up mode
	r.mu.Lock()
	r.eof = false
	r.mu.Unlock()
	r.sseUpToDate = false
	r.sseClosed = false
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
	r.catching = false // Already at tail, go straight to live mode
	return nil
}

// Close closes the reader and releases any resources.
//
// Close is safe to call concurrently with a blocked Read and more than once.
// Closing while a Read is in flight aborts it: the SSE connection is torn down
// and the pending Read returns an error.
func (r *Reader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	activeRead := r.activeRead
	r.activeRead = nil
	stream := r.sseStream
	cancel := r.sseCancel
	r.sseStream = nil
	r.sseCancel = nil
	r.mu.Unlock()

	if activeRead != nil {
		activeRead.cancel(ErrClosed)
	}
	if cancel != nil {
		cancel.cancel()
	}
	if stream != nil {
		stream.Close()
	}
	return nil
}

// Messages returns an iterator for reading messages from the stream.
// For JSON streams, it parses the JSON array and yields individual messages.
// Each Message can be decoded via msg.Decode(&v) or accessed as raw bytes via msg.Bytes().
//
// This enables use with Go 1.22+ range-over-func:
//
//	for msg, err := range reader.Messages(ctx) {
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    var event MyEvent
//	    if err := msg.Decode(&event); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//
// # Error behavior
//
// Permanent stream closure ends iteration cleanly after any final messages; it
// is not yielded as an error.
//
// Errors are yielded to the caller, which decides whether to keep iterating.
// If the caller continues, the iterator distinguishes two cases:
//
//   - Terminal errors — ErrNotFound, ErrGone, ErrBadRequest, ErrClosed,
//     ErrParseError, and other responses that cannot succeed on retry — end
//     iteration after the error is yielded.
//   - Transient errors — network failures, 5xx responses, rate limits — are
//     retried after a backoff that grows from 100ms to 5s, so a failing server
//     is not hammered.
//
// Cancelling ctx ends iteration after yielding ctx.Err().
func (r *Reader) Messages(ctx context.Context) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		backoff := messagesInitialBackoff

		for {
			select {
			case <-ctx.Done():
				yield(Message{}, ctx.Err())
				return
			default:
			}

			result, err := r.Read(ctx)
			if err != nil {
				// Only a protocol EOF is normal iterator completion. Open SSE
				// connections also end with io.EOF when a server rotates them;
				// reconnect those without surfacing a spurious stream closure.
				transientSSEEOF := errors.Is(err, io.EOF) && !r.durableEOF()
				if errors.Is(err, io.EOF) && !transientSSEEOF {
					return
				}
				if !transientSSEEOF {
					if !yield(Message{}, err) {
						return
					}
					// If Read returned because the iterator's own context was
					// cancelled, the cancellation was just yielded above. Stop now
					// rather than entering backoff and yielding ctx.Err() a second
					// time.
					if ctx.Err() != nil {
						return
					}
					if isTerminalReadError(err) {
						return
					}
				}
				// Transient failure: back off before retrying so a persistently
				// broken server is not hammered.
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					yield(Message{}, ctx.Err())
					return
				case <-timer.C:
				}
				backoff = min(backoff*2, messagesMaxBackoff)
				continue
			}
			backoff = messagesInitialBackoff

			// Only application/json has protocol-defined message boundaries.
			// Other content types are opaque bytes even when a chunk happens to
			// contain a syntactically valid JSON array.
			if !isJSONContentType(r.contentType) {
				if len(result.Data) > 0 && !yield(Message{data: result.Data}, nil) {
					return
				}
				if result.Closed {
					return
				}
				continue
			}

			// Parse a JSON array and yield its individual messages (Section 9.1).
			messages, err := parseJSONMessages(result.Data)
			if err != nil {
				// Not valid JSON array - yield as single message
				if len(result.Data) > 0 {
					if !yield(Message{data: result.Data}, nil) {
						return
					}
				}
			} else {
				for _, msg := range messages {
					if !yield(Message{data: msg}, nil) {
						return
					}
				}
			}
			if result.Closed {
				return
			}
		}
	}
}

// Backoff bounds for Messages when reads fail transiently.
const (
	messagesInitialBackoff = 100 * time.Millisecond
	messagesMaxBackoff     = 5 * time.Second
)

// isTerminalReadError reports whether err will keep failing if the same read is
// retried, meaning iteration should stop rather than spin.
//
// Context errors are deliberately not terminal here: they may come from a
// per-operation deadline against a stalled server, which retrying can clear.
// Cancellation of the caller's own context is handled by the iterator directly.
func isTerminalReadError(err error) bool {
	switch {
	case errors.Is(err, ErrNotFound),
		errors.Is(err, ErrGone),
		errors.Is(err, ErrBadRequest),
		errors.Is(err, ErrClosed),
		errors.Is(err, ErrConflict),
		errors.Is(err, ErrPayloadTooLarge),
		errors.Is(err, ErrParseError),
		errors.Is(err, transport.ErrResponseTooLarge):
		return true
	}

	// Transport errors that map to a client-side 4xx cannot succeed on retry.
	// 408 (timeout) and 429 (rate limited) are the exceptions.
	var tErr *transport.Error
	if errors.As(err, &tErr) {
		switch tErr.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooManyRequests:
			return false
		}
		return tErr.StatusCode >= 400 && tErr.StatusCode < 500
	}
	return false
}

// parseJSONMessages parses a JSON array and returns individual message bytes.
// Returns error if data is not a valid JSON array.
// See PROTOCOL.md Section 9.1: JSON Mode.
func parseJSONMessages(data []byte) ([][]byte, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	// Quick check: must start with [ and end with ]
	if data[0] != '[' || data[len(data)-1] != ']' {
		return nil, fmt.Errorf("not a JSON array")
	}

	// Parse as array of raw messages
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	messages := make([][]byte, len(raw))
	for i, r := range raw {
		messages[i] = r
	}
	return messages, nil
}

// isJSONContentType returns true if the content type indicates JSON.
func isJSONContentType(contentType string) bool {
	// Normalize: take media type before semicolon
	ct := contentType
	if idx := strings.IndexByte(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}
	ct = strings.TrimSpace(ct)
	return strings.EqualFold(ct, "application/json")
}

// validateJSON validates an application/json SSE data event. Unlike HTTP GET
// responses, an SSE event may contain either one JSON value or a batched array.
func validateJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}

	// Use json.Valid for quick validation
	if !json.Valid(data) {
		return fmt.Errorf("invalid JSON")
	}
	return nil
}

// validateJSONArray validates the response shape required for application/json
// catch-up and long-poll GET responses (Section 9.1.5).
func validateJSONArray(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return fmt.Errorf("empty response, expected JSON array")
	}
	if data[0] != '[' || data[len(data)-1] != ']' || !json.Valid(data) {
		return fmt.Errorf("expected valid JSON array")
	}
	return nil
}
