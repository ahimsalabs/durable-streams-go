package durablestream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// Reader provides continuous stream reading with automatic mode transitions.
// It manages offset tracking and switches from catch-up to live mode based
// on the client's configured ReadMode.
//
// See PROTOCOL.md Section 5.5-5.7 for read operation details.
type Reader struct {
	client   *Client
	path     string
	offset   Offset
	cursor   string
	readMode ReadMode
	catching bool // true while in catch-up phase
	closed   bool

	// SSE connection state (Section 5.7)
	sseStream transport.EventStream
}

// Read performs a single read operation based on the current state.
// Returns the read result or an error.
//
// During catch-up phase, uses basic GET requests (Section 5.5).
// After UpToDate, switches to live mode based on ReadMode:
//   - ReadModeAuto/ReadModeLongPoll: long-poll requests (Section 5.6)
//   - ReadModeSSE: Server-Sent Events stream (Section 5.7)
func (r *Reader) Read(ctx context.Context) (*StreamData, error) {
	if r.closed {
		return nil, ErrClosed
	}

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

// readCatchUp performs a catch-up read (Section 5.5: Read Stream - Catch-up).
func (r *Reader) readCatchUp(ctx context.Context) (*StreamData, error) {
	resp, err := r.client.transport.Read(ctx, transport.ReadRequest{
		Path:   r.path,
		Offset: r.offset.String(),
	})
	if err != nil {
		return nil, convertTransportError(err)
	}

	// Update state
	r.offset = Offset(resp.NextOffset)
	r.cursor = resp.Cursor

	// Check if we should transition to live mode
	if resp.UpToDate {
		r.catching = false
	}

	return &StreamData{
		Data:       resp.Data,
		NextOffset: Offset(resp.NextOffset),
		Cursor:     resp.Cursor,
		UpToDate:   resp.UpToDate,
	}, nil
}

// readLongPoll performs a long-poll read (Section 5.6: Read Stream - Live Long-poll).
func (r *Reader) readLongPoll(ctx context.Context) (*StreamData, error) {
	resp, err := r.client.transport.LongPoll(ctx, transport.LongPollRequest{
		Path:   r.path,
		Offset: r.offset.String(),
		Cursor: r.cursor,
	})
	if err != nil {
		return nil, convertTransportError(err)
	}

	// Update state
	r.offset = Offset(resp.NextOffset)
	r.cursor = resp.Cursor

	return &StreamData{
		Data:       resp.Data,
		NextOffset: Offset(resp.NextOffset),
		Cursor:     resp.Cursor,
		UpToDate:   resp.UpToDate,
	}, nil
}

// readSSE performs a read using Server-Sent Events (Section 5.7: Read Stream - Live SSE).
func (r *Reader) readSSE(ctx context.Context) (*StreamData, error) {
	// Establish SSE connection if needed
	if r.sseStream == nil {
		stream, err := r.client.transport.SSE(ctx, transport.SSERequest{
			Path:   r.path,
			Offset: r.offset.String(),
		})
		if err != nil {
			return nil, convertTransportError(err)
		}
		r.sseStream = stream
	}

	// Read next event
	event, err := r.sseStream.Next(ctx)
	if err != nil {
		// Close connection on error
		r.sseStream.Close()
		r.sseStream = nil
		return nil, fmt.Errorf("read SSE event: %w", err)
	}

	// Handle control events (Section 5.7)
	if event.Type == "control" {
		// Update state from control event
		if event.NextOffset != "" {
			r.offset = Offset(event.NextOffset)
		}
		if event.Cursor != "" {
			r.cursor = event.Cursor
		}
		// Control events don't contain data, read next event
		return r.readSSE(ctx)
	}

	// Handle data events
	if event.Type == "data" {
		return &StreamData{
			Data:       event.Data,
			NextOffset: r.offset,
			Cursor:     r.cursor,
			UpToDate:   true,
		}, nil
	}

	return nil, fmt.Errorf("unknown SSE event type: %s", event.Type)
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
	if r.sseStream != nil {
		r.sseStream.Close()
		r.sseStream = nil
	}
	r.offset = offset
	r.cursor = ""     // Clear cursor when seeking
	r.catching = true // Reset to catch-up mode
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
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	if r.sseStream != nil {
		r.sseStream.Close()
		r.sseStream = nil
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
func (r *Reader) Messages(ctx context.Context) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		for {
			select {
			case <-ctx.Done():
				yield(Message{}, ctx.Err())
				return
			default:
			}

			result, err := r.Read(ctx)
			if err != nil {
				if !yield(Message{}, err) {
					return
				}
				continue
			}

			// Parse JSON array and yield individual messages (Section 7.1)
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
		}
	}
}

// parseJSONMessages parses a JSON array and returns individual message bytes.
// Returns error if data is not a valid JSON array.
// See PROTOCOL.md Section 7.1: JSON Mode.
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
