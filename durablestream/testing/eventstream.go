package testing

import (
	"context"
	"errors"
	"io"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/transport"
)

// EventStreamStub is a test double for transport.EventStream.
//
// Configure Events to provide a sequence of events, or set Err to simulate
// an error. The stub will return events in order until exhausted, then return
// io.EOF.
//
// Example:
//
//	stub := &EventStreamStub{
//		Events: []transport.Event{
//			{Type: "data", Data: []byte(`{"msg":"hello"}`)},
//			{Type: "control", Data: []byte(`{"streamNextOffset":"2"}`), NextOffset: "2"},
//		},
//	}
//	// Use stub as a transport.EventStream
//	event, err := stub.Next(ctx)
//	// Returns first event, then second, then io.EOF
type EventStreamStub struct {
	Events []transport.Event // Events to return in sequence
	Err    error             // Error to return from Next (if set, returned immediately)
	index  int               // Current position in Events
	closed bool              // Whether Close has been called
}

// Next returns the next event from the Events slice.
//
// Returns Err if set, io.EOF when all events are exhausted or if closed,
// or an error if called after Close.
func (s *EventStreamStub) Next(ctx context.Context) (*transport.Event, error) {
	if s.closed {
		return nil, errors.New("stream closed")
	}
	if s.Err != nil {
		return nil, s.Err
	}
	if s.index >= len(s.Events) {
		return nil, io.EOF
	}
	event := s.Events[s.index]
	s.index++
	return &event, nil
}

// Close marks the stream as closed.
func (s *EventStreamStub) Close() error {
	s.closed = true
	return nil
}

// Reset resets the stub to its initial state, allowing reuse.
func (s *EventStreamStub) Reset() {
	s.index = 0
	s.closed = false
}
