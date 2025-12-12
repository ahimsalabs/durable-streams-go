package testing

import (
	"context"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// RecordingTransport wraps a Transport and records all method calls.
//
// This is useful for testing that your code makes the expected Transport calls
// in the right order with the right arguments.
//
// Example:
//
//	inner := // your real or stub transport
//	recorder := NewRecordingTransport(inner)
//	// Use recorder in your tests
//	// After test runs:
//	calls := recorder.Calls()
//	if len(calls) != 2 {
//		t.Errorf("expected 2 calls, got %d", len(calls))
//	}
//	if calls[0].Method != "Read" {
//		t.Errorf("first call should be Read, got %s", calls[0].Method)
//	}
type RecordingTransport struct {
	inner transport.Transport
	calls []Call
	mu    sync.Mutex
}

// Call represents a recorded method call.
type Call struct {
	Method string // Method name (e.g., "Read", "Append")
	Args   any    // Method-specific request struct
	At     time.Time
}

// NewRecordingTransport creates a RecordingTransport that wraps inner.
func NewRecordingTransport(inner transport.Transport) *RecordingTransport {
	return &RecordingTransport{inner: inner}
}

// record adds a call to the recording.
func (r *RecordingTransport) record(method string, args any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, Call{
		Method: method,
		Args:   args,
		At:     time.Now(),
	})
}

// Read records the call and delegates to inner.
func (r *RecordingTransport) Read(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
	r.record("Read", req)
	return r.inner.Read(ctx, req)
}

// LongPoll records the call and delegates to inner.
func (r *RecordingTransport) LongPoll(ctx context.Context, req transport.LongPollRequest) (*transport.ReadResponse, error) {
	r.record("LongPoll", req)
	return r.inner.LongPoll(ctx, req)
}

// SSE records the call and delegates to inner.
func (r *RecordingTransport) SSE(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
	r.record("SSE", req)
	return r.inner.SSE(ctx, req)
}

// Append records the call and delegates to inner.
func (r *RecordingTransport) Append(ctx context.Context, req transport.AppendRequest) (*transport.AppendResponse, error) {
	r.record("Append", req)
	return r.inner.Append(ctx, req)
}

// Create records the call and delegates to inner.
func (r *RecordingTransport) Create(ctx context.Context, req transport.CreateRequest) (*transport.CreateResponse, error) {
	r.record("Create", req)
	return r.inner.Create(ctx, req)
}

// Delete records the call and delegates to inner.
func (r *RecordingTransport) Delete(ctx context.Context, req transport.DeleteRequest) error {
	r.record("Delete", req)
	return r.inner.Delete(ctx, req)
}

// Head records the call and delegates to inner.
func (r *RecordingTransport) Head(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
	r.record("Head", req)
	return r.inner.Head(ctx, req)
}

// Calls returns a copy of all recorded calls.
func (r *RecordingTransport) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Call, len(r.calls))
	copy(result, r.calls)
	return result
}

// Reset clears all recorded calls.
func (r *RecordingTransport) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// CallCount returns the number of times the specified method was called.
func (r *RecordingTransport) CallCount(method string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, call := range r.calls {
		if call.Method == method {
			count++
		}
	}
	return count
}
