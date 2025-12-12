// Package testing provides test helpers for durablestream.
//
// This package contains test doubles and utilities for testing code that uses
// the durablestream library. The main helpers are:
//
//   - TransportStub: A configurable stub Transport for unit tests
//   - RecordingTransport: A decorator that records all Transport method calls
//   - EventStreamStub: A stub EventStream for testing SSE functionality
//
// Example usage:
//
//	stub := &testing.TransportStub{
//		ReadFunc: func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
//			return &transport.ReadResponse{
//				Data:       []byte(`[{"foo":"bar"}]`),
//				NextOffset: "1",
//				UpToDate:   true,
//			}, nil
//		},
//	}
//	// Use stub as a transport.Transport in your tests
package testing

import (
	"context"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// TransportStub is a test double for transport.Transport.
//
// Set the function fields to control behavior. Unset methods panic with
// a "not implemented" message, making it easy to identify which methods
// your tests need to stub.
//
// Example:
//
//	stub := &TransportStub{
//		ReadFunc: func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
//			return &transport.ReadResponse{Data: []byte("test"), NextOffset: "1"}, nil
//		},
//	}
type TransportStub struct {
	ReadFunc     func(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error)
	LongPollFunc func(ctx context.Context, req transport.LongPollRequest) (*transport.ReadResponse, error)
	SSEFunc      func(ctx context.Context, req transport.SSERequest) (transport.EventStream, error)
	AppendFunc   func(ctx context.Context, req transport.AppendRequest) (*transport.AppendResponse, error)
	CreateFunc   func(ctx context.Context, req transport.CreateRequest) (*transport.CreateResponse, error)
	DeleteFunc   func(ctx context.Context, req transport.DeleteRequest) error
	HeadFunc     func(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error)
}

// Read delegates to ReadFunc or panics if not set.
func (s *TransportStub) Read(ctx context.Context, req transport.ReadRequest) (*transport.ReadResponse, error) {
	if s.ReadFunc == nil {
		panic("TransportStub.Read not implemented")
	}
	return s.ReadFunc(ctx, req)
}

// LongPoll delegates to LongPollFunc or panics if not set.
func (s *TransportStub) LongPoll(ctx context.Context, req transport.LongPollRequest) (*transport.ReadResponse, error) {
	if s.LongPollFunc == nil {
		panic("TransportStub.LongPoll not implemented")
	}
	return s.LongPollFunc(ctx, req)
}

// SSE delegates to SSEFunc or panics if not set.
func (s *TransportStub) SSE(ctx context.Context, req transport.SSERequest) (transport.EventStream, error) {
	if s.SSEFunc == nil {
		panic("TransportStub.SSE not implemented")
	}
	return s.SSEFunc(ctx, req)
}

// Append delegates to AppendFunc or panics if not set.
func (s *TransportStub) Append(ctx context.Context, req transport.AppendRequest) (*transport.AppendResponse, error) {
	if s.AppendFunc == nil {
		panic("TransportStub.Append not implemented")
	}
	return s.AppendFunc(ctx, req)
}

// Create delegates to CreateFunc or panics if not set.
func (s *TransportStub) Create(ctx context.Context, req transport.CreateRequest) (*transport.CreateResponse, error) {
	if s.CreateFunc == nil {
		panic("TransportStub.Create not implemented")
	}
	return s.CreateFunc(ctx, req)
}

// Delete delegates to DeleteFunc or panics if not set.
func (s *TransportStub) Delete(ctx context.Context, req transport.DeleteRequest) error {
	if s.DeleteFunc == nil {
		panic("TransportStub.Delete not implemented")
	}
	return s.DeleteFunc(ctx, req)
}

// Head delegates to HeadFunc or panics if not set.
func (s *TransportStub) Head(ctx context.Context, req transport.HeadRequest) (*transport.HeadResponse, error) {
	if s.HeadFunc == nil {
		panic("TransportStub.Head not implemented")
	}
	return s.HeadFunc(ctx, req)
}
