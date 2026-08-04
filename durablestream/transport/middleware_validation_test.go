package transport

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithRetry_NegativeMaxRetriesStillCallsTransport(t *testing.T) {
	wantErr := errors.New("injected failure")
	var calls atomic.Int32
	mock := &mockTransport{
		readFunc: func(context.Context, ReadRequest) (*ReadResponse, error) {
			calls.Add(1)
			return nil, wantErr
		},
	}

	retried := WithRetry(RetryOptions{
		MaxRetries: -1,
		Retryable:  func(error) bool { return true },
	})(mock)

	resp, err := retried.Read(t.Context(), ReadRequest{Path: "/stream"})
	if resp != nil {
		t.Fatalf("Read response = %#v, want nil", resp)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Read error = %v, want %v", err, wantErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transport calls = %d, want 1", got)
	}
}

func TestWithRetry_NormalizesInvalidBackoffOptions(t *testing.T) {
	mock := &mockTransport{}
	retried := WithRetry(RetryOptions{
		MaxRetries:     1,
		InitialBackoff: -time.Second,
		MaxBackoff:     -time.Second,
		Multiplier:     math.NaN(),
	})(mock)
	opts := retried.(*retryTransport).opts

	if opts.InitialBackoff != 100*time.Millisecond {
		t.Errorf("InitialBackoff = %v, want 100ms", opts.InitialBackoff)
	}
	if opts.MaxBackoff != 10*time.Second {
		t.Errorf("MaxBackoff = %v, want 10s", opts.MaxBackoff)
	}
	if opts.Multiplier != 2 {
		t.Errorf("Multiplier = %v, want 2", opts.Multiplier)
	}
}

func TestWithRetry_ClampsInitialBackoffToMaximum(t *testing.T) {
	mock := &mockTransport{}
	retried := WithRetry(RetryOptions{
		MaxRetries:     1,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     time.Second,
		Multiplier:     2,
	})(mock)

	if got := retried.(*retryTransport).opts.InitialBackoff; got != time.Second {
		t.Fatalf("InitialBackoff = %v, want 1s", got)
	}
}
