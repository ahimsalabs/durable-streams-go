package transport

import (
	"context"
	"log/slog"
	"time"
)

// WithLogging wraps a transport with request/response logging.
//
// Example:
//
//	transport := WithLogging(slog.Default())(baseTransport)
func WithLogging(logger *slog.Logger) Middleware {
	return func(next Transport) Transport {
		return &loggingTransport{next: next, logger: logger}
	}
}

type loggingTransport struct {
	next   Transport
	logger *slog.Logger
}

func (t *loggingTransport) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	start := time.Now()
	resp, err := t.next.Read(ctx, req)
	t.log(ctx, "Read", req.Path, start, err)
	return resp, err
}

func (t *loggingTransport) LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
	start := time.Now()
	resp, err := t.next.LongPoll(ctx, req)
	t.log(ctx, "LongPoll", req.Path, start, err)
	return resp, err
}

func (t *loggingTransport) SSE(ctx context.Context, req SSERequest) (EventStream, error) {
	start := time.Now()
	stream, err := t.next.SSE(ctx, req)
	t.log(ctx, "SSE", req.Path, start, err)
	return stream, err
}

func (t *loggingTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	start := time.Now()
	resp, err := t.next.Append(ctx, req)
	t.log(ctx, "Append", req.Path, start, err)
	return resp, err
}

func (t *loggingTransport) Create(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
	start := time.Now()
	resp, err := t.next.Create(ctx, req)
	t.log(ctx, "Create", req.Path, start, err)
	return resp, err
}

func (t *loggingTransport) Delete(ctx context.Context, req DeleteRequest) error {
	start := time.Now()
	err := t.next.Delete(ctx, req)
	t.log(ctx, "Delete", req.Path, start, err)
	return err
}

func (t *loggingTransport) Head(ctx context.Context, req HeadRequest) (*HeadResponse, error) {
	start := time.Now()
	resp, err := t.next.Head(ctx, req)
	t.log(ctx, "Head", req.Path, start, err)
	return resp, err
}

func (t *loggingTransport) log(ctx context.Context, op, path string, start time.Time, err error) {
	duration := time.Since(start)
	if err != nil {
		t.logger.ErrorContext(ctx, "transport operation failed",
			"op", op,
			"path", path,
			"duration", duration,
			"error", err,
		)
	} else {
		t.logger.DebugContext(ctx, "transport operation",
			"op", op,
			"path", path,
			"duration", duration,
		)
	}
}

// RetryOptions configures retry behavior.
type RetryOptions struct {
	// MaxRetries is the maximum number of retry attempts. Default: 3.
	MaxRetries int

	// InitialBackoff is the initial backoff duration. Default: 100ms.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration. Default: 10s.
	MaxBackoff time.Duration

	// Multiplier is the backoff multiplier. Default: 2.0.
	Multiplier float64

	// Retryable determines if an error should be retried.
	// Default: retries 5xx errors and rate limits (429).
	Retryable func(error) bool
}

// DefaultRetryOptions returns sensible defaults for retry behavior.
func DefaultRetryOptions() RetryOptions {
	return RetryOptions{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		Retryable:      defaultRetryable,
	}
}

func defaultRetryable(err error) bool {
	if e, ok := err.(*Error); ok {
		// Retry server errors and rate limits
		return e.StatusCode >= 500 || e.StatusCode == 429
	}
	return false
}

// WithRetry wraps a transport with retry logic and exponential backoff.
//
// Example:
//
//	transport := WithRetry(DefaultRetryOptions())(baseTransport)
func WithRetry(opts RetryOptions) Middleware {
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.InitialBackoff == 0 {
		opts.InitialBackoff = 100 * time.Millisecond
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 10 * time.Second
	}
	if opts.Multiplier == 0 {
		opts.Multiplier = 2.0
	}
	if opts.Retryable == nil {
		opts.Retryable = defaultRetryable
	}

	return func(next Transport) Transport {
		return &retryTransport{next: next, opts: opts}
	}
}

type retryTransport struct {
	next Transport
	opts RetryOptions
}

func (t *retryTransport) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	var resp *ReadResponse
	err := t.retry(ctx, func() error {
		var err error
		resp, err = t.next.Read(ctx, req)
		return err
	})
	return resp, err
}

func (t *retryTransport) LongPoll(ctx context.Context, req LongPollRequest) (*ReadResponse, error) {
	var resp *ReadResponse
	err := t.retry(ctx, func() error {
		var err error
		resp, err = t.next.LongPoll(ctx, req)
		return err
	})
	return resp, err
}

func (t *retryTransport) SSE(ctx context.Context, req SSERequest) (EventStream, error) {
	var stream EventStream
	err := t.retry(ctx, func() error {
		var err error
		stream, err = t.next.SSE(ctx, req)
		return err
	})
	return stream, err
}

func (t *retryTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	var resp *AppendResponse
	err := t.retry(ctx, func() error {
		var err error
		resp, err = t.next.Append(ctx, req)
		return err
	})
	return resp, err
}

func (t *retryTransport) Create(ctx context.Context, req CreateRequest) (*CreateResponse, error) {
	var resp *CreateResponse
	err := t.retry(ctx, func() error {
		var err error
		resp, err = t.next.Create(ctx, req)
		return err
	})
	return resp, err
}

func (t *retryTransport) Delete(ctx context.Context, req DeleteRequest) error {
	return t.retry(ctx, func() error {
		return t.next.Delete(ctx, req)
	})
}

func (t *retryTransport) Head(ctx context.Context, req HeadRequest) (*HeadResponse, error) {
	var resp *HeadResponse
	err := t.retry(ctx, func() error {
		var err error
		resp, err = t.next.Head(ctx, req)
		return err
	})
	return resp, err
}

func (t *retryTransport) retry(ctx context.Context, op func() error) error {
	backoff := t.opts.InitialBackoff

	for attempt := 0; attempt <= t.opts.MaxRetries; attempt++ {
		err := op()
		if err == nil {
			return nil
		}

		// Don't retry if not retryable or last attempt
		if !t.opts.Retryable(err) || attempt == t.opts.MaxRetries {
			return err
		}

		// Wait with backoff
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * t.opts.Multiplier)
		if backoff > t.opts.MaxBackoff {
			backoff = t.opts.MaxBackoff
		}
	}

	// Unreachable, but compiler needs it
	return nil
}

// Chain combines multiple middleware into a single middleware.
// Middleware is applied in order: Chain(a, b, c)(t) == a(b(c(t))).
//
// Example:
//
//	transport := Chain(
//	    WithRetry(DefaultRetryOptions()),
//	    WithLogging(logger),
//	)(baseTransport)
func Chain(middlewares ...Middleware) Middleware {
	return func(next Transport) Transport {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}
