package transport

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
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
	// MaxRetries is the maximum number of retry attempts. Zero uses the default
	// of 3; a negative value disables retries while still making one attempt.
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
	var e *Error
	if errors.As(err, &e) {
		// Retry server errors and rate limits
		return e.StatusCode >= 500 || e.StatusCode == 429
	}
	return false
}

// WithRetry wraps a transport with retry logic and exponential backoff for
// retry-safe operations. Plain appends, append-and-close requests without
// producer headers, and deletes are passed through exactly once: an ambiguous
// first attempt may already have appended data or deleted an older incarnation,
// and retrying could duplicate the append or delete a stream recreated at the
// same path. Empty close-only requests are idempotent and may be retried.
//
// Example:
//
//	transport := WithRetry(DefaultRetryOptions())(baseTransport)
func WithRetry(opts RetryOptions) Middleware {
	// A negative retry count means "try once, without retries". Most
	// importantly, never let an invalid count skip the underlying operation and
	// return a false nil success.
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	} else if opts.MaxRetries == 0 {
		// Preserve the established zero-value behavior.
		opts.MaxRetries = 3
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = 100 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 10 * time.Second
	}
	if opts.Multiplier < 1 || math.IsNaN(opts.Multiplier) || math.IsInf(opts.Multiplier, 0) {
		opts.Multiplier = 2.0
	}
	if opts.InitialBackoff > opts.MaxBackoff {
		opts.InitialBackoff = opts.MaxBackoff
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

// Append retries only when the request carries idempotent producer headers or
// is a close-only request (Sections 5.2.1 and 5.3).
//
// A plain append is not idempotent: a 502 or 503 from an intermediary can arrive
// after the origin has already committed the data, so retrying would append the
// same bytes twice. With producer headers the server deduplicates by
// (producer, epoch, seq), and an empty close-only request is independently
// idempotent, so those requests are safe to retry.
func (t *retryTransport) Append(ctx context.Context, req AppendRequest) (*AppendResponse, error) {
	if !req.HasProducerHeaders && !(req.Close && len(req.Data) == 0) {
		return t.next.Append(ctx, req)
	}

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

// Delete is deliberately not retried. Although deleting one fixed resource is
// idempotent, a stream path can be recreated after a successful deletion. If an
// intermediary turns that first success into a retryable response, a later
// attempt could delete the replacement incarnation.
func (t *retryTransport) Delete(ctx context.Context, req DeleteRequest) error {
	return t.next.Delete(ctx, req)
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

		// Wait with backoff, jittered to spread retries from concurrent clients
		// that failed at the same moment.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitter(backoff)):
		}

		// Increase backoff for next attempt
		nextBackoff := float64(backoff) * t.opts.Multiplier
		if nextBackoff >= float64(t.opts.MaxBackoff) {
			backoff = t.opts.MaxBackoff
		} else {
			backoff = time.Duration(nextBackoff)
		}
	}

	// Unreachable, but compiler needs it
	return nil
}

// jitter returns a duration uniformly distributed in [d/2, d].
// Values of 1ns or less are returned unchanged.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
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
