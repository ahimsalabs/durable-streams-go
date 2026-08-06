package durablestream

import (
	"errors"
	"fmt"
)

// Sentinel errors for common conditions.
// Use errors.Is() to check for these errors.
var (
	// ErrNotFound indicates the requested stream does not exist.
	ErrNotFound = errors.New("stream not found")

	// ErrGone indicates an HTTP 410 response. Storage implementations reserve
	// it for an offset before the earliest retained position, but the wire
	// protocol also uses 410 for a soft-deleted stream, which Client cannot
	// distinguish from retention without an additional protocol error code.
	ErrGone = errors.New("offset before earliest retained position")

	// ErrSoftDeleted is the storage-level error for a stream deleted by its owner
	// but retained internally because forks still reference its data. Handler
	// maps it to HTTP 410; Client consequently reports [ErrGone]. Direct storage
	// operations fail with ErrSoftDeleted until the last fork releases the data.
	ErrSoftDeleted = errors.New("stream is soft-deleted")

	// ErrConflict indicates a conflict occurred:
	// - Stream exists with different configuration (on create)
	// - Content type mismatch (on append)
	// - Stream-Seq does not sort after the last accepted value (in Storage)
	ErrConflict = errors.New("conflict")

	// ErrSequenceConflict indicates a Stream-Seq conflict on append.
	// This occurs when appending with a sequence value that does not sort after
	// the last accepted value. Generic HTTP 409 responses use [ErrConflict].
	ErrSequenceConflict = errors.New("sequence conflict")

	// ErrClosed indicates a client, reader, connection, or Storage has been
	// shut down. It is distinct from ErrStreamClosed, which is the durable EOF
	// state of one protocol stream.
	ErrClosed = errors.New("stream closed")

	// ErrStreamClosed indicates an append was rejected because the durable
	// stream has reached its permanent EOF. Reads and metadata operations still
	// succeed for a closed stream; they expose the closed state in their result.
	ErrStreamClosed = errors.New("durable stream is closed")

	// ErrBadRequest indicates a malformed or invalid request.
	ErrBadRequest = errors.New("bad request")

	// ErrPayloadTooLarge indicates the request payload exceeds the maximum allowed size.
	ErrPayloadTooLarge = errors.New("payload too large")

	// ErrParseError indicates the server returned a malformed protocol response,
	// such as invalid JSON, SSE framing, or required response metadata.
	ErrParseError = errors.New("parse error: malformed protocol response")
)

// SequenceConflictError reports an append rejected because its sequence did
// not sort after the stream's last accepted sequence. It matches both
// [ErrSequenceConflict] and [ErrConflict], preserving the specific and generic
// conflict classifications.
type SequenceConflictError struct {
	// LastSeq is the stream's last accepted non-empty sequence.
	LastSeq string

	// LastOffset is the final offset of the append that carried LastSeq when
	// the backend knows it. The zero Offset means unknown. For an exact-duplicate
	// retry whose incoming sequence equals LastSeq, a known LastOffset is the
	// deduplicated record's final position.
	LastOffset Offset
}

func (e *SequenceConflictError) Error() string {
	if e.LastOffset.IsZero() {
		return fmt.Sprintf("sequence does not advance past %q", e.LastSeq)
	}
	return fmt.Sprintf("sequence does not advance past %q at offset %q", e.LastSeq, e.LastOffset)
}

// Unwrap makes SequenceConflictError match both sequence-specific and generic
// conflicts with errors.Is.
func (e *SequenceConflictError) Unwrap() []error {
	return []error{ErrSequenceConflict, ErrConflict}
}

// StreamClosedError reports an append rejected because another mutation has
// permanently closed the stream. It unwraps to [ErrStreamClosed], so callers
// can use errors.Is for the general condition and errors.As when they need the
// server's final stream position.
type StreamClosedError struct {
	// Path is the stream path supplied to Client or StreamWriter.
	Path string

	// FinalOffset is the permanent tail reported by Stream-Next-Offset.
	FinalOffset Offset

	// Message is the server's diagnostic text, when provided.
	Message string
}

func (e *StreamClosedError) Error() string {
	message := e.Message
	if message == "" {
		message = ErrStreamClosed.Error()
	}
	if e.Path != "" {
		return fmt.Sprintf("[%s] %s", e.Path, message)
	}
	return message
}

// Unwrap makes StreamClosedError match [ErrStreamClosed].
func (e *StreamClosedError) Unwrap() error { return ErrStreamClosed }

// errorCode represents an internal error code for HTTP status mapping.
// This is not exported - use sentinel errors for error checking.
type errorCode string

const (
	codeBadRequest      errorCode = "bad_request"
	codeForbidden       errorCode = "forbidden"
	codeNotFound        errorCode = "not_found"
	codeConflict        errorCode = "conflict"
	codeGone            errorCode = "gone"
	codePayloadTooLarge errorCode = "payload_too_large"
	codeTooManyRequests errorCode = "too_many_requests"
	codeInternal        errorCode = "internal"
	codeNotImplemented  errorCode = "not_implemented"
	// codeServiceUnavailable marks a transient failure (e.g. storage closed or
	// shutting down); the client may retry.
	codeServiceUnavailable errorCode = "service_unavailable"
)

// httpStatus returns the HTTP status code for an error code.
func (c errorCode) httpStatus() int {
	switch c {
	case codeBadRequest:
		return 400
	case codeForbidden:
		return 403
	case codeNotFound:
		return 404
	case codeConflict:
		return 409
	case codeGone:
		return 410
	case codePayloadTooLarge:
		return 413
	case codeTooManyRequests:
		return 429
	case codeInternal:
		return 500
	case codeNotImplemented:
		return 501
	case codeServiceUnavailable:
		return 503
	default:
		return 500
	}
}

// httpStatusToErrorCode maps HTTP status codes to error codes.
func httpStatusToErrorCode(status int) errorCode {
	switch status {
	case 400:
		return codeBadRequest
	case 403:
		return codeForbidden
	case 404:
		return codeNotFound
	case 409:
		return codeConflict
	case 410:
		return codeGone
	case 413:
		return codePayloadTooLarge
	case 429:
		return codeTooManyRequests
	case 501:
		return codeNotImplemented
	case 503:
		return codeServiceUnavailable
	default:
		return codeInternal
	}
}

// protoError is an internal error type for protocol errors.
// It implements error and can be serialized to JSON for HTTP responses.
type protoError struct {
	Code    errorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *protoError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Is implements errors.Is for sentinel error matching.
func (e *protoError) Is(target error) bool {
	switch e.Code {
	case codeNotFound:
		return target == ErrNotFound
	case codeGone:
		return target == ErrGone
	case codeConflict:
		return target == ErrConflict
	default:
		return false
	}
}

// newError creates a new protocol error.
func newError(code errorCode, message string) *protoError {
	return &protoError{
		Code:    code,
		Message: message,
	}
}
