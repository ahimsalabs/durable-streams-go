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

	// ErrGone indicates the requested offset is before the earliest
	// retained position due to retention/compaction.
	ErrGone = errors.New("offset before earliest retained position")

	// ErrConflict indicates a conflict occurred:
	// - Stream exists with different configuration (on create)
	// - Sequence regression detected (on append with sequence)
	// - Content type mismatch (on append)
	ErrConflict = errors.New("conflict")

	// ErrClosed indicates the stream or connection has been closed.
	ErrClosed = errors.New("stream closed")

	// ErrBadRequest indicates a malformed or invalid request.
	ErrBadRequest = errors.New("bad request")
)

// errorCode represents an internal error code for HTTP status mapping.
// This is not exported - use sentinel errors for error checking.
type errorCode string

const (
	codeBadRequest      errorCode = "bad_request"
	codeNotFound        errorCode = "not_found"
	codeConflict        errorCode = "conflict"
	codeGone            errorCode = "gone"
	codePayloadTooLarge errorCode = "payload_too_large"
	codeTooManyRequests errorCode = "too_many_requests"
	codeInternal        errorCode = "internal"
	codeNotImplemented  errorCode = "not_implemented"
)

// httpStatus returns the HTTP status code for an error code.
func (c errorCode) httpStatus() int {
	switch c {
	case codeBadRequest:
		return 400
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
	default:
		return 500
	}
}

// httpStatusToErrorCode maps HTTP status codes to error codes.
func httpStatusToErrorCode(status int) errorCode {
	switch status {
	case 400:
		return codeBadRequest
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
