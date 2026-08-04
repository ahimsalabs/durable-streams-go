package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrEmptyArray is returned when an empty JSON array is posted.
	ErrEmptyArray = errors.New("empty JSON arrays are not allowed")
	// ErrInvalidJSON is returned when JSON validation fails.
	ErrInvalidJSON = errors.New("invalid JSON")
)

// ProcessJSONAppend validates JSON and flattens one level of arrays.
// Returns individual messages to store.
// Per Section 9.1: "servers MUST flatten exactly one level of the array"
// Per Section 9.1: "Servers MUST validate that appended data is valid JSON"
// Empty arrays are rejected per Section 9.1 (no-op append not allowed).
func ProcessJSONAppend(data []byte) ([][]byte, error) {
	return processJSON(data, false)
}

// ProcessJSONCreate validates JSON and flattens one level of arrays for PUT (create) operations.
// Unlike ProcessJSONAppend, this allows empty arrays per spec Section 9.1:
// "PUT requests with an empty array body (`[]`) are valid and create an empty stream."
func ProcessJSONCreate(data []byte) ([][]byte, error) {
	return processJSON(data, true)
}

// processJSON is the shared implementation for JSON processing.
// allowEmptyArray controls whether empty arrays return an error (POST) or empty slice (PUT).
//
// Array elements are stored as the verbatim bytes the client sent. Decoding into
// interface{} and re-encoding would silently rewrite the payload: integers beyond
// 2^53 lose precision, 1.0 becomes 1, and object keys are reordered. Stream data is
// opaque to the server, so it must round-trip byte for byte.
func processJSON(data []byte, allowEmptyArray bool) ([][]byte, error) {
	if !startsWithArray(data) {
		// Single JSON value (or invalid JSON): validate and store as-is.
		if !json.Valid(data) {
			return nil, fmt.Errorf("%w: not a valid JSON value", ErrInvalidJSON)
		}
		return [][]byte{data}, nil
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	if len(arr) == 0 {
		if allowEmptyArray {
			return [][]byte{}, nil
		}
		return nil, ErrEmptyArray
	}

	messages := make([][]byte, 0, len(arr))
	for i, elem := range arr {
		if !json.Valid(elem) {
			return nil, fmt.Errorf("%w: array element %d is not valid JSON", ErrInvalidJSON, i)
		}
		// Copy: elem aliases data, which the caller may reuse.
		messages = append(messages, bytes.Clone(elem))
	}

	return messages, nil
}

// startsWithArray reports whether the first non-whitespace byte begins a JSON array.
// JSON insignificant whitespace is space, tab, LF and CR (RFC 8259 Section 2).
func startsWithArray(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '['
}

// FormatJSONResponse wraps messages in a JSON array for GET responses.
// Per Section 9.1: "GET responses...MUST return...a JSON array of all messages"
func FormatJSONResponse(messages [][]byte) []byte {
	if len(messages) == 0 {
		return []byte("[]")
	}

	var result strings.Builder
	result.WriteString("[")
	for i, msg := range messages {
		if i > 0 {
			result.WriteString(",")
		}
		result.Write(msg)
	}
	result.WriteString("]")

	return []byte(result.String())
}

// IsJSONContentType returns true if the content type is application/json.
func IsJSONContentType(contentType string) bool {
	parts := strings.Split(contentType, ";")
	mediaType := strings.TrimSpace(parts[0])
	return strings.EqualFold(mediaType, "application/json")
}

// ContentTypesMatch compares two content types case-insensitively for the base media type.
func ContentTypesMatch(a, b string) bool {
	partsA := strings.Split(a, ";")
	partsB := strings.Split(b, ";")
	mediaTypeA := strings.TrimSpace(partsA[0])
	mediaTypeB := strings.TrimSpace(partsB[0])
	return strings.EqualFold(mediaTypeA, mediaTypeB)
}

// SSERequiresBase64 reports whether SSE data events for this content type must be
// base64-encoded.
//
// Per spec Section 5.8, SSE supports all content types: text/* and application/json
// carry UTF-8 text directly, and every other content type is base64-encoded with the
// Stream-SSE-Data-Encoding: base64 response header.
func SSERequiresBase64(contentType string) bool {
	return !IsSSECompatible(contentType)
}

// IsSSECompatible returns true if the content type is carried verbatim (as UTF-8 text)
// in SSE data events: text/* or application/json. Other content types are still valid
// for SSE but are base64-encoded — see SSERequiresBase64.
func IsSSECompatible(contentType string) bool {
	parts := strings.Split(contentType, ";")
	mediaType := strings.TrimSpace(parts[0])

	if strings.EqualFold(mediaType, "application/json") {
		return true
	}

	if strings.HasPrefix(strings.ToLower(mediaType), "text/") {
		return true
	}

	return false
}
