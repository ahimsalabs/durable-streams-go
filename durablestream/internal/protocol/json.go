package protocol

import (
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
// Per spec Section 7.1: "servers MUST flatten exactly one level of the array"
func ProcessJSONAppend(data []byte) ([][]byte, error) {
	var temp interface{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	arr, isArray := temp.([]interface{})
	if !isArray {
		return [][]byte{data}, nil
	}

	if len(arr) == 0 {
		return nil, ErrEmptyArray
	}

	messages := make([][]byte, 0, len(arr))
	for i, elem := range arr {
		msgBytes, err := json.Marshal(elem)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to marshal array element %d: %v", ErrInvalidJSON, i, err)
		}
		messages = append(messages, msgBytes)
	}

	return messages, nil
}

// FormatJSONResponse wraps messages in a JSON array for GET responses.
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

// IsSSECompatible returns true if content type supports SSE mode.
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
