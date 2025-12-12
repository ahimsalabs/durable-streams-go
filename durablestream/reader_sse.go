package durablestream

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// sseConnection manages Server-Sent Events parsing.
type sseConnection struct {
	reader   *bufio.Reader
	response *http.Response
}

// newSSEConnection creates a new SSE connection from an HTTP response.
func newSSEConnection(resp *http.Response) *sseConnection {
	return &sseConnection{
		reader:   bufio.NewReader(resp.Body),
		response: resp,
	}
}

// sseEvent represents a parsed SSE event.
type sseEvent struct {
	Type string          // "data" or "control"
	Data json.RawMessage // Event data as JSON
}

// sseControlEvent represents a control event payload.
type sseControlEvent struct {
	StreamNextOffset string `json:"streamNextOffset"`
	StreamCursor     string `json:"streamCursor,omitempty"`
}

// readEvent reads and parses the next SSE event.
// Returns the event or an error if the connection is closed or parsing fails.
//
// SSE format per PROTOCOL.md Section 5.7:
//
//	event: data
//	data: [
//	data: {"k":"v"},
//	data: {"k":"w"}
//	data: ]
//
//	event: control
//	data: {"streamNextOffset":"123456_789","streamCursor":"abc"}
func (s *sseConnection) readEvent() (*sseEvent, error) {
	var eventType string
	var dataLines []string

	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read SSE line: %w", err)
		}

		// Trim newline characters
		line = strings.TrimRight(line, "\r\n")

		// Empty line indicates end of event
		if line == "" {
			if eventType != "" {
				// Build data from lines
				data := buildSSEData(dataLines)
				return &sseEvent{
					Type: eventType,
					Data: data,
				}, nil
			}
			// Empty event, continue reading
			continue
		}

		// Parse field
		// Per SSE spec: lines starting with ":" are comments, unknown fields are ignored
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(line[5:]))
		}
		// Comment lines (":") and unknown field types are ignored per SSE spec
	}
}

// buildSSEData combines multiple data lines into a single JSON value.
// For multi-line data fields, the SSE spec says to join with newlines.
// However, for JSON data, we need to be careful about whitespace.
func buildSSEData(lines []string) json.RawMessage {
	if len(lines) == 0 {
		return json.RawMessage("{}")
	}

	if len(lines) == 1 {
		return json.RawMessage(lines[0])
	}

	// Join multiple lines with newlines (per SSE spec)
	var buf bytes.Buffer
	for i, line := range lines {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(line)
	}

	return json.RawMessage(buf.Bytes())
}

// Close closes the SSE connection.
func (s *sseConnection) Close() error {
	if s.response != nil && s.response.Body != nil {
		return s.response.Body.Close()
	}
	return nil
}
