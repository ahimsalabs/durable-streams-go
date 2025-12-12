package protocol

import (
	"errors"
	"testing"
)

func TestProcessJSONAppend(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		want     [][]byte
		wantErr  error
		errMatch string
	}{
		{
			name:  "single object",
			input: []byte(`{"key":"value"}`),
			want:  [][]byte{[]byte(`{"key":"value"}`)},
		},
		{
			name:  "single number",
			input: []byte(`42`),
			want:  [][]byte{[]byte(`42`)},
		},
		{
			name:  "single string",
			input: []byte(`"hello"`),
			want:  [][]byte{[]byte(`"hello"`)},
		},
		{
			name:  "single boolean",
			input: []byte(`true`),
			want:  [][]byte{[]byte(`true`)},
		},
		{
			name:  "single null",
			input: []byte(`null`),
			want:  [][]byte{[]byte(`null`)},
		},
		{
			name:  "array with one element",
			input: []byte(`[{"msg":"hello"}]`),
			want:  [][]byte{[]byte(`{"msg":"hello"}`)},
		},
		{
			name:  "array with multiple elements",
			input: []byte(`[{"a":1},{"b":2},{"c":3}]`),
			want: [][]byte{
				[]byte(`{"a":1}`),
				[]byte(`{"b":2}`),
				[]byte(`{"c":3}`),
			},
		},
		{
			name:  "array with mixed types",
			input: []byte(`[1,"two",true,null,{"key":"val"}]`),
			want: [][]byte{
				[]byte(`1`),
				[]byte(`"two"`),
				[]byte(`true`),
				[]byte(`null`),
				[]byte(`{"key":"val"}`),
			},
		},
		{
			name:  "nested array (only flattens one level)",
			input: []byte(`[[1,2],[3,4]]`),
			want: [][]byte{
				[]byte(`[1,2]`),
				[]byte(`[3,4]`),
			},
		},
		{
			name:    "empty array",
			input:   []byte(`[]`),
			wantErr: ErrEmptyArray,
		},
		{
			name:     "invalid JSON",
			input:    []byte(`{invalid`),
			wantErr:  ErrInvalidJSON,
			errMatch: "invalid",
		},
		{
			name:     "incomplete JSON",
			input:    []byte(`{"key":`),
			wantErr:  ErrInvalidJSON,
			errMatch: "invalid",
		},
		{
			name:     "empty input",
			input:    []byte(``),
			wantErr:  ErrInvalidJSON,
			errMatch: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProcessJSONAppend(tt.input)

			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
				if tt.errMatch != "" && err.Error() == "" {
					t.Errorf("expected error to contain %q", tt.errMatch)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("expected %d messages, got %d", len(tt.want), len(got))
			}

			for i := range tt.want {
				if string(got[i]) != string(tt.want[i]) {
					t.Errorf("message %d: expected %s, got %s", i, tt.want[i], got[i])
				}
			}
		})
	}
}

func TestFormatJSONResponse(t *testing.T) {
	tests := []struct {
		name     string
		messages [][]byte
		want     string
	}{
		{
			name:     "empty",
			messages: nil,
			want:     "[]",
		},
		{
			name:     "empty slice",
			messages: [][]byte{},
			want:     "[]",
		},
		{
			name:     "single message",
			messages: [][]byte{[]byte(`{"a":1}`)},
			want:     `[{"a":1}]`,
		},
		{
			name:     "multiple messages",
			messages: [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)},
			want:     `[{"a":1},{"b":2}]`,
		},
		{
			name:     "three messages",
			messages: [][]byte{[]byte(`1`), []byte(`2`), []byte(`3`)},
			want:     `[1,2,3]`,
		},
		{
			name:     "nested arrays",
			messages: [][]byte{[]byte(`[1,2]`), []byte(`[3,4]`)},
			want:     `[[1,2],[3,4]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatJSONResponse(tt.messages)
			if string(got) != tt.want {
				t.Errorf("expected %s, got %s", tt.want, string(got))
			}
		})
	}
}

func TestIsJSONContentType(t *testing.T) {
	tests := []struct {
		contentType string
		want        bool
	}{
		{"application/json", true},
		{"APPLICATION/JSON", true},
		{"Application/Json", true},
		{"application/json; charset=utf-8", true},
		{"application/json;charset=utf-8", true},
		{"text/plain", false},
		{"text/html", false},
		{"application/xml", false},
		{"application/octet-stream", false},
		{"", false},
		{"json", false},
	}

	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			got := IsJSONContentType(tt.contentType)
			if got != tt.want {
				t.Errorf("IsJSONContentType(%q) = %v, want %v", tt.contentType, got, tt.want)
			}
		})
	}
}

func TestContentTypesMatch(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{
			name: "exact match",
			a:    "application/json",
			b:    "application/json",
			want: true,
		},
		{
			name: "case insensitive",
			a:    "APPLICATION/JSON",
			b:    "application/json",
			want: true,
		},
		{
			name: "with charset on first",
			a:    "application/json; charset=utf-8",
			b:    "application/json",
			want: true,
		},
		{
			name: "with charset on second",
			a:    "application/json",
			b:    "application/json; charset=utf-8",
			want: true,
		},
		{
			name: "with charset on both",
			a:    "application/json; charset=utf-8",
			b:    "application/json; charset=iso-8859-1",
			want: true,
		},
		{
			name: "different types",
			a:    "application/json",
			b:    "text/plain",
			want: false,
		},
		{
			name: "partial match - subtype",
			a:    "application/json",
			b:    "application/xml",
			want: false,
		},
		{
			name: "partial match - type",
			a:    "application/json",
			b:    "text/json",
			want: false,
		},
		{
			name: "text types match",
			a:    "text/plain",
			b:    "TEXT/PLAIN",
			want: true,
		},
		{
			name: "empty strings",
			a:    "",
			b:    "",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ContentTypesMatch(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("ContentTypesMatch(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestIsSSECompatible(t *testing.T) {
	tests := []struct {
		contentType string
		want        bool
	}{
		// JSON is SSE compatible
		{"application/json", true},
		{"APPLICATION/JSON", true},
		{"application/json; charset=utf-8", true},

		// text/* is SSE compatible
		{"text/plain", true},
		{"text/html", true},
		{"text/event-stream", true},
		{"TEXT/PLAIN", true},
		{"text/plain; charset=utf-8", true},

		// Other types are not SSE compatible
		{"application/octet-stream", false},
		{"application/xml", false},
		{"image/png", false},
		{"audio/mp3", false},
		{"video/mp4", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			got := IsSSECompatible(tt.contentType)
			if got != tt.want {
				t.Errorf("IsSSECompatible(%q) = %v, want %v", tt.contentType, got, tt.want)
			}
		})
	}
}

// TestHeaderConstants verifies the header constants are defined correctly.
// These are pure constants so we just verify they exist and have reasonable values.
func TestHeaderConstants(t *testing.T) {
	// Verify header names follow HTTP header naming conventions
	headers := []string{
		HeaderStreamTTL,
		HeaderStreamExpiresAt,
		HeaderStreamSeq,
		HeaderStreamCursor,
		HeaderStreamNextOffset,
		HeaderStreamUpToDate,
	}

	for _, h := range headers {
		if h == "" {
			t.Error("empty header constant found")
		}
		// All headers should start with "Stream-"
		if h[:7] != "Stream-" {
			t.Errorf("header %q doesn't follow Stream-* naming convention", h)
		}
	}

	// Verify query parameters
	if QueryOffset != "offset" {
		t.Errorf("QueryOffset = %q, want 'offset'", QueryOffset)
	}
	if QueryLive != "live" {
		t.Errorf("QueryLive = %q, want 'live'", QueryLive)
	}
	if QueryCursor != "cursor" {
		t.Errorf("QueryCursor = %q, want 'cursor'", QueryCursor)
	}

	// Verify live modes
	if LiveModeLongPoll != "long-poll" {
		t.Errorf("LiveModeLongPoll = %q, want 'long-poll'", LiveModeLongPoll)
	}
	if LiveModeSSE != "sse" {
		t.Errorf("LiveModeSSE = %q, want 'sse'", LiveModeSSE)
	}
}
