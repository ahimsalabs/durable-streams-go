package durablestream

import (
	"reflect"
	"testing"
)

// TestValidateOffset tests offset validation per PROTOCOL.md Sections 6 and 10.2.
func TestValidateOffset(t *testing.T) {
	tests := []struct {
		name    string
		offset  string
		wantErr bool
		desc    string
	}{
		// Valid offsets
		{
			name:    "empty offset",
			offset:  "",
			wantErr: false,
			desc:    "empty offset is valid (equivalent to stream start)",
		},
		{
			name:    "simple numeric",
			offset:  "123",
			wantErr: false,
			desc:    "simple numeric offset is valid",
		},
		{
			name:    "timestamp-style offset",
			offset:  "1234567890_123",
			wantErr: false,
			desc:    "timestamp-style offset with underscore is valid",
		},
		{
			name:    "offset with hyphen",
			offset:  "chunk-001-offset-123",
			wantErr: false,
			desc:    "hyphen is valid in offsets",
		},
		{
			name:    "offset with slash",
			offset:  "segment/123",
			wantErr: false,
			desc:    "single slash is valid (not path traversal)",
		},
		{
			name:    "sentinel -1",
			offset:  "-1",
			wantErr: false,
			desc:    "sentinel value -1 is valid",
		},
		{
			name:    "sentinel now",
			offset:  "now",
			wantErr: false,
			desc:    "sentinel value now is valid",
		},
		{
			name:    "single dot",
			offset:  ".",
			wantErr: false,
			desc:    "single dot is valid (not path traversal)",
		},
		{
			name:    "multiple separated dots",
			offset:  "a.b.c",
			wantErr: false,
			desc:    "dots separated by other chars are valid",
		},

		// Invalid: URL query parameter conflict characters (Section 6)
		{
			name:    "comma",
			offset:  "offset,value",
			wantErr: true,
			desc:    "comma MUST NOT be in offset per Section 6",
		},
		{
			name:    "ampersand",
			offset:  "offset&other",
			wantErr: true,
			desc:    "ampersand MUST NOT be in offset per Section 6",
		},
		{
			name:    "equals sign",
			offset:  "offset=value",
			wantErr: true,
			desc:    "equals sign MUST NOT be in offset per Section 6",
		},
		{
			name:    "question mark",
			offset:  "offset?query",
			wantErr: true,
			desc:    "question mark MUST NOT be in offset per Section 6",
		},

		// Invalid: whitespace and control characters
		{
			name:    "space",
			offset:  "offset value",
			wantErr: true,
			desc:    "space is not allowed in offset",
		},
		{
			name:    "tab",
			offset:  "offset\tvalue",
			wantErr: true,
			desc:    "tab is not allowed in offset",
		},
		{
			name:    "newline",
			offset:  "offset\nvalue",
			wantErr: true,
			desc:    "newline is not allowed in offset",
		},
		{
			name:    "carriage return",
			offset:  "offset\rvalue",
			wantErr: true,
			desc:    "carriage return is not allowed in offset",
		},

		// Invalid: path traversal patterns (Section 10.2)
		{
			name:    "double dot path traversal",
			offset:  "..",
			wantErr: true,
			desc:    ".. SHOULD be rejected per Section 10.2 (path traversal)",
		},
		{
			name:    "path traversal at start",
			offset:  "../etc/passwd",
			wantErr: true,
			desc:    ".. at start SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal in middle",
			offset:  "dir/../secret",
			wantErr: true,
			desc:    ".. in middle SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal at end",
			offset:  "file/..",
			wantErr: true,
			desc:    ".. at end SHOULD be rejected (path traversal)",
		},
		{
			name:    "numeric prefix with path traversal",
			offset:  "0/..",
			wantErr: true,
			desc:    "0/.. SHOULD be rejected (path traversal)",
		},
		{
			name:    "path traversal with slash",
			offset:  "0/../../../etc/passwd",
			wantErr: true,
			desc:    "complex path traversal SHOULD be rejected",
		},
		{
			name:    "multiple consecutive dots beyond two",
			offset:  "...",
			wantErr: true,
			desc:    "... contains .. and SHOULD be rejected",
		},
		{
			name:    "path traversal without slash",
			offset:  "prefix..suffix",
			wantErr: true,
			desc:    ".. embedded in string SHOULD be rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOffset(tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOffset(%q) error = %v, wantErr %v\ndesc: %s",
					tt.offset, err, tt.wantErr, tt.desc)
			}
			if err != nil && err.Error() != "invalid offset format" {
				t.Errorf("validateOffset(%q) error message = %q, want 'invalid offset format'",
					tt.offset, err.Error())
			}
		})
	}
}

func TestSplitBySSELineTerminators(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty string",
			input: "",
			want:  []string{""},
		},
		{
			name:  "no terminators",
			input: "hello world",
			want:  []string{"hello world"},
		},
		{
			name:  "single LF",
			input: "hello\nworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "single CR",
			input: "hello\rworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "single CRLF",
			input: "hello\r\nworld",
			want:  []string{"hello", "world"},
		},
		{
			name:  "multiple LF",
			input: "a\nb\nc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "multiple CR",
			input: "a\rb\rc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "multiple CRLF",
			input: "a\r\nb\r\nc",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "mixed terminators",
			input: "a\nb\rc\r\nd",
			want:  []string{"a", "b", "c", "d"},
		},
		{
			name:  "consecutive LF (empty line)",
			input: "a\n\nb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "consecutive CR (empty line)",
			input: "a\r\rb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "consecutive CRLF (empty line)",
			input: "a\r\n\r\nb",
			want:  []string{"a", "", "b"},
		},
		{
			name:  "trailing LF",
			input: "hello\n",
			want:  []string{"hello", ""},
		},
		{
			name:  "trailing CR",
			input: "hello\r",
			want:  []string{"hello", ""},
		},
		{
			name:  "trailing CRLF",
			input: "hello\r\n",
			want:  []string{"hello", ""},
		},
		{
			name:  "leading LF",
			input: "\nhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "leading CR",
			input: "\rhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "leading CRLF",
			input: "\r\nhello",
			want:  []string{"", "hello"},
		},
		{
			name:  "CRLF injection attack payload",
			input: "start\r\revent: control\rdata: {\"cr_injected\":true}\r\rend",
			want:  []string{"start", "", "event: control", "data: {\"cr_injected\":true}", "", "end"},
		},
		{
			name:  "LF-only injection payload",
			input: "start\n\nevent: data\ndata: fake-event\n\nend",
			want:  []string{"start", "", "event: data", "data: fake-event", "", "end"},
		},
		{
			name:  "CRLF mixed injection payload",
			input: "safe content\r\n\r\nevent: control\r\ndata: {\"injected\":true}\r\n\r\nmore safe content",
			want:  []string{"safe content", "", "event: control", "data: {\"injected\":true}", "", "more safe content"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitBySSELineTerminators(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitBySSELineTerminators(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
