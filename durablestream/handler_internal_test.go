package durablestream

import (
	"reflect"
	"testing"
)

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
