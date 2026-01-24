package storage

import (
	"errors"
	"sort"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestFormatOffset(t *testing.T) {
	tests := []struct {
		name       string
		readSeq    int64
		byteOffset int64
		want       durablestream.Offset
	}{
		{
			name:       "zero values",
			readSeq:    0,
			byteOffset: 0,
			want:       "0000000000000000_0000000000000000",
		},
		{
			name:       "simple values",
			readSeq:    1,
			byteOffset: 42,
			want:       "0000000000000001_0000000000000042",
		},
		{
			name:       "large readSeq",
			readSeq:    1234567890123456,
			byteOffset: 0,
			want:       "1234567890123456_0000000000000000",
		},
		{
			name:       "large byteOffset",
			readSeq:    0,
			byteOffset: 9999999999999999,
			want:       "0000000000000000_9999999999999999",
		},
		{
			name:       "both large",
			readSeq:    1000000000000000,
			byteOffset: 2000000000000000,
			want:       "1000000000000000_2000000000000000",
		},
		{
			name:       "typical message offset",
			readSeq:    5,
			byteOffset: 1024,
			want:       "0000000000000005_0000000000001024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatOffset(tt.readSeq, tt.byteOffset)
			if got != tt.want {
				t.Errorf("FormatOffset(%d, %d) = %q, want %q", tt.readSeq, tt.byteOffset, got, tt.want)
			}
		})
	}
}

func TestParseOffset(t *testing.T) {
	tests := []struct {
		name           string
		offset         durablestream.Offset
		wantReadSeq    int64
		wantByteOffset int64
		wantErr        bool
	}{
		{
			name:           "empty string sentinel",
			offset:         "",
			wantReadSeq:    0,
			wantByteOffset: 0,
			wantErr:        false,
		},
		{
			name:           "-1 sentinel",
			offset:         "-1",
			wantReadSeq:    0,
			wantByteOffset: 0,
			wantErr:        false,
		},
		{
			name:           "zero values",
			offset:         "0000000000000000_0000000000000000",
			wantReadSeq:    0,
			wantByteOffset: 0,
			wantErr:        false,
		},
		{
			name:           "simple values",
			offset:         "0000000000000001_0000000000000042",
			wantReadSeq:    1,
			wantByteOffset: 42,
			wantErr:        false,
		},
		{
			name:           "large values",
			offset:         "1234567890123456_9876543210987654",
			wantReadSeq:    1234567890123456,
			wantByteOffset: 9876543210987654,
			wantErr:        false,
		},
		{
			name:           "non-padded values still parse",
			offset:         "1_42",
			wantReadSeq:    1,
			wantByteOffset: 42,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReadSeq, gotByteOffset, err := ParseOffset(tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseOffset(%q) error = %v, wantErr %v", tt.offset, err, tt.wantErr)
				return
			}
			if gotReadSeq != tt.wantReadSeq {
				t.Errorf("ParseOffset(%q) readSeq = %d, want %d", tt.offset, gotReadSeq, tt.wantReadSeq)
			}
			if gotByteOffset != tt.wantByteOffset {
				t.Errorf("ParseOffset(%q) byteOffset = %d, want %d", tt.offset, gotByteOffset, tt.wantByteOffset)
			}
		})
	}
}

func TestParseOffset_Errors(t *testing.T) {
	tests := []struct {
		name   string
		offset durablestream.Offset
	}{
		{
			name:   "missing underscore",
			offset: "12345",
		},
		{
			name:   "invalid readSeq",
			offset: "abc_0000000000000000",
		},
		{
			name:   "invalid byteOffset",
			offset: "0000000000000000_xyz",
		},
		{
			name:   "negative readSeq",
			offset: "-1_0000000000000000",
		},
		{
			name:   "negative byteOffset",
			offset: "0000000000000000_-1",
		},
		{
			name:   "empty parts",
			offset: "_",
		},
		{
			name:   "too many parts",
			offset: "1_2_3",
		},
		{
			name:   "overflow readSeq",
			offset: "99999999999999999999_0",
		},
		{
			name:   "overflow byteOffset",
			offset: "0_99999999999999999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ParseOffset(tt.offset)
			if err == nil {
				t.Errorf("ParseOffset(%q) expected error, got nil", tt.offset)
				return
			}
			if !errors.Is(err, durablestream.ErrBadRequest) {
				t.Errorf("ParseOffset(%q) error = %v, want ErrBadRequest", tt.offset, err)
			}
		})
	}
}

func TestFormatSimpleOffset(t *testing.T) {
	tests := []struct {
		name string
		idx  int64
		want durablestream.Offset
	}{
		{
			name: "zero",
			idx:  0,
			want: "0000000000000000_0000000000000000",
		},
		{
			name: "simple index",
			idx:  42,
			want: "0000000000000000_0000000000000042",
		},
		{
			name: "large index",
			idx:  9999999999999999,
			want: "0000000000000000_9999999999999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatSimpleOffset(tt.idx)
			if got != tt.want {
				t.Errorf("FormatSimpleOffset(%d) = %q, want %q", tt.idx, got, tt.want)
			}
		})
	}
}

func TestFormatOffset_RoundTrip(t *testing.T) {
	testCases := []struct {
		readSeq    int64
		byteOffset int64
	}{
		{0, 0},
		{1, 1},
		{0, 1000000},
		{1000000, 0},
		{12345, 67890},
		{9999999999999999, 9999999999999999},
	}

	for _, tc := range testCases {
		offset := FormatOffset(tc.readSeq, tc.byteOffset)
		gotReadSeq, gotByteOffset, err := ParseOffset(offset)
		if err != nil {
			t.Errorf("RoundTrip(%d, %d) ParseOffset error: %v", tc.readSeq, tc.byteOffset, err)
			continue
		}
		if gotReadSeq != tc.readSeq || gotByteOffset != tc.byteOffset {
			t.Errorf("RoundTrip(%d, %d): got (%d, %d)", tc.readSeq, tc.byteOffset, gotReadSeq, gotByteOffset)
		}
	}
}

func TestOffset_LexicographicSortability(t *testing.T) {
	offsets := []durablestream.Offset{
		FormatOffset(2, 100),
		FormatOffset(0, 0),
		FormatOffset(1, 50),
		FormatOffset(0, 100),
		FormatOffset(1, 0),
		FormatOffset(0, 1),
	}

	sorted := make([]string, len(offsets))
	for i, o := range offsets {
		sorted[i] = string(o)
	}
	sort.Strings(sorted)

	expected := []string{
		string(FormatOffset(0, 0)),
		string(FormatOffset(0, 1)),
		string(FormatOffset(0, 100)),
		string(FormatOffset(1, 0)),
		string(FormatOffset(1, 50)),
		string(FormatOffset(2, 100)),
	}

	for i := range sorted {
		if sorted[i] != expected[i] {
			t.Errorf("Lexicographic sort failed at index %d: got %q, want %q", i, sorted[i], expected[i])
		}
	}
}

func TestOffset_LexicographicSortability_ByteOffsetOnly(t *testing.T) {
	offsets := []durablestream.Offset{
		FormatSimpleOffset(100),
		FormatSimpleOffset(1),
		FormatSimpleOffset(10),
		FormatSimpleOffset(2),
		FormatSimpleOffset(0),
	}

	sorted := make([]string, len(offsets))
	for i, o := range offsets {
		sorted[i] = string(o)
	}
	sort.Strings(sorted)

	expected := []int64{0, 1, 2, 10, 100}
	for i, exp := range expected {
		want := string(FormatSimpleOffset(exp))
		if sorted[i] != want {
			t.Errorf("Simple offset sort failed at index %d: got %q, want %q", i, sorted[i], want)
		}
	}
}

func TestOffset_Compare(t *testing.T) {
	tests := []struct {
		name string
		a    durablestream.Offset
		b    durablestream.Offset
		want int
	}{
		{
			name: "equal offsets",
			a:    FormatOffset(1, 100),
			b:    FormatOffset(1, 100),
			want: 0,
		},
		{
			name: "different readSeq",
			a:    FormatOffset(1, 100),
			b:    FormatOffset(2, 100),
			want: -1,
		},
		{
			name: "different byteOffset",
			a:    FormatOffset(1, 100),
			b:    FormatOffset(1, 200),
			want: -1,
		},
		{
			name: "readSeq takes precedence",
			a:    FormatOffset(1, 999),
			b:    FormatOffset(2, 1),
			want: -1,
		},
		{
			name: "reversed comparison",
			a:    FormatOffset(2, 100),
			b:    FormatOffset(1, 100),
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Compare(tt.b)
			if got != tt.want {
				t.Errorf("Compare(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
