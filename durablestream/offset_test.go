package durablestream

import "testing"

func TestOffset_Compare(t *testing.T) {
	tests := []struct {
		name string
		o1   Offset
		o2   Offset
		want int
		desc string
	}{
		{
			name: "equal offsets",
			o1:   Offset("abc"),
			o2:   Offset("abc"),
			want: 0,
			desc: "identical offsets should compare equal",
		},
		{
			name: "both zero offsets",
			o1:   ZeroOffset,
			o2:   ZeroOffset,
			want: 0,
			desc: "zero offsets should be equal",
		},
		{
			name: "simple lexicographic before",
			o1:   Offset("aaa"),
			o2:   Offset("bbb"),
			want: -1,
			desc: "aaa comes before bbb lexicographically",
		},
		{
			name: "simple lexicographic after",
			o1:   Offset("zzz"),
			o2:   Offset("aaa"),
			want: 1,
			desc: "zzz comes after aaa lexicographically",
		},
		{
			name: "prefix relationship - shorter first",
			o1:   Offset("abc"),
			o2:   Offset("abcd"),
			want: -1,
			desc: "prefix is lexicographically before longer string",
		},
		{
			name: "prefix relationship - longer first",
			o1:   Offset("abcd"),
			o2:   Offset("abc"),
			want: 1,
			desc: "longer string is lexicographically after prefix",
		},
		{
			name: "numeric strings - lexicographic not numeric",
			o1:   Offset("9"),
			o2:   Offset("10"),
			want: 1,
			desc: "lexicographic: '9' (0x39) > '1' (0x31), not numeric ordering",
		},
		{
			name: "numeric strings - 2 vs 10",
			o1:   Offset("2"),
			o2:   Offset("10"),
			want: 1,
			desc: "lexicographic: '2' > '1', demonstrates non-numeric sorting",
		},
		{
			name: "numeric strings - 100 vs 99",
			o1:   Offset("100"),
			o2:   Offset("99"),
			want: -1,
			desc: "lexicographic: '1' < '9', opposite of numeric ordering",
		},
		{
			name: "zero offset vs non-empty",
			o1:   ZeroOffset,
			o2:   Offset("1"),
			want: -1,
			desc: "zero (empty) offset comes before any non-empty offset",
		},
		{
			name: "non-empty vs zero offset",
			o1:   Offset("1"),
			o2:   ZeroOffset,
			want: 1,
			desc: "any non-empty offset comes after zero offset",
		},
		{
			name: "typical timestamp-based offset",
			o1:   Offset("1234567_890"),
			o2:   Offset("1234567_891"),
			want: -1,
			desc: "timestamp-based offsets should sort correctly",
		},
		{
			name: "case sensitivity",
			o1:   Offset("ABC"),
			o2:   Offset("abc"),
			want: -1,
			desc: "uppercase comes before lowercase in ASCII/UTF-8",
		},
		{
			name: "special characters",
			o1:   Offset("abc-123"),
			o2:   Offset("abc_123"),
			want: -1,
			desc: "hyphen (0x2D) < underscore (0x5F)",
		},
		{
			name: "complex realistic offset",
			o1:   Offset("chunk_file_001_offset_12345"),
			o2:   Offset("chunk_file_002_offset_00001"),
			want: -1,
			desc: "realistic chunk-based offset comparison",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.o1.Compare(tt.o2)
			if got != tt.want {
				t.Errorf("Offset.Compare() = %v, want %v\noffset1: %q\noffset2: %q\ndesc: %s",
					got, tt.want, tt.o1, tt.o2, tt.desc)
			}

			// Verify symmetry
			inverse := tt.o2.Compare(tt.o1)
			if tt.want == 0 && inverse != 0 {
				t.Errorf("Compare should be symmetric for equal: o2.Compare(o1) = %v, want 0", inverse)
			} else if tt.want == -1 && inverse != 1 {
				t.Errorf("Compare should be symmetric for less than: o2.Compare(o1) = %v, want 1", inverse)
			} else if tt.want == 1 && inverse != -1 {
				t.Errorf("Compare should be symmetric for greater than: o2.Compare(o1) = %v, want -1", inverse)
			}
		})
	}
}

func TestOffset_IsZero(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   bool
		desc   string
	}{
		{
			name:   "zero offset constant",
			offset: ZeroOffset,
			want:   true,
			desc:   "ZeroOffset constant should be zero",
		},
		{
			name:   "empty string",
			offset: Offset(""),
			want:   true,
			desc:   "empty string is zero offset",
		},
		{
			name:   "non-empty offset",
			offset: Offset("123"),
			want:   false,
			desc:   "non-empty string is not zero offset",
		},
		{
			name:   "single character",
			offset: Offset("0"),
			want:   false,
			desc:   "even '0' is not zero offset (zero is empty string)",
		},
		{
			name:   "whitespace is not zero",
			offset: Offset(" "),
			want:   false,
			desc:   "whitespace is not zero offset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.offset.IsZero()
			if got != tt.want {
				t.Errorf("Offset.IsZero() = %v, want %v\noffset: %q\ndesc: %s",
					got, tt.want, tt.offset, tt.desc)
			}
		})
	}
}

func TestOffset_String(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   string
		desc   string
	}{
		{
			name:   "zero offset",
			offset: ZeroOffset,
			want:   "",
			desc:   "zero offset should return empty string",
		},
		{
			name:   "simple offset",
			offset: Offset("abc123"),
			want:   "abc123",
			desc:   "simple offset should return its value",
		},
		{
			name:   "complex offset",
			offset: Offset("chunk_file_001_offset_12345"),
			want:   "chunk_file_001_offset_12345",
			desc:   "complex offset should return its full value",
		},
		{
			name:   "offset with special characters",
			offset: Offset("123-456_789"),
			want:   "123-456_789",
			desc:   "offset with special characters should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.offset.String()
			if got != tt.want {
				t.Errorf("Offset.String() = %q, want %q\ndesc: %s",
					got, tt.want, tt.desc)
			}
		})
	}
}

// TestOffset_Transitivity verifies that offset comparison is transitive.
// If A < B and B < C, then A < C.
func TestOffset_Transitivity(t *testing.T) {
	offsets := []Offset{
		Offset(""),
		Offset("1"),
		Offset("10"),
		Offset("100"),
		Offset("2"),
		Offset("9"),
		Offset("abc"),
		Offset("abd"),
		Offset("b"),
	}

	for i := 0; i < len(offsets); i++ {
		for j := i + 1; j < len(offsets); j++ {
			for k := j + 1; k < len(offsets); k++ {
				a, b, c := offsets[i], offsets[j], offsets[k]

				// Get all pairwise comparisons
				ab := a.Compare(b)
				bc := b.Compare(c)
				ac := a.Compare(c)

				// Transitivity: if a < b and b < c, then a < c
				if ab < 0 && bc < 0 {
					if ac >= 0 {
						t.Errorf("Transitivity violation: %q < %q and %q < %q, but %q >= %q (compare=%d)",
							a, b, b, c, a, c, ac)
					}
				}

				// Transitivity: if a > b and b > c, then a > c
				if ab > 0 && bc > 0 {
					if ac <= 0 {
						t.Errorf("Transitivity violation: %q > %q and %q > %q, but %q <= %q (compare=%d)",
							a, b, b, c, a, c, ac)
					}
				}
			}
		}
	}
}

// TestOffset_CompareSymmetry verifies that offset comparison is symmetric.
func TestOffset_CompareSymmetry(t *testing.T) {
	offsets := []Offset{
		ZeroOffset,
		Offset("1"),
		Offset("10"),
		Offset("2"),
		Offset("abc"),
		Offset("xyz"),
	}

	for _, o1 := range offsets {
		for _, o2 := range offsets {
			cmp := o1.Compare(o2)
			inverse := o2.Compare(o1)

			// Compare should be antisymmetric
			if cmp == 0 && inverse != 0 {
				t.Errorf("Inconsistency: Compare(%q, %q) = 0, but Compare(%q, %q) = %d",
					o1, o2, o2, o1, inverse)
			}
			if cmp < 0 && inverse <= 0 {
				t.Errorf("Inconsistency: Compare(%q, %q) = %d, but Compare(%q, %q) = %d",
					o1, o2, cmp, o2, o1, inverse)
			}
			if cmp > 0 && inverse >= 0 {
				t.Errorf("Inconsistency: Compare(%q, %q) = %d, but Compare(%q, %q) = %d",
					o1, o2, cmp, o2, o1, inverse)
			}
		}
	}
}
