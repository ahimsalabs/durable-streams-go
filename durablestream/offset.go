// Package durablestream implements the Durable Streams Protocol.
package durablestream

import (
	"fmt"
	"strconv"
)

// Offset represents an opaque position within a stream.
// Per spec Section 6: Offsets are opaque tokens that are lexicographically sortable.
//
// From PROTOCOL.md Section 6:
//  1. Opaque: Clients MUST NOT interpret offset structure or meaning
//  2. Lexicographically Sortable: For any two valid offsets for the same stream,
//     a lexicographic comparison determines their relative position in the stream.
//  3. Persistent: Offsets remain valid for the lifetime of the stream
//     (until deletion or expiration)
type Offset string

// ZeroOffset represents the zero value for an offset, equivalent to the stream start.
const ZeroOffset Offset = ""

// Compare performs a lexicographic comparison of two offsets.
// Returns:
//
//	-1 if o is before other
//	 0 if o equals other
//	 1 if o is after other
//
// Note: This uses lexicographic (byte-wise) ordering, not numeric ordering.
// For example, "9" > "10" lexicographically because '9' (0x39) > '1' (0x31).
func (o Offset) Compare(other Offset) int {
	s1, s2 := string(o), string(other)
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// IsZero returns true if the offset is the zero value (empty string).
func (o Offset) IsZero() bool {
	return o == ZeroOffset
}

// String returns the string representation of the offset.
// Implements fmt.Stringer interface.
func (o Offset) String() string {
	return string(o)
}

// FormatOffset formats an index as a zero-padded 10-digit string offset.
// This is a helper for storage implementations that use sequential integer offsets.
// Uses 10 digits to support up to 9,999,999,999 offsets.
// Per Section 6: Offsets must be lexicographically sortable and strictly increasing.
func FormatOffset(idx int64) Offset {
	return Offset(fmt.Sprintf("%010d", idx))
}

// ParseOffset parses an offset string back to an index.
// Returns 0 for empty string or "-1" (stream beginning sentinel).
// Returns ErrBadRequest for invalid or negative offsets.
// This is a helper for storage implementations that use sequential integer offsets.
func ParseOffset(offset Offset) (int64, error) {
	if offset == "" || offset == "-1" {
		return 0, nil
	}
	idx, err := strconv.ParseInt(string(offset), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid offset %q: %w", offset, ErrBadRequest)
	}
	if idx < 0 {
		return 0, fmt.Errorf("negative offset %q: %w", offset, ErrBadRequest)
	}
	return idx, nil
}
