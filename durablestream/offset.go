// Package durablestream implements the Durable Streams Protocol.
package durablestream

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
