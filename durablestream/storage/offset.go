// Package storage provides offset formatting helpers for durable stream storage
// implementations.
//
// This package implements the textual offset representation used by the
// reference Node.js implementation at
// https://github.com/electric-sql/durable-streams.
//
// The format is <readSeq>_<position>, with both components zero-padded to at
// least 16 digits. The position remains storage-defined and opaque to clients;
// implementations must not imply that it is necessarily a byte count.
package storage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// FormatOffset formats a read sequence and storage-defined position into a durable stream offset
// using the reference implementation format: "%016d_%016d".
//
// This format is used by the reference Node.js implementation and ensures:
//   - Lexicographic sortability (zero-padding maintains numeric order)
//   - Support for very large streams (16 digits = up to 10^16 entries)
//   - Clear separation of read sequence and storage position
//
// Example: FormatOffset(1, 42) returns "0000000000000001_0000000000000042"
func FormatOffset(readSeq, position int64) durablestream.Offset {
	return durablestream.Offset(fmt.Sprintf("%016d_%016d", readSeq, position))
}

// ParseOffset parses a durable stream offset back to its read sequence and
// storage-defined position.
//
// Returns (0, 0, nil) for empty string ("") and "-1" sentinels, which indicate
// "start of stream" per the protocol.
//
// Returns an error wrapped with ErrBadRequest for:
//   - Invalid format (missing underscore separator)
//   - Non-numeric components
//   - Negative values
func ParseOffset(offset durablestream.Offset) (readSeq, position int64, err error) {
	s := string(offset)
	if s == "" || s == "-1" {
		return 0, 0, nil
	}

	parts := strings.SplitN(s, "_", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid offset format %q: expected <readSeq>_<byteOffset>: %w", offset, durablestream.ErrBadRequest)
	}

	readSeq, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid readSeq in offset %q: %w", offset, durablestream.ErrBadRequest)
	}
	if readSeq < 0 {
		return 0, 0, fmt.Errorf("negative readSeq in offset %q: %w", offset, durablestream.ErrBadRequest)
	}

	position, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid position in offset %q: %w", offset, durablestream.ErrBadRequest)
	}
	if position < 0 {
		return 0, 0, fmt.Errorf("negative position in offset %q: %w", offset, durablestream.ErrBadRequest)
	}

	return readSeq, position, nil
}

// FormatSimpleOffset is a helper that formats an offset with readSeq=0.
// This is useful for simple sequential storage implementations that don't use
// read sequences, which is the most common case.
//
// Example: FormatSimpleOffset(42) returns "0000000000000000_0000000000000042"
func FormatSimpleOffset(idx int64) durablestream.Offset {
	return FormatOffset(0, idx)
}
