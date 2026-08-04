// Package protocol contains internal HTTP protocol constants and utilities.
package protocol

import (
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

// Cursor generation constants per protocol spec Section 10.1.
const (
	// cursorInterval is the duration of each cursor interval (20 seconds).
	cursorInterval = 20 * time.Second

	// cursorMinJitter is the minimum jitter added when client cursor >= current (1 second).
	cursorMinJitter = 1

	// cursorMaxJitter is the maximum jitter added when client cursor >= current (3600 seconds).
	cursorMaxJitter = 3600

	// maxCursorDigits bounds arbitrary-precision parsing and response growth for
	// the untrusted cursor query parameter. Server-generated cursors are far
	// shorter; 64 decimal digits leaves ample room beyond int64 rollover.
	maxCursorDigits = 64
)

// cursorEpoch is October 9, 2024 00:00:00 UTC (protocol-defined epoch for cursor calculation).
var cursorEpoch = time.Date(2024, 10, 9, 0, 0, 0, 0, time.UTC)

// GenerateCursor generates a cursor value for CDN collapsing.
// Per spec Section 10.1:
//   - Cursors are based on 20-second intervals from October 9, 2024 00:00:00 UTC
//   - When clientCursor >= current interval, jitter is added to ensure monotonic progression
//   - Returns the cursor as a decimal string
func GenerateCursor(clientCursor string) string {
	return GenerateCursorAt(time.Now(), clientCursor)
}

// GenerateCursorAt generates a cursor at a specific time (for testing).
func GenerateCursorAt(now time.Time, clientCursor string) string {
	// Calculate current interval number
	elapsed := now.Sub(cursorEpoch)
	currentInterval := int64(elapsed / cursorInterval)

	// If the client provided a cursor, check for collision. Cursors are opaque
	// decimal integers and can outlive an int64 interval counter; arbitrary
	// precision also prevents a malicious MaxInt64 cursor from wrapping the
	// response negative when jitter is added.
	if isDecimalCursor(clientCursor) {
		clientInterval, ok := new(big.Int).SetString(clientCursor, 10)
		current := big.NewInt(currentInterval)
		if ok && clientInterval.Cmp(current) >= 0 {
			// Client cursor >= current interval: add jitter to ensure monotonic progression
			// Jitter is 1-3600 seconds worth of intervals
			jitterSeconds := cursorMinJitter + rand.Intn(cursorMaxJitter-cursorMinJitter+1)
			jitterIntervals := int64(jitterSeconds) * int64(time.Second) / int64(cursorInterval)
			// Jitter shorter than one interval truncates to zero, which would
			// return the client's own cursor. The spec requires a strictly
			// greater value, so round those up to the next interval.
			if jitterIntervals == 0 {
				jitterIntervals = 1
			}
			return clientInterval.Add(clientInterval, big.NewInt(jitterIntervals)).String()
		}
	}

	return strconv.FormatInt(currentInterval, 10)
}

// ValidCursor reports whether cursor is a bounded decimal cursor that this
// server can advance monotonically. HTTP handlers reject non-empty values for
// which this returns false rather than silently returning a smaller cursor.
func ValidCursor(cursor string) bool {
	return isDecimalCursor(cursor)
}

func isDecimalCursor(cursor string) bool {
	if cursor == "" || len(cursor) > maxCursorDigits {
		return false
	}
	for i := range len(cursor) {
		if cursor[i] < '0' || cursor[i] > '9' {
			return false
		}
	}
	return true
}
